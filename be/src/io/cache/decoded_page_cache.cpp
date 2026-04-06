// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "io/cache/decoded_page_cache.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <charconv>
#include <filesystem>
#include <limits>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "util/time.h"

namespace doris {
namespace io {

// ── Static instance ───────────────────────────────────────────────────────────
DecodedPageCache* DecodedPageCache::_s_instance = nullptr;

// ── Lifecycle ─────────────────────────────────────────────────────────────────

void DecodedPageCache::create_global_cache(const std::string& base_path,
                                           size_t capacity_bytes,
                                           int shard_count) {
    DCHECK((shard_count & (shard_count - 1)) == 0)
            << "decoded_page_cache_shard_count must be power of 2, got: " << shard_count;
    DCHECK(_s_instance == nullptr) << "DecodedPageCache already initialized";

    auto* cache = new DecodedPageCache();
    cache->_shards.resize(shard_count);
    size_t per_shard = capacity_bytes / shard_count;

    for (int i = 0; i < shard_count; ++i) {
        auto& shard      = cache->_shards[i];
        shard.capacity   = per_shard;
        shard.dir_path   = fmt::format("{}/shard_{:03d}", base_path, i);

        std::error_code ec;
        std::filesystem::create_directories(shard.dir_path, ec);
        if (ec) {
            LOG(WARNING) << "[DecodedPageCache] Cannot create shard dir "
                         << shard.dir_path << ": " << ec.message();
            continue;
        }
        // 0700: owner access only — consistent with 0600 .dpc file permissions.
        // KMS-encrypted disk provides physical-level protection.
        // OS-level restriction prevents other host processes from listing the dir.
        std::filesystem::permissions(shard.dir_path,
                std::filesystem::perms::owner_all,
                std::filesystem::perm_options::replace, ec);
        if (ec) {
            LOG(WARNING) << "[DecodedPageCache] Cannot set permissions on "
                         << shard.dir_path << ": " << ec.message();
        }
    }

    // FIX 6: writer thread count from config (tunable per SSD tier).
    // ESSD PL1 (350 MB/s, 26800 IOPS): 4 threads (default).
    // Each thread owns a contiguous range of shards — no cross-thread contention.
    // Thread i handles shards [i * range_size .. (i+1) * range_size).
    int threads = std::max(1, std::min(config::decoded_page_cache_writer_threads, shard_count));
    cache->_writer_threads.reserve(threads);
    for (int t = 0; t < threads; ++t) {
        cache->_writer_threads.emplace_back(
                [cache, t]() { cache->_writer_thread_fn(t); });
    }

    _s_instance = cache;

    LOG(INFO) << "[DecodedPageCache] Initialized"
              << " shards=" << shard_count
              << " capacity_per_shard=" << per_shard / (1024 * 1024) << "MB"
              << " writer_threads=" << threads
              << " base=" << base_path;

    // FIX 6: Rebuild in-memory index from existing .dpc files asynchronously.
    // Queries can start immediately; pages become visible as shards are scanned.
    for (int i = 0; i < shard_count; ++i) {
        std::thread([cache, i]() {
            cache->_rebuild_shard_index(cache->_shards[i]);
        }).detach();
    }
}

DecodedPageCache* DecodedPageCache::instance() {
    return _s_instance;
}

DecodedPageCache::~DecodedPageCache() {
    _stop.store(true, std::memory_order_release);
    // Wake all writer threads (they wait on per-shard CVs)
    for (auto& shard : _shards) {
        shard.write_queue_cv.notify_all();
    }
    for (auto& t : _writer_threads) {
        if (t.joinable()) t.join();
    }
}

// ── Core operations ───────────────────────────────────────────────────────────

std::optional<DecodedPageCache::Entry> DecodedPageCache::lookup(const Key& key) {
    auto& shard = _get_shard(key);
    std::lock_guard<std::mutex> lock(shard.mutex);

    auto it = shard.index.find(key);
    if (it == shard.index.end()) {
        _miss_count.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    // Update approximate LRU timestamp — no list operation needed.
    it->second.last_access_ms = MonotonicMillis();
    _hit_count.fetch_add(1, std::memory_order_relaxed);
    return it->second;
}

void DecodedPageCache::insert_async(const Key& key, const Slice& decoded_data,
                                    uint32_t element_size, uint32_t num_elements,
                                    uint32_t col_type, uint32_t scale) {
    if (!config::decoded_page_cache_async_write) {
        auto& shard = _get_shard(key);
        (void)_write_to_ssd(shard, key, decoded_data, element_size, num_elements,
                            col_type, scale);
        return;
    }

    auto& shard = _get_shard(key);

    // Copy decoded bytes BEFORE acquiring the lock.
    // On ESSD PL1 (350 MB/s, 64KB pages), each page copy is ~0.18ms.
    // Doing this inside the lock would serialize all 480 concurrent scanner
    // threads through a single memory allocator under the shard mutex.
    // Copying outside the lock means the allocator runs concurrently.
    // We accept the rare case of copying data that we later discard (if the
    // key was inserted by another thread between copy and lock acquisition).
    WriteTask task;
    task.key          = key;
    task.element_size = element_size;
    task.num_elements = num_elements;
    task.col_type     = col_type;
    task.scale        = scale;
    task.data.assign(reinterpret_cast<const uint8_t*>(decoded_data.data),
                     reinterpret_cast<const uint8_t*>(decoded_data.data)
                         + decoded_data.size);

    // FIX 4: single shard lock for both index check AND pending check.
    // Lock held only for fast hash lookups and deque push — no allocation inside.
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        if (shard.index.count(key))          return; // already on SSD — task discarded
        if (shard.pending_writes.count(key)) return; // already queued — task discarded
        shard.pending_writes.insert(key);

        // FIX 7: per-shard write queue — no global queue mutex.
        if ((int)shard.write_queue.size() >= config::decoded_page_cache_write_queue_size) {
            // Per-shard queue full — drop and remove from pending.
            shard.pending_writes.erase(key);
            return;
        }

        shard.write_queue.push_back(std::move(task));
        shard.write_queue_cv.notify_one();
    }
}

Status DecodedPageCache::insert(const Key& key, const Slice& decoded_data,
                                uint32_t element_size, uint32_t num_elements,
                                uint32_t col_type, uint32_t scale) {
    auto& shard = _get_shard(key);
    return _write_to_ssd(shard, key, decoded_data, element_size, num_elements,
                         col_type, scale);
}

void DecodedPageCache::invalidate(const Key& key) {
    // Remove a single specific key from the in-memory index and delete its .dpc file.
    // Called when page_io.cpp detects a corrupt .dpc file (bad footer) so the
    // entry is not returned on future lookups. Without this, the corrupt file
    // stays in the index forever and every query falls through to full decode.
    // On ESSD PL1: unlink() is fast (~0.2ms), index erase is O(1).
    auto& shard = _get_shard(key);
    std::lock_guard<std::mutex> lock(shard.mutex);

    auto it = shard.index.find(key);
    if (it == shard.index.end()) return; // already gone

    ::unlink(it->second.file_path.c_str());
    // FIX 8: guard used_bytes subtraction against underflow (size_t is unsigned)
    size_t to_sub = HEADER_SIZE + it->second.data_size;
    shard.used_bytes = (shard.used_bytes >= to_sub) ? shard.used_bytes - to_sub : 0;
    shard.pending_writes.erase(key); // cancel any in-flight write for this key
    shard.index.erase(it);
    _evict_count.fetch_add(1, std::memory_order_relaxed);
}

void DecodedPageCache::erase_by_file_hash(uint64_t file_hash) {
    // FIX 3: KEY ROUTING PROPERTY — all pages for file_hash are in ONE shard.
    // Touch only that shard, not all 64.
    Key routing_key {file_hash, 0, 0};
    auto& shard = _get_shard(routing_key);

    std::lock_guard<std::mutex> lock(shard.mutex);
    std::vector<Key> to_erase;
    for (auto& [key, entry] : shard.index) {
        if (key.file_hash == file_hash) {
            to_erase.push_back(key);
            ::unlink(entry.file_path.c_str());
            // FIX 8: guard unsigned subtraction against underflow
            size_t to_sub = HEADER_SIZE + entry.data_size;
            shard.used_bytes = (shard.used_bytes >= to_sub) ? shard.used_bytes - to_sub : 0;
            _evict_count.fetch_add(1, std::memory_order_relaxed);
        }
    }
    for (auto& k : to_erase) {
        shard.pending_writes.erase(k); // cancel any pending writes for this segment
        shard.index.erase(k);
    }
}

void DecodedPageCache::prefetch_column(uint64_t file_hash, int32_t column_unique_id) {
    // FIX 2: KEY ROUTING PROPERTY — all pages for file_hash are in ONE shard.
    // Touch only that shard, not all 64.
    Key routing_key {file_hash, column_unique_id, 0};
    auto& shard = _get_shard(routing_key);

    // Collect matching file paths under lock (fast — just unordered_map iteration).
    std::vector<std::pair<std::string, size_t>> to_prefetch;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto& [key, entry] : shard.index) {
            if (key.file_hash == file_hash && key.column_unique_id == column_unique_id) {
                to_prefetch.emplace_back(entry.file_path,
                                         HEADER_SIZE + entry.data_size);
            }
        }
    }

    // Issue fadvise outside the lock — open() may block briefly.
    // posix_fadvise returns immediately; kernel does async readahead.
    for (auto& [path, size] : to_prefetch) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd >= 0) {
            ::posix_fadvise(fd, 0, (off_t)size, POSIX_FADV_WILLNEED);
            ::posix_fadvise(fd, 0, (off_t)size, POSIX_FADV_SEQUENTIAL);
            ::close(fd);
        }
    }
}

// ── Metrics ───────────────────────────────────────────────────────────────────

size_t DecodedPageCache::used_bytes() const {
    size_t total = 0;
    for (auto& shard : _shards) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        total += shard.used_bytes;
    }
    return total;
}

// ── Private: writer thread pool ───────────────────────────────────────────────

void DecodedPageCache::_writer_thread_fn(int thread_id) {
    // FIX 1: this thread owns shards [start_shard .. end_shard).
    // No other writer thread touches these shards.
    int total_shards = (int)_shards.size();
    int threads      = (int)_writer_threads.size();
    int range        = (total_shards + threads - 1) / threads;
    int start_shard  = thread_id * range;
    int end_shard    = std::min(start_shard + range, total_shards);

    while (!_stop.load(std::memory_order_acquire)) {
        bool did_work = false;

        for (int si = start_shard; si < end_shard; ++si) {
            auto& shard = _shards[si];

            WriteTask task;
            {
                std::unique_lock<std::mutex> lock(shard.mutex);
                if (shard.write_queue.empty()) continue;
                task = std::move(shard.write_queue.front());
                shard.write_queue.pop_front();
            }

            Slice data(reinterpret_cast<char*>(task.data.data()), task.data.size());
            auto st = _write_to_ssd(shard, task.key, data,
                                    task.element_size, task.num_elements,
                                    task.col_type, task.scale);
            if (!st.ok()) {
                VLOG(1) << "[DecodedPageCache] Write failed: " << st.msg();
            }

            // FIX 4: remove from per-shard pending_writes (under shard lock).
            {
                std::lock_guard<std::mutex> lock(shard.mutex);
                shard.pending_writes.erase(task.key);
            }
            did_work = true;
        }

        if (!did_work) {
            // No work found across all owned shards.
            // Sleep on the first owned shard's CV with a short timeout.
            // The timeout ensures we poll other shards in range even if only
            // some shards' CVs were notified (each insert_async notifies only
            // the shard that received work, not the thread's designated CV shard).
            if (start_shard < end_shard) {
                auto& shard = _shards[start_shard];
                std::unique_lock<std::mutex> lock(shard.mutex);
                // 5ms timeout: responsive enough for write throughput,
                // low enough CPU overhead when all queues are empty.
                shard.write_queue_cv.wait_for(lock, std::chrono::milliseconds(5), [&] {
                    if (_stop.load(std::memory_order_relaxed)) return true;
                    // Check all owned shards for any pending work
                    for (int si = start_shard; si < end_shard; ++si) {
                        if (!_shards[si].write_queue.empty()) return true;
                    }
                    return false;
                });
            }
        }
    }
}

// ── Private: SSD write ────────────────────────────────────────────────────────

Status DecodedPageCache::_write_to_ssd(Shard& shard, const Key& key, const Slice& data,
                                        uint32_t element_size, uint32_t num_elements,
                                        uint32_t col_type, uint32_t scale) {
    std::string file_path = shard.make_file_path(key);

    // Build 64-byte header (zero-padded reserved bytes).
    // Default Doris page size is 64KB (STORAGE_PAGE_SIZE_DEFAULT_VALUE=65536),
    // well within uint32_t range. DCHECK guards against unexpected large pages.
    DCHECK(data.size <= std::numeric_limits<uint32_t>::max())
            << "[DecodedPageCache] Page too large to store: " << data.size;
    uint8_t header[HEADER_SIZE] = {};
    memcpy(header, MAGIC, 8);
    uint32_t elem_sz = element_size, num_elem = num_elements;
    uint32_t ct = col_type,          sc = scale;
    uint32_t data_sz = (uint32_t)data.size;
    memcpy(header +  8, &elem_sz,  4);
    memcpy(header + 12, &num_elem, 4);
    memcpy(header + 16, &ct,       4);
    memcpy(header + 20, &sc,       4);
    memcpy(header + 24, &data_sz,  4);
    // bytes [28..63] = 0 (reserved)

    // ── SSD write — NO SHARD LOCK (FIX 6: slow operation, 50-150ms) ──────────
    // 0600: owner read/write only.
    // KMS-encrypted disk handles physical/forensic protection.
    // 0600 prevents other OS processes on the same host from reading .dpc files.
    int fd = ::open(file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        return Status::IOError("[DecodedPageCache] open failed: {}", file_path);
    }

    ::posix_fadvise(fd, 0, (off_t)(HEADER_SIZE + data.size), POSIX_FADV_SEQUENTIAL);

    bool write_ok = (::write(fd, header, HEADER_SIZE) == (ssize_t)HEADER_SIZE)
                 && (::write(fd, data.data, data.size) == (ssize_t)data.size);
    ::close(fd);

    if (!write_ok) {
        ::unlink(file_path.c_str());
        return Status::IOError("[DecodedPageCache] write failed: {}", file_path);
    }

    // Hint OS to prefetch this file into page cache immediately after write.
    // Run 2 reads often come from RAM (OS cache) rather than SSD.
    int rfd = ::open(file_path.c_str(), O_RDONLY);
    if (rfd >= 0) {
        ::posix_fadvise(rfd, 0, (off_t)(HEADER_SIZE + data.size), POSIX_FADV_WILLNEED);
        ::close(rfd);
    }

    // ── Index update — WITH SHARD LOCK (fast: microseconds) ──────────────────
    {
        std::lock_guard<std::mutex> lock(shard.mutex);

        // Another thread may have written the same key while we were on SSD.
        // Discard our duplicate to avoid wasting disk space.
        if (shard.index.count(key)) {
            ::unlink(file_path.c_str());
            return Status::OK();
        }

        Entry entry {file_path, data.size, MonotonicMillis()};
        shard.index[key] = entry;
        shard.used_bytes += HEADER_SIZE + data.size;

        if (shard.used_bytes > shard.capacity) {
            shard.evict_to_fit();
        }
    }

    return Status::OK();
}

// ── Private: startup index rebuild ───────────────────────────────────────────

// FIX 6: scan existing .dpc files after BE restart and rebuild the in-memory
// index so that warm pages are immediately usable without re-decoding.
void DecodedPageCache::_rebuild_shard_index(Shard& shard) {
    std::error_code ec;

    // Collect all valid .dpc entries first (no lock held during filesystem scan).
    struct RebuildEntry { Key key; std::string path; size_t file_size; };
    std::vector<RebuildEntry> candidates;

    for (auto& dir_entry : std::filesystem::directory_iterator(shard.dir_path, ec)) {
        if (ec) break;
        if (dir_entry.path().extension() != ".dpc") continue;

        Key key;
        if (!_parse_key_from_filename(dir_entry.path().filename().native(), key)) {
            continue;
        }

        std::error_code sz_ec;
        auto file_size = dir_entry.file_size(sz_ec);
        if (sz_ec || file_size <= HEADER_SIZE) {
            ::unlink(dir_entry.path().c_str()); // truncated/corrupt — remove
            continue;
        }
        candidates.push_back({key, dir_entry.path().native(), (size_t)file_size});
    }

    if (candidates.empty()) return;

    // Insert all candidates under a single lock acquisition.
    size_t rebuilt = 0;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto& c : candidates) {
            if (!shard.index.count(c.key)) {
                Entry entry {c.path, c.file_size - HEADER_SIZE, MonotonicMillis()};
                shard.index[c.key] = entry;
                shard.used_bytes += c.file_size;
                ++rebuilt;
            }
        }
        // Single eviction pass after all entries inserted.
        if (shard.used_bytes > shard.capacity) {
            shard.evict_to_fit();
        }
    }

    if (rebuilt > 0) {
        VLOG(1) << "[DecodedPageCache] Rebuilt " << rebuilt
                << " entries from " << shard.dir_path;
    }
}

bool DecodedPageCache::_parse_key_from_filename(const std::string& filename, Key& out_key) {
    // Expected format: {file_hash:016x}_{col_id}_{page_offset}.dpc
    // Example: a1b2c3d4e5f60708_42_65536.dpc
    if (filename.size() < 20) return false;

    const char* p   = filename.c_str();
    const char* end = p + filename.size();

    // Parse file_hash (16 hex chars)
    uint64_t file_hash = 0;
    for (int i = 0; i < 16; ++i, ++p) {
        if (p >= end) return false;
        char c = *p;
        int digit = (c >= '0' && c <= '9') ? c - '0'
                  : (c >= 'a' && c <= 'f') ? c - 'a' + 10
                  : (c >= 'A' && c <= 'F') ? c - 'A' + 10
                  : -1;
        if (digit < 0) return false;
        file_hash = (file_hash << 4) | (uint64_t)digit;
    }
    if (p >= end || *p++ != '_') return false;

    // Parse col_id (decimal)
    int32_t col_id = 0;
    auto [p2, ec2] = std::from_chars(p, end, col_id);
    if (ec2 != std::errc{}) return false;
    p = p2;
    if (p >= end || *p++ != '_') return false;

    // Parse page_offset (decimal)
    uint64_t page_offset = 0;
    auto [p3, ec3] = std::from_chars(p, end, page_offset);
    if (ec3 != std::errc{}) return false;
    p = p3;

    // Must end with ".dpc"
    if (std::string(p, end) != ".dpc") return false;

    out_key = {file_hash, col_id, page_offset};
    return true;
}

// ── Private: shard helpers ────────────────────────────────────────────────────

std::string DecodedPageCache::Shard::make_file_path(const Key& key) const {
    return fmt::format("{}/{:016x}_{}_{}.dpc",
                       dir_path, key.file_hash,
                       key.column_unique_id, key.page_offset);
}

void DecodedPageCache::Shard::evict_to_fit() {
    // Approximate LRU — scan index for entry with smallest last_access_ms.
    // O(n) but eviction is rare (triggered only when capacity exceeded).
    // Called under shard lock.
    // Note: _evict_count is a class member — access via outer class pointer not
    // available here. Caller (erase_by_file_hash) increments it explicitly.
    // evict_to_fit() is called from _write_to_ssd and _rebuild_shard_index
    // where we don't have the outer class pointer easily, so we accept that
    // evict_to_fit evictions are not counted in _evict_count. Only
    // erase_by_file_hash (compaction) increments _evict_count.
    while (used_bytes > capacity && !index.empty()) {
        auto oldest_it = index.begin();
        int64_t oldest_ms = oldest_it->second.last_access_ms;
        for (auto it = std::next(index.begin()); it != index.end(); ++it) {
            if (it->second.last_access_ms < oldest_ms) {
                oldest_ms = it->second.last_access_ms;
                oldest_it = it;
            }
        }
        ::unlink(oldest_it->second.file_path.c_str());
        // FIX 8: guard unsigned subtraction against underflow
        size_t to_sub = HEADER_SIZE + oldest_it->second.data_size;
        used_bytes = (used_bytes >= to_sub) ? used_bytes - to_sub : 0;
        index.erase(oldest_it);
    }
}

} // namespace io
} // namespace doris
