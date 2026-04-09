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

#pragma once

// DecodedPageCache — Tier 1 SSD cache for decoded column pages.
//
// ── Position in the storage hierarchy ────────────────────────────────────────
//   OSS → [BlockFileCache: compressed SSD]  (Tier 2, existing)
//       → [DecodedPageCache: decoded SSD]   (Tier 1, THIS CLASS)
//       → [StoragePageCache: decoded RAM]   (Tier 0, existing)
//
// ── Problem solved ────────────────────────────────────────────────────────────
//   In cloud/decoupled mode every query re-decodes the same compressed column
//   data from SSD (BIT_SHUFFLE + LZ4). For TPC-H Q1 this decode costs ~35sec
//   per query even when BlockFileCache has 100% hit rate.
//   This cache stores the already-decoded result on a separate SSD partition so
//   subsequent queries read decoded bytes with a single pread() — no LZ4, no
//   BIT_SHUFFLE.
//
// ── ONLY ACTIVE when config::enable_decoded_page_cache = true ────────────────
//   When disabled every method is a no-op and zero overhead is added.
//
// ── Cache key: (file_hash, column_unique_id, page_offset) ───────────────────
//   file_hash        — uint64_t low half of BlockFileCache::hash(segment_filename)
//   column_unique_id — ColumnMetaPB.unique_id(), stable across schema evolution
//   page_offset      — byte offset of the page inside the segment file
//
//   KEY ROUTING PROPERTY: all pages for the same segment file hash to the SAME
//   shard (routing uses file_hash & (shard_count-1)). This means:
//     - prefetch_column() checks only 1 shard (not all 64)
//     - erase_by_file_hash() erases from only 1 shard (not all 64)
//
// ── .dpc file format on SSD ───────────────────────────────────────────────────
//   [0..63]  64-byte header (magic + metadata, 64-byte aligned)
//   [64..]   full decoded page_slice (elements + nullmap + footer + footer_size)
//            64-byte aligned start for SIMD
//            Caller parses *body and *footer from this buffer — same as normal
//            decode path in page_io.cpp line 312.
//
// ── Thread safety ─────────────────────────────────────────────────────────────
//   Each shard has an independent std::mutex covering:
//     - index (key → entry)
//     - lru_list / lru_iter
//     - pending_writes (per-shard dedup set, replaces former global mutex)
//     - write_queue (per-shard write queue, replaces former global queue)
//   No cross-shard locks are ever held simultaneously.
//   SSD writes happen outside the shard lock (slow: 50-150ms).
//   pread() from .dpc files happens with no lock at all (fully parallel).
//
// ── Write path (Run 1) ────────────────────────────────────────────────────────
//   Query decodes page → insert_async() deduplicates via shard.pending_writes →
//   pushes to shard.write_queue → shard's writer thread writes .dpc file →
//   updates shard index. Query is NOT blocked by the SSD write.
//
// ── Read path (Run 2+) ────────────────────────────────────────────────────────
//   lookup() returns Entry → caller pread()s decoded bytes at HEADER_SIZE offset
//   → parses *footer → sets *body → serves to execution. No decode needed.
//
// ── OS Page Cache bonus (Run 3+) ─────────────────────────────────────────────
//   posix_fadvise(FADV_WILLNEED) after write + in prefetch_column() pre-warms
//   Linux OS Page Cache. Effectively serves decoded pages at RAM speed.
//
// ── Startup index rebuild ─────────────────────────────────────────────────────
//   On BE restart, existing .dpc files on SSD are scanned and the in-memory
//   index is rebuilt asynchronously. Cache is immediately available for writes;
//   existing pages become visible as the scan progresses.
//
// ── Security ─────────────────────────────────────────────────────────────────
//   .dpc files: 0600 (owner read/write only)
//   shard dirs:  0700 (owner access only)
//   KMS-encrypted disk provides at-rest encryption at the physical level.

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "util/slice.h"

namespace doris {
namespace io {

class DecodedPageCache {
public:
    // ── Public constants ──────────────────────────────────────────────────────
    // Every .dpc file begins with a 64-byte header.
    // The decoded page_slice (elements + nullmap + footer + footer_size(4))
    // begins at byte offset HEADER_SIZE — 64-byte aligned for SIMD.
    static constexpr size_t HEADER_SIZE = 64;
    static constexpr char   MAGIC[8]    = {'D', 'P', 'C', 'V', '0', '0', '0', '1'};

    // Number of background writer threads (one per SHARDS_PER_WRITER shards).
    // Number of background writer threads — now a runtime config.
    // See config::decoded_page_cache_writer_threads (default 4 for ESSD PL1).
    // Kept as a compile-time default for reference only; actual value comes from config.
    static constexpr int WRITER_THREAD_COUNT = 4;

    // ── Cache key ─────────────────────────────────────────────────────────────
    struct Key {
        uint64_t file_hash;        // low 64 bits of BlockFileCache::hash(segment_basename)
        int32_t  column_unique_id; // ColumnMetaPB.unique_id() — stable across schema changes
        uint64_t page_offset;      // byte offset of this page in the segment file

        bool operator==(const Key& o) const noexcept {
            return file_hash        == o.file_hash
                && column_unique_id == o.column_unique_id
                && page_offset      == o.page_offset;
        }
    };

    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            size_t h = k.file_hash;
            h ^= (size_t)(uint32_t)k.column_unique_id * 0x9e3779b97f4a7c15ULL;
            h ^= k.page_offset                         * 0x517cc1b727220a95ULL;
            return h;
        }
    };

    // ── Cache entry ───────────────────────────────────────────────────────────
    struct Entry {
        std::string file_path;      // full path to .dpc file on SSD
        size_t      data_size;      // size of page_slice (excludes 64-byte header)
        int64_t     last_access_ms; // MonotonicMillis() — for approximate LRU eviction
    };

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    // Create and register the global singleton instance.
    //   base_path:      directory on SSD for decoded pages (must exist and be writable)
    //   capacity_bytes: total size limit across all shards
    //   shard_count:    number of independent shards — MUST be power of 2
    static void create_global_cache(const std::string& base_path,
                                    size_t capacity_bytes,
                                    int shard_count);

    // Returns the global instance, or nullptr if not initialized.
    static DecodedPageCache* instance();

    ~DecodedPageCache();

    // ── Core operations ───────────────────────────────────────────────────────

    // Check if a decoded page exists on SSD.
    // Returns Entry on hit; caller pread()s at offset HEADER_SIZE then parses
    // *footer and *body exactly as the normal decode path does.
    // Returns nullopt on miss.
    // Hot path — must be fast (single shard lock, ~1-2μs).
    std::optional<Entry> lookup(const Key& key);

    // Enqueue a decoded page for async write to SSD.
    // Non-blocking: returns immediately.
    // Deduplicates: if key is already on SSD or in the write queue, silently drops.
    // If per-shard write queue is full: silently drops (cache miss on next read).
    void insert_async(const Key& key,
                      const Slice& decoded_data,
                      uint32_t element_size,
                      uint32_t num_elements,
                      uint32_t col_type,
                      uint32_t scale);

    // Synchronous insert — for testing and decoded_page_cache_async_write=false mode.
    Status insert(const Key& key,
                  const Slice& decoded_data,
                  uint32_t element_size,
                  uint32_t num_elements,
                  uint32_t col_type,
                  uint32_t scale);

    // Remove all decoded pages for a segment identified by file_hash.
    // Called from Rowset::clear_cache() on compaction/deletion.
    // KEY ROUTING: all pages for a given file_hash are in ONE shard —
    // this operation touches only that shard, not all 64.
    void erase_by_file_hash(uint64_t file_hash);

    // Issue posix_fadvise(FADV_WILLNEED) for all decoded pages of a column
    // in a given segment. Triggers async kernel prefetch into OS Page Cache.
    // Called from get_segment_iterators() during tablet sync window
    // (~200ms before scan starts) so decoded data arrives before reading.
    // KEY ROUTING: touches only the one shard that holds this file_hash.
    void prefetch_column(uint64_t file_hash, int32_t column_unique_id);

    // Remove a single specific key from the cache index and delete its .dpc file.
    // Called from page_io.cpp when a .dpc file is found to have a corrupt footer.
    // Without this, a corrupt entry stays in the index forever and every subsequent
    // lookup returns it, causing repeated decode fallthrough with no self-healing.
    void invalidate(const Key& key);

    // ── Metrics ───────────────────────────────────────────────────────────────
    int64_t hit_count()   const { return _hit_count.load(std::memory_order_relaxed); }
    int64_t miss_count()  const { return _miss_count.load(std::memory_order_relaxed); }
    int64_t evict_count() const { return _evict_count.load(std::memory_order_relaxed); }
    size_t  used_bytes()  const;

private:
    // ── Shard ─────────────────────────────────────────────────────────────────
    // Each shard manages its own directory, write queue, pending set, and
    // eviction policy under a single independent mutex.
    // No cross-shard locking ever occurs.
    struct WriteTask {
        Key key;
        std::vector<uint8_t> data; // owns the decoded bytes (copied from caller)
        uint32_t element_size;
        uint32_t num_elements;
        uint32_t col_type;
        uint32_t scale;
    };

    struct Shard {
        mutable std::mutex mutex;

        // In-memory index: key → entry (protected by mutex)
        std::unordered_map<Key, Entry, KeyHash> index;

        // Approximate LRU: Entry.last_access_ms updated on every lookup.
        // Eviction scans index for oldest entry — O(n) but eviction is rare.
        // Avoids std::list overhead (44 bytes/entry × millions = GBs at 0.5PB scale).
        // (No lru_list or lru_iter needed.)

        // Dedup set: prevents duplicate writes when multiple concurrent instances
        // decode the same cold page simultaneously. Per-shard = no global mutex.
        std::unordered_set<Key, KeyHash> pending_writes;

        // Per-shard write queue: each shard's writer thread drains only this queue.
        // Eliminates the global write queue mutex bottleneck at high concurrency.
        std::deque<WriteTask>   write_queue;
        std::condition_variable write_queue_cv;

        size_t used_bytes = 0; // total bytes (header + data) across all entries
        size_t capacity   = 0; // per-shard capacity = total_capacity / shard_count

        // SSD directory for this shard, e.g. /nvme2/doris-decoded/shard_03/
        std::string dir_path;

        // Evict the oldest entry (by last_access_ms) until used_bytes <= capacity.
        // Called under shard lock. Approximate LRU — scans index once, O(n).
        void evict_to_fit();

        // Build .dpc file path: {dir}/{file_hash:016x}_{col_id}_{page_offset}.dpc
        std::string make_file_path(const Key& key) const;
    };

    // std::unique_ptr<Shard> because Shard contains std::mutex and
    // std::condition_variable which are neither copyable nor moveable.
    // vector<unique_ptr> supports resize without requiring Shard to be moveable.
    std::vector<std::unique_ptr<Shard>> _shards;

    // Shard routing: bitmask for power-of-2 shard count.
    // KEY ROUTING PROPERTY: all pages for the same file_hash → same shard.
    // This is why prefetch_column() and erase_by_file_hash() only need 1 shard.
    Shard& _get_shard(const Key& key) {
        return *_shards[key.file_hash & (_shards.size() - 1)];
    }

    // ── Writer thread pool ────────────────────────────────────────────────────
    // config::decoded_page_cache_writer_threads threads, each responsible for a
    // contiguous range of shards. Thread i handles shards
    // [i * range .. (i+1) * range). No cross-thread contention.
    std::vector<std::thread> _writer_threads;
    std::atomic<bool>        _stop {false};

    // Background writer: thread_id determines which shard range to drain.
    void _writer_thread_fn(int thread_id);

    // Write a single decoded page to SSD.
    // SSD write: NO shard lock held (slow: 50-150ms on NVMe).
    // Index update: shard lock held (fast: microseconds).
    Status _write_to_ssd(Shard& shard, const Key& key, const Slice& data,
                         uint32_t element_size, uint32_t num_elements,
                         uint32_t col_type, uint32_t scale);

    // Scan existing .dpc files in a shard directory and rebuild the in-memory
    // index. Called asynchronously at startup for each shard.
    void _rebuild_shard_index(Shard& shard);

    // Parse the cache key from a .dpc filename.
    // Format: {file_hash:016x}_{col_id}_{page_offset}.dpc
    // Returns false if filename does not match the expected format.
    static bool _parse_key_from_filename(const std::string& filename, Key& out_key);

    // ── Metrics ───────────────────────────────────────────────────────────────
    std::atomic<int64_t> _hit_count   {0};
    std::atomic<int64_t> _miss_count  {0};
    std::atomic<int64_t> _evict_count {0};

    static DecodedPageCache* _s_instance;
};

} // namespace io
} // namespace doris
