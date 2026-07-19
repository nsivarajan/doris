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

// TODO(PR-5): Integration smoke test — run against a real OSS bucket.
// Requires environment variables:
//   OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_REGION,
//   OSS_BUCKET, OSS_PREFIX
// Set these (or use doris_cloud.conf test_oss_* keys) to exercise the
// full accessor lifecycle end-to-end.  Without them all integration tests
// are skipped and only the unit tests run.

#include "recycler/oss_accessor.h"

#include <butil/guid.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <unordered_set>

#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "cpp/sync_point.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!cloud::init_glog("oss_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

// ---------------------------------------------------------------------------
// Unit tests — pure logic, no network, no real OSS credentials needed
// ---------------------------------------------------------------------------

TEST(OSSConfTest, from_obj_store_info_non_oss_provider_returns_nullopt) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_S3);
    obj_info.set_endpoint("s3.us-east-1.amazonaws.com");
    obj_info.set_bucket("my-bucket");
    obj_info.set_prefix("my-prefix");
    obj_info.set_region("us-east-1");

    auto conf = OSSConf::from_obj_store_info(obj_info);
    EXPECT_FALSE(conf.has_value());
}

TEST(OSSConfTest, from_obj_store_info_simple_credentials) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("my-bucket");
    obj_info.set_prefix("my-prefix");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_ak("my-ak");
    obj_info.set_sk("my-sk");

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::SIMPLE);
    EXPECT_EQ(conf->access_key_id, "my-ak");
    EXPECT_EQ(conf->access_key_secret, "my-sk");
    EXPECT_EQ(conf->bucket, "my-bucket");
    EXPECT_EQ(conf->prefix, "my-prefix");
    EXPECT_EQ(conf->region, "cn-hangzhou");
    EXPECT_EQ(conf->endpoint, "https://oss-cn-hangzhou.aliyuncs.com");
}

TEST(OSSConfTest, from_obj_store_info_endpoint_scheme_normalized) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-beijing.aliyuncs.com"); // no scheme
    obj_info.set_bucket("b");
    obj_info.set_region("cn-beijing");

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->endpoint, "https://oss-cn-beijing.aliyuncs.com");
}

TEST(OSSConfTest, from_obj_store_info_https_scheme_unchanged) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("https://oss-cn-beijing.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-beijing");

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->endpoint, "https://oss-cn-beijing.aliyuncs.com");
}

TEST(OSSConfTest, from_obj_store_info_explicit_instance_profile) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::INSTANCE_PROFILE);
}

TEST(OSSConfTest, from_obj_store_info_env_provider) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_cred_provider_type(CredProviderTypePB::ENV);

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::ENV);
}

TEST(OSSConfTest, from_obj_store_info_anonymous_provider) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_cred_provider_type(CredProviderTypePB::ANONYMOUS);

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::ANONYMOUS);
}

TEST(OSSConfTest, from_obj_store_info_unknown_type_falls_to_default) {
    // WEB_IDENTITY (AWS-only) should map to DEFAULT, not SIMPLE with empty creds
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_cred_provider_type(CredProviderTypePB::WEB_IDENTITY);

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::DEFAULT);
}

TEST(OSSConfTest, from_obj_store_info_no_aksk_no_type_defaults_to_instance_profile) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    // No ak/sk, no explicit cred_provider_type

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::INSTANCE_PROFILE);
}

TEST(OSSConfTest, from_obj_store_info_role_arn_extracted) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);
    obj_info.set_role_arn("acs:ram::123:role/MyRole");
    obj_info.set_external_id("ext-id");

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->role_arn, "acs:ram::123:role/MyRole");
    EXPECT_EQ(conf->external_id, "ext-id");
}

TEST(OSSConfTest, from_obj_store_info_skip_aksk) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    obj_info.set_bucket("b");
    obj_info.set_region("cn-hangzhou");
    obj_info.set_ak("my-ak");
    obj_info.set_sk("my-sk");

    auto conf = OSSConf::from_obj_store_info(obj_info, /*skip_aksk=*/true);
    ASSERT_TRUE(conf.has_value());
    EXPECT_TRUE(conf->access_key_id.empty());
    EXPECT_TRUE(conf->access_key_secret.empty());
}

TEST(OssLastModifiedTest, iso8601_parses_correctly) {
    // Standard OSS ListObjects LastModified format
    int64_t t = parse_oss_last_modified("2024-06-15T10:30:00.000Z");
    EXPECT_GT(t, 0);
    EXPECT_LT(t, INT64_MAX);
}

TEST(OssLastModifiedTest, empty_string_returns_int64_max) {
    EXPECT_EQ(parse_oss_last_modified(""), INT64_MAX);
}

TEST(OssLastModifiedTest, invalid_string_returns_int64_max) {
    EXPECT_EQ(parse_oss_last_modified("not-a-date"), INT64_MAX);
}

TEST(OssLastModifiedTest, ordering_preserved) {
    int64_t t1 = parse_oss_last_modified("2024-01-01T00:00:00.000Z");
    int64_t t2 = parse_oss_last_modified("2024-06-01T00:00:00.000Z");
    EXPECT_LT(t1, t2);
}

// ---------------------------------------------------------------------------
// Integration tests — skipped when OSS env vars are absent
// ---------------------------------------------------------------------------

class OSSAccessorTest : public testing::Test {
public:
    static void SetUpTestSuite() {
        auto get = [](const char* k) {
            const char* v = std::getenv(k);
            return v ? std::string(v) : std::string();
        };
        ak = get("OSS_ACCESS_KEY_ID");
        sk = get("OSS_ACCESS_KEY_SECRET");
        endpoint = get("OSS_ENDPOINT");
        region = get("OSS_REGION");
        bucket = get("OSS_BUCKET");
        prefix = get("OSS_PREFIX");

        // Fall back to doris_cloud.conf test_oss_* keys
        if (ak.empty() && !config::test_oss_ak.empty()) {
            ak = config::test_oss_ak;
            sk = config::test_oss_sk;
            endpoint = config::test_oss_endpoint;
            region = config::test_oss_region;
            bucket = config::test_oss_bucket;
            prefix = config::test_oss_prefix;
        }
    }

    void SetUp() override {
        if (ak.empty() || endpoint.empty() || bucket.empty()) {
            GTEST_SKIP() << "OSS integration test skipped: set OSS_ACCESS_KEY_ID, "
                            "OSS_ACCESS_KEY_SECRET, OSS_ENDPOINT, OSS_REGION, OSS_BUCKET, "
                            "OSS_PREFIX to run";
        }
    }

    static std::string ak, sk, endpoint, region, bucket, prefix;
};

std::string OSSAccessorTest::ak;
std::string OSSAccessorTest::sk;
std::string OSSAccessorTest::endpoint;
std::string OSSAccessorTest::region;
std::string OSSAccessorTest::bucket;
std::string OSSAccessorTest::prefix;

// Shared integration test body — same assertions as test_s3_accessor in s3_accessor_test.cpp
static void test_oss_accessor(OSSAccessor& accessor) {
    std::string file1 = "data/10000/1_0.dat";

    int ret = accessor.delete_directory("");
    ASSERT_NE(ret, 0);
    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);

    ret = accessor.put_file(file1, "");
    ASSERT_EQ(ret, 0);
    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 0);

    std::unique_ptr<ListIterator> iter;
    ret = accessor.list_directory("data", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(iter && iter->is_valid() && iter->has_next());
    ASSERT_EQ(iter->next()->path, file1);
    ASSERT_FALSE(iter->has_next());

    ret = accessor.list_directory("data/", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(iter->has_next());
    ASSERT_EQ(iter->next()->path, file1);
    ASSERT_FALSE(iter->has_next());

    ret = accessor.list_directory("data/100", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());

    ret = accessor.delete_file(file1);
    ASSERT_EQ(ret, 0);
    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 1); // NOT_FOUND
    ret = accessor.delete_file(file1);
    EXPECT_EQ(ret, 0); // idempotent

    // Batch put
    std::vector<std::string> files;
    for (int dir = 10000; dir < 10005; ++dir) {
        for (int suffix = 0; suffix < 5; ++suffix) {
            files.push_back(fmt::format("data/{}/1/{}.dat", dir, suffix));
        }
    }
    for (auto&& file : files) {
        ASSERT_EQ(accessor.put_file(file, ""), 0);
    }

    // list_all — verify mtime freshness (< 60 s)
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    std::unordered_set<std::string> listed;
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    for (auto f = iter->next(); f.has_value(); f = iter->next()) {
        EXPECT_LT(now - f->mtime_s, 60) << "mtime too old: " << f->path;
        listed.insert(std::move(f->path));
    }
    ASSERT_EQ(listed.size(), files.size());

    // delete_files (batch)
    std::vector<std::string> to_delete;
    for (int i = 0; i < 5; ++i) {
        to_delete.push_back(std::move(files.back()));
        files.pop_back();
    }
    ASSERT_EQ(accessor.delete_files(to_delete), 0);

    // delete_directory
    std::string del_dir = "data/10001";
    ASSERT_EQ(accessor.delete_directory(del_dir), 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& f) { return f.starts_with(del_dir); }),
                files.end());

    // delete_prefix
    std::string del_prefix = "data/10003/";
    ASSERT_EQ(accessor.delete_prefix(del_prefix), 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& f) { return f.starts_with(del_prefix); }),
                files.end());

    // verify remaining
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    listed.clear();
    for (auto f = iter->next(); f.has_value(); f = iter->next()) {
        listed.insert(std::move(f->path));
    }
    ASSERT_EQ(listed.size(), files.size());

    // delete_all
    ASSERT_EQ(accessor.delete_all(), 0);
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
}

TEST_F(OSSAccessorTest, simple_credentials) {
    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(
            OSSConf {
                    .endpoint = endpoint,
                    .bucket = bucket,
                    .prefix = prefix + "/OSSAccessorTest/" + butil::GenerateGUID(),
                    .region = region,
                    .access_key_id = ak,
                    .access_key_secret = sk,
                    .provider_type = OSSCredProviderType::SIMPLE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    // Force small page size to exercise pagination logic
    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "ObjStorageClient::delete_objects_recursively_",
            [](auto&& args) {
                auto* batch = try_any_cast<size_t*>(args);
                *batch = 7;
            },
            &guards.emplace_back());

    test_oss_accessor(*accessor);
}

TEST_F(OSSAccessorTest, from_obj_store_info_round_trip) {
    // Verify OSSConf::from_obj_store_info → OSSAccessor::create works end-to-end
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_endpoint(endpoint);
    obj_info.set_bucket(bucket);
    obj_info.set_prefix(prefix + "/OSSAccessorRoundTrip/" + butil::GenerateGUID());
    obj_info.set_region(region);
    obj_info.set_ak(ak);
    obj_info.set_sk(sk);

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_EQ(conf->provider_type, OSSCredProviderType::SIMPLE);

    std::shared_ptr<OSSAccessor> accessor;
    ASSERT_EQ(OSSAccessor::create(*conf, &accessor), 0);

    // Basic smoke: put + exists + delete
    ASSERT_EQ(accessor->put_file("smoke/test.dat", "hello"), 0);
    ASSERT_EQ(accessor->exists("smoke/test.dat"), 0);
    ASSERT_EQ(accessor->delete_file("smoke/test.dat"), 0);
    ASSERT_EQ(accessor->exists("smoke/test.dat"), 1);
    ASSERT_EQ(accessor->delete_all(), 0);
}

} // namespace doris::cloud
