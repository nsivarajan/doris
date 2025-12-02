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

#include <string>

namespace doris {

// OSS (AliCloud Object Storage Service) configuration constants
constexpr char OSS_AK[] = "OSS_ACCESS_KEY";
constexpr char OSS_SK[] = "OSS_SECRET_KEY";
constexpr char OSS_ENDPOINT[] = "OSS_ENDPOINT";
constexpr char OSS_REGION[] = "OSS_REGION";
constexpr char OSS_TOKEN[] = "OSS_TOKEN";
constexpr char OSS_ROLE_ARN[] = "OSS_ROLE_ARN";
constexpr char OSS_EXTERNAL_ID[] = "OSS_EXTERNAL_ID";
constexpr char OSS_MAX_CONN_SIZE[] = "OSS_MAX_CONN_SIZE";
constexpr char OSS_REQUEST_TIMEOUT_MS[] = "OSS_REQUEST_TIMEOUT_MS";
constexpr char OSS_CONN_TIMEOUT_MS[] = "OSS_CONNECTION_TIMEOUT_MS";

} // namespace doris
