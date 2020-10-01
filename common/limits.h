//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_LIMITS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_LIMITS_H_


namespace google {
namespace spanner {
namespace emulator {
namespace limits {

// Maximum number of cloud labels that can be assigned to a resource.
constexpr int kMaxNumCloudLabels = 64;

// Maximum size of an incoming gRPC message.
constexpr int64_t kMaxGRPCIncomingMessageSize = 100 * 1024 * 1024;

// Maximum size of an outgoing gRPC message.
constexpr int64_t kMaxGRPCOutgoingMessageSize = 100 * 1024 * 1024;

// Maxmimum size of a gRPC error message.
constexpr int64_t kMaxGRPCErrorMessageLength = 1024;

// Maximum number of outstanding transactions per session.
constexpr int kMaxTransactionsPerSession = 32;

// Maximum number of tables per database.
constexpr int kMaxTablesPerDatabase = 2560;

// Maximum length of a schema identifier e.g. table/column/index name.
constexpr int kMaxSchemaIdentifierLength = 128;

// Maximum and minimum length of long-running operation IDs for schema changes.
constexpr int kDatabaseOpIdMinLength = 2;
constexpr int kDatabaseOpIdMaxLength = 128;

// Maximum number of columns per table.
constexpr int kMaxColumnsPerTable = 1024;

// Maximum number of key columns in a table.
constexpr int kMaxKeyColumns = 16;

// Maximum interleaving depth, counting from the top-level table's children.
constexpr int kMaxInterleavingDepth = 7;

// Maximum number of indexes per database.
constexpr int kMaxIndexesPerDatabase = kMaxTablesPerDatabase * 2;

// Maximum number of indexes per table.
constexpr int kMaxIndexesPerTable = 64;

// Maximum number of sessions that can be created in a batch.
constexpr int32_t kMaxBatchCreateSessionsCount = 100;

// Maximum number of databases for a given instance.
constexpr int kMaxDatabasesPerInstance = 100;

// Minimum database name length.
constexpr int kMinDatabaseNameLength = 2;

// Maximum database name length.
constexpr int kMaxDatabaseNameLength = 30;

// Minimum instance name length.
constexpr int kMinInstanceNameLength = 2;

// Maximum instance name length.
constexpr int kMaxInstanceNameLength = 64;

// Maximum length for a BYTES column value.
constexpr int kMaxBytesColumnLength = (10 << 20);  // 10 MB

// Maximum length for a STRING column value.
constexpr int kMaxStringColumnLength = (kMaxBytesColumnLength / 4);  // 2621440

// Maximum size of a response that is returned by a streaming read or query. If
// total aggregate response is larger than this limit it will be chunked into
// pieces, each no larger than this limit.
constexpr int64_t kMaxStreamingChunkSize = 1024 * 1024;  // 1 MB

// Maximum size of a key in bytes.
constexpr int kMaxKeySizeBytes = 8 * 1024;  // 8 KB

// Maximum size of the query string.
constexpr int kMaxQueryStringSize = 1024 * 1024;  // 1 K

// Maximum depth of column expressions.
constexpr int kColumnExpressionMaxDepth = 20;

}  // namespace limits
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_LIMITS_H_
