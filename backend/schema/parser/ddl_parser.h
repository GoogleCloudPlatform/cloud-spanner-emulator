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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_PARSER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_PARSER_H_

#include <string>

#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "backend/schema/ddl/operations.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace ddl {

// The option to enable the use of cloud spanner commit timestamps for a column.
extern const char kCommitTimestampOptionName[];
// The PostgreSQL option to enable the use of cloud spanner commit timestamps
// for a column.
extern const char kPGCommitTimestampOptionName[];
// The option to set the value capture type for a change stream.
extern const char kChangeStreamValueCaptureTypeOptionName[];
// The option to set the retention period for a change stream.
extern const char kChangeStreamRetentionPeriodOptionName[];
// The option to set exclude based on modification types or ttl deletes for a
// change stream.
extern const char kChangeStreamExcludeInsertOptionName[];
extern const char kChangeStreamExcludeUpdateOptionName[];
extern const char kChangeStreamExcludeDeleteOptionName[];
extern const char kChangeStreamExcludeTtlDeletesOptionName[];
extern const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kChangeStreamBooleanOptions;

extern const char kModelColumnRequiredOptionName[];
extern const char kModelDefaultBatchSizeOptionName[];
extern const char kModelEndpointOptionName[];
extern const char kModelEndpointsOptionName[];

extern const char kWitnessLocationOptionName[];
extern const char kDefaultLeaderOptionName[];
extern const char kReadLeaseRegionsOptionName[];
extern const char kVersionRetentionPeriodOptionName[];
extern const char kDefaultSequenceKindOptionName[];
extern const char kDefaultTimeZoneOptionName[];

extern const char kLocalityGroupOptionName[];
extern const char kLocalityGroupStorageOptionName[];
extern const char kLocalityGroupStorageOptionSSDVal[];
extern const char kLocalityGroupStorageOptionHDDVal[];
extern const char kLocalityGroupSpillTimeSpanOptionName[];
extern const char kInternalLocalityGroupStorageOptionName[];
extern const char kInternalLocalityGroupSpillTimeSpanOptionName[];
extern const char kDefaultLocalityGroupName[];

extern const char kPlacementDefaultLeaderOptionName[];
extern const char kPlacementInstancePartitionOptionName[];

extern const char kCassandraTypeOptionName[];

absl::Status ParseDDLStatement(absl::string_view ddl, DDLStatement* statement);

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_PARSER_H_
