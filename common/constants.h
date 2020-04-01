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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CONSTANTS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CONSTANTS_H_

#include "zetasql/public/type.h"

// Name of the function to write the commit timestamp in a DML statement. Cloud
// Spanner selects the commit timestamp when the transaction commits.
constexpr char kPendingCommitTimestampFunctionName[] =
    "PENDING_COMMIT_TIMESTAMP";

// String used to tell cloud spanner to insert the commit timestamp into a
// TIMESTAMP column with allow_commit_timestamp option set to true upon
// transaction commit.
constexpr char kCommitTimestampIdentifier[] = "spanner.commit_timestamp()";

// Max googlesql timestamp value is used as a sentinel by transaction store to
// identify if client requested commit timestamp to be read/inserted. At flush,
// this sentinel value is replaced by actual transaction commit timestamp.
//
// Note that for a non-commit timestamp column, this is a valid column value to
// be passed by a client directly and won't be replaced by commit timestamp.
//
// Whereas, a timestamp column with allow_commit_timestamp set to true can't
// have a timestamp in future and thus this sentinel value is not a valid value
// for the column to be passed by a client.
constexpr absl::Time kCommitTimestampValueSentinel =
    absl::FromUnixMicros(zetasql::types::kTimestampMax);

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_COMMON_CONSTANTS_H_
