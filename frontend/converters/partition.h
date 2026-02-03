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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PARTITION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PARTITION_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "frontend/proto/partition_token.pb.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Converts a partition token into a byte string.
absl::StatusOr<std::string> PartitionTokenToString(
    const PartitionToken& partition_token);

// Converts a byte string into a partition token.
absl::StatusOr<PartitionToken> PartitionTokenFromString(
    absl::string_view token);

// Converts a streaming partition token into a byte string.
absl::StatusOr<std::string> StreamingPartitionTokenToString(
    const StreamingPartitionToken& partition_token);

// Converts a byte string into a streaming partition token.
absl::StatusOr<StreamingPartitionToken> StreamingPartitionTokenFromString(
    absl::string_view token);

// Converts a streaming partition token metadata into a byte string.
absl::StatusOr<std::string> StreamingPartitionTokenMetadataToString(
    const StreamingPartitionTokenMetadata& partition_token_metadata);

// Converts a byte string into a streaming partition token metadata.
absl::StatusOr<StreamingPartitionTokenMetadata>
StreamingPartitionTokenMetadataFromString(absl::string_view token);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_CONVERTERS_PARTITION_H_
