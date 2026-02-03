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

#include "frontend/converters/partition.h"

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

absl::StatusOr<std::string> PartitionTokenToString(
    const PartitionToken& partition_token) {
  std::string binary_string, token_string;
  ZETASQL_RET_CHECK(partition_token.SerializeToString(&binary_string))
      << "Failed to serialize proto: " << absl::StrCat(partition_token);
  absl::WebSafeBase64Escape(binary_string, &token_string);
  return token_string;
}

absl::StatusOr<PartitionToken> PartitionTokenFromString(
    absl::string_view token) {
  std::string binary_string;
  if (!absl::WebSafeBase64Unescape(token, &binary_string)) {
    return error::InvalidPartitionToken();
  }

  PartitionToken partition_token;
  if (!partition_token.ParseFromString(binary_string)) {
    return error::InvalidPartitionToken();
  }
  return partition_token;
}

absl::StatusOr<std::string> StreamingPartitionTokenToString(
    const StreamingPartitionToken& partition_token) {
  std::string binary_string, token_string;
  ZETASQL_RET_CHECK(partition_token.SerializeToString(&binary_string))
      << "Failed to serialize proto: " << absl::StrCat(partition_token);
  absl::WebSafeBase64Escape(binary_string, &token_string);
  return token_string;
}

absl::StatusOr<StreamingPartitionToken> StreamingPartitionTokenFromString(
    absl::string_view token) {
  std::string binary_string;
  if (!absl::WebSafeBase64Unescape(token, &binary_string)) {
    return error::InvalidStreamingPartitionToken();
  }

  StreamingPartitionToken partition_token;
  if (!partition_token.ParseFromString(binary_string)) {
    return error::InvalidStreamingPartitionToken();
  }
  return partition_token;
}

absl::StatusOr<std::string> StreamingPartitionTokenMetadataToString(
    const StreamingPartitionTokenMetadata& partition_token_metadata) {
  std::string binary_string, token_string;
  ZETASQL_RET_CHECK(partition_token_metadata.SerializeToString(&binary_string))
      << "Failed to serialize proto: "
      << absl::StrCat(partition_token_metadata);
  absl::WebSafeBase64Escape(binary_string, &token_string);
  return token_string;
}

absl::StatusOr<StreamingPartitionTokenMetadata>
StreamingPartitionTokenMetadataFromString(absl::string_view token) {
  std::string binary_string;
  if (!absl::WebSafeBase64Unescape(token, &binary_string)) {
    return error::InvalidStreamingPartitionTokenMetadata();
  }

  StreamingPartitionTokenMetadata partition_token_metadata;
  if (!partition_token_metadata.ParseFromString(binary_string)) {
    return error::InvalidStreamingPartitionTokenMetadata();
  }
  return partition_token_metadata;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
