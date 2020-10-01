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

#include "zetasql/base/statusor.h"
#include "absl/strings/substitute.h"
#include "common/errors.h"
#include "zetasql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

zetasql_base::StatusOr<std::string> PartitionTokenToString(
    const PartitionToken& partition_token) {
  std::string binary_string, token_string;
  ZETASQL_RET_CHECK(partition_token.SerializeToString(&binary_string))
      << "Failed to serialize proto: " << partition_token.ShortDebugString();
  absl::WebSafeBase64Escape(binary_string, &token_string);
  return token_string;
}

zetasql_base::StatusOr<PartitionToken> PartitionTokenFromString(
    const std::string& token) {
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

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
