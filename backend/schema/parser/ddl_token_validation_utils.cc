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

#include "backend/schema/parser/ddl_token_validation_utils.h"

#include "zetasql/public/strings.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {

bool IsValidFloatLiteralImage(const std::string& image) {
  if ((absl::StartsWith(image, "0") || absl::StartsWith(image, "-0")) &&
      !absl::StrContains(image, ".") && !absl::StrContains(image, "e") &&
      !absl::StrContains(image, "E")) {
    return false;
  }
  return true;
}

absl::Status ValidateBytesLiteralImage(const std::string& image,
                                       std::string* error) {
  std::string unused_out;
  int unused_error_position;
  return zetasql::ParseBytesLiteral(image, &unused_out, error,
                                      &unused_error_position);
}

absl::Status ValidateStringLiteralImage(const std::string& image, bool force,
                                        std::string* error) {
  std::string unused_out;
  int unused_error_position;
  if (force) {
    return zetasql::ParseStringLiteral(image, &unused_out, error,
                                         &unused_error_position);
  }
  return absl::OkStatus();
}

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
