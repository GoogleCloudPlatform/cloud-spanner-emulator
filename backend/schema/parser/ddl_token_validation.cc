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

#include "backend/schema/parser/ddl_token_validation.h"

#include "backend/schema/parser/DDLParserConstants.h"
#include "backend/schema/parser/Token.h"
#include "backend/schema/parser/ddl_token_validation_utils.h"
#include "zetasql/public/strings.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {


void ValidateBytesLiteral(Token* token) {
  std::string unused_error;
  absl::Status status = ValidateBytesLiteralImage(token->image, &unused_error);
  if (!status.ok()) {
    token->kind = ILLEGAL_BYTES_ESCAPE;
    return;
  }
}

void ValidateStringLiteral(Token* token, bool unused) {
  std::string unused_error;
  absl::Status status =
      ValidateStringLiteralImage(token->image, /*force=*/true, &unused_error);
  if (!status.ok()) {
    token->kind = ILLEGAL_STRING_ESCAPE;
    return;
  }
}


}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
