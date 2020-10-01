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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_CHAR_STREAM_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_CHAR_STREAM_H_

#include "backend/schema/parser/CharStream.h"
#include "backend/schema/parser/ddl_token_base.h"
#include "absl/strings/string_view.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {
class DDLCharStream : public CharStream {
 public:
  explicit DDLCharStream(absl::string_view text)
      : CharStream(text.data(), text.size(), 1 /* start line */,
                   1 /* start column */,
                   text.size() + 1 /* buffer size -- to assure one buffer */) {}

  int token_begin() const { return tokenBegin; }
};


}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif
