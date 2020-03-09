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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_TOKEN_BASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_TOKEN_BASE_H_

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace ddl {
class TokenBase {
 public:
  int absolute_begin_offset() const { return absolute_begin_offset_; }
  int length() const { return length_; }

  void set_absolute_position(int begin, int length) {
    absolute_begin_offset_ = begin;
    length_ = length;
  }

 private:
  int absolute_begin_offset_ = 0;
  int length_ = 0;
};

}  // namespace ddl
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif
