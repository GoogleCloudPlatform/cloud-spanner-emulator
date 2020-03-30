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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_NODE_BASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_PARSER_DDL_NODE_BASE_H_

#include <string>

#include "zetasql/base/logging.h"
#include "absl/strings/numbers.h"
#include "absl/strings/match.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class NodeBase {
 public:
  NodeBase() : begin_line_(0), begin_column_(0), end_line_(0), end_column_(0) {}

  int begin_line() const { return begin_line_; }
  int begin_column() const { return begin_column_; }
  int absolute_begin_column() const { return absolute_begin_column_; }
  int end_line() const { return end_line_; }
  void set_end_line(int line) { end_line_ = line; }
  int end_column() const { return end_column_; }
  void set_end_column(int column) { end_column_ = column; }
  int absolute_end_column() const { return absolute_end_column_; }

  const std::string& image() const { return image_; }
  int64_t image_as_int64() const {
    const bool is_hex = absl::StartsWithIgnoreCase(image_, "0x") ||
                        absl::StartsWithIgnoreCase(image_, "-0x");
    int64_t rv;
    if (is_hex) {
      if (!absl::numbers_internal::safe_strtoi_base(image_, &rv, 16)) {
        return -1;
      }
      return rv;
    }
    if (!absl::SimpleAtoi(image_, &rv)) return -1;
    return rv;
  }
  void set_image(const std::string& i) { image_ = i; }
  template<typename T>
  void SetLocationInfo(const T* token) {
    begin_line_ = token->beginLine;
    begin_column_ = token->beginColumn;
    end_line_ = token->endLine;
    end_column_ = token->endColumn;

    absolute_begin_column_ = token->absolute_begin_offset();
    absolute_end_column_ = absolute_begin_column_ + token->length();
  }
  template<typename T>
  void UpdateLocationInfo(const T* token) {
    end_line_ = token->endLine;
    end_column_ = token->endColumn;

    absolute_end_column_ = token->absolute_begin_offset() + token->length();
  }

 private:
  int begin_line_, begin_column_, end_line_, end_column_;
  int absolute_begin_column_, absolute_end_column_;
  std::string image_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif
