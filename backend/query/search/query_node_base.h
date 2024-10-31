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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_NODE_BASE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_NODE_BASE_H_

#include <string>

#include "zetasql/base/logging.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class QueryNodeBase {
  // Base node class for parsed search query tree. The node stores the
  // matched token information for future process.
 public:
  int begin_line() const { return begin_line_; }
  int begin_column() const { return begin_column_; }
  int absolute_begin_column() const { return absolute_begin_column_; }
  int end_line() const { return end_line_; }
  void set_end_line(int line) { end_line_ = line; }
  int end_column() const { return end_column_; }
  void set_end_column(int column) { end_column_ = column; }
  int absolute_end_column() const { return absolute_end_column_; }

  const std::string& image() const { return image_; }
  void set_image(const std::string& i) { image_ = i; }
  template <typename T>
  void SetLocationInfo(const T& token) {
    begin_line_ = token.beginLine;
    begin_column_ = token.beginColumn;
    end_line_ = token.endLine;
    end_column_ = token.endColumn;

    absolute_begin_column_ = token.absolute_begin_offset();
    absolute_end_column_ = absolute_begin_column_ + token.length();
  }
  template <typename T>
  void UpdateLocationInfo(const T& token) {
    end_line_ = token.endLine;
    end_column_ = token.endColumn;

    absolute_end_column_ = token.absolute_begin_offset() + token.length();
  }

 private:
  // Store the location of the matched token. line and column denotes the
  // matched token's position in the input doc. absolute_begin_column_ indicate
  // the matched token's position in the input stream.
  int begin_line_ = 0;
  int begin_column_ = 0;
  int end_line_ = 0;
  int end_column_ = 0;
  int absolute_begin_column_ = 0;
  int absolute_end_column_ = 0;
  std::string image_;
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_NODE_BASE_H_
