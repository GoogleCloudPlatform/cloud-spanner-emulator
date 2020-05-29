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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ROW_CURSOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ROW_CURSOR_H_

#include <cassert>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "backend/access/read.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// Simple implementation of RowCursor used for testing purposes.
class TestRowCursor : public backend::RowCursor {
 public:
  TestRowCursor(const std::vector<std::string> column_names,
                const std::vector<const zetasql::Type*> column_types,
                const std::vector<std::vector<zetasql::Value>> values)
      : idx_(-1),
        column_names_(column_names),
        column_types_(column_types),
        values_(values) {
    // Column names, column types, and rows within values must have matching
    // lengths.
    assert(column_names_.size() == column_types_.size());
    for (int i = 0; i < values.size(); ++i) {
      assert(column_names_.size() == values_[i].size());
    }
  }

  bool Next() override { return ++idx_ < values_.size(); }

  absl::Status Status() const override { return absl::OkStatus(); }

  int NumColumns() const override { return column_names_.size(); }

  const std::string ColumnName(int i) const override {
    return column_names_[i];
  }

  const zetasql::Value ColumnValue(int i) const override {
    return values_[idx_][i];
  }

  const zetasql::Type* ColumnType(int i) const override {
    return column_types_[i];
  }

 private:
  size_t idx_;
  std::vector<std::string> column_names_;
  std::vector<const zetasql::Type*> column_types_;
  std::vector<std::vector<zetasql::Value>> values_;
};

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ROW_CURSOR_H_
