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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ROW_READER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ROW_READER_H_

#include <memory>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "backend/access/read.h"
#include "backend/common/case.h"
#include "common/errors.h"
#include "tests/common/row_cursor.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

// A helper RowCursor which wraps another RowCursor and exposes a subset of its
// columns.
class ColumnRemappedRowCursor : public backend::RowCursor {
 public:
  ColumnRemappedRowCursor(std::unique_ptr<RowCursor> wrapped_cursor,
                          std::vector<std::string> column_names)
      : wrapped_cursor_(std::move(wrapped_cursor)) {
    for (const std::string& column_name : column_names) {
      for (int i = 0; i < wrapped_cursor_->NumColumns(); ++i) {
        if (wrapped_cursor_->ColumnName(i) == column_name) {
          column_index_map_.push_back(i);
          break;
        }
      }
    }
    ZETASQL_DCHECK_EQ(column_index_map_.size(), column_names.size());
  }

  bool Next() override { return wrapped_cursor_->Next(); }

  absl::Status Status() const override { return wrapped_cursor_->Status(); }

  int NumColumns() const override { return column_index_map_.size(); }

  const std::string ColumnName(int i) const override {
    return wrapped_cursor_->ColumnName(column_index_map_[i]);
  }

  const zetasql::Value ColumnValue(int i) const override {
    return wrapped_cursor_->ColumnValue(column_index_map_[i]);
  }

  const zetasql::Type* ColumnType(int i) const override {
    return wrapped_cursor_->ColumnType(column_index_map_[i]);
  }

 private:
  std::unique_ptr<backend::RowCursor> wrapped_cursor_;
  std::vector<int> column_index_map_;
};

// A fake implementation of DBReader for testing purposes.
//
// 'index' and 'key_set' in the input are ignored.
class TestRowReader : public backend::RowReader {
 public:
  struct Table {
    std::vector<std::string> column_names;
    std::vector<const zetasql::Type*> column_types;
    std::vector<std::vector<zetasql::Value>> column_values;
  };

  explicit TestRowReader(backend::CaseInsensitiveStringMap<Table> tables)
      : tables_(tables) {}

  absl::Status Read(const backend::ReadArg& read_arg,
                    std::unique_ptr<backend::RowCursor>* cursor) override {
    std::string table_name = read_arg.table;
    if (!tables_.contains(table_name)) {
      return google::spanner::emulator::error::TableNotFound(table_name);
    }
    *cursor = absl::make_unique<ColumnRemappedRowCursor>(
        absl::make_unique<TestRowCursor>(tables_[table_name].column_names,
                                         tables_[table_name].column_types,
                                         tables_[table_name].column_values),
        read_arg.columns);
    return absl::OkStatus();
  }

 private:
  backend::CaseInsensitiveStringMap<Table> tables_;
};

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_COMMON_ROW_READER_H_
