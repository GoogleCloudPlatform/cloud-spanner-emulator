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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_TABLE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_TABLE_H_

#include <memory>
#include <optional>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/base/statusor.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/query/queryable_column.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A wrapper over Table class which implements zetasql::Table.
// QueryableTable builds instances of EvalutorTableIterator by reading data of
// the table through a RowReader.
class QueryableTable : public zetasql::Table {
 public:
  QueryableTable(const backend::Table* table, RowReader* reader);

  std::string Name() const override { return wrapped_table_->Name(); }

  // FullName is used in debugging so it's OK to not include full path here.
  std::string FullName() const override { return Name(); }

  int NumColumns() const override { return wrapped_table_->columns().size(); }

  const zetasql::Column* GetColumn(int i) const override {
    return columns_[i].get();
  }

  const zetasql::Column* FindColumnByName(
      const std::string& name) const override;

  std::optional<std::vector<int>> PrimaryKey() const override {
    return primary_key_column_indexes_.empty()
               ? std::nullopt
               : std::make_optional(primary_key_column_indexes_);
  }

  const backend::Table* wrapped_table() const { return wrapped_table_; }

  // Override CreateEvaluatorTableIterator.
  zetasql_base::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(
      absl::Span<const int> column_idxs) const override;

 private:
  // The underlying Table object which backes the QueryableTable.
  const backend::Table* wrapped_table_;

  // A RowReader which data of the table can be read from to build a
  // EvalutorTableIterator when CreateEvaluatorTableIterator is called.
  RowReader* reader_;

  // The columns in the table.
  std::vector<std::unique_ptr<const QueryableColumn>> columns_;

  // A list of ordinal indexes of the primary key columns of the table.
  std::vector<int> primary_key_column_indexes_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_TABLE_H_
