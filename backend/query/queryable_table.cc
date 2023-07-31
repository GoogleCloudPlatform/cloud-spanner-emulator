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

#include "backend/query/queryable_table.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/value.h"
#include "absl/memory/memory.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/query/queryable_column.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// An implementation of EvaluatorTableIterator which wraps a RowCursor.
//
// Used by QueryableTable::CreateEvaluatorTableIterator.
class RowCursorEvaluatorTableIterator
    : public zetasql::EvaluatorTableIterator {
 public:
  explicit RowCursorEvaluatorTableIterator(std::unique_ptr<RowCursor> cursor)
      : cursor_(std::move(cursor)) {
    values_.reserve(cursor_->NumColumns());
    for (int i = 0; i < cursor_->NumColumns(); ++i) {
      values_.push_back(zetasql::values::Null(cursor_->ColumnType(i)));
    }
  }

  int NumColumns() const override { return cursor_->NumColumns(); }

  std::string GetColumnName(int i) const override {
    return cursor_->ColumnName(i);
  }

  const zetasql::Type* GetColumnType(int i) const override {
    return cursor_->ColumnType(i);
  }

  bool NextRow() override {
    if (cursor_->Next()) {
      for (int i = 0; i < cursor_->NumColumns(); ++i) {
        values_[i] = cursor_->ColumnValue(i);
      }
      return true;
    } else {
      return false;
    }
  }

  const zetasql::Value& GetValue(int i) const override { return values_[i]; }

  absl::Status Status() const override { return cursor_->Status(); }

  // Cancel is best-effort and not required.
  absl::Status Cancel() override { return absl::OkStatus(); }

 private:
  // The wrapped RowCursor.
  std::unique_ptr<RowCursor> cursor_;

  // Values of the current row. EvaluatorTableIterator::GetValue need to return
  // a reference so we need to buffer the values instead of simply delegate to
  // RowCursor::ColumnValue.
  std::vector<zetasql::Value> values_;
};

QueryableTable::QueryableTable(
    const backend::Table* table, RowReader* reader,
    std::optional<const zetasql::AnalyzerOptions> options,
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory)
    : wrapped_table_(table), reader_(reader) {
  for (const auto* column : table->columns()) {
    std::unique_ptr<const zetasql::AnalyzerOutput> output = nullptr;
    if (options.has_value() && column->has_default_value()) {
      // (b/270162778) Implicit cast default expressions to the destination
      // column types so that explicit cast of non-primitive types is not
      // required.
      constexpr char kExpression[] = "CAST (($0) AS $1)";
      std::string sql = absl::Substitute(
          kExpression, column->expression().value(),
          column->GetType()->TypeName(zetasql::PRODUCT_EXTERNAL));
      absl::Status s = zetasql::AnalyzeExpression(
          sql, options.value(), catalog, type_factory, &output);
      ZETASQL_DCHECK(s.ok()) << "Failed to analyze default expression for column "
                     << column->FullName() << "\n";
    }
    if (column->has_default_value()) {
      zetasql::Column::ExpressionAttributes::ExpressionKind expression_kind =
          zetasql::Column::ExpressionAttributes::ExpressionKind::DEFAULT;
      zetasql::Column::ExpressionAttributes expression_attributes =
          zetasql::Column::ExpressionAttributes(expression_kind,
                                                  column->expression().value(),
                                                  output->resolved_expr());
      columns_.push_back(std::make_unique<const QueryableColumn>(
          column, std::move(output),
          std::make_optional(expression_attributes)));
    } else {
      columns_.push_back(std::make_unique<const QueryableColumn>(
          column, std::move(output), std::nullopt));
    }
  }

  // Populate primary_key_column_indexes_.
  for (const auto& key_column : table->primary_key()) {
    for (int i = 0; i < wrapped_table_->columns().size(); ++i) {
      if (key_column->column() == wrapped_table_->columns()[i]) {
        primary_key_column_indexes_.push_back(i);
        break;
      }
    }
  }
}

absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
QueryableTable::CreateEvaluatorTableIterator(
    absl::Span<const int> column_idxs) const {
  ZETASQL_RET_CHECK_NE(reader_, nullptr);

  std::vector<std::string> column_names;
  for (int idx : column_idxs) {
    column_names.push_back(GetColumn(idx)->Name());
  }

  ReadArg read_arg;
  read_arg.table = Name();
  read_arg.key_set = KeySet::All();
  read_arg.columns = column_names;
  std::unique_ptr<RowCursor> cursor;
  ZETASQL_RETURN_IF_ERROR(reader_->Read(read_arg, &cursor));
  return std::make_unique<RowCursorEvaluatorTableIterator>(std::move(cursor));
}

const zetasql::Column* QueryableTable::FindColumnByName(
    const std::string& name) const {
  const auto* to_find = wrapped_table_->FindColumn(name);
  auto it = std::find_if(columns_.begin(), columns_.end(),
                         [to_find](const auto& column) {
                           return column->wrapped_column() == to_find;
                         });
  if (it == columns_.end()) {
    return nullptr;
  }
  return it->get();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
