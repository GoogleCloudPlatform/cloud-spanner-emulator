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
#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/log/check.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"  //
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/query/queryable_column.h"
#include "backend/schema/catalog/column.h"
#include "common/constants.h"
#include "common/feature_flags.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

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

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
QueryableTable::AnalyzeColumnExpression(
    const Column* column, zetasql::TypeFactory* type_factory,
    zetasql::Catalog* catalog,
    std::optional<const zetasql::AnalyzerOptions> opt_options) const {
  std::unique_ptr<const zetasql::AnalyzerOutput> output = nullptr;
  bool enable_generated_pk =
      EmulatorFeatureFlags::instance().flags().enable_generated_pk;
  bool is_generated_column = enable_generated_pk && column->is_generated();
  if (opt_options.has_value() && (column->has_default_value()
                                  || (is_generated_column)
                                  )) {
    zetasql::AnalyzerOptions options = opt_options.value();
    if (is_generated_column) {
      for (const Column* dep : column->dependent_columns()) {
        ZETASQL_RETURN_IF_ERROR(
            options.AddExpressionColumn(dep->Name(), dep->GetType()))
            << "Failed to add dependent column " << dep->Name()
            << " for generated column : " << column->FullName();
      }
    }
    absl::Status s = zetasql::AnalyzeExpressionForAssignmentToType(
        column->expression().value(), options, catalog, type_factory,
        column->GetType(), &output);
    std::string expression_type = "default";
    if (is_generated_column) {
      expression_type = "generated";
    }
    ZETASQL_RETURN_IF_ERROR(zetasql::AnalyzeExpressionForAssignmentToType(
        column->expression().value(), options, catalog, type_factory,
        column->GetType(), &output))
        << "Failed to analyze " << expression_type << " expression for column "
        << column->FullName();
  }
  return std::move(output);
}

QueryableTable::QueryableTable(
    const backend::Table* table, RowReader* reader,
    std::optional<const zetasql::AnalyzerOptions> opt_options,
    zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory)
    : wrapped_table_(table), reader_(reader) {
  bool enable_generated_pk =
      EmulatorFeatureFlags::instance().flags().enable_generated_pk;
  for (const auto* column : table->columns()) {
    absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
        analyzer_output =
            AnalyzeColumnExpression(column, type_factory, catalog, opt_options);
    ABSL_CHECK_OK(analyzer_output.status());  // Crash OK
    std::unique_ptr<const zetasql::AnalyzerOutput> output =
        std::move(analyzer_output.value());
    bool is_generated_column = enable_generated_pk && column->is_generated();
    if (column->has_default_value()
        || (is_generated_column)
    ) {
      zetasql::Column::ExpressionAttributes::ExpressionKind expression_kind =
          zetasql::Column::ExpressionAttributes::ExpressionKind::DEFAULT;
      if (is_generated_column) {
        expression_kind =
            zetasql::Column::ExpressionAttributes::ExpressionKind::GENERATED;
      }
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
  // Pending commit timestamp restrictions for queries are implemented in
  // QueryValidator so we do not need enforcement during the read here.
  // Furthermore, without enabling this certain internal reads issued by the
  // ZetaSQL reference implementation will be rejected.
  read_arg.allow_pending_commit_timestamps = true;

  // If current table is a change stream internal data/partition table, change
  // the read arg to access internal tables directly.
  if (wrapped_table_->owner_change_stream() != nullptr) {
    absl::string_view change_stream_name = read_arg.table;
    if (absl::StartsWith(read_arg.table, kChangeStreamPartitionTablePrefix)) {
      absl::ConsumePrefix(&change_stream_name,
                          kChangeStreamPartitionTablePrefix);
      read_arg.change_stream_for_partition_table = change_stream_name;
    } else {
      absl::ConsumePrefix(&change_stream_name, kChangeStreamDataTablePrefix);
      read_arg.change_stream_for_data_table = change_stream_name;
    }
  }
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
