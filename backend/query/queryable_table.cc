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
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/analyzer.h"
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/analyzer_output.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"  //
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"
#include "backend/datamodel/key_set.h"
#include "backend/query/column_expression_analysis_cache.h"
#include "backend/query/queryable_column.h"
#include "backend/schema/catalog/column.h"
#include "common/constants.h"
#include "common/feature_flags.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// An implementation of EvaluatorTableIterator which wraps a RowCursor.
//
// Used by QueryableTable::CreateEvaluatorTableIterator.
class RowCursorEvaluatorTableIterator
    : public googlesql::EvaluatorTableIterator {
 public:
  explicit RowCursorEvaluatorTableIterator(std::unique_ptr<RowCursor> cursor)
      : cursor_(std::move(cursor)) {
    values_.reserve(cursor_->NumColumns());
    for (int i = 0; i < cursor_->NumColumns(); ++i) {
      values_.push_back(googlesql::values::Null(cursor_->ColumnType(i)));
    }
  }

  int NumColumns() const override { return cursor_->NumColumns(); }

  std::string GetColumnName(int i) const override {
    return cursor_->ColumnName(i);
  }

  const googlesql::Type* GetColumnType(int i) const override {
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

  const googlesql::Value& GetValue(int i) const override { return values_[i]; }

  absl::Status Status() const override { return cursor_->Status(); }

  // Cancel is best-effort and not required.
  absl::Status Cancel() override { return absl::OkStatus(); }

 private:
  // The wrapped RowCursor.
  std::unique_ptr<RowCursor> cursor_;

  // Values of the current row. EvaluatorTableIterator::GetValue need to return
  // a reference so we need to buffer the values instead of simply delegate to
  // RowCursor::ColumnValue.
  std::vector<googlesql::Value> values_;
};

namespace {

// The most keys a scan is narrowed to. A filter that would pass this is left
// out, and the read stays as wide as the filters before it made it.
constexpr size_t kMaxKeysFromFilters = 10000;

// Narrowing is on unless the process is started with this variable set, so a
// result can be compared against a whole-table read.
bool KeyFilterPushdownEnabled() {
  static const bool enabled =
      std::getenv("SPANNER_EMULATOR_DISABLE_KEY_FILTER_PUSHDOWN") == nullptr;
  return enabled;
}

}  // namespace

// An EvaluatorTableIterator that reads when its first row is asked for, so
// that the filters the evaluator offers before then can narrow the read from
// the whole table to the keys that can match.
//
// Only equality and IN filters on the leading primary key columns are used.
// The evaluator applies every filter again to the rows it is given, so a filter
// left unused costs a wider read and never a wrong result.
class KeyFilteredTableIterator : public googlesql::EvaluatorTableIterator {
 public:
  // 'leading_key_positions' holds, for the primary key columns in key order up
  // to the first one the scan does not read, the position of that column among
  // the columns of 'read_arg'.
  KeyFilteredTableIterator(RowReader* reader, ReadArg read_arg,
                           std::vector<const googlesql::Type*> column_types,
                           std::vector<int> leading_key_positions)
      : reader_(reader),
        read_arg_(std::move(read_arg)),
        column_types_(std::move(column_types)),
        leading_key_positions_(std::move(leading_key_positions)) {}

  int NumColumns() const override {
    return static_cast<int>(read_arg_.columns.size());
  }

  std::string GetColumnName(int i) const override {
    return read_arg_.columns[i];
  }

  const googlesql::Type* GetColumnType(int i) const override {
    return column_types_[i];
  }

  absl::Status SetColumnFilterMap(
      absl::flat_hash_map<int, std::unique_ptr<googlesql::ColumnFilter>>
          filter_map) override {
    if (rows_ != nullptr) {
      return absl::OkStatus();
    }
    std::vector<Key> prefixes = {Key()};
    int narrowed_columns = 0;
    for (int position : leading_key_positions_) {
      auto filter = filter_map.find(position);
      if (filter == filter_map.end() ||
          filter->second->kind() != googlesql::ColumnFilter::kInList) {
        break;
      }
      const std::vector<googlesql::Value>& wanted = filter->second->in_list();
      const googlesql::Type* column_type = column_types_[position];
      // Only for types whose SQL equality is the equality of their key values,
      // and only for values of the column's own type: anything else is left to
      // the evaluator.
      if (!(column_type->IsInt64() || column_type->IsString() ||
            column_type->IsBytes() || column_type->IsBool() ||
            column_type->IsDate() || column_type->IsTimestamp()) ||
          !std::all_of(wanted.begin(), wanted.end(),
                       [column_type](const googlesql::Value& value) {
                         return value.type()->Equals(column_type);
                       }) ||
          prefixes.size() * wanted.size() > kMaxKeysFromFilters) {
        break;
      }
      std::vector<Key> extended;
      extended.reserve(prefixes.size() * wanted.size());
      for (const Key& prefix : prefixes) {
        for (const googlesql::Value& value : wanted) {
          Key key = prefix;
          key.AddColumn(value);
          extended.push_back(std::move(key));
        }
      }
      prefixes = std::move(extended);
      ++narrowed_columns;
    }
    if (narrowed_columns == 0) {
      return absl::OkStatus();
    }
    // An empty list is a filter no row passes, and leaves an empty key set.
    KeySet key_set;
    for (const Key& prefix : prefixes) {
      key_set.AddRange(KeyRange::Prefix(prefix));
    }
    read_arg_.key_set = key_set;
    return absl::OkStatus();
  }

  bool NextRow() override {
    if (rows_ == nullptr) {
      std::unique_ptr<RowCursor> cursor;
      read_status_ = reader_->Read(read_arg_, &cursor);
      if (!read_status_.ok()) {
        return false;
      }
      rows_ =
          std::make_unique<RowCursorEvaluatorTableIterator>(std::move(cursor));
    }
    return rows_->NextRow();
  }

  const googlesql::Value& GetValue(int i) const override {
    return rows_->GetValue(i);
  }

  absl::Status Status() const override {
    return rows_ == nullptr ? read_status_ : rows_->Status();
  }

  // Cancel is best-effort and not required.
  absl::Status Cancel() override { return absl::OkStatus(); }

 private:
  RowReader* reader_;

  // What is read, its key set narrowed by SetColumnFilterMap.
  ReadArg read_arg_;

  std::vector<const googlesql::Type*> column_types_;

  std::vector<int> leading_key_positions_;

  // The rows, once the read has been made.
  std::unique_ptr<RowCursorEvaluatorTableIterator> rows_;

  absl::Status read_status_;
};


absl::StatusOr<std::shared_ptr<const googlesql::AnalyzerOutput>>
QueryableTable::AnalyzeColumnExpression(
    const Column* column, googlesql::TypeFactory* type_factory,
    googlesql::Catalog* catalog,
    const std::optional<const googlesql::AnalyzerOptions>& opt_options,
    ColumnExpressionAnalysisCache* analyses) const {
  std::shared_ptr<const googlesql::AnalyzerOutput> output = nullptr;
  bool enable_generated_pk =
      EmulatorFeatureFlags::instance().flags().enable_generated_pk;
  bool is_generated_column = enable_generated_pk && column->is_generated();
  if (opt_options.has_value() &&
      (column->has_default_value() || (is_generated_column))) {
    std::string key;
    if (analyses != nullptr) {
      key = ColumnExpressionAnalysisCache::KeyFor(column, opt_options.value());
      output = analyses->Find(key);
      if (output != nullptr) {
        return output;
      }
    }
    googlesql::AnalyzerOptions options = opt_options.value();
    if (is_generated_column) {
      for (const Column* dep : column->dependent_columns()) {
        GOOGLESQL_RETURN_IF_ERROR(
            options.AddExpressionColumn(dep->Name(), dep->GetType()))
            << "Failed to add dependent column " << dep->Name()
            << " for generated column : " << column->FullName();
      }
    }
    std::string expression_type = "default";
    if (is_generated_column) {
      expression_type = "generated";
    }
    std::unique_ptr<const googlesql::AnalyzerOutput> analyzed;
    GOOGLESQL_RETURN_IF_ERROR(googlesql::AnalyzeExpressionForAssignmentToType(
        column->expression().value(), options, catalog, type_factory,
        column->GetType(), &analyzed))
        << "Failed to analyze " << expression_type << " expression for column "
        << column->FullName();
    output = std::move(analyzed);
    if (analyses != nullptr) {
      analyses->Store(key, output);
    }
  }
  return output;
}

QueryableTable::QueryableTable(
    const backend::Table* table, RowReader* reader,
    const std::optional<const googlesql::AnalyzerOptions>& opt_options,
    googlesql::Catalog* catalog, googlesql::TypeFactory* type_factory,
    bool is_synonym, ColumnExpressionAnalysisCache* analyses)
    : is_synonym_(is_synonym), wrapped_table_(table), reader_(reader) {
  bool enable_generated_pk =
      EmulatorFeatureFlags::instance().flags().enable_generated_pk;
  for (const auto* column : table->columns()) {
    absl::StatusOr<std::shared_ptr<const googlesql::AnalyzerOutput>>
        analyzer_output =
            AnalyzeColumnExpression(column, type_factory, catalog, opt_options,
                                    analyses);
    ABSL_CHECK_OK(analyzer_output.status());  // Crash OK
    std::shared_ptr<const googlesql::AnalyzerOutput> output =
        std::move(analyzer_output.value());
    bool is_generated_column = enable_generated_pk && column->is_generated();
    if (column->has_default_value() || (is_generated_column)) {
      googlesql::Column::ExpressionAttributes::ExpressionKind expression_kind =
          googlesql::Column::ExpressionAttributes::ExpressionKind::DEFAULT;
      if (is_generated_column) {
        expression_kind =
            googlesql::Column::ExpressionAttributes::ExpressionKind::GENERATED;
      }
      googlesql::Column::ExpressionAttributes expression_attributes =
          googlesql::Column::ExpressionAttributes(expression_kind,
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

absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
QueryableTable::CreateEvaluatorTableIterator(
    absl::Span<const int> column_idxs) const {
  GOOGLESQL_RET_CHECK_NE(reader_, nullptr);

  std::vector<std::string> column_names;
  for (int idx : column_idxs) {
    column_names.push_back(GetColumn(idx)->Name());
  }

  ReadArg read_arg;
  read_arg.table = FullName();
  read_arg.key_set = KeySet::All();
  read_arg.columns = column_names;
  // Pending commit timestamp restrictions for queries are implemented in
  // QueryValidator so we do not need enforcement during the read here.
  // Furthermore, without enabling this certain internal reads issued by the
  // GoogleSQL reference implementation will be rejected.
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

  // The leading primary key columns this scan reads, by their position in it.
  std::vector<int> leading_key_positions;
  if (KeyFilterPushdownEnabled() &&
      wrapped_table_->owner_change_stream() == nullptr &&
      primary_key_column_indexes_.size() ==
          wrapped_table_->primary_key().size()) {
    for (int key_column_idx : primary_key_column_indexes_) {
      auto scanned =
          std::find(column_idxs.begin(), column_idxs.end(), key_column_idx);
      if (scanned == column_idxs.end()) {
        break;
      }
      leading_key_positions.push_back(
          static_cast<int>(scanned - column_idxs.begin()));
    }
  }
  if (leading_key_positions.empty()) {
    std::unique_ptr<RowCursor> cursor;
    GOOGLESQL_RETURN_IF_ERROR(reader_->Read(read_arg, &cursor));
    return std::make_unique<RowCursorEvaluatorTableIterator>(
        std::move(cursor));
  }

  std::vector<const googlesql::Type*> column_types;
  column_types.reserve(column_idxs.size());
  for (int idx : column_idxs) {
    column_types.push_back(GetColumn(idx)->GetType());
  }
  return std::make_unique<KeyFilteredTableIterator>(
      reader_, std::move(read_arg), std::move(column_types),
      std::move(leading_key_positions));
}

const googlesql::Column* QueryableTable::FindColumnByName(
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
