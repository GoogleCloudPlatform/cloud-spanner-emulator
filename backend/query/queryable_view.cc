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

#include "backend/query/queryable_view.h"

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/types/span.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// An implementation of EvaluatorTableIterator which wraps a RowCursor that
// contains the result of executing the view definition.
class ViewRowCursorEvaluatorTableIterator
    : public zetasql::EvaluatorTableIterator {
 public:
  ViewRowCursorEvaluatorTableIterator(std::unique_ptr<RowCursor> cursor,
                                      absl::Span<const int> column_idxs)
      : cursor_(std::move(cursor)),
        column_idxs_(column_idxs.begin(), column_idxs.end()) {
    for (int i = 0; i < cursor_->NumColumns(); ++i) {
      if (column_idxs_.find(i) != column_idxs_.end()) {
        values_.push_back(zetasql::Value::Null(cursor_->ColumnType(i)));
        column_names_.push_back(cursor_->ColumnName(i));
      }
    }
  }

  int NumColumns() const override { return values_.size(); }

  std::string GetColumnName(int i) const override { return column_names_[i]; }

  const zetasql::Type* GetColumnType(int i) const override {
    return values_[i].type();
  }

  bool NextRow() override {
    if (!cursor_->Next()) {
      return false;
    }

    for (int i = 0, j = 0; i < cursor_->NumColumns(); ++i) {
      if (column_idxs_.find(i) != column_idxs_.end()) {
        values_[j++] = cursor_->ColumnValue(i);
      }
    }
    return true;
  }

  const zetasql::Value& GetValue(int i) const override {
    return values_.at(i);
  }

  absl::Status Status() const override { return cursor_->Status(); }
  absl::Status Cancel() override { return absl::OkStatus(); }

 private:
  // The wrapped RowCursor.
  std::unique_ptr<RowCursor> cursor_;

  // The indexes of the columns in the original view's definition
  // (and therefore its result) that the EvaluatorTableIterator must emit.
  absl::flat_hash_set<int> column_idxs_;

  // Names of the output columns of the EvaluatorTableIterator.
  std::vector<std::string> column_names_;

  // Values of the current row of the EvaluatorTableIterator.
  std::vector<zetasql::Value> values_;
};
}  // namespace

QueryableView::QueryableView(const backend::View* view,
                             QueryEvaluator* query_evaluator)
    : wrapped_view_(view), query_evaluator_(query_evaluator) {
  for (const View::Column& column : view->columns()) {
    columns_.push_back(std::make_unique<const zetasql::SimpleColumn>(
        view->Name(), column.name, column.type,
        /*is_pseudo_column:*/ false, /*is_writable_column:*/ false));
  }
}

absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
QueryableView::CreateEvaluatorTableIterator(
    absl::Span<const int> column_idxs) const {
  ZETASQL_RET_CHECK_NE(query_evaluator_, nullptr);
  auto view_body = wrapped_view_->body_origin().has_value()
                       ? wrapped_view_->body_origin().value()
                       : wrapped_view_->body();
  ZETASQL_ASSIGN_OR_RETURN(auto cursor, query_evaluator_->Evaluate(view_body));
  return std::make_unique<ViewRowCursorEvaluatorTableIterator>(
      std::move(cursor), column_idxs);
}

const zetasql::Column* QueryableView::FindColumnByName(
    const std::string& name) const {
  for (const auto& c : columns_) {
    if (absl::EqualsIgnoreCase(name, c->Name())) {
      return c.get();
    }
  }

  return nullptr;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
