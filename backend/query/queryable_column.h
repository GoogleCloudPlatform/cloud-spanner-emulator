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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_COLUMN_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_COLUMN_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "googlesql/public/analyzer_output.h"
#include "googlesql/public/catalog.h"
#include "googlesql/public/type.h"
#include "absl/strings/str_cat.h"
#include "backend/schema/catalog/column.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// QueryableColumn is a thin wrapper over backend::Column class which implements
// the googlesql::Column interface. The intent is to have a cleaner separation
// of interfaces between backend/catalog and backend/query and remove any
// GoogleSQL dependencies from the former.
class QueryableColumn : public googlesql::Column {
 public:
  explicit QueryableColumn(const backend::Column* column)
      : wrapped_column_(column) {}

  QueryableColumn(const backend::Column* column,
                  std::unique_ptr<const googlesql::AnalyzerOutput> output,
                  std::optional<const googlesql::Column::ExpressionAttributes>
                      expression_attributes)
      : wrapped_column_(column),
        output_(std::move(output)),
        column_expression_(expression_attributes) {}

  std::string Name() const override { return wrapped_column_->Name(); }

  std::string FullName() const override { return wrapped_column_->FullName(); }

  const googlesql::Type* GetType() const override {
    return wrapped_column_->GetType();
  }

  bool IsWritableColumn() const override {
    return !wrapped_column_->is_generated();
  }

  bool IsPseudoColumn() const override { return wrapped_column_->hidden(); }

  // Returns optional ExpressionAttributes if a column has default or generated
  // Expression.
  std::optional<const googlesql::Column::ExpressionAttributes> GetExpression()
      const override {
    return column_expression_;
  }

  const backend::Column* wrapped_column() const { return wrapped_column_; }

 private:
  // The underlying schema column.
  const backend::Column* wrapped_column_;
  // The AnalyzerOutput that holds the column's ResolvedExpr, representing
  // default value expression.
  const std::unique_ptr<const googlesql::AnalyzerOutput> output_ = nullptr;
  // Column Expression for generated or default columns.
  std::optional<const googlesql::Column::ExpressionAttributes>
      column_expression_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_COLUMN_H_
