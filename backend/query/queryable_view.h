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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_VIEW_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_VIEW_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/evaluator_table_iterator.h"
#include "zetasql/public/simple_catalog.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/access/read.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/view.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Evaluating a view requires evaluating an arbitrary query. This interface
// provides a way for a queryable view to compile and execute an arbitrary view
// definition.
class QueryEvaluator {
 public:
  virtual ~QueryEvaluator() = default;
  virtual absl::StatusOr<std::unique_ptr<RowCursor>> Evaluate(
      const std::string& query) = 0;
};

// A wrapper over View class which implements the zetasql::Table interface.
class QueryableView : public zetasql::Table {
 public:
  QueryableView(const backend::View* view,
                QueryEvaluator* query_evaluator = nullptr);

  std::string Name() const override {
    return std::string(SDLObjectName::GetInSchemaName(wrapped_view_->Name()));
  }

  // FullName includes schema if present.
  std::string FullName() const override { return wrapped_view_->Name(); }

  int NumColumns() const override { return columns_.size(); }

  const zetasql::Column* GetColumn(int i) const override {
    return columns_[i].get();
  }

  const zetasql::Column* FindColumnByName(
      const std::string& name) const override;

  std::optional<std::vector<int>> PrimaryKey() const override {
    return std::nullopt;
  }

  const backend::View* wrapped_view() const { return wrapped_view_; }

  // Override CreateEvaluatorTableIterator.
  absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
  CreateEvaluatorTableIterator(
      absl::Span<const int> column_idxs) const override;

 private:
  // The underlying View object which backs the QueryableView.
  const backend::View* wrapped_view_;

  // The columns in the view.
  std::vector<std::unique_ptr<const zetasql::SimpleColumn>> columns_;

  // Holds an object which is used to evaluate the view definition.
  QueryEvaluator* query_evaluator_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_VIEW_H_
