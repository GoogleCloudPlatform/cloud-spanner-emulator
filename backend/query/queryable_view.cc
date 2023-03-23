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

QueryableView::QueryableView(const backend::View* view) : wrapped_view_(view) {
  for (const View::Column& column : view->columns()) {
    columns_.push_back(std::make_unique<const zetasql::SimpleColumn>(
        view->Name(), column.name, column.type,
        /*is_pseudo_column:*/ false, /*is_writable_column:*/ false));
  }
}

absl::StatusOr<std::unique_ptr<zetasql::EvaluatorTableIterator>>
QueryableView::CreateEvaluatorTableIterator(
    absl::Span<const int> column_idxs) const {
  // TODO : Implement querying on the view.
  return nullptr;
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
