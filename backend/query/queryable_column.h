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

#include <string>

#include "zetasql/public/catalog.h"
#include "zetasql/public/type.h"
#include "absl/strings/str_cat.h"
#include "backend/schema/catalog/column.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// QueryableColumn is a thin wrapper over backend::Column class which implements
// the zetasql::Column interface. The intent is to have a cleaner separation
// of interfaces between backend/catalog and backend/query and remove any
// ZetaSQL dependencies from the former.
class QueryableColumn : public zetasql::Column {
 public:
  QueryableColumn(const backend::Column* column) : wrapped_column_(column) {}

  std::string Name() const override { return wrapped_column_->Name(); }

  std::string FullName() const override { return wrapped_column_->FullName(); }

  const zetasql::Type* GetType() const override {
    return wrapped_column_->GetType();
  }

  bool IsWritableColumn() const override {
    return !wrapped_column_->is_generated();
  }

  const backend::Column* wrapped_column() const { return wrapped_column_; }

 private:
  // The underlying schema column.
  const backend::Column* wrapped_column_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_COLUMN_H_
