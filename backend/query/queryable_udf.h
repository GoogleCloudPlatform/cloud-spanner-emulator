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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_UDF_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_UDF_H_

#include <string>

#include "zetasql/public/function.h"
#include "zetasql/public/types/type_factory.h"
#include "backend/schema/catalog/udf.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A wrapper over Udf class which implements zetasql::Function.
// QueryableUdf has a reference to the backend Udf.
class QueryableUdf : public zetasql::Function {
 public:
  explicit QueryableUdf(const backend::Udf* backend_udf,
                        std::string default_time_zone,
                        zetasql::Catalog* catalog = nullptr,
                        zetasql::TypeFactory* type_factory = nullptr);

  const backend::Udf* wrapped_udf() const { return wrapped_udf_; }

 private:
  static zetasql::FunctionOptions CreateFunctionOptions(
      const backend::Udf* udf, std::string default_time_zone,
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory);

  // The underlying Udf object which backs the QueryableUdf.
  const backend::Udf* wrapped_udf_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_UDF_H_
