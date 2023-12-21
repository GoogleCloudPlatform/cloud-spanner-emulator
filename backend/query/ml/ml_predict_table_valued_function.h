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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_TABLE_VALUED_FUNCTION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_TABLE_VALUED_FUNCTION_H_

#include <memory>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/table_valued_function.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class MlPredictTableValuedFunction : public zetasql::TableValuedFunction {
 public:
  explicit MlPredictTableValuedFunction(bool safe);

  absl::Status Resolve(
      const zetasql::AnalyzerOptions* analyzer_options,
      const std::vector<zetasql::TVFInputArgumentType>& actual_arguments,
      const zetasql::FunctionSignature& concrete_signature,
      zetasql::Catalog* catalog, zetasql::TypeFactory* type_factory,
      std::shared_ptr<zetasql::TVFSignature>* output_tvf_signature)
      const override;

  bool IsSafe() const { return safe_; }

 private:
  const bool safe_ = false;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_ML_ML_PREDICT_TABLE_VALUED_FUNCTION_H_
