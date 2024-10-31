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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_MODEL_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_MODEL_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type.h"
#include "absl/strings/str_cat.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class QueryableModelColumn : public zetasql::Column {
 public:
  QueryableModelColumn(const backend::Model* model,
                       const backend::Model::ModelColumn* column)
      : wrapped_model_(model), wrapped_column_(column) {}

  std::string Name() const override { return wrapped_column_->name; }
  std::string FullName() const override {
    return absl::StrCat(wrapped_model_->Name(), ".", wrapped_column_->name);
  }
  const zetasql::Type* GetType() const override {
    return wrapped_column_->type;
  }
  bool IsWritableColumn() const override { return false; }

  bool required() const { return wrapped_column_->is_required.value_or(true); }

 private:
  const backend::Model* const wrapped_model_;
  const backend::Model::ModelColumn* const wrapped_column_;
};

class QueryableModel : public zetasql::Model {
 public:
  explicit QueryableModel(const backend::Model* model);

  std::string Name() const override {
    return std::string(SDLObjectName::GetInSchemaName(wrapped_model_->Name()));
  }
  std::string FullName() const override { return wrapped_model_->Name(); }
  uint64_t NumInputs() const override { return wrapped_model_->input().size(); }
  const zetasql::Column* GetInput(int i) const override {
    return input_columns_[i].get();
  }
  uint64_t NumOutputs() const override {
    return wrapped_model_->output().size();
  }
  const zetasql::Column* GetOutput(int i) const override {
    return output_columns_[i].get();
  }
  const zetasql::Column* FindInputByName(
      const std::string& name) const override {
    for (auto& column : input_columns_) {
      if (column->Name() == name) {
        return column.get();
      }
    }
    return nullptr;
  }
  const zetasql::Column* FindOutputByName(
      const std::string& name) const override {
    for (auto& column : output_columns_) {
      if (column->Name() == name) {
        return column.get();
      }
    }
    return nullptr;
  }

 private:
  const backend::Model* const wrapped_model_;
  std::vector<std::unique_ptr<const QueryableModelColumn>> input_columns_;
  std::vector<std::unique_ptr<const QueryableModelColumn>> output_columns_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_MODEL_H_
