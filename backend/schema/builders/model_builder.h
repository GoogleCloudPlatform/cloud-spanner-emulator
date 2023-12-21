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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_MODEL_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_MODEL_BUILDER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/validators/model_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new model
class Model::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(new Model(
            ModelValidator::Validate, ModelValidator::ValidateUpdate))) {}

  const Model* get() const { return instance_.get(); }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& add_input_column(const Model::ModelColumn& column) {
    instance_->input_.push_back(column);
    return *this;
  }

  Builder& add_output_column(const Model::ModelColumn& column) {
    instance_->output_.push_back(column);
    return *this;
  }

  Builder& set_remote(bool is_remote) {
    instance_->is_remote_ = is_remote;
    return *this;
  }

  Builder& set_default_batch_size(std::optional<int64_t> default_batch_size) {
    instance_->default_batch_size_ = default_batch_size;
    return *this;
  }

  Builder& set_endpoint(std::optional<std::string> endpoint) {
    instance_->endpoint_ = std::move(endpoint);
    return *this;
  }

  Builder& set_endpoints(std::vector<std::string> endpoints) {
    instance_->endpoints_ = std::move(endpoints);
    return *this;
  }

  std::unique_ptr<const Model> build() { return std::move(instance_); }

 private:
  std::unique_ptr<Model> instance_;
};

class Model::Editor {
 public:
  explicit Editor(Model* instance) : instance_(instance) {}
  const Model* get() const { return instance_; }

  Editor& set_default_batch_size(std::optional<int64_t> default_batch_size) {
    instance_->default_batch_size_ = default_batch_size;
    return *this;
  }

  Editor& set_endpoint(std::optional<std::string> endpoint) {
    instance_->endpoint_ = std::move(endpoint);
    return *this;
  }

  Editor& set_endpoints(std::vector<std::string> endpoints) {
    instance_->endpoints_ = std::move(endpoints);
    return *this;
  }

  Editor& copy_from(const Model* model) {
    instance_->name_ = model->name_;
    instance_->is_remote_ = model->is_remote_;
    instance_->input_ = model->input_;
    instance_->output_ = model->output_;
    instance_->endpoint_ = model->endpoint_;
    instance_->endpoints_ = model->endpoints_;
    instance_->default_batch_size_ = model->default_batch_size_;
    return *this;
  }

 private:
  // Not owned.
  Model* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_MODEL_BUILDER_H_
