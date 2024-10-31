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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_UDF_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_UDF_BUILDER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/function_signature.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/validators/udf_validator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

class Udf::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new Udf(UdfValidator::Validate, UdfValidator::ValidateUpdate))) {}

  std::unique_ptr<const Udf> build() { return std::move(instance_); }

  const Udf* get() const { return instance_.get(); }

  Builder& set_name(absl::string_view name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_postgresql_oid(std::optional<uint32_t> postgresql_oid) {
    if (postgresql_oid.has_value()) {
      instance_->set_postgresql_oid(postgresql_oid.value());
    }
    return *this;
  }

  Builder& set_sql_body(absl::string_view body) {
    instance_->body_ = body;
    return *this;
  }

  Builder& add_dependency(const SchemaNode* dependency) {
    instance_->dependencies_.push_back(dependency);
    return *this;
  }

  Builder& add_dependent(const SchemaNode* dependent) {
    instance_->dependents_.push_back(dependent);
    return *this;
  }

  Builder& set_sql_security(Udf::SqlSecurity sql_security) {
    instance_->security_ = sql_security;
    return *this;
  }

  Builder& set_body_origin(absl::string_view body_origin) {
    instance_->body_origin_ = body_origin;
    return *this;
  }

  Builder& set_signature(
      std::unique_ptr<zetasql::FunctionSignature> signature) {
    instance_->signature_ = std::move(signature);
    return *this;
  }

  Builder& set_determinism_level(Udf::Determinism determinism_level) {
    instance_->determinism_level_ = determinism_level;
    return *this;
  }

 private:
  std::unique_ptr<Udf> instance_;
};

class Udf::Editor {
 public:
  explicit Editor(Udf* instance) : instance_(instance) {}

  const Udf* get() const { return instance_; }

  // The only kind of 'edit' possible on a Udf is a complete
  // replacement of its definition.
  Editor& copy_from(const Udf* udf) {
    instance_->name_ = udf->name_;
    instance_->body_ = udf->body_;
    instance_->dependencies_ = udf->dependencies_;
    instance_->dependents_ = udf->dependents_;
    instance_->security_ = udf->security_;
    instance_->body_origin_ = udf->body_origin_;
    instance_->signature_ =
        udf->signature_
            ? absl::make_unique<zetasql::FunctionSignature>(*udf->signature_)
            : nullptr;
    instance_->determinism_level_ = udf->determinism_level_;
    return *this;
  }

 private:
  // Not owned.
  Udf* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_UDF_BUILDER_H_
