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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_DATABASE_OPTIONS_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_DATABASE_OPTIONS_BUILDER_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "backend/schema/catalog/database_options.h"
#include "backend/schema/validators/database_options_validator.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new database options
class DatabaseOptions::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new DatabaseOptions(DatabaseOptionsValidator::Validate,
                                DatabaseOptionsValidator::ValidateUpdate))) {}

  std::unique_ptr<const DatabaseOptions> build() {
    return std::move(instance_);
  }

  Builder& set_db_name(const std::string& name) {
    instance_->database_name_ = name;
    return *this;
  }
  const DatabaseOptions* get() const { return instance_.get(); }
  Builder& set_options(::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

 private:
  std::unique_ptr<DatabaseOptions> instance_;
};

class DatabaseOptions::Editor {
 public:
  explicit Editor(DatabaseOptions* instance) : instance_(instance) {}

  const DatabaseOptions* get() const { return instance_; }

  Editor& set_options(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

 private:
  // Not owned.
  DatabaseOptions* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_DATABASE_OPTIONS_BUILDER_H_
