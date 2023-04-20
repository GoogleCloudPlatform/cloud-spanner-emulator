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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_CHANGE_STREAM_VALIDATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_CHANGE_STREAM_VALIDATOR_H_

#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Implementation of ChangeStream::Validate().
class ChangeStreamValidator {
 public:
  ChangeStreamValidator(const ddl::CreateChangeStream& create, Schema& schema)
      : create_(create), schema_(schema) {}
  static absl::Status Validate(const ChangeStream* change_stream,
                               SchemaValidationContext* context);
  static absl::Status ValidateForClause();
  // Validates the limit on the max number of change streams tracking the same
  // table or non-key column.
  absl::Status ValidateLimits();
  absl::Status ForEachTrackedObject(
      const ddl::CreateChangeStream& create, Schema& schema,
      absl::FunctionRef<absl::Status(Table*)> table_cb);
  static absl::Status ValidateUpdate(const ChangeStream* change_stream,
                                     const ChangeStream* old_change_stream,
                                     SchemaValidationContext* context);

 private:
  const ddl::CreateChangeStream& create_;
  Schema& schema_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_VALIDATORS_CHANGE_STREAM_VALIDATOR_H_
