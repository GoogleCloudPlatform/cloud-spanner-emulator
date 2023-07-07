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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_CHANGE_STREAM_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_CHANGE_STREAM_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "backend/common/ids.h"
#include "backend/schema/ddl/operations.pb.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
class Table;
class Column;
class ChangeStream : public SchemaNode {
 public:
  // Returns the name of this change stream.
  std::string Name() const { return name_; }

  const ddl::ChangeStreamForClause* for_clause() const { return for_clause_; }

  const ddl::SetOption* options() const { return options_; }

  // SchemaNode interface implementation.
  // ------------------------------------

  std::optional<SchemaNameInfo> GetSchemaNameInfo() const override {
    return SchemaNameInfo{
        .name = name_, .kind = "ChangeStream", .global = true};
  }

  absl::Status Validate(SchemaValidationContext* context) const override;

  absl::Status ValidateUpdate(const SchemaNode* old,
                              SchemaValidationContext* context) const override;
  std::string DebugString() const override;
  class Builder;
  class Editor;

 private:
  friend class ChangeStreamValidator;
  using ValidationFn = std::function<absl::Status(const ChangeStream*,
                                                  SchemaValidationContext*)>;
  using UpdateValidationFn = std::function<absl::Status(
      const ChangeStream*, const ChangeStream*, SchemaValidationContext*)>;
  // Constructors are private and only friend classes are able to build.
  ChangeStream(const ValidationFn& validate,
               const UpdateValidationFn& validate_update)
      : validate_(validate), validate_update_(validate_update) {}
  ChangeStream(const ChangeStream&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new ChangeStream(*this));
  }

  absl::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // Validation delegates.
  const ValidationFn validate_;

  const UpdateValidationFn validate_update_;

  // Name of this change stream.
  std::string name_;

  // Name of this change stream's table valued function.
  std::string tvf_name_;

  // TODO: assign the ID during change stream creation
  // A unique ID for identifying this change stream in the schema that owns this
  // change stream.
  ChangeStreamID id_;

  // Parsed retention period in seconds, use for query timestamp validation,
  // default is 1 day in seconds
  int64_t parsed_retention_period_ = 24 * 60 * 60;

  // Timestamp which this change stream is created, use for prevent querying
  // change stream before creation
  absl::Time creation_time_;

  const ddl::ChangeStreamForClause* for_clause_ = nullptr;

  const ddl::SetOption* options_ = nullptr;

  // The backing table that stores the change stream data.
  const Table* change_stream_data_table_;

  // The table that stores the partition data.
  const Table* change_stream_partition_table_;
};
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_CHANGE_STREAM_H_
