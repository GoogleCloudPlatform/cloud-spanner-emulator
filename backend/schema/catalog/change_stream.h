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

inline constexpr absl::string_view kChangeStreamRetentionPeriodDefault = "1d";
inline constexpr absl::string_view kChangeStreamValueCaptureTypeDefault =
    "OLD_AND_NEW_VALUES";

class Table;
class Column;
class ChangeStream : public SchemaNode {
 public:
  // Returns the name of this change stream.
  std::string Name() const { return name_; }

  std::string tvf_name() const { return tvf_name_; }

  const ChangeStreamID id() const { return id_; }

  // Returns the tables and columns that is tracked.
  absl::flat_hash_map<std::string, std::vector<std::string>>
  tracked_tables_columns() const {
    return tracked_tables_columns_;
  }

  // Returns the backing table which stores the change stream data.
  const Table* change_stream_data_table() const {
    return change_stream_data_table_;
  }

  const Table* change_stream_partition_table() const {
    return change_stream_partition_table_;
  }

  std::optional<std::string> retention_period() const {
    return retention_period_;
  }

  int64_t parsed_retention_period() const { return parsed_retention_period_; }

  absl::Time creation_time() const { return creation_time_; }

  std::optional<std::string> value_capture_type() const {
    return value_capture_type_;
  }

  bool track_all() const { return track_all_; }

  const ddl::ChangeStreamForClause* for_clause() const {
    return for_clause_.has_value() ? &for_clause_.value() : nullptr;
  }

  const ::google::protobuf::RepeatedPtrField<ddl::SetOption> options() const {
    return options_;
  }

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

  // The tables and columns that this change stream tracks.
  absl::flat_hash_map<std::string, std::vector<std::string>>
      tracked_tables_columns_;

  // We should not set the default value for the set options; otherwise, we
  // cannot know if they are specified in the DDL statement.
  std::optional<std::string> retention_period_;

  std::optional<std::string> value_capture_type_;

  bool track_all_ = false;

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

  // A copy of for clause statement. We cannot use a pointer here because
  // ddl::ChangeStreamForClause will be destroyed after parsing and we will get
  // a segmentation fault when accessing its content. A unique pointer does not
  // work since we need to support ShallowClone() and the ownership will be
  // passed to the new object.
  std::optional<ddl::ChangeStreamForClause> for_clause_;

  ::google::protobuf::RepeatedPtrField<ddl::SetOption> options_;

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
