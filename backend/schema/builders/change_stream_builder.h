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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHANGE_STREAM_BUILDER_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHANGE_STREAM_BUILDER_H_

#include <cerrno>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "backend/schema/validators/change_stream_validator.h"
#include "google/protobuf/repeated_ptr_field.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Build a new change stream
class ChangeStream::Builder {
 public:
  Builder()
      : instance_(absl::WrapUnique(
            new ChangeStream(ChangeStreamValidator::Validate,
                             ChangeStreamValidator::ValidateUpdate))) {}

  std::unique_ptr<const ChangeStream> build() { return std::move(instance_); }

  const ChangeStream* get() const { return instance_.get(); }

  Builder& set_id(const std::string& id) {
    instance_->id_ = id;
    return *this;
  }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_tvf_name(const std::string& tvf_name) {
    instance_->tvf_name_ = tvf_name;
    return *this;
  }

  Builder& set_change_stream_data_table(const Table* table) {
    instance_->change_stream_data_table_ = table;
    return *this;
  }

  Builder& set_change_stream_partition_table(const Table* table) {
    instance_->change_stream_partition_table_ = table;
    return *this;
  }

  Builder& add_tracked_tables_columns(std::string table_name,
                                      std::vector<std::string> columns) {
    instance_->tracked_tables_columns_[table_name] = columns;
    return *this;
  }

  Builder& set_options(::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

  Builder& set_retention_period(std::optional<std::string> retention_period) {
    instance_->retention_period_ = retention_period;
    return *this;
  }

  Builder& set_value_capture_type(
      std::optional<std::string> value_capture_type) {
    instance_->value_capture_type_ = value_capture_type;
    return *this;
  }

  Builder& set_for_clause(const ddl::ChangeStreamForClause& for_clause) {
    instance_->for_clause_ = for_clause;
    return *this;
  }

  Builder& set_track_all(bool track_all) {
    instance_->track_all_ = track_all;
    return *this;
  }

  Builder& set_creation_time(absl::Time creation_time) {
    instance_->creation_time_ = creation_time;
    return *this;
  }

 private:
  std::unique_ptr<ChangeStream> instance_;
};

class ChangeStream::Editor {
 public:
  explicit Editor(ChangeStream* instance) : instance_(instance) {}

  const ChangeStream* get() const { return instance_; }

  Editor& add_tracked_tables_columns(std::string table_name,
                                     std::vector<std::string> columns) {
    instance_->tracked_tables_columns_[table_name] = columns;
    return *this;
  }

  Editor& add_tracked_table_column(std::string table_name, std::string column) {
    instance_->tracked_tables_columns_[table_name].push_back(column);
    return *this;
  }

  Editor& clear_tracked_tables_columns() {
    instance_->tracked_tables_columns_.clear();
    return *this;
  }

  Editor& set_options(
      const ::google::protobuf::RepeatedPtrField<ddl::SetOption> options) {
    instance_->options_ = options;
    return *this;
  }

  Editor& set_retention_period(std::optional<std::string> retention_period) {
    instance_->retention_period_ = retention_period;
    return *this;
  }

  Editor& set_parsed_retention_period(int64_t parsed_retention_period) {
    instance_->parsed_retention_period_ = parsed_retention_period;
    return *this;
  }

  Editor& set_value_capture_type(
      std::optional<std::string> value_capture_type) {
    instance_->value_capture_type_ = value_capture_type;
    return *this;
  }

  Editor& set_for_clause(const ddl::ChangeStreamForClause& for_clause) {
    instance_->for_clause_ = for_clause;
    return *this;
  }

  Editor& set_track_all(bool track_all) {
    instance_->track_all_ = track_all;
    return *this;
  }

  Editor& clear_for_clause() {
    instance_->for_clause_.reset();
    return *this;
  }

 private:
  // Not owned.
  ChangeStream* instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_BUILDERS_CHANGE_STREAM_BUILDER_H_
