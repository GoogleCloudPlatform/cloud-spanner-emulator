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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_INDEX_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_INDEX_H_

#include <memory>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "absl/memory/memory.h"
#include "absl/strings/substitute.h"
#include "backend/common/ids.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Index represents a secondary index on a table.
//
// The index stores pointers to the indexed table as well as the backing data
// table which stores the index information (index columns + indexed table
// primary key columns + storing columns). The primary key of the backing data
// table will be the primary key of the indexed table prefixed by the index
// columns (with duplicate columns removed) to guarantee uniqueness of the data
// table key.
//
// Example:
//
// CREATE TABLE Albums (
//   SingerId     INT64 NOT NULL,
//   AlbumId      INT64 NOT NULL,
//   AlbumTitle   STRING(MAX),
//   ReleaseDate  DATE
// ) PRIMARY KEY (SingerId, AlbumId),
//   INTERLEAVE IN PARENT Singers ON DELETE CASCADE;
//
// CREATE INDEX AlbumsByAlbumTitle ON Albums(AlbumTitle);
//
// This will create an index 'AlbumsByAlbumTitle' on the table 'Albums'. The
// index data table will store the column 'AlbumTitle' which is the index
// column, followed by 'SingerId', 'AlbumId' which are the primary key columns
// of indexed table. If any storing columns were present they would be appended
// to the end.
//
class Index : public SchemaNode {
 public:
  // Returns the name of this index.
  std::string Name() const { return name_; }

  // Returns the table that is indexed.
  const Table* indexed_table() const { return indexed_table_; }

  // Returns the backing table which stores the index data.
  const Table* index_data_table() const { return index_data_table_; }

  // Returns the parent table that the index data table is interleaved in, if
  // one exists.
  const Table* parent() const;

  // Returns the key columns of the index as declared in the CREATE INDEX
  // statement.
  absl::Span<const KeyColumn* const> key_columns() const {
    return key_columns_;
  }

  // Returns the list of all the storing columns.
  absl::Span<const Column* const> stored_columns() const {
    return stored_columns_;
  }

  // Returns true if this is a unique index.
  bool is_unique() const { return is_unique_; }

  // Returns true if null filtering is enabled for this index.
  bool is_null_filtered() const { return is_null_filtered_; }

  // Returns a detailed string which lists information about this index.
  std::string FullDebugString() const;

  // SchemaNode interface implementation.
  // ------------------------------------

  zetasql_base::Status Validate(SchemaValidationContext* context) const override;

  zetasql_base::Status ValidateUpdate(const SchemaNode* old,
                              SchemaValidationContext* context) const override;

  std::string DebugString() const override;

  class Builder;

 private:
  Index() = default;
  Index(const Index&) = default;

  std::unique_ptr<SchemaNode> ShallowClone() const override {
    return absl::WrapUnique(new Index(*this));
  }

  zetasql_base::Status DeepClone(SchemaGraphEditor* editor,
                         const SchemaNode* orig) override;

  // The name of this index.
  std::string name_;

  // The table that this index references.
  const Table* indexed_table_;

  // The backing table that stores the index data.
  const Table* index_data_table_;

  // The columns declared as the index's key, in the same order
  // as they appear in the CREATE INDEX statement. References are
  // to the corresponding KeyColumn(s) in 'index_data_table_'.
  std::vector<const KeyColumn*> key_columns_;

  // Additional columns specified in the 'STORING' clause in the same
  // order as they appear in the CREATE INDEX statement. References are
  // to the corresponding columns in 'index_data_table_'.
  std::vector<const Column*> stored_columns_;

  // Whether the indexed columns form a unique key. If true, additional
  // constraints will be checked to enforce uniqueness for the Index.
  bool is_unique_ = false;

  // Whether NULL value results should be filtered out.
  bool is_null_filtered_ = false;
};

class Index::Builder {
 public:
  Builder() : instance_(absl::WrapUnique(new Index())) {}

  std::unique_ptr<const Index> build() { return std::move(instance_); }

  const Index* get() const { return instance_.get(); }

  Builder& set_name(const std::string& name) {
    instance_->name_ = name;
    return *this;
  }

  Builder& set_indexed_table(const Table* table) {
    instance_->indexed_table_ = table;
    return *this;
  }

  Builder& set_index_data_table(const Table* table) {
    instance_->index_data_table_ = table;
    return *this;
  }

  Builder& add_key_column(const KeyColumn* column) {
    instance_->key_columns_.push_back(column);
    return *this;
  }

  Builder& add_stored_column(const Column* column) {
    instance_->stored_columns_.push_back(column);
    return *this;
  }

  Builder& set_unique(bool is_unique) {
    instance_->is_unique_ = is_unique;
    return *this;
  }

  Builder& set_null_filtered(bool null_filtered) {
    instance_->is_null_filtered_ = null_filtered;
    return *this;
  }

 private:
  std::unique_ptr<Index> instance_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_INDEX_H_
