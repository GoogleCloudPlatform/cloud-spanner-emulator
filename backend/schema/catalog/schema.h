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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/change_stream.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/model.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_graph.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace database_api = ::google::spanner::admin::database::v1;

// Schema represents a single generation of schema of a database. It is an
// abstract class that provides a convenient interface for looking up schema
// objects. A Schema doesn't take ownership of the schema objects. That is
// managed by the SchemaGraph passed to it and which must outlive the
// Schema instance.
class Schema {
 public:
  Schema()
      : graph_(SchemaGraph::CreateEmpty())
        ,
        dialect_(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {}

  explicit Schema(const SchemaGraph* graph
                  ,
                  const database_api::DatabaseDialect& dialect);

  virtual ~Schema() = default;

  // Returns the generation number of this schema.
  int64_t generation() const { return generation_; }

  // Dumps the schema to ddl::DDLStatementList.
  ddl::DDLStatementList Dump() const;

  // Finds a view by its name. Returns a const pointer of the view, or
  // nullptr if the view is not found. Name comparison is case-insensitive.
  const View* FindView(const std::string& view_name) const;

  // Same as FindView but case-sensitive.
  const View* FindViewCaseSensitive(const std::string& view_name) const;

  // Finds a table by its name. Returns a const pointer of the table, or nullptr
  // if the table is not found. Name comparison is case-insensitive.
  const Table* FindTable(const std::string& table_name) const;

  // Same as FindTable but case-sensitive.
  const Table* FindTableCaseSensitive(const std::string& table_name) const;

  // Finds an index by its name. Returns a const pointer of the index, or
  // nullptr if the index is not found. Name comparison is case-insensitive.
  const Index* FindIndex(const std::string& index_name) const;

  // Same as FindIndex but case-sensitive.
  const Index* FindIndexCaseSensitive(const std::string& index_name) const;

  // Finds a change stream by its name. Returns a const pointer of the change
  // stream, or nullptr if the change stream is not found. Name comparison is
  // case-insensitive.
  const ChangeStream* FindChangeStream(
      const std::string& change_stream_name) const;

  // Finds a sequence by its name. Returns a const pointer of the sequence, or
  // a nullptr if the sequence is not found. Name comparison is
  // case-insensitive.
  const Sequence* FindSequence(const std::string& sequence_name) const;

  // Finds a model by its name. Returns a const pointer of the model,
  // or nullptr if the change stream is not found. Name comparison is
  // case-insensitive.
  const Model* FindModel(const std::string& model_name) const;

  // List all the user-visible tables in this schema.
  absl::Span<const Table* const> tables() const { return tables_; }

  absl::Span<const View* const> views() const { return views_; }

  // List all the user-visible change streams in this schema.
  absl::Span<const ChangeStream* const> change_streams() const {
    return change_streams_;
  }

  // List all the user-visible sequences in this schema.
  absl::Span<const Sequence* const> sequences() const { return sequences_; }

  // List all the user-visible models in this schema.
  absl::Span<const Model* const> models() const { return models_; }

  // Return the underlying SchemaGraph owning the objects in the schema.
  const SchemaGraph* GetSchemaGraph() const { return graph_; }

  // Returns the number of indices in the schema.
  int num_index() const { return index_map_.size(); }

  // Returns the number of change streams in the schema.
  int num_change_stream() const { return change_streams_map_.size(); }

  // Returns the number of sequences in the schema.
  int num_sequence() const { return sequences_map_.size(); }

  // Returns the database dialect.
  database_api::DatabaseDialect dialect() const { return dialect_; }

 private:
  // Tries to find the managed index from the non-fingerprint part of the
  // index name.
  const Index* FindManagedIndex(const std::string& index_name) const;

  // Manages the lifetime of all schema objects. Maintains the order
  // in which the nodes were added to the graph. Must outlive *this.
  const SchemaGraph* graph_;

  // The generation number of this schema.
  int64_t generation_ = 0;

  // A vector that maintains the original order of tables in the DDL.
  std::vector<const Table*> tables_;

  // A map that owns all the views. Key is the name of the view. Hash and
  // comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const View*> views_map_;

  // A vector that maintains the original order of views in the DDL.
  std::vector<const View*> views_;

  // A map that owns all the tables. Key is the name of the tables. Hash and
  // comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Table*> tables_map_;

  // A vector that maintains the original order of change streams in the DDL.
  std::vector<const ChangeStream*> change_streams_;

  // A map that owns all the change streams. Key is the name of the change
  // stream. Hash and comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const ChangeStream*> change_streams_map_;

  // A vector that maintains the original order of models in the DDL.
  std::vector<const Model*> models_;

  // A map that owns all the models. Key is the name of the model.
  // Hash and comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Model*> models_map_;

  // A map that owns all of the indexes. Key is the name of the index. Hash and
  // comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Index*> index_map_;

  // A vector that maintains the original order of sequences in the DDL.
  std::vector<const Sequence*> sequences_;

  // A map that owns all the sequences. Key is the name of the sequence. Hash
  // and comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Sequence*> sequences_map_;

  // Holds the database dialect for this schema.
  const database_api::DatabaseDialect dialect_;
};

// A Schema that also owns the SchemaGraph that manages the lifetime of the
// schema nodes.
class OwningSchema : public Schema {
 public:
  explicit OwningSchema(std::unique_ptr<const SchemaGraph> graph
                        ,
                        const database_api::DatabaseDialect& dialect)
      : Schema(graph.get()
               ,
               dialect),
        graph_(std::move(graph)) {}

  ~OwningSchema() override = default;

 private:
  std::unique_ptr<const SchemaGraph> graph_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_SCHEMA_H_
