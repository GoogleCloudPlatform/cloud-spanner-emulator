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
#include <vector>

#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_graph.h"
#include "backend/schema/graph/schema_node.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Schema represents a single generation of schema of a database. It is an
// abstract class that provides a convenient interface for looking up schema
// objects. A Schema doesn't take ownership of the schema objects. That is
// managed by the SchemaGraph passed to it and which must outlive the
// Schema instance.
class Schema {
 public:
  Schema()
      : graph_(SchemaGraph::CreateEmpty())
  {}

  explicit Schema(const SchemaGraph* graph
  );

  virtual ~Schema() = default;

  // Returns the generation number of this schema.
  int64_t generation() const { return generation_; }

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

  // List all the user-visible tables in this schema.
  absl::Span<const Table* const> tables() const { return tables_; }
  absl::Span<const View* const> views() const { return views_; }

  // Return the underlying SchemaGraph owning the objects in the schema.
  const SchemaGraph* GetSchemaGraph() const { return graph_; }

  // Returns the number of indices in the schema.
  int num_index() const { return index_map_.size(); }

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

  // A map that owns all of the indexes. Key is the name of the index. Hash and
  // comparison on the keys are case-insensitive.
  CaseInsensitiveStringMap<const Index*> index_map_;
};

// A Schema that also owns the SchemaGraph that manages the lifetime of the
// schema nodes.
class OwningSchema : public Schema {
 public:
  explicit OwningSchema(std::unique_ptr<const SchemaGraph> graph
                        )
      : Schema(graph.get()
               ),
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
