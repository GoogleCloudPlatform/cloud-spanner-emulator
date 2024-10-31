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

#include "backend/schema/catalog/named_schema.h"

#include <string>
#include <vector>

#include "absl/status/status.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/udf.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_graph_editor.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status NamedSchema::Validate(SchemaValidationContext* context) const {
  return validate_(this, context);
}

absl::Status NamedSchema::ValidateUpdate(
    const SchemaNode* orig, SchemaValidationContext* context) const {
  return validate_update_(this, orig->template As<const NamedSchema>(),
                          context);
}

template <typename T>
absl::Status NamedSchema::CloneOrDeleteSchemaObjects(
    SchemaGraphEditor* editor, std::vector<const T*>& schema_objects,
    CaseInsensitiveStringMap<const T*>& schema_objects_map) {
  return editor->CloneOrDeleteSchemaObjectsNameMappings(schema_objects,
                                                        schema_objects_map);
}

absl::Status NamedSchema::CloneOrDeleteSchemaObjects(
    SchemaGraphEditor* editor, std::vector<const Table*>& schema_objects,
    CaseInsensitiveStringMap<const Table*>& schema_objects_map) {
  ZETASQL_RETURN_IF_ERROR(editor->CloneOrDeleteSchemaObjectsNameMappings(
      schema_objects, schema_objects_map));

  // Tables must also handle synonyms.
  for (auto it = synonyms_.begin(); it != synonyms_.end();) {
    ZETASQL_ASSIGN_OR_RETURN(const auto* schema_node, editor->Clone(*it));
    if (schema_node->is_deleted()) {
      synonyms_map_.erase((*it)->synonym());
      it = synonyms_.erase(it);
    } else {
      const Table* cloned_object = schema_node->As<const Table>();
      *it = cloned_object;
      synonyms_map_[cloned_object->synonym()] = cloned_object;
      ++it;
    }
  }

  return absl::OkStatus();
}

absl::Status NamedSchema::DeepClone(SchemaGraphEditor* editor,
                                    const SchemaNode* orig) {
  ZETASQL_RETURN_IF_ERROR(CloneOrDeleteSchemaObjects(editor, tables_, tables_map_));
  ZETASQL_RETURN_IF_ERROR(CloneOrDeleteSchemaObjects(editor, views_, views_map_));
  ZETASQL_RETURN_IF_ERROR(
      CloneOrDeleteSchemaObjects(editor, sequences_, sequences_map_));
  ZETASQL_RETURN_IF_ERROR(CloneOrDeleteSchemaObjects(editor, indexes_, indexes_map_));
  ZETASQL_RETURN_IF_ERROR(CloneOrDeleteSchemaObjects(editor, udfs_, udfs_map_));

  return absl::OkStatus();
}

const Table* NamedSchema::FindTable(const std::string& table_name) const {
  auto itr = tables_map_.find(table_name);
  if (itr == tables_map_.end()) {
    return FindTableUsingSynonym(table_name);
  }
  return itr->second;
}

const Table* NamedSchema::FindTableUsingSynonym(
    const std::string& table_synonym) const {
  auto itr = synonyms_map_.find(table_synonym);
  if (itr == synonyms_map_.end()) {
    return nullptr;
  }
  return itr->second;
};

const View* NamedSchema::FindView(const std::string& view_name) const {
  auto itr = views_map_.find(view_name);
  if (itr == views_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Sequence* NamedSchema::FindSequence(
    const std::string& sequence_name) const {
  auto itr = sequences_map_.find(sequence_name);
  if (itr == sequences_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Index* NamedSchema::FindIndex(const std::string& index_name) const {
  auto itr = indexes_map_.find(index_name);
  if (itr == indexes_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

const Udf* NamedSchema::FindUdf(const std::string& udf_name) const {
  auto itr = udfs_map_.find(udf_name);
  if (itr == udfs_map_.end()) {
    return nullptr;
  }
  return itr->second;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
