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

#include "backend/schema/validators/view_validator.h"

#include <ctime>
#include <memory>
#include <stack>
#include <string>
#include <vector>

#include "zetasql/public/options.pb.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/container/flat_hash_map.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "backend/common/case.h"
#include "backend/common/graph_dependency_helper.h"
#include "backend/datamodel/types.h"
#include "backend/query/analyzer_options.h"
#include "backend/query/catalog.h"
#include "backend/query/queryable_view.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/foreign_key.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/sql_expression_validators.h"
#include "common/errors.h"
#include "common/limits.h"
#include "zetasql/base/ret_check.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::flat_hash_set<const SchemaNode*> GatherTransitiveDependenciesForView(
    const absl::flat_hash_set<const SchemaNode*>& initial_set) {
  absl::flat_hash_set<const SchemaNode*> transitive;
  std::stack<const SchemaNode*> explore;

  for (const auto& dep : initial_set) {
    transitive.insert(dep);
    explore.push(dep);
  }

  while (!explore.empty()) {
    auto dep = explore.top();
    explore.pop();
    // Only explore dependencies that are views.
    if (auto view = dynamic_cast<const View*>(dep); view != nullptr) {
      for (const auto& dependency : view->dependencies()) {
        if (transitive.insert(dependency).second) explore.push(dependency);
      }
    }
  }
  return transitive;
}

namespace {

absl::Status ValidateViewSignatureChange(absl::string_view modify_action,
                                         absl::string_view dependency_name,
                                         const View* dependent_view,
                                         const Schema* temp_new_schema,
                                         zetasql::TypeFactory* type_factory) {
  // Re-analyze the dependent view based on the new definition of the dependency
  // in the temporary new schema.
  std::vector<View::Column> new_columns;
  absl::flat_hash_set<const SchemaNode*> unused_new_deps;
  auto status = AnalyzeViewDefinition(
      dependent_view->Name(), dependent_view->body(), temp_new_schema,
      type_factory, &new_columns, &unused_new_deps);
  if (!status.ok()) {
    return error::DependentViewBecomesInvalid(modify_action, dependency_name,
                                              dependent_view->Name(),
                                              status.message());
  }
  // The number of columns of the dependent view should not change after
  // re-analysis.
  ZETASQL_RET_CHECK_EQ(new_columns.size(), dependent_view->columns().size());
  auto original_columns = dependent_view->columns();
  for (int i = 0; i < new_columns.size(); i++) {
    if (!absl::EqualsIgnoreCase(original_columns[i].name,
                                new_columns[i].name)) {
      return error::DependentViewColumnRename(
          modify_action, dependency_name, dependent_view->Name(),
          original_columns[i].name, new_columns[i].name);
    }
    if (original_columns[i].type != new_columns[i].type) {
      return error::DependentViewColumnRetype(
          modify_action, dependency_name, dependent_view->Name(),
          original_columns[i].type->TypeName(zetasql::PRODUCT_EXTERNAL),
          new_columns[i].type->TypeName(zetasql::PRODUCT_EXTERNAL));
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ViewValidator::Validate(const View* view,
                                     SchemaValidationContext* context) {
  ZETASQL_RET_CHECK(!view->name_.empty());

  return absl::OkStatus();
}

absl::Status ViewValidator::ValidateUpdate(const View* view,
                                           const View* old_view,
                                           SchemaValidationContext* context) {
  // Name should not change during cloning.
  ZETASQL_RET_CHECK_EQ(view->Name(), old_view->Name());
  if (view->is_deleted()) {
    context->global_names()->RemoveName(view->Name());
    return absl::OkStatus();
  }

  for (const SchemaNode* dependency : view->dependencies()) {
    // Cannot drop dependencies of the view.
    if (dependency->is_deleted()) {
      // If the deleted dependency is a column that was marked as deleted
      // as a result of a table drop, then we use the table to report the error
      // message.
      if (auto dep_column = dependency->As<const Column>();
          dep_column != nullptr) {
        auto dep_table = dep_column->table();
        if (dep_table->is_deleted()) {
          dependency = dep_table;
        }
      }
      return error::InvalidDropDependentViews(
          dependency->GetSchemaNameInfo().value().kind,
          dependency->GetSchemaNameInfo().value().name, view->Name());
    }

    // If a dependency was updated during the schema change then we need to
    // re-analyze *this.
    if (context->IsModifiedNode(dependency)) {
      std::string modify_action, dependency_name;
      if (auto dep_view = dependency->As<const View>(); dep_view != nullptr) {
        dependency_name = dep_view->Name();
        modify_action = "alter view";
      }
      if (auto dep_table = dependency->As<const Table>();
          dep_table != nullptr) {
        dependency_name = dep_table->Name();
        modify_action = "alter table";
      }
      if (auto dep_column = dependency->As<const Column>();
          dep_column != nullptr) {
        dependency_name = dep_column->FullName();
        modify_action = "alter column";
      }
      ZETASQL_RETURN_IF_ERROR(ValidateViewSignatureChange(
          modify_action, dependency_name, view, context->tmp_new_schema(),
          context->type_factory()));
    }
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
