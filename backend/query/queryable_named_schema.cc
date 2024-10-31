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

#include "backend/query/queryable_named_schema.h"

#include <string>

#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/named_schema.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

QueryableNamedSchema::QueryableNamedSchema(
    const backend::NamedSchema* backend_named_schema) {
  wrapped_named_schema_ = backend_named_schema;
}

absl::Status QueryableNamedSchema::GetTable(const std::string& name,
                                            const zetasql::Table** table,
                                            const FindOptions& options) {
  *table = nullptr;
  if (auto it = views_.find(name); it != views_.end()) {
    *table = it->second.get();
    return absl::OkStatus();
  }

  if (auto it = tables_.find(name); it != tables_.end()) {
    *table = it->second.get();
    return absl::OkStatus();
  }

  return error::TableNotFound(name);
}

absl::Status QueryableNamedSchema::GetModel(const std::string& name,
                                            const zetasql::Model** model,
                                            const FindOptions& options) {
  *model = nullptr;
  if (auto it = models_.find(name); it != models_.end()) {
    *model = it->second.get();
    return absl::OkStatus();
  }
  return error::ModelNotFound(name);
}

absl::Status QueryableNamedSchema::GetSequence(
    const std::string& name, const zetasql::Sequence** sequence,
    const FindOptions& options) {
  *sequence = nullptr;
  if (auto it = sequences_.find(name); it != sequences_.end()) {
    *sequence = it->second.get();
    return absl::OkStatus();
  }
  return error::SequenceNotFound(name);
}

absl::Status QueryableNamedSchema::GetFunction(
    const std::string& name, const zetasql::Function** function,
    const FindOptions& options) {
  *function = nullptr;
  if (auto it = udfs_.find(name); it != udfs_.end()) {
    *function = it->second.get();
    return absl::OkStatus();
  }
  return error::FunctionNotFound(name);
}

// Should not be called but is required per the zetasql::Catalog interface.
absl::Status QueryableNamedSchema::GetType(const std::string& name,
                                           const zetasql::Type** type,
                                           const FindOptions& options) {
  return error::TypeNotFound(name);
}

absl::Status QueryableNamedSchema::GetTables(
    absl::flat_hash_set<const zetasql::Table*>* output) const {
  for (auto iter = tables_.begin(); iter != tables_.end(); ++iter) {
    output->insert(iter->second.get());
  }
  for (auto iter = views_.begin(); iter != views_.end(); ++iter) {
    output->insert(iter->second.get());
  }
  return absl::OkStatus();
}

absl::Status QueryableNamedSchema::GetFunctions(
    absl::flat_hash_set<const zetasql::Function*>* output) const {
  function_catalog_->GetFunctions(output);
  return absl::OkStatus();
}

absl::Status QueryableNamedSchema::GetCatalogs(
    absl::flat_hash_set<const zetasql::Catalog*>* output) const {
  return absl::OkStatus();
}

absl::Status QueryableNamedSchema::GetTypes(
    absl::flat_hash_set<const zetasql::Type*>* output) const {
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
