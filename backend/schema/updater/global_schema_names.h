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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_GLOBAL_SCHEMA_NAMES_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_GLOBAL_SCHEMA_NAMES_H_

#include "zetasql/base/statusor.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/schema.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Manages global schema names during a schema update. Includes only names of
// top-level schema objects, such as the names of tables.
class GlobalSchemaNames {
 public:
  GlobalSchemaNames() = default;
  ~GlobalSchemaNames() = default;
  GlobalSchemaNames(const GlobalSchemaNames&) = default;
  GlobalSchemaNames& operator=(const GlobalSchemaNames&) = default;
  GlobalSchemaNames(GlobalSchemaNames&&) = default;
  GlobalSchemaNames& operator=(GlobalSchemaNames&&) = default;

  // Returns true if a global name exists.
  bool HasName(const std::string& name) const { return names_.contains(name); }

  // Adds a new global schema name. Returns an error if the name already exists.
  absl::Status AddName(absl::string_view type, const std::string& name);

  // Removes a name if it exists; does nothing otherwise.
  void RemoveName(const std::string& name) { names_.erase(name); }

  // Generates and adds a unique check constraint name. If a gererated name
  // already exists, its sequence number is increased until a unique name is
  // found.
  zetasql_base::StatusOr<std::string> GenerateCheckConstraintName(
      absl::string_view table_name);

  // Generates and adds a unique foreign key name. If a gererated name already
  // exists, its sequence number is increased until a unique name is found.
  zetasql_base::StatusOr<std::string> GenerateForeignKeyName(
      absl::string_view referencing_table_name,
      absl::string_view referenced_table_name);

  // Generates a unique name for a managed index. The same name is always
  // generated for the same arguments. Callers can then reuse an existing index
  // with the same name, if any, or create a new index.
  static zetasql_base::StatusOr<std::string> GenerateManagedIndexName(
      absl::string_view table_name,
      const std::vector<std::string>& column_names, bool null_filtered,
      bool unique);

  // Validates a schema name.
  static absl::Status ValidateSchemaName(absl::string_view type,
                                         absl::string_view name);

  // Validates a schema constraint name.
  static absl::Status ValidateConstraintName(absl::string_view table_name,
                                             absl::string_view constraint_type,
                                             absl::string_view constraint_name);

 private:
  // Generates and adds a unique name. A sequence number is incremented and
  // added to the name until a unique name is found.
  std::string GenerateSequencedName(absl::string_view type,
                                    absl::string_view base,
                                    absl::string_view fingerprint);

  // Unique set of global names.
  CaseInsensitiveStringSet names_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_GLOBAL_SCHEMA_NAMES_H_
