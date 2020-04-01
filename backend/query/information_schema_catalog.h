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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_

#include <string>
#include <vector>

#include "zetasql/public/simple_catalog.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "backend/schema/catalog/schema.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// InformationSchemaCatalog provides the INFORMATION_SCHEMA tables.
//
// ZetaSQL reference implementation accesses table data via the catalog objects
// themselves. Hence, this class also provides the data backing the infromation
// schema tables.
//
// Cloud Spanner's information schema is documented at:
//   https://cloud.google.com/spanner/docs/information-schema
//
// The emulator only exposes the default (user) schema via INFORMATION_SCHEMA.
// In production, in addition to the default schema, INFORMATION_SCHEMA and
// SPANNER_SYS schemas are also exposed.
//
// This class is tested via tests/conformance/cases/information_schema.cc
class InformationSchemaCatalog : public zetasql::SimpleCatalog {
 public:
  static constexpr char kName[] = "INFORMATION_SCHEMA";

  explicit InformationSchemaCatalog(const Schema* default_schema);

 private:
  const Schema* default_schema_;

  void AddSchemataTable();
  void AddTablesTable();
  void AddColumnsTable();
  void AddIndexesTable();
  void AddIndexColumnsTable();
  void AddColumnOptionsTable();
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFORMATION_SCHEMA_CATALOG_H_
