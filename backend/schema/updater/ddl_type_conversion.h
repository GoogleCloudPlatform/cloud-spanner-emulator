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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_DDL_TYPE_CONVERSION_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_DDL_TYPE_CONVERSION_H_

#include "zetasql/public/type.h"
#include "zetasql/base/statusor.h"
#include "backend/schema/ddl/operations.pb.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Converts the ColumnType in DDL statements to zetasql::Type.
zetasql_base::StatusOr<const zetasql::Type*> DDLColumnTypeToGoogleSqlType(
    const ddl::ColumnType& ddl_type, zetasql::TypeFactory* type_factory);

// Converts zetasql::Type to its equivalent DDL ColumnType. Returns a
// ddl::ColumnType::UNKNOWN_TYPE type if the passed in googlesql type is not a
// recognized type. REQUIRES: type!=nullptr.
ddl::ColumnType GoogleSqlTypeToDDLColumnType(const zetasql::Type* type);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_UPDATER_DDL_TYPE_CONVERSION_H_
