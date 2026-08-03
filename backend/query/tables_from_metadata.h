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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_TABLES_FROM_METADATA_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_TABLES_FROM_METADATA_H_

#include <memory>
#include <string>
#include <vector>

#include "googlesql/public/simple_catalog.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Maps the type specified in the information catalog metadata for a Spanner
// GoogleSQL database to a GoogleSQL type.
static const absl::NoDestructor<
    absl::flat_hash_map<std::string, const googlesql::Type*>>
    kSpannerTypeToGSQLType{{
        {"BOOL", googlesql::types::BoolType()},
        {"DATE", googlesql::types::DateType()},
        {"INT64", googlesql::types::Int64Type()},
        {"FLOAT64", googlesql::types::FloatType()},
        {"STRING(32)", googlesql::types::StringType()},
        {"STRING(100)", googlesql::types::StringType()},
        {"STRING(MAX)", googlesql::types::StringType()},
        {"TIMESTAMP", googlesql::types::TimestampType()},
        {"JSON", googlesql::types::JsonType()},
        {"ARRAY<STRING(MAX)>", googlesql::types::StringArrayType()},
    }};

// Maps the type specified in the information catalog metadata for a Spanner
// PostgreSQL database to a GoogleSQL type.
static const absl::NoDestructor<
    absl::flat_hash_map<std::string, const googlesql::Type*>>
    kSpannerPGTypeToGSQLType{{
        {"bigint", googlesql::types::Int64Type()},
        {"bigint[]", googlesql::types::Int64ArrayType()},
        {"boolean", googlesql::types::BoolType()},
        {"character varying", googlesql::types::StringType()},
        {"character varying[]", googlesql::types::StringArrayType()},
        {"double precision", googlesql::types::DoubleType()},
        {"oid", postgres_translator::spangres::datatypes::GetPgOidType()},
        {"oid[]",
         postgres_translator::spangres::datatypes::GetPgOidArrayType()},
        {"timestamp with time zone", googlesql::types::TimestampType()},
    }};

// Given a list of ColumnsMetaEntry items, returns SimpleTables that can be
// added to a SimpleCatalog mapped by the table name. The tables are created by
// mapping the spanner type in the ColumnsMetaEntry to the GoogleSQL type given
// by the provided mapping. Only tables for the given supported list of tables
// is returned. The metadata entries must be ordered by table name.
absl::flat_hash_map<std::string, std::unique_ptr<googlesql::SimpleTable>>
AddTablesFromMetadata(
    const std::vector<ColumnsMetaEntry>& metadata_entries,
    const absl::flat_hash_map<std::string, const googlesql::Type*>&
        spanner_to_gsql_type,
    const absl::flat_hash_set<std::string>& supported_tables);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_TABLES_FROM_METADATA_H_
