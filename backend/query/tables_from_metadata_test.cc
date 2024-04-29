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

#include "backend/query/tables_from_metadata.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/base/no_destructor.h"

namespace google::spanner::emulator::backend {

namespace {

TEST(AddTablesFromMetadata, EmptyMetadata) {
  std::vector<ColumnsMetaEntry> metadata;
  absl::flat_hash_map<std::string, const zetasql::Type*> type_map;
  absl::flat_hash_set<std::string> supported_tables;

  EXPECT_EQ(AddTablesFromMetadata(metadata, type_map, supported_tables).size(),
            0);
}

static const zetasql_base::NoDestructor<std::vector<ColumnsMetaEntry>> kMetadata({
    {"SCHEMATA", "CATALOG_NAME", "NO", "STRING(MAX)"},
    {"SCHEMATA", "SCHEMA_NAME", "NO", "STRING(MAX)"},
    {"SCHEMATA", "EFFECTIVE_TIMESTAMP", "YES", "INT64"},
    {"SPANNER_STATISTICS", "CATALOG_NAME", "NO", "STRING(MAX)"},
    {"SPANNER_STATISTICS", "SCHEMA_NAME", "NO", "STRING(MAX)"},
    {"SPANNER_STATISTICS", "ALLOW_GC", "NO", "BOOL"},
    {"TABLES", "TABLE_CATALOG", "NO", "STRING(MAX)"},
    {"TABLES", "TABLE_SCHEMA", "NO", "STRING(MAX)"},
    {"TABLES", "TABLE_NAME", "NO", "STRING(MAX)"},
});

static const zetasql_base::NoDestructor<
    absl::flat_hash_map<std::string, const zetasql::Type*>>
    kTypeMap{{
        {"BOOL", zetasql::types::BoolType()},
        {"INT64", zetasql::types::Int64Type()},
        {"STRING(MAX)", zetasql::types::StringType()},
    }};

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kSupportedTables{{
        "SCHEMATA",
        "SPANNER_STATISTICS",
        "TABLES",
    }};

TEST(AddTablesFromMetadata, AllTablesSupported) {
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      result = AddTablesFromMetadata(*kMetadata, *kTypeMap, *kSupportedTables);
  absl::flat_hash_set<std::string> result_table_names;
  for (auto it = result.begin(); it != result.end(); ++it) {
    result_table_names.insert(it->first);
  }
  EXPECT_THAT(result_table_names, *kSupportedTables);
}

class AddTablesFromMetadataForSomeTables
    : public testing::TestWithParam<absl::string_view> {};

TEST_P(AddTablesFromMetadataForSomeTables, SomeTablesSupported) {
  absl::flat_hash_set<std::string> supported_tables{std::string(GetParam())};
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::SimpleTable>>
      result = AddTablesFromMetadata(*kMetadata, *kTypeMap, supported_tables);
  absl::flat_hash_set<std::string> result_table_names;
  for (auto it = result.begin(); it != result.end(); ++it) {
    result_table_names.insert(it->first);
  }
  EXPECT_THAT(result_table_names, supported_tables);
}

INSTANTIATE_TEST_SUITE_P(SupportedTables, AddTablesFromMetadataForSomeTables,
                         testing::Values("SCHEMATA", "SPANNER_STATISTICS",
                                         "TABLES"));

TEST(AddTablesFromMetadata, InvalidMetadata) {
  std::vector<ColumnsMetaEntry> metadata{
      {"SCHEMATA", "CATALOG_NAME", "NO", "STRING(MAX)"},
      {"SCHEMATA", "SCHEMA_NAME", "NO", "STRING(MAX)"},
      {"SPANNER_STATISTICS", "CATALOG_NAME", "NO", "STRING(MAX)"},
      {"SPANNER_STATISTICS", "SCHEMA_NAME", "NO", "STRING(MAX)"},
      {"SCHEMATA", "EFFECTIVE_TIMESTAMP", "YES", "INT64"},
      {"SPANNER_STATISTICS", "ALLOW_GC", "NO", "BOOL"},
  };
  ASSERT_DEATH(AddTablesFromMetadata(metadata, *kTypeMap, *kSupportedTables),
               "invalid metadata");
}

}  // namespace

}  // namespace google::spanner::emulator::backend
