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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "tests/conformance/common/database_test_base.h"
#include "zetasql/base/no_destructor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

static const zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kSupportedButEmptyTables{{
        "pg_available_extension_versions",
        "pg_available_extensions",
        "pg_backend_memory_contexts",
        "pg_config",
        "pg_cursors",
        "pg_file_settings",
        "pg_hba_file_rules",
        "pg_matviews",
        "pg_policies",
        "pg_prepared_xacts",
        "pg_publication_tables",
        "pg_rules",
        "pg_shmem_allocations",
    }};

class PGCatalogTest : public DatabaseTest {
 public:
  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

 protected:
  absl::Status PopulateDatabase() { return absl::OkStatus(); }
};

TEST_F(PGCatalogTest, PGTables) {
  EXPECT_THAT(Query(R"sql(SELECT schemaname, tablename, hasindexes
                          FROM pg_catalog.pg_tables)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(PGCatalogTest, PGViews) {
  EXPECT_THAT(Query(R"sql(SELECT schemaname, viewname, definition
                          FROM pg_catalog.pg_views)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(PGCatalogTest, PGIndexes) {
  EXPECT_THAT(Query(R"sql(SELECT schemaname, tablename, indexname
                          FROM pg_catalog.pg_indexes)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(PGCatalogTest, PGSettings) {
  auto expected = std::vector<ValueRow>({
      {"max_index_keys", "16", "Preset Options",
       "Shows the maximum number of index keys.", "internal", "integer",
       "default", "16", "16", "16", "16", false},  // NOLINT
  });
  EXPECT_THAT(Query(R"sql(
      SELECT
        name,
        setting,
        category,
        short_desc,
        context,
        vartype,
        source,
        min_val,
        max_val,
        boot_val,
        reset_val,
        pending_restart
      FROM
        pg_catalog.pg_settings)sql"),
              IsOkAndHoldsRows(expected));
}

TEST_F(PGCatalogTest, SupportedButEmptyTables) {
  for (const auto& table_name : *kSupportedButEmptyTables) {
    EXPECT_THAT(Query(absl::Substitute(R"sql(SELECT * FROM pg_catalog.$0)sql",
                                       table_name)),
                IsOkAndHoldsRows({}));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
