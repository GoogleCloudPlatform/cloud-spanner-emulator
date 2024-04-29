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
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "google/cloud/spanner/value.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "re2/re2.h"

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
  PGCatalogTest() : feature_flags_({.enable_postgresql_interface = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("information_schema.test");
  }

  cloud::spanner::Value Ns() { return Null<std::string>(); }
  cloud::spanner::Value Nb() { return Null<bool>(); }

  // Returns the given rows, replacing matching string patterns with their
  // actual values from the given results.
  static std::vector<ValueRow> ExpectedRows(
      const absl::StatusOr<std::vector<ValueRow>>& results,
      const std::vector<ValueRow> rows) {
    if (!results.ok()) {
      return rows;
    }
    std::vector<ValueRow> expected;
    for (const ValueRow& row : rows) {
      ValueRow next;
      for (int i = 0; i < row.values().size(); ++i) {
        Value value = row.values()[i];
        if (value.get<std::string>().ok()) {
          std::string pattern = value.get<std::string>().value();
          value = Value(FindString(results, i, pattern));
        }
        next.add(value);
      }
      expected.push_back(next);
    }
    return expected;
  }

  // Returns the first result string that matches a pattern. Returns the pattern
  // if none match. One use case is to match generated names that have
  // different signatures between production and emulator.
  static std::string FindString(
      const absl::StatusOr<std::vector<ValueRow>>& results, int field_index,
      const std::string& pattern) {
    for (const auto& row : results.value()) {
      auto value = row.values()[field_index].get<std::string>().value();
      if (RE2::FullMatch(value, pattern)) {
        return value;
      }
    }
    return pattern;
  }

 protected:
  absl::Status PopulateDatabase() { return absl::OkStatus(); }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGCatalogTest, PGTables) {
  auto expected = std::vector<ValueRow>({
      {"public", "base", Ns(), Ns(), true, Nb(), Nb(), Nb()},
      {"public", "cascade_child", Ns(), Ns(), true, Nb(), Nb(), Nb()},
      {"public", "no_action_child", Ns(), Ns(), true, Nb(), Nb(), Nb()},
      {"public", "row_deletion_policy", Ns(), Ns(), false, Nb(), Nb(), Nb()},
  });
  EXPECT_THAT(Query(R"sql(
      SELECT
        schemaname,
        tablename,
        tableowner,
        tablespace,
        hasindexes,
        hasrules,
        hastriggers,
        rowsecurity
      FROM
        pg_catalog.pg_tables
      ORDER BY
        tablename)sql"),
              IsOkAndHoldsRows({expected}));
}

TEST_F(PGCatalogTest, PGViews) {
  auto expected = std::vector<ValueRow>(
      {{"public", "base_view", Ns(), "SELECT key1 FROM base"}});
  EXPECT_THAT(Query(R"sql(SELECT schemaname, viewname, viewowner, definition
                          FROM pg_catalog.pg_views)sql"),
              IsOkAndHoldsRows({expected}));
}

TEST_F(PGCatalogTest, PGIndexes) {
  auto results = Query(R"sql(
      SELECT
        schemaname,
        tablename,
        indexname,
        tablespace,
        indexdef
      FROM
        pg_catalog.pg_indexes
      ORDER BY
        tablename,
        indexname)sql");
  // NOLINTBEGIN
  EXPECT_THAT(
      *results,
      ExpectedRows(
          *results,
          {{"public", "base", "IDX_base_bool_value_key2_N_\\w{16}", Ns(), Ns()},
           {"public", "base", "PK_base", Ns(), Ns()},
           {"public", "cascade_child",
            "IDX_cascade_child_child_key_value1_U_\\w{16}", Ns(), Ns()},
           {"public", "cascade_child", "PK_cascade_child", Ns(), Ns()},
           {"public", "cascade_child", "cascade_child_by_value", Ns(), Ns()},
           {"public", "no_action_child", "PK_no_action_child", Ns(), Ns()},
           {"public", "no_action_child", "no_action_child_by_value", Ns(),
            Ns()},
           {"public", "row_deletion_policy", "PK_row_deletion_policy", Ns(),
            Ns()}}));
  // NOLINTEND
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
