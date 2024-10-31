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

#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "google/cloud/spanner/oid.h"
#include "google/cloud/spanner/value.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::google::cloud::spanner::PgOid;

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
  cloud::spanner::Value Ni64() { return Null<int64_t>(); }
  cloud::spanner::Value Nd() { return Null<double>(); }
  cloud::spanner::Value NOid() { return Null<PgOid>(); }
  cloud::spanner::Value Ni64Array() { return Null<std::vector<int64_t>>(); }
  cloud::spanner::Value NOidArray() { return Null<std::vector<PgOid>>(); }

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

TEST_F(PGCatalogTest, PGAm) {
  auto expected =
      std::vector<ValueRow>({{PgOid(75001), "spanner_default", "t"},
                             {PgOid(75002), "spanner_default", "i"}});
  EXPECT_THAT(Query(R"sql(
      SELECT
        oid,
        amname,
        amtype
      FROM
        pg_catalog.pg_am
      ORDER BY
        amtype DESC)sql"),
              IsOkAndHoldsRows({expected}));
}

TEST_F(PGCatalogTest, PGAttrdef) {
  // Oid assignment differs from production so we cannot assert those.
  auto expected = std::vector<ValueRow>({
      {19, "(key1 + '1'::bigint)"},
      {20, "length(key2)"},
      {21, "'100'::bigint"},
      {22, "CURRENT_TIMESTAMP"},
  });
  EXPECT_THAT(Query(R"sql(
      SELECT
        adnum,
        adbin
      FROM
        pg_catalog.pg_attrdef
      ORDER BY
        oid)sql"),
              IsOkAndHoldsRows(expected));

  // Instead, assert that the distinct OID counts.
  EXPECT_THAT(Query(R"sql(
      SELECT
        COUNT(DISTINCT oid),
        COUNT(DISTINCT adrelid)
      FROM
        pg_catalog.pg_attrdef)sql"),
              IsOkAndHoldsRows({{4, 1}}));
}

TEST_F(PGCatalogTest, PGClass) {
  constexpr absl::string_view query_template = R"sql(
      SELECT
        relname,
        nspname,
        relam,
        relhasindex,
        relpersistence,
        relkind,
        relchecks,
        relispopulated
      FROM
        pg_catalog.pg_class AS c
      JOIN
        pg_catalog.pg_namespace AS n
      ON
        c.relnamespace = n.oid
      WHERE
        relkind = '%s' AND relnamespace != 11 AND relnamespace != 75003
        AND relnamespace != 75004
      ORDER BY
        relnamespace, relname)sql";

  auto sequence_results = Query(absl::StrFormat(query_template, "S"));
  auto expected_sequence_rows = std::vector<ValueRow>({
      {"test_sequence", "public", PgOid(0), false, "p", "S", 0, true},
  });
  ZETASQL_EXPECT_OK(sequence_results);
  EXPECT_THAT(*sequence_results,
              ExpectedRows(*sequence_results, expected_sequence_rows));

  auto index_results = Query(absl::StrFormat(query_template, "i"));
  auto expected_index_rows = std::vector<ValueRow>({
      {"IDX_base_bool_value_key2_N_\\w{16}", "public", PgOid(75002), false, "p",
       "i", 0, true},
      {"IDX_cascade_child_child_key_value1_U_\\w{16}", "public", PgOid(75002),
       false, "p", "i", 0, true},
      {"PK_base", "public", PgOid(75002), false, "p", "i", 0, true},
      {"PK_cascade_child", "public", PgOid(75002), false, "p", "i", 0, true},
      {"PK_no_action_child", "public", PgOid(75002), false, "p", "i", 0, true},
      {"PK_row_deletion_policy", "public", PgOid(75002), false, "p", "i", 0,
       true},
      {"cascade_child_by_value", "public", PgOid(75002), false, "p", "i", 0,
       true},
      {"no_action_child_by_value", "public", PgOid(75002), false, "p", "i", 0,
       true},

      {"PK_ns_table_1", "named_schema", PgOid(75002), false, "p", "i", 0, true},
      {"PK_ns_table_2", "named_schema", PgOid(75002), false, "p", "i", 0, true},
      {"ns_index", "named_schema", PgOid(75002), false, "p", "i", 0, true},
  });
  ZETASQL_EXPECT_OK(index_results);
  EXPECT_THAT(*index_results,
              ExpectedRows(*index_results, expected_index_rows));

  auto table_results = Query(absl::StrFormat(query_template, "r"));
  auto expected_table_rows = std::vector<ValueRow>({
      {"base", "public", PgOid(75001), true, "p", "r", 2, true},
      {"cascade_child", "public", PgOid(75001), true, "p", "r", 0, true},
      {"no_action_child", "public", PgOid(75001), true, "p", "r", 0, true},
      {"row_deletion_policy", "public", PgOid(75001), false, "p", "r", 0, true},

      {"ns_table_1", "named_schema", PgOid(75001), true, "p", "r", 0, true},
      {"ns_table_2", "named_schema", PgOid(75001), false, "p", "r", 0, true},
  });
  ZETASQL_EXPECT_OK(table_results);
  EXPECT_THAT(*table_results,
              ExpectedRows(*table_results, expected_table_rows));

  auto view_results = Query(absl::StrFormat(query_template, "v"));
  auto expected_view_rows = std::vector<ValueRow>({
      {"base_view", "public", PgOid(0), false, "p", "v", 0, true},
      {"ns_view", "named_schema", PgOid(0), false, "p", "v", 0, true},
  });
  ZETASQL_EXPECT_OK(view_results);
  EXPECT_THAT(*view_results, ExpectedRows(*view_results, expected_view_rows));

  // Assert oids are distinct.
  EXPECT_THAT(Query(R"sql(
      SELECT
        COUNT(DISTINCT oid) = COUNT(1)
      FROM
        pg_catalog.pg_class)sql"),
              IsOkAndHoldsRows({{true}}));

  // Check empty rows are empty.
  EXPECT_THAT(Query(R"sql(
      SELECT
        reltype,
        reloftype,
        relowner,
        relfilenode,
        reltablespace,
        relpages,
        reltuples,
        relallvisible,
        reltoastrelid,
        relisshared,
        relhasrules,
        relhastriggers,
        relhassubclass,
        relrowsecurity,
        relforcerowsecurity,
        relreplident,
        relispartition,
        relrewrite,
        relfrozenxid,
        relminmxid,
        reloptions,
        relpartbound
      FROM
        pg_catalog.pg_class
      GROUP BY
        reltype,
        reloftype,
        relowner,
        relfilenode,
        reltablespace,
        relpages,
        reltuples,
        relallvisible,
        reltoastrelid,
        relisshared,
        relhasrules,
        relhastriggers,
        relhassubclass,
        relrowsecurity,
        relforcerowsecurity,
        relreplident,
        relispartition,
        relrewrite,
        relfrozenxid,
        relminmxid,
        reloptions,
        relpartbound)sql"),
              IsOkAndHoldsRows({{NOid(), NOid(), NOid(), NOid(), NOid(), Ni64(),
                                 Nd(),   Ni64(), NOid(), Nb(),   Nb(),   Nb(),
                                 Nb(),   Nb(),   Nb(),   Ns(),   Nb(),   NOid(),
                                 Ni64(), Ni64(), Ns(),   Ns()}}));
};

TEST_F(PGCatalogTest, PGIndex) {
  auto expected = std::vector<ValueRow>({
      {"IDX_base_bool_value_key2_N_\\w{16}",
       "base",
       2,
       2,
       false,
       false,
       false,
       Nb(),
       false,
       true,
       false,
       true,
       true,
       false,
       std::vector<int>{3, 2},
       NOidArray(),
       NOidArray(),
       Ni64Array(),
       Ns(),
       Ns()},
      {"IDX_cascade_child_child_key_value1_U_\\w{16}",
       "cascade_child",
       2,
       2,
       true,
       false,
       false,
       Nb(),
       false,
       true,
       false,
       true,
       true,
       false,
       std::vector<int>{3, 4},
       NOidArray(),
       NOidArray(),
       Ni64Array(),
       Ns(),
       Ns()},
      {"cascade_child_by_value",
       "cascade_child",
       4,
       3,
       true,
       false,
       false,
       Nb(),
       false,
       true,
       false,
       true,
       true,
       false,
       std::vector<int>{1, 2, 5, 4},
       NOidArray(),
       NOidArray(),
       Ni64Array(),
       Ns(),
       Ns()},
      {"no_action_child_by_value",
       "no_action_child",
       1,
       1,
       false,
       false,
       false,
       Nb(),
       false,
       true,
       false,
       true,
       true,
       false,
       std::vector<int>{4},
       NOidArray(),
       NOidArray(),
       Ni64Array(),
       Ns(),
       Ns()},
      {"ns_index",  "ns_table_1", 1,           1,     true,
       false,       false,        Nb(),        false, true,
       false,       true,         true,        false, std::vector<int>{1},
       NOidArray(), NOidArray(),  Ni64Array(), Ns(),  Ns()},
  });
  auto results = Query(R"sql(
      SELECT
        c.relname,
        t.relname,
        indnatts,
        indnkeyatts,
        indisunique,
        indisprimary,
        indisexclusion,
        indimmediate,
        indisclustered,
        indisvalid,
        indcheckxmin,
        indisready,
        indislive,
        indisreplident,
        indkey,
        indcollation,
        indclass,
        indoption,
        indexprs,
        indpred
      FROM
        pg_catalog.pg_index as i
      JOIN pg_catalog.pg_class as c ON i.indexrelid = c.oid
      JOIN pg_catalog.pg_class as t ON i.indrelid = t.oid
      WHERE indisprimary = false
      ORDER BY
        t.relname, c.relname)sql");
  EXPECT_THAT(*results, ExpectedRows(*results, expected));
}

TEST_F(PGCatalogTest, DISABLED_PGIndexes) {
  // TODO: Reenable and add named schema rows once pg_catalog is
  // fixed.
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

TEST_F(PGCatalogTest, PGNamespace) {
  // Check that the system namespaces have the correct OIDs.
  auto expected =
      std::vector<ValueRow>({{PgOid(11), "pg_catalog", NOid()},
                             {PgOid(2200), "public", NOid()},
                             {PgOid(75003), "information_schema", NOid()},
                             {PgOid(75004), "spanner_sys", NOid()}});
  EXPECT_THAT(Query(R"sql(
      SELECT
        oid,
        nspname,
        nspowner
      FROM
        pg_catalog.pg_namespace
      WHERE oid < 100000
      ORDER BY
        oid)sql"),
              IsOkAndHoldsRows(expected));

  expected = std::vector<ValueRow>({
      {"named_schema", NOid()},
  });

  // Check that user namespaces are surfaced.
  EXPECT_THAT(Query(R"sql(
      SELECT
        nspname,
        nspowner
      FROM
        pg_catalog.pg_namespace
      WHERE oid >= 100000
      ORDER BY
        oid)sql"),
              IsOkAndHoldsRows(expected));

  // Check that the user namespaces have unique OIDs.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto results, Query(R"sql(
      SELECT
        COUNT(DISTINCT oid)
      FROM
        pg_catalog.pg_namespace
      WHERE oid >= 100000
      GROUP BY
        oid)sql"));

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].values().size(), 1);
  EXPECT_EQ(results[0].values()[0].get<int64_t>().value(), expected.size());
}

TEST_F(PGCatalogTest, PGSequence) {
  auto expected = std::vector<ValueRow>({
      {"test_sequence", 20, 1234, Ni64(), Ni64(), Ni64(), 1000, false},
  });
  EXPECT_THAT(Query(R"sql(
      SELECT
        c.relname,
        seqtypid,
        seqstart,
        seqincrement,
        seqmax,
        seqmin,
        seqcache,
        seqcycle
      FROM
        pg_catalog.pg_sequence as s
      JOIN pg_catalog.pg_class as c on s.seqrelid = c.oid)sql"),
              IsOkAndHoldsRows(expected));
}

TEST_F(PGCatalogTest, PGSequences) {
  auto expected = std::vector<ValueRow>({
      {"public", "test_sequence", Ns(), 1234, Ni64(), Ni64(), Ni64(), false,
       1000, Ni64()},
  });
  EXPECT_THAT(Query(R"sql(
      SELECT
        schemaname,
        sequencename,
        sequenceowner,
        start_value,
        min_value,
        max_value,
        increment_by,
        cycle,
        cache_size,
        last_value
      FROM
        pg_catalog.pg_sequences
      ORDER BY
        schemaname)sql"),
              IsOkAndHoldsRows({expected}));
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

TEST_F(PGCatalogTest, DISABLED_PGTables) {
  // TODO: Reenable and add named schema rows once pg_catalog is
  // fixed.
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
  auto expected = std::vector<ValueRow>({
      {"public", "base_view", Ns(), "SELECT key1 FROM base"},
      {"named_schema", "ns_view", Ns(),
       "SELECT key1 FROM named_schema.ns_table_1 t"},
  });
  EXPECT_THAT(Query(
                  R"sql(
      SELECT
        schemaname, viewname, viewowner, definition
      FROM
        pg_catalog.pg_views
      WHERE
        schemaname != 'pg_catalog' AND schemaname != 'information_schema' AND
        schemaname != 'spanner_sys'
      ORDER BY
        definition, schemaname, viewname)sql"),
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
