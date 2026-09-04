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
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "google/cloud/spanner/numeric.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using googlesql_base::testing::StatusIs;

class PrimaryKeysTest
    : public DatabaseTest,
      public ::testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchemaFromFile("primary_keys.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectPrimaryKeysTests, PrimaryKeysTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<PrimaryKeysTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(PrimaryKeysTest, CanInsertRowWithMultiPartKey) {
  // Insert a row with a fully-specified key.
  GOOGLESQL_ASSERT_OK(Insert("tablewithnullablekey", {"key1", "key2", "col1"},
                   {"key1_val", "key2_val", "col1_val"}));

  // Verify that it exists.
  EXPECT_THAT(ReadAll("tablewithnullablekey", {"key1", "key2", "col1"}),
              IsOkAndHoldsRows({{"key1_val", "key2_val", "col1_val"}}));
}

TEST_P(PrimaryKeysTest, CannotInsertWithoutRequiredKeyColumn) {
  // Check that we cannot do an insert if we skip key1 which is required.
  EXPECT_THAT(Insert("tablewithnullablekey", {"key2"}, {"key2_val"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(PrimaryKeysTest, CanInsertWithNullableKeyColumn) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "PG does not support nullable primary key columns.";
  }
  // Insert a row without specifying key2, it should be seen as a NULL.
  GOOGLESQL_ASSERT_OK(Insert("tablewithnullablekey", {"key1", "col1"},
                   {"key1_val", "col1_val"}));

  // Verify that the row exists with NULL as the value for key2.
  EXPECT_THAT(
      ReadAll("tablewithnullablekey", {"key1", "key2", "col1"}),
      IsOkAndHoldsRows({{"key1_val", Null<std::string>(), "col1_val"}}));
}

TEST_P(PrimaryKeysTest, CanInsertRowWithExplicitNullKeyColumn) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "PG does not support nullable primary key columns.";
  }
  // Insert a row with key2 explicitly specified as NULL.
  GOOGLESQL_ASSERT_OK(Insert("tablewithnullablekey", {"key1", "key2", "col1"},
                   {"key1_val", Null<std::string>(), "col1_val"}));

  // Verify that the row exists with NULL as the value for key2.
  EXPECT_THAT(
      ReadAll("tablewithnullablekey", {"key1", "key2", "col1"}),
      IsOkAndHoldsRows({{"key1_val", Null<std::string>(), "col1_val"}}));
}

TEST_P(PrimaryKeysTest, CannotInsertNullForNotNullKeyColumn) {
  // Try to insert a row with key1 explicitly specified as NULL.
  EXPECT_THAT(Insert("tablewithnullablekey", {"key1", "key2", "col1"},
                     {Null<std::string>(), "key2_val", "col1_val"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(PrimaryKeysTest, CannotInsertKeyTooLarge) {
  std::string long_str(8192, 'a');
  EXPECT_THAT(
      Insert("tablewithnullablekey", {"key1", "key2"}, {long_str, "abc"}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(PrimaryKeysTest, NumericKey) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "PG does not support PG.NUMERIC as a primary key type.";
  }
  Numeric key1 =
      cloud::spanner::MakeNumeric("-9999999999999999123.456789").value();
  Numeric key2 = cloud::spanner::MakeNumeric("123.456789").value();
  Numeric key3 = cloud::spanner::MakeNumeric("0").value();

  GOOGLESQL_ASSERT_OK(Insert("TableWithNumericKey", {"key", "val"}, {key1, "val1"}));

  GOOGLESQL_ASSERT_OK(Insert("TableWithNumericKey", {"key", "val"}, {key2, "val2"}));

  GOOGLESQL_ASSERT_OK(Insert("TableWithNumericKey", {"key", "val"}, {key3, "val3"}));

  // Verify that it exists.
  EXPECT_THAT(
      ReadAll("TableWithNumericKey", {"key", "val"}),
      IsOkAndHoldsRows({{key2, "val2"}, {key3, "val3"}, {key1, "val1"}}));
}

TEST_P(PrimaryKeysTest, TableWithoutPrimaryKey_BasicOperations) {
  // Verify that we can insert rows into a table without a primary key.
  GOOGLESQL_ASSERT_OK(Insert("tablewithoutpk", {"col1"}, {"val1"}));
  GOOGLESQL_ASSERT_OK(Insert("tablewithoutpk", {"col1"}, {"val2"}));

  // Verify that SELECT * does not return the hidden rowid column.
  EXPECT_THAT(Query("SELECT * FROM tablewithoutpk ORDER BY col1"),
              IsOkAndHoldsRows({{"val1"}, {"val2"}}));

  // Verify that SELECT rowid explicitly works and returns auto-generated
  // row IDs.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto rows, Query("SELECT rowid, col1 FROM tablewithoutpk ORDER BY col1"));
  EXPECT_EQ(rows.size(), 2);

  int64_t rowid1 = rows[0].values()[0].get<int64_t>().value();
  int64_t rowid2 = rows[1].values()[0].get<int64_t>().value();
  EXPECT_NE(rowid1, rowid2);
  EXPECT_EQ(rows[0].values()[1].get<std::string>().value(), "val1");
  EXPECT_EQ(rows[1].values()[1].get<std::string>().value(), "val2");

  // Verify that we can update rows using rowid in WHERE clause.
  std::string update_query =
      "UPDATE tablewithoutpk SET col1 = 'val1_updated' WHERE rowid = " +
      std::to_string(rowid1);
  GOOGLESQL_ASSERT_OK(CommitDml({SqlStatement(update_query)}));

  EXPECT_THAT(Query("SELECT col1 FROM tablewithoutpk WHERE rowid = " +
                    std::to_string(rowid1)),
              IsOkAndHoldsRows({{"val1_updated"}}));

  // Verify that we can delete rows using rowid in WHERE clause.
  std::string delete_query =
      "DELETE FROM tablewithoutpk WHERE rowid = " + std::to_string(rowid1);
  GOOGLESQL_ASSERT_OK(CommitDml({SqlStatement(delete_query)}));

  EXPECT_THAT(Query("SELECT col1 FROM tablewithoutpk"),
              IsOkAndHoldsRows({{"val2"}}));
}

TEST_P(PrimaryKeysTest, TableWithoutPrimaryKey_ExplicitRowIdInsert) {
  // Verify that we can insert explicit values into the rowid column.
  GOOGLESQL_ASSERT_OK(Insert("tablewithoutpk", {"rowid", "col1"}, {100, "explicit_val"}));

  // Verify that we can read it back.
  EXPECT_THAT(Query("SELECT rowid, col1 FROM tablewithoutpk WHERE rowid = 100"),
              IsOkAndHoldsRows({{100, "explicit_val"}}));

  // Verify that auto-generation still works after explicit insert.
  GOOGLESQL_ASSERT_OK(Insert("tablewithoutpk", {"col1"}, {"auto_val"}));

  // Verify both rows exist. Order by col1 to ensure stable ordering in test
  // assertion.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto rows, Query("SELECT rowid, col1 FROM tablewithoutpk ORDER BY col1"));
  EXPECT_EQ(rows.size(), 2);

  EXPECT_EQ(rows[0].values()[1].get<std::string>().value(), "auto_val");
  int64_t auto_rowid = rows[0].values()[0].get<int64_t>().value();
  EXPECT_NE(auto_rowid, 100);

  EXPECT_EQ(rows[1].values()[1].get<std::string>().value(), "explicit_val");
  EXPECT_EQ(rows[1].values()[0].get<int64_t>().value(), 100);
}

TEST_P(PrimaryKeysTest, TableWithoutPrimaryKey_RowIdConflict) {
  // Verify that trying to create a table with rowid column and no PK fails.
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        UpdateSchema({R"(
      CREATE TABLE tablewithrowidconflict (
        rowid bigint,
        col1 varchar
      )
    )"}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 testing::HasSubstr(
                     "Duplicate column name tablewithrowidconflict.rowid")));
  } else {
    EXPECT_THAT(
        UpdateSchema({R"(
      CREATE TABLE tablewithrowidconflict (
        rowid INT64,
        col1 STRING(MAX)
      )
    )"}),
        StatusIs(absl::StatusCode::kFailedPrecondition,
                 testing::HasSubstr(
                     "Duplicate column name tablewithrowidconflict.rowid")));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
