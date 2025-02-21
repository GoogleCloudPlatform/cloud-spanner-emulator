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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

using ::zetasql_base::testing::StatusIs;

// How to read the acronyms in this file
// FK - Foreign Key
// PK - FK is in the same order as the Primary Key.
// PKP - FK is in the same order as the PK, but is a prefix.
// PKPOutOfOrder - FK is in a different order than the PK, and it's a PK prefix.
// None - None of the above, FK can be defined on PK or non-PK column.

class ForeignKeyActionsTest
    : public DatabaseTest,
      public ::testing::WithParamInterface<database_api::DatabaseDialect> {
 protected:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    flags.enable_fk_delete_cascade_action = true;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchemaFromFile("foreign_key_actions.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectForeignKeyActionsTests, ForeignKeyActionsTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<ForeignKeyActionsTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(ForeignKeyActionsTest, ReferencedPK_ReferencingPK) {
  // Referenced key: [1].
  // Referencing key: [1].
  ZETASQL_ASSERT_OK(Insert("referencedpk", {"referenced_pk"}, {1}));
  ZETASQL_ASSERT_OK(Insert("referencedpk", {"referenced_pk"}, {2}));
  ZETASQL_ASSERT_OK(Insert("referencingpk_pk", {"referencing_pk"}, {1}));
  ZETASQL_ASSERT_OK(Insert("referencingpk_pk", {"referencing_pk"}, {2}));
  // Delete Referenced key: [1] cascade delete Referencing key: [1].
  ZETASQL_EXPECT_OK(Commit({MakeDelete("referencedpk", Singleton(1))}));
  EXPECT_THAT(ReadAll("referencingpk_pk", {"referencing_pk"}),
              IsOkAndHoldsRows({{2}}));
}

TEST_P(ForeignKeyActionsTest, ReferencedPK_ReferencingPK_Restriction) {
  // You can not insert and delete the same referenced row within the same
  // transaction.
  EXPECT_THAT(Commit({
                  MakeInsert("referencedpk", {"referenced_pk"}, 1),
                  MakeInsert("referencedpk", {"referenced_pk"}, 2),
                  MakeDelete("referencedpk", Singleton(1)),
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyActionsTest, ReferencedPK_ReferencingPK_NoRestrictionWithDml) {
  // You can insert and delete the same referenced row within the same
  // transaction under different flush boundaries. Each DML statement in the
  // transaction converts into mutations and flushes separately, therefore FK
  // transaction restriction should not stop this transaction.
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO referencedpk (referenced_pk) Values (1)"),
       SqlStatement("INSERT INTO referencedpk (referenced_pk) Values (2)"),
       SqlStatement("DELETE FROM referencedpk WHERE referenced_pk = 1")}));
}

TEST_P(ForeignKeyActionsTest,
       ReferencedPK_ReferencingPK_Restriction_WithEmptyRangeDelete) {
  ZETASQL_ASSERT_OK(Insert("referencedpk", {"referenced_pk"}, {1}));
  // You can not insert and delete the same referenced row within the same
  // transaction irrespective of the order of the mutations.
  EXPECT_THAT(Commit({
                  MakeDelete("referencedpk", ClosedClosed(Key(2), Key(3))),
                  MakeInsert("referencedpk", {"referenced_pk"}, 2),
                  MakeInsert("referencedpk", {"referenced_pk"}, 4),
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  EXPECT_THAT(ReadAll("referencedpk", {"referenced_pk"}),
              IsOkAndHoldsRows({{1}}));
}

TEST_P(ForeignKeyActionsTest, ReferencedPK_ReferencingPK_NoRestriction) {
  // You can delete a referenced row and insert the different referenced value
  // within the same transaction.
  ZETASQL_ASSERT_OK(Insert("referencedpk", {"referenced_pk"}, {1}));
  ZETASQL_EXPECT_OK(Commit({
      MakeInsert("referencedpk", {"referenced_pk"}, 2),
      MakeDelete("referencedpk", Singleton(1)),
      MakeInsert("referencingpk_pk", {"referencing_pk"}, 2),
  }));

  EXPECT_THAT(ReadAll("referencedpk", {"referenced_pk"}),
              IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(ReadAll("referencingpk_pk", {"referencing_pk"}),
              IsOkAndHoldsRows({{2}}));
}

TEST_P(ForeignKeyActionsTest, ReferencedPK_ReferencingNonPK) {
  // Referenced key: [1].
  // Referencing key: [1].
  ZETASQL_ASSERT_OK(Insert("referencedpk", {"referenced_pk"}, {1}));
  ZETASQL_ASSERT_OK(Insert("referencedpk", {"referenced_pk"}, {2}));
  ZETASQL_ASSERT_OK(Insert("referencingnonpk_pk", {"pk", "val"}, {10, 1}));
  ZETASQL_ASSERT_OK(Insert("referencingnonpk_pk", {"pk", "val"}, {20, 2}));
  // Delete Referenced key: [1] cascade delete Referencing key: [1].
  ZETASQL_EXPECT_OK(Commit({MakeDelete("referencedpk", Singleton(1))}));
  EXPECT_THAT(ReadAll("referencingnonpk_pk", {"pk", "val"}),
              IsOkAndHoldsRows({{20, 2}}));
}

TEST_P(ForeignKeyActionsTest, ReferencedNonPK_ReferencingPK) {
  // Referenced key: [1].
  // Referencing key: [1].
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {2, "B"}));
  ZETASQL_ASSERT_OK(
      Insert("referencingpk_nonpk", {"referencing_pk", "val"}, {"A", 10}));
  ZETASQL_ASSERT_OK(
      Insert("referencingpk_nonpk", {"referencing_pk", "val"}, {"B", 20}));
  // Delete Referenced key: [1] cascade delete Referencing key: [1].
  ZETASQL_EXPECT_OK(Commit({MakeDelete("referencednonpk", Singleton(1))}));
  EXPECT_THAT(ReadAll("referencingpk_nonpk", {"referencing_pk"}),
              IsOkAndHoldsRows({{"B"}}));
}

TEST_P(ForeignKeyActionsTest, ReferencedNonPK_ReferencingNonPK) {
  // Referenced key: [1].
  // Referencing key: [1].
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {2, "B"}));
  ZETASQL_ASSERT_OK(Insert("referencingnonpk_nonpk", {"pk", "val_str"}, {10, "A"}));
  ZETASQL_ASSERT_OK(Insert("referencingnonpk_nonpk", {"pk", "val_str"}, {20, "B"}));
  // Delete Referenced key: [1] cascade delete Referencing key: [1].
  ZETASQL_EXPECT_OK(Commit({MakeDelete("referencednonpk", Singleton(1))}));
  EXPECT_THAT(ReadAll("referencingnonpk_nonpk", {"pk"}),
              IsOkAndHoldsRows({{20}}));
}

TEST_P(ForeignKeyActionsTest, ReferencedPK_ReferencingPK_DESC_OutOfOrder) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "Primary key DESC is not supported in PostgreSQL";
  } else {
    ZETASQL_EXPECT_OK(SetSchema({
        R"(CREATE TABLE referencedpk1 (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
          ) PRIMARY KEY (referenced_pk1, referenced_pk2)
        )",
        R"(CREATE TABLE referencingpk_desc_referencedpk_outoforder (
            referenced_pk1 INT64 NOT NULL,
            referenced_pk2 INT64 NOT NULL,
            FOREIGN KEY (referenced_pk1, referenced_pk2)
              REFERENCES referencedpk1 (referenced_pk2, referenced_pk1)
                ON DELETE CASCADE
          ) PRIMARY KEY (referenced_pk1 DESC, referenced_pk2)
        )"}));
  }
  ZETASQL_ASSERT_OK(
      Insert("referencedpk1", {"referenced_pk1", "referenced_pk2"}, {1, 3}));
  ZETASQL_ASSERT_OK(
      Insert("referencedpk1", {"referenced_pk1", "referenced_pk2"}, {2, 1}));
  ZETASQL_ASSERT_OK(Insert("referencingpk_desc_referencedpk_outoforder",
                   {"referenced_pk1", "referenced_pk2"}, {1, 2}));

  ZETASQL_EXPECT_OK(Commit({
      MakeDelete("referencedpk1", OpenClosed(Key(0), Key(2))),
  }));

  EXPECT_THAT(ReadAll("referencedpk1", {"referenced_pk1", "referenced_pk2"}),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAll("referencingpk_desc_referencedpk_outoforder",
                      {"referenced_pk1", "referenced_pk2"}),
              IsOkAndHoldsRows({}));
}

TEST_P(ForeignKeyActionsTest, ReferencingPK_DESC_ReferencedNonPK) {
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP() << "Primary key DESC is not supported in PostgreSQL";
  } else {
    ZETASQL_EXPECT_OK(SetSchema({
        R"(
          CREATE TABLE referencednonpk1 (
            referenced_pk INT64 NOT NULL,
            referenced_col INT64,
          ) PRIMARY KEY (referenced_pk)
        )",
        R"(CREATE TABLE referencingpk_desc_referencednonpk (
            referenced_pk INT64 NOT NULL,
            FOREIGN KEY (referenced_pk)
              REFERENCES referencednonpk1 (referenced_col) ON DELETE CASCADE
          ) PRIMARY KEY (referenced_pk DESC)
        )"}));
  }
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk1", {"referenced_pk", "referenced_col"}, {1, 3}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk1", {"referenced_pk", "referenced_col"}, {2, 1}));
  ZETASQL_ASSERT_OK(
      Insert("referencingpk_desc_referencednonpk", {"referenced_pk"}, {1}));

  ZETASQL_EXPECT_OK(Commit({
      MakeDelete("referencednonpk1", OpenClosed(Key(0), Key(2))),
  }));

  EXPECT_THAT(ReadAll("referencednonpk1", {"referenced_pk"}),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(ReadAll("referencingpk_desc_referencednonpk", {"referenced_pk"}),
              IsOkAndHoldsRows({}));
}

TEST_P(ForeignKeyActionsTest, ReferencedNonPK_ReferencingNonPK_Restriction) {
  // Referenced key: [1, "A"], [2, "B"].
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {2, "B"}));

  // You can not update and delete the same referenced row within the same
  // transaction.
  EXPECT_THAT(Commit({
                  MakeUpdate("referencednonpk",
                             {"referenced_pk", "referenced_col"}, 2, "C"),
                  MakeDelete("referencednonpk", ClosedClosed(Key(1), Key(2))),
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));

  EXPECT_THAT(ReadAll("referencednonpk", {"referenced_pk"}),
              IsOkAndHoldsRows({{1}, {2}}));
  EXPECT_THAT(ReadAll("referencingnonpk_nonpk", {"pk"}), IsOkAndHoldsRows({}));
}

TEST_P(ForeignKeyActionsTest,
       ReferencedNonPK_ReferencingNonPK_RestrictionWithReplaceMutation) {
  // Referenced key: [1, "A"], [2, "B"].
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {2, "B"}));

  // You can not replace and delete the same referenced row within the same
  // transaction.
  EXPECT_THAT(Commit({
                  MakeReplace("referencednonpk",
                              {"referenced_pk", "referenced_col"}, 2, "C"),
                  MakeDelete("referencednonpk", ClosedClosed(Key(1), Key(2))),
              }),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(ForeignKeyActionsTest, ReferencedNonPK_ReferencingNonPK_NoRestriction) {
  // Referenced key: [1, "A"], [2, "B"].
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {2, "B"}));
  // You can delete a referenced row and update a different referenced row
  // within the same transaction.
  ZETASQL_EXPECT_OK(Commit({
      MakeUpdate("referencednonpk", {"referenced_pk", "referenced_col"}, 2,
                 "C"),
      MakeDelete("referencednonpk", Singleton(1)),
      MakeInsert("referencingnonpk_nonpk", {"pk", "val_str"}, 10, "C"),
      MakeInsert("referencingnonpk_nonpk", {"pk", "val_str"}, 20, "C"),
  }));
  EXPECT_THAT(ReadAll("referencednonpk", {"referenced_pk"}),
              IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(ReadAll("referencingnonpk_nonpk", {"pk"}),
              IsOkAndHoldsRows({{10}, {20}}));
}

TEST_P(
    ForeignKeyActionsTest,
    ReferencedNonPK_ReferencingNonPK_NoRestriction_ReferencedColumnNotInvolve) {
  // Referenced key: [1, "A"], [2, "B"].
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(
      Insert("referencednonpk", {"referenced_pk", "referenced_col"}, {2, "B"}));
  // You can delete a referenced row and update a different referenced row
  // within the same transaction.
  ZETASQL_EXPECT_OK(Commit({
      MakeUpdate("referencednonpk", {"referenced_pk"}, 2),
      MakeDelete("referencednonpk", Singleton(1)),
      MakeInsert("referencingnonpk_nonpk", {"pk", "val_str"}, 10, "B"),
      MakeInsert("referencingnonpk_nonpk", {"pk", "val_str"}, 20, "B"),
  }));
  EXPECT_THAT(ReadAll("referencednonpk", {"referenced_pk"}),
              IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(ReadAll("referencingnonpk_nonpk", {"pk"}),
              IsOkAndHoldsRows({{10}, {20}}));
}

TEST_P(
    ForeignKeyActionsTest,
    ReferencedNonPKNoAction_ReferencingNonPKNoAction_NoRestriction_OnDelete) {
  // Referenced key: [1, "A"], [2, "B"].
  ZETASQL_ASSERT_OK(Insert("referencednonpk_noaction",
                   {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(Insert("referencednonpk_noaction",
                   {"referenced_pk", "referenced_col"}, {2, "B"}));

  // When table has FK with no action, update and delete the same referenced row
  // within the same transaction is allowed.
  ZETASQL_EXPECT_OK(Commit({
      MakeUpdate("referencednonpk_noaction",
                 {"referenced_pk", "referenced_col"}, 2, "C"),
      MakeDelete("referencednonpk_noaction", OpenClosed(Key(1), Key(2))),
      MakeInsert("referencingnonpk_nonpk_noaction", {"pk", "val_str"}, 10, "A"),
      MakeInsert("referencingnonpk_nonpk_noaction", {"pk", "val_str"}, 20, "A"),
  }));

  EXPECT_THAT(ReadAll("referencednonpk_noaction", {"referenced_pk"}),
              IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(ReadAll("referencingnonpk_nonpk_noaction", {"pk"}),
              IsOkAndHoldsRows({{10}, {20}}));
}

TEST_P(
    ForeignKeyActionsTest,
    ReferencedNonPKNoAction_ReferencingNonPKNoAction_NoRestriction_OnReplace) {
  // Referenced key: [1, "A"], [2, "B"].
  ZETASQL_ASSERT_OK(Insert("referencednonpk_noaction",
                   {"referenced_pk", "referenced_col"}, {1, "A"}));
  ZETASQL_ASSERT_OK(Insert("referencednonpk_noaction",
                   {"referenced_pk", "referenced_col"}, {2, "B"}));
  // You can not replace and delete the same referenced row within the same
  // transaction.
  ZETASQL_EXPECT_OK(Commit({
      MakeReplace("referencednonpk_noaction",
                  {"referenced_pk", "referenced_col"}, 2, "C"),
      MakeDelete("referencednonpk_noaction", OpenClosed(Key(1), Key(2))),
      MakeInsert("referencingnonpk_nonpk_noaction", {"pk", "val_str"}, 10, "A"),
      MakeInsert("referencingnonpk_nonpk_noaction", {"pk", "val_str"}, 20, "A"),
  }));

  EXPECT_THAT(ReadAll("referencednonpk_noaction", {"referenced_pk"}),
              IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(ReadAll("referencingnonpk_nonpk_noaction", {"pk"}),
              IsOkAndHoldsRows({{10}, {20}}));
}

TEST_P(ForeignKeyActionsTest, Chain_Interleave_ReferencingPK) {
  // This test verifies that interleave works correctly when the referencing
  // columns in the last table are the primary key.
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_EXPECT_OK(SetSchema({
        R"(CREATE TABLE group1_level1 (
            group1_level1_pk varchar NOT NULL,
            PRIMARY KEY (group1_level1_pk)
          )
        )",
        R"(CREATE TABLE group1_level2 (
            group1_level1_pk varchar NOT NULL,
            group1_level2_pk bigint NOT NULL,
            PRIMARY KEY (group1_level1_pk, group1_level2_pk)
          ) INTERLEAVE IN PARENT group1_level1 ON DELETE CASCADE
        )",
        R"(CREATE TABLE group2_level1 (
            group2_level1_pk1 varchar NOT NULL,
            group2_level1_pk2 bigint NOT NULL,
            FOREIGN KEY (group2_level1_pk1, group2_level1_pk2)
              REFERENCES group1_level2 (group1_level1_pk, group1_level2_pk)
              ON DELETE CASCADE,
            PRIMARY KEY (group2_level1_pk1, group2_level1_pk2)
          )
        )"}));

  } else {
    ZETASQL_EXPECT_OK(SetSchema({
        R"(CREATE TABLE group1_level1 (
            group1_level1_pk STRING(MAX) NOT NULL,
          ) PRIMARY KEY (group1_level1_pk)
        )",
        R"(CREATE TABLE group1_level2 (
            group1_level1_pk STRING(MAX) NOT NULL,
            group1_level2_pk INT64 NOT NULL,
          ) PRIMARY KEY (group1_level1_pk, group1_level2_pk),
          INTERLEAVE IN PARENT group1_level1 ON DELETE CASCADE
        )",
        R"(CREATE TABLE group2_level1 (
            group2_level1_pk1 STRING(MAX) NOT NULL,
            group2_level1_pk2 INT64 NOT NULL,
            FOREIGN KEY (group2_level1_pk1, group2_level1_pk2)
              REFERENCES group1_level2 (group1_level1_pk, group1_level2_pk)
              ON DELETE CASCADE,
          ) PRIMARY KEY (group2_level1_pk1, group2_level1_pk2)
        )"}));
  }

  ZETASQL_ASSERT_OK(Insert("group1_level1", {"group1_level1_pk"}, {"ABC"}));
  ZETASQL_ASSERT_OK(Insert("group1_level1", {"group1_level1_pk"}, {"XYZ"}));
  ZETASQL_ASSERT_OK(Insert("group1_level2", {"group1_level1_pk", "group1_level2_pk"},
                   {"ABC", 100}));
  ZETASQL_ASSERT_OK(Insert("group1_level2", {"group1_level1_pk", "group1_level2_pk"},
                   {"XYZ", 200}));
  ZETASQL_ASSERT_OK(Insert("group2_level1", {"group2_level1_pk1", "group2_level1_pk2"},
                   {"ABC", 100}));
  ZETASQL_ASSERT_OK(Insert("group2_level1", {"group2_level1_pk1", "group2_level1_pk2"},
                   {"XYZ", 200}));
  // Delete Referenced key: ["ABC"] cascade delete from the interleave child
  // table and the referencing table.
  ZETASQL_EXPECT_OK(Commit({MakeDelete("group1_level1", Singleton("ABC"))}));
  EXPECT_THAT(
      ReadAll("group2_level1", {"group2_level1_pk1", "group2_level1_pk2"}),
      IsOkAndHoldsRows({{"XYZ", 200}}));
}

TEST_P(ForeignKeyActionsTest, Chain_ForeignKeyForeignKeyInterleave) {
  // This test verifies that the chain FK->FK->Interleave works correctly.
  if (dialect_ == database_api::DatabaseDialect::POSTGRESQL) {
    ZETASQL_EXPECT_OK(SetSchema({
        R"(CREATE TABLE group1_level1 (
        group1_level1_pk varchar NOT NULL,
        PRIMARY KEY (group1_level1_pk)
      )
      )",
        R"(CREATE TABLE group2_level1 (
        group2_level1_pk bigint NOT NULL,
        val_str varchar,
        FOREIGN KEY (val_str)
          REFERENCES group1_level1 (group1_level1_pk) ON DELETE CASCADE,
        PRIMARY KEY (group2_level1_pk)
      )
      )",
        R"(CREATE TABLE group3_level1 (
        group3_level1_pk varchar NOT NULL,
        val_int bigint,
        FOREIGN KEY (val_int)
          REFERENCES group2_level1 (group2_level1_pk) ON DELETE CASCADE,
        PRIMARY KEY (group3_level1_pk)
      )
      )",
        R"(CREATE TABLE group3_level2 (
        group3_level1_pk varchar NOT NULL,
        group3_level2_pk bigint NOT NULL,
        PRIMARY KEY (group3_level1_pk, group3_level2_pk)
      ) INTERLEAVE IN PARENT group3_level1 ON DELETE CASCADE
        )"}));
  } else {
    ZETASQL_EXPECT_OK(SetSchema({
        R"(CREATE TABLE group1_level1 (
        group1_level1_pk STRING(MAX) NOT NULL,
      ) PRIMARY KEY (group1_level1_pk)
      )",
        R"(CREATE TABLE group2_level1 (
        group2_level1_pk INT64 NOT NULL,
        val_str STRING(MAX),
        FOREIGN KEY (val_str)
          REFERENCES group1_level1 (group1_level1_pk) ON DELETE CASCADE
      ) PRIMARY KEY (group2_level1_pk)
      )",
        R"(CREATE TABLE group3_level1 (
        group3_level1_pk STRING(MAX) NOT NULL,
        val_int INT64,
        FOREIGN KEY (val_int)
          REFERENCES group2_level1 (group2_level1_pk) ON DELETE CASCADE
      ) PRIMARY KEY (group3_level1_pk)
      )",
        R"(CREATE TABLE group3_level2 (
        group3_level1_pk STRING(MAX) NOT NULL,
        group3_level2_pk INT64 NOT NULL,
      ) PRIMARY KEY (group3_level1_pk, group3_level2_pk),
        INTERLEAVE IN PARENT group3_level1 ON DELETE CASCADE
        )"}));
  }

  ZETASQL_ASSERT_OK(Insert("group1_level1", {"group1_level1_pk"}, {"ABC"}));
  ZETASQL_ASSERT_OK(Insert("group1_level1", {"group1_level1_pk"}, {"XYZ"}));
  ZETASQL_ASSERT_OK(
      Insert("group2_level1", {"group2_level1_pk", "val_str"}, {1, "ABC"}));
  ZETASQL_ASSERT_OK(
      Insert("group2_level1", {"group2_level1_pk", "val_str"}, {2, "XYZ"}));
  ZETASQL_ASSERT_OK(
      Insert("group3_level1", {"group3_level1_pk", "val_int"}, {"ABC", 1}));
  ZETASQL_ASSERT_OK(
      Insert("group3_level1", {"group3_level1_pk", "val_int"}, {"XYZ", 2}));
  ZETASQL_ASSERT_OK(Insert("group3_level2", {"group3_level1_pk", "group3_level2_pk"},
                   {"ABC", 10}));
  ZETASQL_ASSERT_OK(Insert("group3_level2", {"group3_level1_pk", "group3_level2_pk"},
                   {"XYZ", 20}));
  // Delete Referenced key: ["ABC"] cascade delete from the referencing table
  // and the interleave child table.
  ZETASQL_EXPECT_OK(Commit({MakeDelete("group1_level1", Singleton("ABC"))}));
  EXPECT_THAT(
      ReadAll("group3_level2", {"group3_level1_pk", "group3_level2_pk"}),
      IsOkAndHoldsRows({{"XYZ", 20}}));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
