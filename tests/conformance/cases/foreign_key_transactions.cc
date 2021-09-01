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

#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

using zetasql_base::testing::StatusIs;

class ForeignKeyTransactionsTest : public DatabaseTest {
 protected:
  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchema({
        R"(
        CREATE TABLE T (
          A INT64,
          B INT64,
        ) PRIMARY KEY(A, B)
      )",
        R"(
        CREATE TABLE U (
          X INT64,
          Y INT64,
          FOREIGN KEY (Y, X) REFERENCES T (A, B),
        ) PRIMARY KEY(X)
      )",
        R"(
        CREATE TABLE Parent (
          A INT64 NOT NULL,
        ) PRIMARY KEY(A)
      )",
        R"(
        CREATE TABLE Child (
          A INT64 NOT NULL,
          B INT64 NOT NULL,
        ) PRIMARY KEY(A, B),
          INTERLEAVE IN PARENT Parent ON DELETE CASCADE
      )",
        R"(
        CREATE TABLE Referencing (
          X INT64 NOT NULL,
          FOREIGN KEY (X) REFERENCES Child (B),
        ) PRIMARY KEY(X)
      )",
        R"(
        CREATE TABLE NumericT1 (
          A NUMERIC,
          B NUMERIC,
        ) PRIMARY KEY(A, B)
      )",
        R"(
        CREATE TABLE NumericT2 (
          X NUMERIC,
          Y NUMERIC,
          FOREIGN KEY (Y, X) REFERENCES NumericT1 (A, B),
        ) PRIMARY KEY(X)
      )",
    });
  }
};

TEST_F(ForeignKeyTransactionsTest, InsertReferencingRowWithReferencedRow) {
  // Insert the referenced row second to ensure a constraint violation isn't
  // trigger prematurely. The referencing key is [1,2] since the foreign key
  // defines the referencing key as (Y, X).
  ZETASQL_EXPECT_OK(Commit(
      {MakeInsert("U", {"X", "Y"}, 2, 1), MakeInsert("T", {"A", "B"}, 1, 2)}));
}

TEST_F(ForeignKeyTransactionsTest, InsertReferencingRowWithoutReferencedRow) {
  // The referencing key [2,1] does not match the referenced key [1,2].
  EXPECT_THAT(Commit({MakeInsert("U", {"X", "Y"}, 1, 2),
                      MakeInsert("T", {"A", "B"}, 1, 2)}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest,
       InsertAndUpdateReferencingRowWithReferencedRow) {
  // Insert an initially invalid referencing key [2,2], but later update it to
  // the valid [1,2].
  ZETASQL_EXPECT_OK(Commit({MakeInsert("U", {"X", "Y"}, 2, 2),
                    MakeInsert("T", {"A", "B"}, 1, 2),
                    MakeUpdate("U", {"X", "Y"}, 2, 1)}));
}

TEST_F(ForeignKeyTransactionsTest,
       InsertAndUpdateReferencingRowWithoutReferencedRow) {
  // Insert an initially valid referencing key [1,2], but later update it to
  // the invalid [2,2].
  EXPECT_THAT(Commit({MakeInsert("U", {"X", "Y"}, 2, 1),
                      MakeInsert("T", {"A", "B"}, 1, 2),
                      MakeUpdate("U", {"X", "Y"}, 2, 2)}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, InsertAndDeleteReferencingRow) {
  // Insert an invalid referencing key [2,1], but delete it before the write.
  ZETASQL_EXPECT_OK(Commit(
      {MakeInsert("U", {"X", "Y"}, 1, 2), MakeDelete("U", Singleton(1))}));
}

TEST_F(ForeignKeyTransactionsTest, UpdateReferencingRowWithReferencedRow) {
  // Referenced keys: [1,2], [3,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {3, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Updated referencing keys: [3,2].
  ZETASQL_EXPECT_OK(Commit({MakeUpdate("U", {"X", "Y"}, 2, 3)}));
}

TEST_F(ForeignKeyTransactionsTest, UpdateReferencingRowWithoutReferencedRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Updated referencing keys: [3,2].
  EXPECT_THAT(Commit({MakeUpdate("U", {"X", "Y"}, 2, 3)}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, UpdateAndDeleteReferencingRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Updated referencing keys: [3,2].
  // Final referencing keys: <empty>.
  ZETASQL_EXPECT_OK(Commit(
      {MakeUpdate("U", {"X", "Y"}, 2, 3), MakeDelete("U", Singleton(2))}));
}

TEST_F(ForeignKeyTransactionsTest, DeleteReferencedRowWithReferencingRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Attempt to delete the referenced key.
  EXPECT_THAT(Commit({MakeDelete("T", Singleton(1))}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, DeleteReferencedRowWithoutReferencingRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Delete the referencing and referenced pair of rows.
  ZETASQL_EXPECT_OK(Commit(
      {MakeDelete("T", Singleton(1, 2)), MakeDelete("U", Singleton(2))}));
}

TEST_F(ForeignKeyTransactionsTest, DeleteMissingReferencedRow) {
  // Deletion of a nonexistent referenced row should succeed.
  ZETASQL_EXPECT_OK(Commit({MakeDelete("T", Singleton(-1, -1))}));
}

TEST_F(ForeignKeyTransactionsTest, InsertReferencingRowWithNulls) {
  // Insertion of referencing rows with null values is always allowed.
  ZETASQL_EXPECT_OK(Commit({MakeInsert("U", {"X", "Y"}, 2, Null<std::int64_t>())}));
}

TEST_F(ForeignKeyTransactionsTest, UpdateReferencingRowWithNull) {
  // Updating a referencing row with a null value is always allowed.
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  ZETASQL_EXPECT_OK(Commit({MakeUpdate("U", {"X", "Y"}, 2, Null<std::int64_t>())}));
}

TEST_F(ForeignKeyTransactionsTest,
       UpdateReferencingRowWithNonNullWithReferencedRow) {
  // Updating a referencing row with a non-null value succeeds if there is a
  // matching referenced row.
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, Null<std::int64_t>()}));
  ZETASQL_EXPECT_OK(Commit({MakeUpdate("U", {"X", "Y"}, 2, 1)}));
}

TEST_F(ForeignKeyTransactionsTest,
       UpdateReferencingRowWithNonNullWithoutReferencedRow) {
  // Updating a referencing row with a non-null value must have a matching
  // referenced row.
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, Null<std::int64_t>()}));
  EXPECT_THAT(Commit({MakeUpdate("U", {"X", "Y"}, 2, 1)}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, DeleteReferencedRowWithNulls) {
  // Deletion of referenced rows with null values is always allowed.
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, Null<std::int64_t>()}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {Null<std::int64_t>(), 1}));
  ZETASQL_EXPECT_OK(Commit({MakeDelete("T", Singleton(1, Null<std::int64_t>()))}));
}

TEST_F(ForeignKeyTransactionsTest, DeleteParentRowWithReferencingRow) {
  // Referenced keys: [2].
  // Referencing keys: [2].
  ZETASQL_ASSERT_OK(Insert("Parent", {"A"}, {1}));
  ZETASQL_ASSERT_OK(Insert("Child", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("Referencing", {"X"}, {2}));
  // Attempt to delete the referenced key. In this case the referenced row is
  // deleted indirectly by deleting its parent row.
  EXPECT_THAT(Commit({MakeDelete("Parent", Singleton(1))}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, DeleteParentRowWithoutReferencingRow) {
  // Referenced keys: [2].
  // Referencing keys: [2].
  ZETASQL_ASSERT_OK(Insert("Parent", {"A"}, {1}));
  ZETASQL_ASSERT_OK(Insert("Child", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("Referencing", {"X"}, {2}));
  // Delete the referencing and the referenced pair of rows. In this case the
  // referenced row is deleted indirectly by deleting its parent row.
  ZETASQL_EXPECT_OK(Commit({MakeDelete("Parent", Singleton(1)),
                    MakeDelete("Referencing", Singleton(2))}));
}

TEST_F(ForeignKeyTransactionsTest, DmlInsertReferencingRowWithReferencedRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("INSERT T(A, B) Values (1, 2)"),
                       SqlStatement("INSERT U(X, Y) Values (2, 1)")}));
}

TEST_F(ForeignKeyTransactionsTest,
       DmlInsertReferencingRowWithoutReferencedRow) {
  // Each DML statement is applied before executing the next statement. The
  // order of insertion cannot be reversed.
  EXPECT_THAT(CommitDml({SqlStatement("INSERT U(X, Y) Values (2, 1)"),
                         SqlStatement("INSERT T(A, B) Values (1, 2)")}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, DmlUpdateReferencingRowWithReferencedRow) {
  // Referenced keys: [1,2], [3,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {3, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Updated referencing keys: [3,2].
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("UPDATE U SET Y = 3 WHERE X = 2")}));
}

TEST_F(ForeignKeyTransactionsTest,
       DmlUpdateReferencingRowWithoutReferencedRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Updated referencing keys: [3,2].
  EXPECT_THAT(CommitDml({SqlStatement("UPDATE U SET Y = 3 WHERE X = 2")}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest, DmlDeleteReferencedRowWithReferencingRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Attempt to delete the referenced key.
  EXPECT_THAT(CommitDml({SqlStatement("DELETE T WHERE A = 1")}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyTransactionsTest,
       DmlDeleteReferencedRowWithoutReferencingRow) {
  // Referenced keys: [1,2].
  // Referencing keys: [1,2].
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  // Delete the referencing and referenced pair of rows.
  ZETASQL_EXPECT_OK(CommitDml({SqlStatement("DELETE U WHERE X = 2"),
                       SqlStatement("DELETE T WHERE A = 1")}));
}

TEST_F(ForeignKeyTransactionsTest, SwapReferencedKey) {
  // Referenced keys: [2].
  // Referencing keys: [2].
  ZETASQL_ASSERT_OK(Insert("Parent", {"A"}, {1}));
  ZETASQL_ASSERT_OK(Insert("Parent", {"A"}, {2}));
  ZETASQL_ASSERT_OK(Insert("Child", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("Referencing", {"X"}, {2}));
  // Delete the referenced row and insert a new row with the same referenced
  // values.
  ZETASQL_EXPECT_OK(Commit({MakeDelete("Child", Singleton(1, 2)),
                    MakeInsert("Child", {"A", "B"}, 2, 2)}));
}

TEST_F(ForeignKeyTransactionsTest,
       NumericInsertReferencingRowWithReferencedRow) {
  Numeric v1 = cloud::spanner::MakeNumeric("-999999999.456789").value();
  Numeric v2 = cloud::spanner::MakeNumeric("123.456789").value();
  ZETASQL_EXPECT_OK(Commit({MakeInsert("NumericT2", {"X", "Y"}, v2, v1),
                    MakeInsert("NumericT1", {"A", "B"}, v1, v2)}));
}

TEST_F(ForeignKeyTransactionsTest,
       NumericInsertReferencedRowAndIncorrectReferencingRow) {
  Numeric v1 = cloud::spanner::MakeNumeric("-999999999.456789").value();
  Numeric v2 = cloud::spanner::MakeNumeric("123.456789").value();
  // The referencing key [2,1] does not match the referenced key [1,2].
  EXPECT_THAT(Commit({MakeInsert("NumericT2", {"X", "Y"}, v1, v2),
                      MakeInsert("NumericT1", {"A", "B"}, v1, v2)}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
