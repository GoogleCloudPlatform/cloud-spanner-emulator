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

#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

using zetasql_base::testing::StatusIs;

class ForeignKeyVerifiersTest : public DatabaseTest {
 protected:
  absl::Status SetUpDatabase() override {
    return SetSchema(
        {"CREATE TABLE T (A INT64 NOT NULL, B INT64) PRIMARY KEY(A)",
         "CREATE TABLE U (X INT64 NOT NULL, Y INT64) PRIMARY KEY(X)"});
  }
};

TEST_F(ForeignKeyVerifiersTest, ValidKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {1, 2}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(X,Y) REFERENCES T(A,B)"}));
}

// Indexes include all of the indexed table's primary key columns in its
// primary key columns. This can make the index primary key wider than the
// indexed table's primary key. Data validation must therefore do prefix lookups
// instead of point lookups. Add some tests for a mixture of wide keys.
TEST_F(ForeignKeyVerifiersTest, ValidWideKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {3, 2}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y) REFERENCES T(B)"}));
}

TEST_F(ForeignKeyVerifiersTest, ValidWideReferencingKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {3, 1}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y) REFERENCES T(A)"}));
}

TEST_F(ForeignKeyVerifiersTest, ValidWideReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 4}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(X) REFERENCES T(B)"}));
}

TEST_F(ForeignKeyVerifiersTest, InvalidKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {1, 3}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(X,Y) REFERENCES T(A,B)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyVerifiersTest, ValidReversedKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {2, 1}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y,X) REFERENCES T(A,B)"}));
}

TEST_F(ForeignKeyVerifiersTest, InvalidReversedKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {1, 2}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y,X) REFERENCES T(A,B)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyVerifiersTest, InvalidWideReferencingKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {3, 4}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y) REFERENCES T(A)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyVerifiersTest, InvalidWideReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {3, 4}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(X) REFERENCES T(B)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyVerifiersTest, EmptyReferencingTable) {
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y) REFERENCES T(B)"}));
}

TEST_F(ForeignKeyVerifiersTest, ReferencingNullValues) {
  ZETASQL_ASSERT_OK(Insert("U", {"X", "Y"}, {1, Null<std::int64_t>()}));
  ZETASQL_EXPECT_OK(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(X,Y) REFERENCES T(A,B)"}));
}

TEST_F(ForeignKeyVerifiersTest, NonMatchingReferencedRow) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_EXPECT_OK(UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(Y) REFERENCES T(B)"}));
}

TEST_F(ForeignKeyVerifiersTest, DuplicateReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {3, 2}));
  EXPECT_THAT(
      UpdateSchema({"ALTER TABLE U ADD FOREIGN KEY(X) REFERENCES T(B)"}),
      StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_F(ForeignKeyVerifiersTest, CreateValidTable) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {3, 4}));
  ZETASQL_EXPECT_OK(UpdateSchema({R"(
      CREATE TABLE V (
        X INT64,
        Y INT64,
        FOREIGN KEY(X) REFERENCES T(B)
      ) PRIMARY KEY(X))"}));
}

TEST_F(ForeignKeyVerifiersTest, CreateTableWithDuplidateReferencedKeys) {
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("T", {"A", "B"}, {3, 2}));
  EXPECT_THAT(UpdateSchema({R"(
      CREATE TABLE V (
        X INT64,
        Y INT64,
        FOREIGN KEY(X) REFERENCES T(B)
      ) PRIMARY KEY(X))"}),
              StatusIs(absl::StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
