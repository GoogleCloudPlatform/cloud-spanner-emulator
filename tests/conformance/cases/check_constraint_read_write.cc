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

#include <cstddef>
#include <cstdint>

#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

using zetasql_base::testing::StatusIs;

class CheckConstraintTest : public DatabaseTest {
 protected:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        "CREATE TABLE T("
        " K INT64,"
        " V INT64,"
        " V_STR STRING(MAX),"
        " CONSTRAINT v_gt_zero CHECK (V > 0),"
        " CONSTRAINT v_left_shift_gt_zero CHECK ((V << 1) > 0),"
        " CONSTRAINT v_str_gt_zero CHECK(V_STR > '0'),"
        " ) PRIMARY KEY (K)",
        "CREATE TABLE T_GC("
        " K INT64,"
        " V INT64,"
        " G1 INT64 AS (V) STORED,"
        " G2 INT64 AS (G1) STORED,"
        " G3 INT64 AS (V + 5) STORED,"
        " CONSTRAINT g1_gt_zero CHECK (G1 > 0),"
        " CONSTRAINT g2_gt_zero CHECK(G2 > 0),"
        " CONSTRAINT g3_gt_ten CHECK(G3 > 10),"
        " ) PRIMARY KEY (K)",
    });
  }
};

TEST_F(CheckConstraintTest, Insert) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, 10}));
  EXPECT_THAT(Read("T", {"K", "V"}, Key(1)), IsOkAndHoldsRow({1, 10}));

  ZETASQL_ASSERT_OK(Insert("T", {"K", "V_STR"}, {2, "10"}));
  EXPECT_THAT(Read("T", {"K", "V_STR"}, Key(2)), IsOkAndHoldsRow({2, "10"}));

  ZETASQL_ASSERT_OK(Insert("T_GC", {"K", "V"}, {1, 10}));
  EXPECT_THAT(Read("T_GC", {"K", "V", "G1", "G2", "G3"}, Key(1)),
              IsOkAndHoldsRow({1, 10, 10, 10, 15}));
}

TEST_F(CheckConstraintTest, InsertNull) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, Null<std::int64_t>()}));
  EXPECT_THAT(Read("T", {"K", "V"}, Key(1)),
              IsOkAndHoldsRow({1, Null<std::int64_t>()}));

  ZETASQL_ASSERT_OK(Insert("T_GC", {"K", "V"}, {1, Null<std::int64_t>()}));
  EXPECT_THAT(Read("T_GC", {"K", "V", "G1", "G2", "G3"}, Key(1)),
              IsOkAndHoldsRow({1, Null<std::int64_t>(), Null<std::int64_t>(),
                               Null<std::int64_t>(), Null<std::int64_t>()}));
}

TEST_F(CheckConstraintTest, InsertWithCoercion) {
  // v_left_shift_gt_zero ZETASQL_CHECK ((V << 1) > 0) is OK.
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, (int32_t)1073741824}));
  EXPECT_THAT(Read("T", {"K", "V"}, Key(1)),
              IsOkAndHoldsRow({1, (int64_t)1073741824}));
}

TEST_F(CheckConstraintTest, InsertViolation) {
  EXPECT_THAT(Insert("T", {"K", "V"}, {1, -1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T`.`v_gt_zero` is violated")));
  EXPECT_THAT(
      Insert("T", {"K", "V_STR"}, {1, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `T`.`v_str_gt_zero` is violated")));

  EXPECT_THAT(Insert("T_GC", {"K", "V"}, {1, 1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T_GC`.`g3_gt_ten` is violated")));
}

TEST_F(CheckConstraintTest, Update) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("T", {"K", "V"}, {1, 5}));
  EXPECT_THAT(Read("T", {"K", "V"}, Key(1)), IsOkAndHoldsRow({1, 5}));

  ZETASQL_ASSERT_OK(Insert("T", {"K", "V_STR"}, {2, "10"}));
  ZETASQL_ASSERT_OK(Update("T", {"K", "V_STR"}, {2, "5"}));
  EXPECT_THAT(Read("T", {"K", "V_STR"}, Key(2)), IsOkAndHoldsRow({2, "5"}));

  ZETASQL_ASSERT_OK(Insert("T_GC", {"K", "V"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("T_GC", {"K", "V"}, {1, 15}));
  EXPECT_THAT(Read("T_GC", {"K", "V", "G1", "G2", "G3"}, Key(1)),
              IsOkAndHoldsRow({1, 15, 15, 15, 20}));
}

TEST_F(CheckConstraintTest, UpdateNull) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("T", {"K", "V"}, {1, Null<std::int64_t>()}));
  EXPECT_THAT(Read("T", {"K", "V"}, Key(1)),
              IsOkAndHoldsRow({1, Null<std::int64_t>()}));

  ZETASQL_ASSERT_OK(Insert("T_GC", {"K", "V"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("T_GC", {"K", "V"}, {1, Null<std::int64_t>()}));
  EXPECT_THAT(Read("T_GC", {"K", "V", "G1", "G2", "G3"}, Key(1)),
              IsOkAndHoldsRow({1, Null<std::int64_t>(), Null<std::int64_t>(),
                               Null<std::int64_t>(), Null<std::int64_t>()}));

  ZETASQL_ASSERT_OK(Insert("T", {"K", "V_STR"}, {2, "10"}));
  ZETASQL_ASSERT_OK(Update("T", {"K", "V_STR"}, {2, Null<std::string>()}));
  EXPECT_THAT(Read("T", {"K", "V_STR"}, Key(2)),
              IsOkAndHoldsRow({2, Null<std::string>()}));
}

TEST_F(CheckConstraintTest, UpdateViolation) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V"}, {1, 10}));
  EXPECT_THAT(Update("T", {"K", "V"}, {1, -1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T`.`v_gt_zero` is violated")));

  ZETASQL_ASSERT_OK(Insert("T_GC", {"K", "V"}, {1, 10}));
  EXPECT_THAT(Update("T_GC", {"K", "V"}, {1, 1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T_GC`.`g3_gt_ten` is violated")));

  ZETASQL_ASSERT_OK(Insert("T", {"K", "V_STR"}, {2, "10"}));
  EXPECT_THAT(
      Update("T", {"K", "V_STR"}, {2, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `T`.`v_str_gt_zero` is violated")));
}

TEST_F(CheckConstraintTest, UpdateNonExistingKey) {
  EXPECT_THAT(Update("T", {"K", "V"}, {1, 10}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(CheckConstraintTest, InsertOrUpdate) {
  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "V", "V_STR"}, {1, 10, "10"}));
  EXPECT_THAT(Read("T", {"K", "V", "V_STR"}, Key(1)),
              IsOkAndHoldsRow({1, 10, "10"}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("T", {"K", "V", "V_STR"}, {1, 5, "5"}));
  EXPECT_THAT(Read("T", {"K", "V", "V_STR"}, Key(1)),
              IsOkAndHoldsRow({1, 5, "5"}));
}

TEST_F(CheckConstraintTest, InsertOrUpdateViolation) {
  EXPECT_THAT(InsertOrUpdate("T", {"K", "V", "V_STR"}, {1, -1, "10"}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T`.`v_gt_zero` is violated")));

  EXPECT_THAT(
      InsertOrUpdate("T", {"K", "V", "V_STR"}, {1, 10, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `T`.`v_str_gt_zero` is violated")));
}

TEST_F(CheckConstraintTest, Replace) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V", "V_STR"}, {1, 10, "10"}));
  ZETASQL_ASSERT_OK(Replace("T", {"K", "V", "V_STR"}, {1, 5, "5"}));
  EXPECT_THAT(Read("T", {"K", "V", "V_STR"}, Key(1)),
              IsOkAndHoldsRow({1, 5, "5"}));
}

TEST_F(CheckConstraintTest, ReplaceViolation) {
  ZETASQL_ASSERT_OK(Insert("T", {"K", "V", "V_STR"}, {1, 10, "10"}));
  EXPECT_THAT(Replace("T", {"K", "V", "V_STR"}, {1, -1, "10"}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T`.`v_gt_zero` is violated")));
  EXPECT_THAT(
      Replace("T", {"K", "V", "V_STR"}, {1, 10, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `T`.`v_str_gt_zero` is violated")));
}

TEST_F(CheckConstraintTest, DML) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT T(K, V, V_STR) VALUES (1, 1, '1')"),
                 SqlStatement("UPDATE T SET V = 2, V_STR = '2' WHERE K = 1")}));
  EXPECT_THAT(Query("SELECT K, V, V_STR FROM T"),
              IsOkAndHoldsRows({{1, 2, "2"}}));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT T_GC(K, V) VALUES (1, 10)"),
                       SqlStatement("UPDATE T_GC SET V = 20 WHERE K = 1")}));
  EXPECT_THAT(Query("SELECT K, V, G1, G2, G3 FROM T_GC"),
              IsOkAndHoldsRows({{1, 20, 20, 20, 25}}));
}

TEST_F(CheckConstraintTest, DMLViolation) {
  EXPECT_THAT(CommitDml({SqlStatement("INSERT T(K, V) VALUES (1, -1)")}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T`.`v_gt_zero` is violated")));
  EXPECT_THAT(CommitDml({SqlStatement("INSERT T_GC(K, V) VALUES (1, 1)")}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `T_GC`.`g3_gt_ten` is violated")));
}

TEST_F(CheckConstraintTest, MultipleMutationPerRow) {
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T", {"K", "V", "V_STR"}, 1, -1, "1"),
      MakeUpdate("T", {"K", "V", "V_STR"}, 1, 1, "-2"),
      MakeUpdate("T", {"K", "V", "V_STR"}, 1, 2, "2"),
      MakeInsert("T", {"K", "V", "V_STR"}, 2, 1, "1"),
      MakeDelete("T", Singleton(2)),
  }));
  EXPECT_THAT(ReadAll("T",
                      {
                          "K",
                          "V",
                          "V_STR",
                      }),
              IsOkAndHoldsRows({{1, 2, "2"}}));

  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("T_GC", {"K", "V"}, 1, 0),
      MakeUpdate("T_GC", {"K", "V"}, 1, 10),
  }));

  EXPECT_THAT(ReadAll("T_GC", {"K", "V", "G1", "G2", "G3"}),
              IsOkAndHoldsRows({{1, 10, 10, 10, 15}}));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
