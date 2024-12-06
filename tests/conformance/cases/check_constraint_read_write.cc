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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {
namespace {

using zetasql_base::testing::StatusIs;

class CheckConstraintTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 protected:
  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("check_constraint.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectCheckConstraintTests, CheckConstraintTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<CheckConstraintTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(CheckConstraintTest, Insert) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v"}, {1, 10}));
  EXPECT_THAT(Read("t", {"k", "v"}, Key(1)), IsOkAndHoldsRow({1, 10}));

  ZETASQL_ASSERT_OK(Insert("t", {"k", "v_str"}, {2, "10"}));
  EXPECT_THAT(Read("t", {"k", "v_str"}, Key(2)), IsOkAndHoldsRow({2, "10"}));

  ZETASQL_ASSERT_OK(Insert("t_gc", {"k", "v"}, {1, 10}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // g2 is not defined in PG as generated columns cannot reference other
    // generated columns.
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, 10, 10, 15}));
  } else {
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g2", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, 10, 10, 10, 15}));
  }
}

TEST_P(CheckConstraintTest, InsertNull) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v"}, {1, Null<std::int64_t>()}));
  EXPECT_THAT(Read("t", {"k", "v"}, Key(1)),
              IsOkAndHoldsRow({1, Null<std::int64_t>()}));

  ZETASQL_ASSERT_OK(Insert("t_gc", {"k", "v"}, {1, Null<std::int64_t>()}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // g2 is not defined in PG as generated columns cannot reference other
    // generated columns.
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, Null<std::int64_t>(), Null<std::int64_t>(),
                                 Null<std::int64_t>()}));
  } else {
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g2", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, Null<std::int64_t>(), Null<std::int64_t>(),
                                 Null<std::int64_t>(), Null<std::int64_t>()}));
  }
}

TEST_P(CheckConstraintTest, InsertWithCoercion) {
  // v_left_shift_gt_zero ZETASQL_VLOG ((v << 1) > 0) is OK.
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v"}, {1, (int32_t)1073741824}));
  EXPECT_THAT(Read("t", {"k", "v"}, Key(1)),
              IsOkAndHoldsRow({1, (int64_t)1073741824}));
}

TEST_P(CheckConstraintTest, InsertViolation) {
  EXPECT_THAT(Insert("t", {"k", "v"}, {1, -1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t`.`v_gt_zero` is violated")));
  EXPECT_THAT(
      Insert("t", {"k", "v_str"}, {1, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `t`.`v_str_gt_zero` is violated")));

  EXPECT_THAT(Insert("t_gc", {"k", "v"}, {1, 1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t_gc`.`g3_gt_ten` is violated")));
}

TEST_P(CheckConstraintTest, Update) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("t", {"k", "v"}, {1, 5}));
  EXPECT_THAT(Read("t", {"k", "v"}, Key(1)), IsOkAndHoldsRow({1, 5}));

  ZETASQL_ASSERT_OK(Insert("t", {"k", "v_str"}, {2, "10"}));
  ZETASQL_ASSERT_OK(Update("t", {"k", "v_str"}, {2, "5"}));
  EXPECT_THAT(Read("t", {"k", "v_str"}, Key(2)), IsOkAndHoldsRow({2, "5"}));

  ZETASQL_ASSERT_OK(Insert("t_gc", {"k", "v"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("t_gc", {"k", "v"}, {1, 15}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // g2 is not defined in PG as generated columns cannot reference other
    // generated columns.
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, 15, 15, 20}));
  } else {
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g2", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, 15, 15, 15, 20}));
  }
}

TEST_P(CheckConstraintTest, UpdateNull) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("t", {"k", "v"}, {1, Null<std::int64_t>()}));
  EXPECT_THAT(Read("t", {"k", "v"}, Key(1)),
              IsOkAndHoldsRow({1, Null<std::int64_t>()}));

  ZETASQL_ASSERT_OK(Insert("t_gc", {"k", "v"}, {1, 10}));
  ZETASQL_ASSERT_OK(Update("t_gc", {"k", "v"}, {1, Null<std::int64_t>()}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // g2 is not defined in PG as generated columns cannot reference other
    // generated columns.
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, Null<std::int64_t>(), Null<std::int64_t>(),
                                 Null<std::int64_t>()}));
  } else {
    EXPECT_THAT(Read("t_gc", {"k", "v", "g1", "g2", "g3"}, Key(1)),
                IsOkAndHoldsRow({1, Null<std::int64_t>(), Null<std::int64_t>(),
                                 Null<std::int64_t>(), Null<std::int64_t>()}));
  }

  ZETASQL_ASSERT_OK(Insert("t", {"k", "v_str"}, {2, "10"}));
  ZETASQL_ASSERT_OK(Update("t", {"k", "v_str"}, {2, Null<std::string>()}));
  EXPECT_THAT(Read("t", {"k", "v_str"}, Key(2)),
              IsOkAndHoldsRow({2, Null<std::string>()}));
}

TEST_P(CheckConstraintTest, UpdateViolation) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v"}, {1, 10}));
  EXPECT_THAT(Update("t", {"k", "v"}, {1, -1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t`.`v_gt_zero` is violated")));

  ZETASQL_ASSERT_OK(Insert("t_gc", {"k", "v"}, {1, 10}));
  EXPECT_THAT(Update("t_gc", {"k", "v"}, {1, 1}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t_gc`.`g3_gt_ten` is violated")));

  ZETASQL_ASSERT_OK(Insert("t", {"k", "v_str"}, {2, "10"}));
  EXPECT_THAT(
      Update("t", {"k", "v_str"}, {2, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `t`.`v_str_gt_zero` is violated")));
}

TEST_P(CheckConstraintTest, UpdateNonExistingKey) {
  EXPECT_THAT(Update("t", {"k", "v"}, {1, 10}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(CheckConstraintTest, InsertOrUpdate) {
  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "v", "v_str"}, {1, 10, "10"}));
  EXPECT_THAT(Read("t", {"k", "v", "v_str"}, Key(1)),
              IsOkAndHoldsRow({1, 10, "10"}));

  ZETASQL_ASSERT_OK(InsertOrUpdate("t", {"k", "v", "v_str"}, {1, 5, "5"}));
  EXPECT_THAT(Read("t", {"k", "v", "v_str"}, Key(1)),
              IsOkAndHoldsRow({1, 5, "5"}));
}

TEST_P(CheckConstraintTest, InsertOrUpdateViolation) {
  EXPECT_THAT(InsertOrUpdate("t", {"k", "v", "v_str"}, {1, -1, "10"}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t`.`v_gt_zero` is violated")));

  EXPECT_THAT(
      InsertOrUpdate("t", {"k", "v", "v_str"}, {1, 10, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `t`.`v_str_gt_zero` is violated")));
}

TEST_P(CheckConstraintTest, Replace) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v", "v_str"}, {1, 10, "10"}));
  ZETASQL_ASSERT_OK(Replace("t", {"k", "v", "v_str"}, {1, 5, "5"}));
  EXPECT_THAT(Read("t", {"k", "v", "v_str"}, Key(1)),
              IsOkAndHoldsRow({1, 5, "5"}));
}

TEST_P(CheckConstraintTest, ReplaceViolation) {
  ZETASQL_ASSERT_OK(Insert("t", {"k", "v", "v_str"}, {1, 10, "10"}));
  EXPECT_THAT(Replace("t", {"k", "v", "v_str"}, {1, -1, "10"}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t`.`v_gt_zero` is violated")));
  EXPECT_THAT(
      Replace("t", {"k", "v", "v_str"}, {1, 10, "-1"}),
      StatusIs(absl::StatusCode::kOutOfRange,
               testing::HasSubstr(
                   "Check constraint `t`.`v_str_gt_zero` is violated")));
}

TEST_P(CheckConstraintTest, DML) {
  ZETASQL_ASSERT_OK(
      CommitDml({SqlStatement("INSERT INTO t(k, v, v_str) VALUES (1, 1, '1')"),
                 SqlStatement("UPDATE t SET v = 2, v_str = '2' WHERE k = 1")}));
  EXPECT_THAT(Query("SELECT k, v, v_str FROM t"),
              IsOkAndHoldsRows({{1, 2, "2"}}));

  ZETASQL_ASSERT_OK(CommitDml({SqlStatement("INSERT INTO t_gc(k, v) VALUES (1, 10)"),
                       SqlStatement("UPDATE t_gc SET v = 20 WHERE k = 1")}));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // g2 is not defined in PG as generated columns cannot reference other
    // generated columns.
    EXPECT_THAT(Query("SELECT k, v, g1, g3 FROM t_gc"),
                IsOkAndHoldsRows({{1, 20, 20, 25}}));
  } else {
    EXPECT_THAT(Query("SELECT k, v, g1, g2, g3 FROM t_gc"),
                IsOkAndHoldsRows({{1, 20, 20, 20, 25}}));
  }
}

TEST_P(CheckConstraintTest, DMLViolation) {
  EXPECT_THAT(CommitDml({SqlStatement("INSERT INTO t(k, v) VALUES (1, -1)")}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t`.`v_gt_zero` is violated")));
  EXPECT_THAT(CommitDml({SqlStatement("INSERT INTO t_gc(k, v) VALUES (1, 1)")}),
              StatusIs(absl::StatusCode::kOutOfRange,
                       testing::HasSubstr(
                           "Check constraint `t_gc`.`g3_gt_ten` is violated")));
}

TEST_P(CheckConstraintTest, MultipleMutationPerRow) {
  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("t", {"k", "v", "v_str"}, 1, -1, "1"),
      MakeUpdate("t", {"k", "v", "v_str"}, 1, 1, "-2"),
      MakeUpdate("t", {"k", "v", "v_str"}, 1, 2, "2"),
      MakeInsert("t", {"k", "v", "v_str"}, 2, 1, "1"),
      MakeDelete("t", Singleton(2)),
  }));
  EXPECT_THAT(ReadAll("t",
                      {
                          "k",
                          "v",
                          "v_str",
                      }),
              IsOkAndHoldsRows({{1, 2, "2"}}));

  ZETASQL_ASSERT_OK(Commit({
      MakeInsert("t_gc", {"k", "v"}, 1, 0),
      MakeUpdate("t_gc", {"k", "v"}, 1, 10),
  }));

  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    // g2 is not defined in PG as generated columns cannot reference other
    // generated columns.
    EXPECT_THAT(ReadAll("t_gc", {"k", "v", "g1", "g3"}),
                IsOkAndHoldsRows({{1, 10, 10, 15}}));
  } else {
    EXPECT_THAT(ReadAll("t_gc", {"k", "v", "g1", "g2", "g3"}),
                IsOkAndHoldsRows({{1, 10, 10, 10, 15}}));
  }
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
