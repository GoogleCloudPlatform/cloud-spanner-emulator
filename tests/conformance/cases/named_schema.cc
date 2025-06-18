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
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::absl::StatusCode;
using ::zetasql_base::testing::StatusIs;

class NamedSchemaTest
    : public DatabaseTest,
      public ::testing::WithParamInterface<database_api::DatabaseDialect> {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_views = true,
        });
    return SetSchemaFromFile("named_schema.test");
  }

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

 protected:
  absl::StatusOr<int64_t> GetCurrentSequenceState(const std::string& name) {
    std::string query;
    if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
      query = "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE $0)";
    } else {
      query = "SELECT spanner.get_internal_sequence_state('$0')";
    }
    ZETASQL_ASSIGN_OR_RETURN(std::vector<ValueRow> query_result,
                     Query(absl::Substitute(query, name)));
    ZETASQL_RET_CHECK(query_result[0].values()[0].get<int64_t>().ok());
    return *(query_result[0].values()[0].get<int64_t>());
  }

  std::unique_ptr<ScopedEmulatorFeatureFlagsSetter> feature_flags_setter_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectNamedSchemaTests, NamedSchemaTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<NamedSchemaTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(NamedSchemaTest, BasicUsingDml) {
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {3, 4}));

  EXPECT_THAT(Query("SELECT t.col1, t.col2 FROM mynamedschema.t"),
              IsOkAndHoldsUnorderedRows({{1, 2}, {3, 4}}));

  // Using DML to insert rows.
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("INSERT INTO mynamedschema.t(col1, col2) VALUES (5, 6)")}));
  EXPECT_THAT(Query("SELECT t.col1, t.col2 FROM mynamedschema.t"),
              IsOkAndHoldsUnorderedRows({{1, 2}, {3, 4}, {5, 6}}));

  // Using DML to update rows.
  ZETASQL_EXPECT_OK(CommitDml(
      {SqlStatement("UPDATE mynamedschema.t SET col2 = 10 WHERE col1 = 5")}));
  EXPECT_THAT(Query("SELECT t.col1, t.col2 FROM mynamedschema.t"),
              IsOkAndHoldsUnorderedRows({{1, 2}, {3, 4}, {5, 10}}));

  // Using DML to delete rows.
  ZETASQL_EXPECT_OK(
      CommitDml({SqlStatement("DELETE FROM mynamedschema.t WHERE col1 = 1")}));
  EXPECT_THAT(Query("SELECT t.col1, t.col2 FROM mynamedschema.t"),
              IsOkAndHoldsUnorderedRows({{3, 4}, {5, 10}}));
}

TEST_P(NamedSchemaTest, Basic) {
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {3, 4}));

  // Verify that "as" statement works.
  EXPECT_THAT(Query("SELECT t.col1, t.col2 FROM mynamedschema.t as t"),
              IsOkAndHoldsUnorderedRows({{1, 2}, {3, 4}}));

  EXPECT_THAT(Query("SELECT * FROM mynamedschema.v"),
              IsOkAndHoldsUnorderedRows({{1}, {3}}));

  EXPECT_THAT(ReadAllWithIndex("mynamedschema.t", "mynamedschema.idx",
                               {"col1", "col2"}),
              IsOkAndHoldsRows({{3, 4}, {1, 2}}));
}

TEST_P(NamedSchemaTest, TableWithConstraint) {
  ZETASQL_ASSERT_OK(Insert("mynamedschema.con_t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Insert("mynamedschema.con_t", {"col1", "col2"}, {11, 2}),
              StatusIs(StatusCode::kOutOfRange));
}

TEST_P(NamedSchemaTest, TableWithSynonym) {
  std::string update_statement;
  if (dialect_ == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    update_statement =
        R"(ALTER TABLE mynamedschema.t ADD SYNONYM mynamedschema.syn)";
  } else {
    update_statement =
        R"(ALTER TABLE mynamedschema.t ADD SYNONYM "mynamedschema.syn")";
  }
  ZETASQL_ASSERT_OK(UpdateSchema({update_statement}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Query("SELECT * FROM mynamedschema.syn"),
              IsOkAndHoldsUnorderedRows({{1, 2}}));
}

TEST_P(NamedSchemaTest, TableWithGeneratedAndDefaultColumn) {
  ZETASQL_ASSERT_OK(Insert("mynamedschema.gen_t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Query("SELECT * FROM mynamedschema.gen_t"),
              IsOkAndHoldsUnorderedRows({{1, 2, 3, 10}}));
}

TEST_P(NamedSchemaTest, TableWithSequence) {
  ZETASQL_ASSERT_OK(Insert("mynamedschema.seq_t", {"string_col"}, {"one"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t count_1,
                       GetCurrentSequenceState("mynamedschema.myseq"));

  ZETASQL_ASSERT_OK(Insert("mynamedschema.seq_t", {"string_col"}, {"two"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t count_2,
                       GetCurrentSequenceState("mynamedschema.myseq"));

  ZETASQL_ASSERT_OK(Insert("mynamedschema.seq_t", {"string_col"}, {"four"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t count_3,
                       GetCurrentSequenceState("mynamedschema.myseq"));

  EXPECT_GE(count_3, count_2);
  EXPECT_GE(count_2, count_1);

  EXPECT_THAT(Query("SELECT string_col FROM mynamedschema.seq_t"),
              IsOkAndHoldsUnorderedRows({{"one"}, {"two"}, {"four"}}));
}

TEST_P(NamedSchemaTest, TableWithForeignKey) {
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("fk_t", {"col1", "col2"}, {5, 1}));
  EXPECT_THAT(Insert("fk_t", {"col1", "col2"}, {11, 5}),
              StatusIs(StatusCode::kFailedPrecondition));

  // Verify cross schema foreign keys work.
  ZETASQL_ASSERT_OK(Insert("mynamedschema2.fk_t", {"col1", "col2"}, {5, 1}));
  EXPECT_THAT(Insert("mynamedschema2.fk_t", {"col1", "col2"}, {11, 5}),
              StatusIs(StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
