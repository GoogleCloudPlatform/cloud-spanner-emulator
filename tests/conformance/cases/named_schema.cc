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

class NamedSchemaTest : public DatabaseTest {
  absl::Status SetUpDatabase() override {
    feature_flags_setter_ = std::make_unique<ScopedEmulatorFeatureFlagsSetter>(
        EmulatorFeatureFlags::Flags{
            .enable_views = true,
        });
    return absl::OkStatus();
  }

 protected:
  absl::StatusOr<int64_t> GetCurrentSequenceState(const std::string& name) {
    std::string query;
    query = "SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE $0)";
    ZETASQL_ASSIGN_OR_RETURN(std::vector<ValueRow> query_result,
                     Query(absl::Substitute(query, name)));
    ZETASQL_RET_CHECK(query_result[0].values()[0].get<int64_t>().ok());
    return *(query_result[0].values()[0].get<int64_t>());
  }

  std::unique_ptr<ScopedEmulatorFeatureFlagsSetter> feature_flags_setter_;
};

TEST_F(NamedSchemaTest, BasicUsingDml) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE SCHEMA mynamedschema)"}));
  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE TABLE mynamedschema.t (col1 INT64, col2 INT64) PRIMARY KEY (col1))"}));

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

TEST_F(NamedSchemaTest, Basic) {
  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE SCHEMA mynamedschema)",
       R"(CREATE TABLE mynamedschema.t (col1 INT64, col2 INT64) PRIMARY KEY (col1))"}));

  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {3, 4}));

  // Verify that "as" statement works.
  EXPECT_THAT(Query("SELECT t.col1, t.col2 FROM mynamedschema.t as t"),
              IsOkAndHoldsUnorderedRows({{1, 2}, {3, 4}}));

  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE VIEW mynamedschema.v SQL SECURITY INVOKER AS SELECT t.col1 FROM
      mynamedschema.t)"}));

  EXPECT_THAT(Query("SELECT * FROM mynamedschema.v"),
              IsOkAndHoldsUnorderedRows({{1}, {3}}));

  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE UNIQUE INDEX mynamedschema.idx ON mynamedschema.t (col2 DESC))"}));
  EXPECT_THAT(ReadAllWithIndex("mynamedschema.t", "mynamedschema.idx",
                               {"col1", "col2"}),
              IsOkAndHoldsRows({{3, 4}, {1, 2}}));
}

TEST_F(NamedSchemaTest, TableWithConstraint) {
  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE SCHEMA mynamedschema)",
       R"(CREATE TABLE mynamedschema.t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
       R"(ALTER TABLE mynamedschema.t ADD CONSTRAINT C CHECK (col1 < 10))"}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Insert("mynamedschema.t", {"col1", "col2"}, {11, 2}),
              StatusIs(StatusCode::kOutOfRange));
}

TEST_F(NamedSchemaTest, TableWithSynonym) {
  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE SCHEMA mynamedschema)",
       R"(CREATE TABLE mynamedschema.t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
       R"(ALTER TABLE mynamedschema.t ADD SYNONYM syn)"}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Query("SELECT * FROM syn"), IsOkAndHoldsUnorderedRows({{1, 2}}));
}

TEST_F(NamedSchemaTest, TableWithGeneratedAndDefaultColumn) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE SCHEMA mynamedschema)",
                          R"(CREATE TABLE mynamedschema.t (
       col1 INT64,
       col2 INT64,
       col3 INT64 AS (col1 + col2) STORED,
       col4 INT64 DEFAULT (10)
       ) PRIMARY KEY (col1))"}));
  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"col1", "col2"}, {1, 2}));
  EXPECT_THAT(Query("SELECT * FROM mynamedschema.t"),
              IsOkAndHoldsUnorderedRows({{1, 2, 3, 10}}));
}

TEST_F(NamedSchemaTest, TableWithSequence) {
  ZETASQL_ASSERT_OK(UpdateSchema({R"(CREATE SCHEMA mynamedschema)",
                          R"(CREATE SEQUENCE mynamedschema.myseq OPTIONS (
        sequence_kind = "bit_reversed_positive"))",
                          R"(CREATE TABLE mynamedschema.t(
                          int64_col INT64 NOT NULL DEFAULT
                (GET_NEXT_SEQUENCE_VALUE (SEQUENCE mynamedschema.myseq)),
                string_col string(max))
                PRIMARY KEY (int64_col)
      )"}));

  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"string_col"}, {"one"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t count_1,
                       GetCurrentSequenceState("mynamedschema.myseq"));

  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"string_col"}, {"two"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t count_2,
                       GetCurrentSequenceState("mynamedschema.myseq"));

  ZETASQL_ASSERT_OK(Insert("mynamedschema.t", {"string_col"}, {"four"}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t count_3,
                       GetCurrentSequenceState("mynamedschema.myseq"));

  EXPECT_GE(count_3, count_2);
  EXPECT_GE(count_2, count_1);

  EXPECT_THAT(Query("SELECT string_col FROM mynamedschema.t"),
              IsOkAndHoldsUnorderedRows({{"one"}, {"two"}, {"four"}}));
}

TEST_F(NamedSchemaTest, TableWithForeignKey) {
  ZETASQL_ASSERT_OK(UpdateSchema({
      R"(CREATE SCHEMA mynamedschema1)",
      R"(CREATE TABLE mynamedschema1.t1 (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
      R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
      R"(ALTER TABLE t ADD CONSTRAINT C1 FOREIGN KEY (col2) REFERENCES
      mynamedschema1.t1(col1))",
  }));

  ZETASQL_ASSERT_OK(Insert("mynamedschema1.t1", {"col1", "col2"}, {1, 2}));
  ZETASQL_ASSERT_OK(Insert("t", {"col1", "col2"}, {5, 1}));
  EXPECT_THAT(Insert("t", {"col1", "col2"}, {11, 5}),
              StatusIs(StatusCode::kFailedPrecondition));

  // Verify cross schema foreign keys work.
  ZETASQL_ASSERT_OK(UpdateSchema(
      {R"(CREATE SCHEMA mynamedschema2)",
       R"(CREATE TABLE mynamedschema2.t2 (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
       R"(ALTER TABLE mynamedschema2.t2 ADD CONSTRAINT C2 FOREIGN KEY (col2)
      REFERENCES mynamedschema1.t1(col1))"}));

  ZETASQL_ASSERT_OK(Insert("mynamedschema2.t2", {"col1", "col2"}, {5, 1}));
  EXPECT_THAT(Insert("mynamedschema2.t2", {"col1", "col2"}, {11, 5}),
              StatusIs(StatusCode::kFailedPrecondition));
}

}  // namespace
}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
