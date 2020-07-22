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

#include <tuple>

#include "gmock/gmock.h"
#include "absl/status/status.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "tests/conformance/common/database_test_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using cloud::spanner::Bytes;
using zetasql_base::testing::StatusIs;

class QueryTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    return SetSchema({
        R"(
          CREATE TABLE Users(
            UserId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64
          ) PRIMARY KEY (UserId)
        )",
        R"(
          CREATE TABLE Threads (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            Starred    BOOL
          ) PRIMARY KEY (UserId, ThreadId),
          INTERLEAVE IN PARENT Users ON DELETE CASCADE
        )",
        R"(
          CREATE TABLE Messages (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            MessageId  INT64 NOT NULL,
            Subject    STRING(MAX),
          ) PRIMARY KEY (UserId, ThreadId, MessageId),
          INTERLEAVE IN PARENT Threads ON DELETE CASCADE
        )",
    });
  }

 protected:
  void PopulateDatabase() {
    ZETASQL_EXPECT_OK(MultiInsert(
        "Users", {"UserId", "Name"},
        {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}}));

    ZETASQL_EXPECT_OK(MultiInsert("Threads", {"UserId", "ThreadId", "Starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));

    ZETASQL_EXPECT_OK(MultiInsert("Messages",
                          {"UserId", "ThreadId", "MessageId", "Subject"},
                          {{1, 1, 1, "a code review"},
                           {1, 1, 2, "Re: a code review"},
                           {1, 2, 1, "Congratulations Douglas"},
                           {1, 3, 1, "Reminder to write feedback"},
                           {1, 4, 1, "Meeting this week"},
                           {2, 1, 1, "Lunch today?"},
                           {2, 2, 1, "Suzanne Collins will be absent"},
                           {3, 1, 1, "Interview Notification"}}));
  }
};

TEST_F(QueryTest, CanCastScalarTypes) {
  EXPECT_THAT(Query(R"(
    SELECT true                     bool_field,
           CAST('abc'  AS STRING)   string_field,
           CAST(-1     AS INT64)    int64_field,
           CAST(1.1    AS FLOAT64)  float64_field)"),
              IsOkAndHoldsRows({{true, "abc", -1, 1.1}}));
}

TEST_F(QueryTest, CanExecuteBasicSelectStatement) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT t.ThreadId, t.Starred "
                    "FROM Users, Threads t "
                    "WHERE Users.UserId = t.UserId "
                    "AND Users.UserId=1 AND t.ThreadId=4"),
              IsOkAndHoldsRows({{4, false}}));
}

TEST_F(QueryTest, CanExecuteNestedSelectStatement) {
  PopulateDatabase();

  using StructType = std::tuple<std::pair<std::string, std::string>>;
  std::vector<StructType> struct_arr{
      StructType{{"Subject", "Re: a code review"}},
      StructType{{"Subject", "a code review"}}};

  EXPECT_THAT(Query("SELECT Threads.ThreadId, Threads.Starred,"
                    "       ARRAY(SELECT AS STRUCT Messages.Subject "
                    "             FROM Messages"
                    "             WHERE Messages.UserId = Threads.UserId AND"
                    "                   Messages.ThreadId = Threads.ThreadId "
                    "             ORDER BY Messages.Subject ASC) Messages "
                    "FROM Users JOIN Threads ON Users.UserId = Threads.UserId "
                    "WHERE Users.UserId=1 AND Threads.ThreadId=1 "
                    "ORDER BY Threads.ThreadId, Threads.Starred"),
              IsOkAndHoldsRows({{1, true, Value(struct_arr)}}));
}

TEST_F(QueryTest, CanExecuteEmptySelectStatement) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT t.ThreadId, t.Starred "
                    "FROM Users, Threads t "
                    "WHERE Users.UserId = t.UserId "
                    "AND Users.UserId=10 AND t.ThreadId=40"),
              IsOkAndHoldsRows({}));
}

TEST_F(QueryTest, CannotExecuteInvalidSelectStatement) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT invalid-identifier "
                    "FROM Users, Threads t "
                    "WHERE Users.UserId = t.UserId "
                    "AND Users.UserId=10 AND t.ThreadId=40"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryTest, HashFunctions) {
  const char hash[] = {'\xb1', '\n', '\x8d', '\xb1', 'd',    '\xe0',
                       'u',    'A',  '\x05', '\xb7', '\xa9', '\x9b',
                       '\xe7', '.',  '?',    '\xe5'};
  EXPECT_THAT(Query("SELECT MD5(\"Hello World\") as md5"),
              IsOkAndHoldsRow({Value(Bytes(hash))}));
}

TEST_F(QueryTest, JSONFunctions) {
  EXPECT_THAT(Query(R"(SELECT JSON_VALUE('{"a": {"b": "world"}}', '$.a.b'))"),
              IsOkAndHoldsRow({Value("world")}));
}

TEST_F(QueryTest, FormatFunction) {
  EXPECT_THAT(Query(R"(SELECT FORMAT('%s %s', 'hello', 'world'))"),
              IsOkAndHoldsRow({Value("hello world")}));

  EXPECT_THAT(Query(R"(SELECT SAFE.FORMAT('%s %s', 'hello', 'world'))"),
              IsOkAndHoldsRow({Value("hello world")}));
}

TEST_F(QueryTest, DISABLED_NETFunctions) {
  const std::string query = R"(SELECT NET.IPV4_TO_INT64(b"\x00\x00\x00\x00"))";
  EXPECT_THAT(Query(query), StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryTest, CanReturnArrayOfStructTypedColumns) {
  using EmptyStruct = std::tuple<>;
  EXPECT_THAT(
      Query("SELECT ARRAY(SELECT STRUCT<>())"),
      IsOkAndHoldsRow({Value(std::vector<EmptyStruct>{EmptyStruct{}})}));

  using SimpleStruct = std::tuple<int64_t>;
  EXPECT_THAT(
      Query("SELECT ARRAY(SELECT STRUCT<INT64>(1))"),
      IsOkAndHoldsRow({Value(std::vector<SimpleStruct>{SimpleStruct{1}})}));
}

TEST_F(QueryTest, FunctionAliasesAreAvailable) {
  EXPECT_THAT(Query("SELECT CHARACTER_LENGTH('abc')"), IsOkAndHoldsRow({3}));
  EXPECT_THAT(Query("SELECT CHAR_LENGTH('abc')"), IsOkAndHoldsRow({3}));

  EXPECT_THAT(Query("SELECT POWER(2,2)"), IsOkAndHoldsRow({4.0}));
  EXPECT_THAT(Query("SELECT POW(2,2)"), IsOkAndHoldsRow({4.0}));

  EXPECT_THAT(Query("SELECT CEILING(1.6)"), IsOkAndHoldsRow({2.0}));
  EXPECT_THAT(Query("SELECT CEIL(1.6)"), IsOkAndHoldsRow({2.0}));
}

TEST_F(QueryTest, Params) {
  // The majority of the test cases set the parameter to a certain value and
  // expect the returned row to contain said value. This lambda just captures
  // that pattern.
  auto expect_selected = [this](Value v) {
    EXPECT_THAT(QueryWithParams("SELECT @param",
                                {{"param", v}, {"unused_param", Value(6)}}),
                IsOkAndHoldsRow({v}));
  };

  expect_selected(Value(6));
  expect_selected(Value("str"));
  expect_selected(Value(""));
  expect_selected(Value(Bytes("bytes")));
  expect_selected(
      Value(MakeTimestamp(absl::ToChronoTime(absl::FromUnixNanos(1)))));
  expect_selected(Value(MakeTimestamp(absl::ToChronoTime(
      absl::FromCivil(absl::CivilDay(1970, 1, 11), absl::FixedTimeZone(0))))));
  expect_selected(Value(std::vector<bool>{true, false}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param * @param",
                      {{"param", Value(-2.0)}, {"unused_param", Value(6)}}),
      IsOkAndHoldsRow({4.0}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param * @param",
                      {{"param", Value(-0.0)}, {"unused_param", Value(6)}}),
      IsOkAndHoldsRow({0.0}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param * @param",
                      {{"param", Value(2.0)}, {"unused_param", Value(6)}}),
      IsOkAndHoldsRow({4.0}));

  EXPECT_THAT(QueryWithParams("SELECT @`p\\`ram`", {{"p`ram", Value(6)}}),
              IsOkAndHoldsRow({6}));

  EXPECT_THAT(
      QueryWithParams("SELECT @param", {{std::string(130, 'x'), Value(6)}}),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(QueryTest, DefaultTimeZoneIsPacificTime) {
  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(Query("SELECT TIMESTAMP '2020-12-01'"),
              IsOkAndHoldsRow({Value(MakeTimestamp(absl::ToChronoTime(
                  absl::FromCivil(absl::CivilDay(2020, 12, 1), time_zone))))}));
}

TEST_F(QueryTest, CheckQuerySizeLimitsAreEnforced) {
  // Check that the query size limits enforcement is in place.
  auto many_joins_query = [](int num_joins) {
    std::string join_query = "SELECT t0.UserId FROM Users AS t0";
    for (int i = 1; i <= num_joins; ++i) {
      join_query = join_query + "\n" +
                   absl::StrFormat("JOIN Users AS t%d ON t%d.UserId = t%d.Age ",
                                   i, i - 1, i);
    }
    return join_query;
  };
  EXPECT_THAT(Query(many_joins_query(/*num_joins=*/16)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("joins exceeds")));
}

TEST_F(QueryTest, QueryStringSizeLimit) {
  auto query = absl::Substitute("SELECT \"$0\"", std::string(1024 * 1024, 'a'));
  EXPECT_THAT(Query(query),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Query string length")));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
