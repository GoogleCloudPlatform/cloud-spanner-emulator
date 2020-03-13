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
  zetasql_base::Status SetUpDatabase() override {
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
        )"});
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
              StatusIs(zetasql_base::StatusCode::kInvalidArgument));
}

TEST_F(QueryTest, HashFunctions) {
  const unsigned char hash[] = {'\xb1', '\n', '\x8d', '\xb1', 'd',    '\xe0',
                                'u',    'A',  '\x05', '\xb7', '\xa9', '\x9b',
                                '\xe7', '.',  '?',    '\xe5'};
  EXPECT_THAT(Query("SELECT MD5(\"Hello World\") as md5"),
              IsOkAndHoldsRow({Value(Bytes(hash))}));
}

TEST_F(QueryTest, JSONFunctions) {
  EXPECT_THAT(Query(R"(SELECT JSON_VALUE('{"a": {"b": "world"}}', '$.a.b'))"),
              IsOkAndHoldsRow({Value("world")}));
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

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
