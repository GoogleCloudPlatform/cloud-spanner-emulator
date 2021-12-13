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
#include "absl/strings/str_format.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/numeric.h"
#include "tests/common/scoped_feature_flags_setter.h"
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
    EmulatorFeatureFlags::Flags flags;
    flags.enable_json_type = true;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);

    return SetSchema({
        R"(
          CREATE TABLE Users(
            UserId     INT64 NOT NULL,
            Name       STRING(MAX),
            Age        INT64,
          ) PRIMARY KEY (UserId)
        )",
        R"(
          CREATE TABLE Threads (
            UserId     INT64 NOT NULL,
            ThreadId   INT64 NOT NULL,
            Starred    BOOL,
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
        R"(
          CREATE TABLE ScalarTypesTable (
            intVal INT64 NOT NULL,
            boolVal BOOL,
            bytesVal BYTES(MAX),
            dateVal DATE,
            floatVal FLOAT64,
            stringVal STRING(MAX),
            numericVal NUMERIC,
            timestampVal TIMESTAMP,
            jsonVal JSON,
          ) PRIMARY KEY(intVal)
        )",
        R"(
          CREATE TABLE NumericTable(
            key     NUMERIC,
            val     INT64,
          ) PRIMARY KEY (key)
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

    ZETASQL_EXPECT_OK(MultiInsert(
        "ScalarTypesTable",
        {"intVal", "boolVal", "bytesVal", "dateVal", "floatVal", "stringVal",
         "numericVal", "timestampVal", "jsonVal"},
        {{0, Null<bool>(), Null<Bytes>(), Null<Date>(), Null<double>(),
          Null<std::string>(), Null<Numeric>(), Null<Timestamp>(),
          Null<Json>()},
         {1, true, Bytes("bytes"), Date(2020, 12, 1), 345.123, "stringValue",
          cloud::spanner::MakeNumeric("123.456789").value(), Timestamp(),
          Json("{\"key\":123}")}}));

    ZETASQL_EXPECT_OK(MultiInsert("NumericTable", {"key", "val"},
                          {{Null<Numeric>(), Null<std::int64_t>()},
                           {cloud::spanner::MakeNumeric("-12.3").value(), -1},
                           {cloud::spanner::MakeNumeric("0").value(), 0},
                           {cloud::spanner::MakeNumeric("12.3").value(), 1}}));
  }
};

TEST_F(QueryTest, CanReadScalarTypes) {
  PopulateDatabase();
  EXPECT_THAT(
      Query("SELECT * FROM ScalarTypesTable ORDER BY intVal ASC"),
      IsOkAndHoldsRows(
          {{0, Null<bool>(), Null<Bytes>(), Null<Date>(), Null<double>(),
            Null<std::string>(), Null<Numeric>(), Null<Timestamp>(),
            Null<Json>()},
           {1, true, Bytes("bytes"), Date(2020, 12, 1), 345.123, "stringValue",
            cloud::spanner::MakeNumeric("123.456789").value(), Timestamp(),
            Json("{\"key\":123}")}}));
}

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
  EXPECT_THAT(Query(R"(SELECT JSON_QUERY(JSON '{"a":"str", "b":2}', '$.a'))"),
              IsOkAndHoldsRow({Value(Json(R"("str")"))}));
  EXPECT_THAT(Query(R"(SELECT JSON_QUERY('{"a":"str", "b":2}', '$.a'))"),
              IsOkAndHoldsRow({Value(R"("str")")}));
  EXPECT_THAT(Query(R"(SELECT JSON_QUERY(JSON '{"a":null}', "$.a"))"),
              IsOkAndHoldsRow({Value(Json("null"))}));
  EXPECT_THAT(Query(R"(SELECT JSON_QUERY(JSON '{"a":null}', "$.b"))"),
              IsOkAndHoldsRow({Null<Json>()}));

  EXPECT_THAT(Query(R"(SELECT JSON_VALUE(JSON '{"a":"str", "b":2}', '$.a'))"),
              IsOkAndHoldsRow({Value("str")}));
  EXPECT_THAT(Query(R"(SELECT JSON_VALUE(JSON '{"a":null}', "$.a"))"),
              IsOkAndHoldsRow({Null<std::string>()}));
  EXPECT_THAT(Query(R"(SELECT JSON_VALUE(JSON '{"a":null}', "$"))"),
              IsOkAndHoldsRow({Null<std::string>()}));

  EXPECT_THAT(Query(R"(SELECT PARSE_JSON('{"a":[1,2],"b":"str"}'))"),
              IsOkAndHoldsRow({Value(Json(R"({"a":[1,2],"b":"str"})"))}));
  EXPECT_THAT(Query(R"(SELECT PARSE_JSON('{"id":123456789012345678901}'))"),
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kOutOfRange));
  EXPECT_THAT(Query(R"(
    SELECT PARSE_JSON('{"id":123456789012345678901}', wide_number_mode=>'exact')
  )"),
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      Query(R"(
    SELECT PARSE_JSON('{"id":123456789012345678901}', wide_number_mode=>'round')
  )"),
      IsOkAndHoldsRow({Value(Json(R"({"id":1.2345678901234568e+20})"))}));

  EXPECT_THAT(Query(R"(SELECT TO_JSON(123))"),
              IsOkAndHoldsRow({Value(Json("123"))}));
  EXPECT_THAT(Query(R"(
    SELECT TO_JSON(12345678901234567, stringify_wide_numbers=>FALSE)
  )"),
              IsOkAndHoldsRow({Value(Json("12345678901234567"))}));
  EXPECT_THAT(Query(R"(
    SELECT TO_JSON(12345678901234567, stringify_wide_numbers=>TRUE)
  )"),
              IsOkAndHoldsRow({Value(Json("\"12345678901234567\""))}));
  EXPECT_THAT(Query(R"(
    SELECT TO_JSON(NUMERIC "123456789.123456789", stringify_wide_numbers=>FALSE)
  )"),
              StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(Query(R"(SELECT TO_JSON_STRING(JSON '{"a":"str", "b":2}'))"),
              IsOkAndHoldsRow({R"({"a":"str","b":2})"}));

  EXPECT_THAT(Query(R"(SELECT FORMAT("%'p", JSON '{"a":"str", "b":2}'))"),
              IsOkAndHoldsRow({R"({"a":"str","b":2})"}));
  EXPECT_THAT(Query(R"(SELECT FORMAT("%'P", JSON '{"a":"str", "b":2}'))"),
              IsOkAndHoldsRow({"{\n"
                               "  \"a\": \"str\",\n"
                               "  \"b\": 2\n"
                               "}"}));
  EXPECT_THAT(Query(R"(SELECT FORMAT("%'t", JSON '{"a":"str", "b":2}'))"),
              IsOkAndHoldsRow({R"({"a":"str","b":2})"}));
  EXPECT_THAT(Query(R"(SELECT FORMAT("%'T", JSON '{"a":"str", "b":2}'))"),
              IsOkAndHoldsRow({R"(JSON '{"a":"str","b":2}')"}));

  EXPECT_THAT(Query(R"(SELECT JSON '{"a":"str", "b":[1,2,3]}'["a"])"),
              IsOkAndHoldsRow({Value(Json(R"("str")"))}));
  EXPECT_THAT(Query(R"(SELECT JSON '{"a":"str", "b":[1,2,3]}'["b"])"),
              IsOkAndHoldsRow({Value(Json(R"([1,2,3])"))}));
  EXPECT_THAT(Query(R"(SELECT JSON '{"a":"str", "b":[1,2,3]}'["b"][1])"),
              IsOkAndHoldsRow({Value(Json("2"))}));

  EXPECT_THAT(Query(R"(SELECT JSON '{"a":"str", "b":[1,2,3]}'.a)"),
              IsOkAndHoldsRow({Value(Json(R"("str")"))}));
  EXPECT_THAT(Query(R"(SELECT JSON '{"a":"str", "b":[1,2,3]}'.b)"),
              IsOkAndHoldsRow({Value(Json("[1,2,3]"))}));
  EXPECT_THAT(Query(R"(SELECT JSON '{"a":"str", "b":[1,2,3]}'.c)"),
              IsOkAndHoldsRow({Value(Null<Json>())}));
}

TEST_F(QueryTest, FormatFunction) {
  EXPECT_THAT(Query(R"(SELECT FORMAT('%s %s', 'hello', 'world'))"),
              IsOkAndHoldsRow({Value("hello world")}));

  EXPECT_THAT(Query(R"(SELECT SAFE.FORMAT('%s %s', 'hello', 'world'))"),
              IsOkAndHoldsRow({Value("hello world")}));
}

TEST_F(QueryTest, DateTimestampArithmeticFunctions) {
  EXPECT_THAT(Query(R"(SELECT DATE_ADD(DATE '2020-12-25', INTERVAL 5 DAY))"),
              IsOkAndHoldsRow({Value(absl::CivilDay(2020, 12, 30))}));

  EXPECT_THAT(Query(R"(SELECT DATE_SUB(DATE '2020-12-25', INTERVAL 5 DAY))"),
              IsOkAndHoldsRow({Value(absl::CivilDay(2020, 12, 20))}));

  EXPECT_THAT(
      Query(R"(SELECT DATE_DIFF(DATE '2020-12-25', DATE '2020-12-20', DAY))"),
      IsOkAndHoldsRow({Value(5)}));

  EXPECT_THAT(Query(R"(SELECT DATE_TRUNC(DATE '2020-12-25', MONTH))"),
              IsOkAndHoldsRow({Value(absl::CivilDay(2020, 12, 1))}));

  EXPECT_THAT(Query(R"(SELECT EXTRACT(WEEK FROM DATE '2020-12-25'))"),
              IsOkAndHoldsRow({Value(51)}));

  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("UTC", &time_zone));
  auto time_value =
      absl::FromCivil(absl::CivilMinute(2020, 12, 1, 15, 30), time_zone);

  EXPECT_THAT(Query(R"(SELECT TIMESTAMP_ADD(
     TIMESTAMP "2020-12-01 15:30:00+00",
     INTERVAL 10 MINUTE
  ))"),
              IsOkAndHoldsRow({MakeTimestamp(
                  absl::ToChronoTime(time_value + absl::Minutes(10)))}));

  EXPECT_THAT(Query(R"(SELECT TIMESTAMP_SUB(
      TIMESTAMP "2020-12-01 15:30:00+00",
      INTERVAL 10 MINUTE
  ))"),
              IsOkAndHoldsRow({MakeTimestamp(
                  absl::ToChronoTime(time_value - absl::Minutes(10)))}));

  EXPECT_THAT(Query(R"(SELECT TIMESTAMP_DIFF(
    TIMESTAMP "2020-12-01 15:30:00+00",
    TIMESTAMP "2020-12-01 14:30:00+00",
    HOUR
  ))"),
              IsOkAndHoldsRow({Value(1)}));

  EXPECT_THAT(Query(R"(SELECT TIMESTAMP_TRUNC(
    TIMESTAMP "2020-12-25 15:30:00+00", DAY, "UTC"
  ))"),
              IsOkAndHoldsRow({MakeTimestamp(absl::ToChronoTime(
                  absl::FromCivil(absl::CivilDay(2020, 12, 25), time_zone)))}));

  EXPECT_THAT(Query(R"(SELECT EXTRACT(
     HOUR FROM TIMESTAMP "2020-12-25 15:30:00+00"
     AT TIME ZONE "UTC"
  ))"),
              IsOkAndHoldsRow({Value(15)}));
}

TEST_F(QueryTest, NETFunctions) {
  EXPECT_THAT(Query(R"(SELECT NET.IPV4_TO_INT64(b"\x00\x00\x00\x00"))"),
              IsOkAndHoldsRow({0}));
  EXPECT_THAT(Query(R"(SELECT NET.IP_FROM_STRING("0.0.0.0"),
                              NET.SAFE_IP_FROM_STRING("0.0.0.0"),
                              NET.IP_TO_STRING(b"0000"),
                              NET.IP_NET_MASK(4, 0),
                              NET.IP_TRUNC(b"0000", 4),
                              NET.IPV4_FROM_INT64(0),
                              NET.IPV4_TO_INT64(b"0000"),
                              NET.HOST("A"),
                              NET.PUBLIC_SUFFIX("B"),
                              NET.REG_DOMAIN("C"))"),
              zetasql_base::testing::IsOk());
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

TEST_F(QueryTest, CannotQueryArrayOfEmptyStruct) {
  EXPECT_THAT(
      Query("SELECT ARRAY<STRUCT<int64_val INT64>>[]"),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          "Unsupported query shape: Spanner does not support array constructor "
          "syntax for an empty array where array elements are Structs."));

  EXPECT_THAT(Query("SELECT ARRAY<INT64>[]"),
              IsOkAndHoldsRow({Value(std::vector<int64_t>{})}));
}

TEST_F(QueryTest, QueryColumnCannotBeStruct) {
  EXPECT_THAT(Query("SELECT STRUCT<INT64>(1)"),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_F(QueryTest, FunctionAliasesAreAvailable) {
  EXPECT_THAT(Query("SELECT CHARACTER_LENGTH('abc')"), IsOkAndHoldsRow({3}));
  EXPECT_THAT(Query("SELECT CHAR_LENGTH('abc')"), IsOkAndHoldsRow({3}));

  EXPECT_THAT(Query("SELECT POWER(2,2)"), IsOkAndHoldsRow({4.0}));
  EXPECT_THAT(Query("SELECT POW(2,2)"), IsOkAndHoldsRow({4.0}));

  EXPECT_THAT(Query("SELECT CEILING(1.6)"), IsOkAndHoldsRow({2.0}));
  EXPECT_THAT(Query("SELECT CEIL(1.6)"), IsOkAndHoldsRow({2.0}));
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
  EXPECT_THAT(Query(many_joins_query(/*num_joins=*/21)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("joins exceeds")));
}

TEST_F(QueryTest, QueryStringSizeLimit) {
  auto query = absl::Substitute("SELECT \"$0\"", std::string(1024 * 1024, 'a'));
  EXPECT_THAT(Query(query),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Query string length")));
}

TEST_F(QueryTest, NumericKey) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT t.val FROM NumericTable t WHERE t.key < 0"),
              IsOkAndHoldsRows({{-1}}));

  EXPECT_THAT(Query("SELECT t.val FROM NumericTable t WHERE t.key = 0"),
              IsOkAndHoldsRows({{0}}));

  EXPECT_THAT(Query("SELECT t.val FROM NumericTable t WHERE t.key > 0"),
              IsOkAndHoldsRows({{1}}));

  EXPECT_THAT(Query("SELECT t.val FROM NumericTable t WHERE t.key IS NOT NULL "
                    "ORDER BY t.key ASC"),
              IsOkAndHoldsRows({{-1}, {0}, {1}}));

  EXPECT_THAT(Query("SELECT t.val FROM NumericTable t WHERE t.key IS NULL"),
              IsOkAndHoldsRows({{Null<std::int64_t>()}}));

  EXPECT_THAT(Query("SELECT t.val FROM NumericTable t WHERE t.key < -12.1 OR "
                    "t.key > 12.2 ORDER BY t.key ASC"),
              IsOkAndHoldsRows({{-1}, {1}}));
}

TEST_F(QueryTest, SelectStarExcept) {
  PopulateDatabase();
  EXPECT_THAT(
      Query("SELECT * EXCEPT (boolVal, bytesVal, dateVal, floatVal, stringVal, "
            "                 numericVal, timestampVal, jsonVal)"
            "FROM ScalarTypesTable ORDER BY intVal"),
      IsOkAndHoldsRows({{0}, {1}}));
}

TEST_F(QueryTest, StddevAndVariance) {
  PopulateDatabase();
  EXPECT_THAT(Query(R"(SELECT stddev(intval) > .6 FROM ScalarTypesTable)"),
              IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(Query(R"(SELECT stddev_samp(intval) > .6 FROM ScalarTypesTable)"),
              IsOkAndHoldsRows({{true}}));

  EXPECT_THAT(Query(R"(SELECT var_samp(intval) > 0.4 FROM ScalarTypesTable)"),
              IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(Query(R"(SELECT variance(intval) > 0.4 FROM ScalarTypesTable)"),
              IsOkAndHoldsRows({{true}}));
}

TEST_F(QueryTest, RegexString) {
  EXPECT_THAT(Query(R"_(SELECT SAFE.REGEXP_CONTAINS("s", ")"))_"),
              IsOkAndHoldsRows({{Null<bool>()}}));
}

TEST_F(QueryTest, NamedArguments) {
  EXPECT_THAT(
      Query(
          R"_(SELECT PARSE_JSON('{"id":123}', wide_number_mode => 'exact'))_"),
      IsOkAndHoldsRows({{Json("{\"id\":123}")}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
