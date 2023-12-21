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

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/substitute.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/numeric.h"
#include "common/feature_flags.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using cloud::spanner::Bytes;
using cloud::spanner::JsonB;
using cloud::spanner::MakePgNumeric;
using cloud::spanner::PgNumeric;
using postgres_translator::spangres::datatypes::common::MaxNumericString;
using zetasql_base::testing::StatusIs;

class QueryTest
    : public DatabaseTest,
      public testing::WithParamInterface<database_api::DatabaseDialect> {
 public:
  QueryTest()
      : feature_flags_(
            {.enable_postgresql_interface = true,
             .enable_bit_reversed_positive_sequences = true,
             .enable_bit_reversed_positive_sequences_postgresql = true}) {}

  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    EmulatorFeatureFlags::Flags flags;
    emulator::test::ScopedEmulatorFeatureFlagsSetter setter(flags);
    return SetSchemaFromFile("query.test");
  }

 protected:
  void PopulateScalarTypesTable() {
    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      ZETASQL_EXPECT_OK(MultiInsert(
          "scalar_types_table",
          {"int_val", "bool_val", "bytes_val", "date_val", "float_val",
           "string_val", "numeric_val", "timestamp_val", "json_val"},
          {{0, Null<bool>(), Null<Bytes>(), Null<Date>(), Null<double>(),
            Null<std::string>(), Null<PgNumeric>(), Null<Timestamp>(),
            Null<JsonB>()},
           {1, true, Bytes("bytes"), Date(2020, 12, 1), 345.123, "stringValue",
            *MakePgNumeric("1.23"), Timestamp(), JsonB(R"({"key": 123})")}}));
    } else {
      ZETASQL_EXPECT_OK(MultiInsert(
          "scalar_types_table",
          {"int_val", "bool_val", "bytes_val", "date_val", "float_val",
           "string_val", "numeric_val", "timestamp_val", "json_val"},
          {{0, Null<bool>(), Null<Bytes>(), Null<Date>(), Null<double>(),
            Null<std::string>(), Null<Numeric>(), Null<Timestamp>(),
            Null<Json>()},
           {1, true, Bytes("bytes"), Date(2020, 12, 1), 345.123, "stringValue",
            cloud::spanner::MakeNumeric("123.456789").value(), Timestamp(),
            Json("{\"key\":123}")}}));
    }
  }

  void PopulateDatabase() {
    ZETASQL_EXPECT_OK(MultiInsert(
        "users", {"user_id", "name"},
        {{1, "Douglas Adams"}, {2, "Suzanne Collins"}, {3, "J.R.R. Tolkien"}}));

    ZETASQL_EXPECT_OK(MultiInsert("threads", {"user_id", "thread_id", "starred"},
                          {{1, 1, true},
                           {1, 2, true},
                           {1, 3, true},
                           {1, 4, false},
                           {2, 1, false},
                           {2, 2, true},
                           {3, 1, false}}));

    ZETASQL_EXPECT_OK(MultiInsert("messages",
                          {"user_id", "thread_id", "message_id", "subject"},
                          {{1, 1, 1, "a code review"},
                           {1, 1, 2, "Re: a code review"},
                           {1, 2, 1, "Congratulations Douglas"},
                           {1, 3, 1, "Reminder to write feedback"},
                           {1, 4, 1, "Meeting this week"},
                           {2, 1, 1, "Lunch today?"},
                           {2, 2, 1, "Suzanne Collins will be absent"},
                           {3, 1, 1, "Interview Notification"}}));

    PopulateScalarTypesTable();

    if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
      ZETASQL_EXPECT_OK(MultiInsert("numeric_table", {"key", "val"},
                            {{-2, Null<PgNumeric>()},
                             {-1, *MakePgNumeric("-12.3")},
                             {0, *MakePgNumeric("0")},
                             {1, *MakePgNumeric("12.3")}}));
    } else {
      ZETASQL_EXPECT_OK(
          MultiInsert("numeric_table", {"key", "val"},
                      {{Null<Numeric>(), Null<std::int64_t>()},
                       {cloud::spanner::MakeNumeric("-12.3").value(), -1},
                       {cloud::spanner::MakeNumeric("0").value(), 0},
                       {cloud::spanner::MakeNumeric("12.3").value(), 1}}));
    }
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectQueryTests, QueryTest,
    testing::Values(database_api::DatabaseDialect::GOOGLE_STANDARD_SQL,
                    database_api::DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<QueryTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(QueryTest, CanReadScalarTypes) {
  PopulateDatabase();
  auto query = Query("SELECT * FROM scalar_types_table ORDER BY int_val ASC");
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        query,
        IsOkAndHoldsRows({{0, Null<bool>(), Null<Bytes>(), Null<Date>(),
                           Null<double>(), Null<std::string>(),
                           Null<PgNumeric>(), Null<Timestamp>(), Null<JsonB>()},
                          {1, true, Bytes("bytes"), Date(2020, 12, 1), 345.123,
                           "stringValue", *MakePgNumeric("1.23"), Timestamp(),
                           JsonB(R"({"key": 123})")}}));
  } else {
    EXPECT_THAT(query, IsOkAndHoldsRows(
                           {{0, Null<bool>(), Null<Bytes>(), Null<Date>(),
                             Null<double>(), Null<std::string>(),
                             Null<Numeric>(), Null<Timestamp>(), Null<Json>()},
                            {1, true, Bytes("bytes"), Date(2020, 12, 1),
                             345.123, "stringValue",
                             cloud::spanner::MakeNumeric("123.456789").value(),
                             Timestamp(), Json("{\"key\":123}")}}));
  }
}

TEST_P(QueryTest, CanCastScalarTypes) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query(R"(
      SELECT true                     bool_field,
             CAST('abc'  AS varchar)  string_field,
             CAST(-1     AS bigint)   bigint_field,
             CAST(1.1      AS float8)   double_field)"),
                IsOkAndHoldsRows({{true, "abc", -1, 1.1}}));
  } else {
    EXPECT_THAT(Query(R"(
      SELECT true                     bool_field,
             CAST('abc'  AS STRING)   string_field,
             CAST(-1     AS INT64)    int64_field,
             CAST(1.1    AS FLOAT64)  float64_field)"),
                IsOkAndHoldsRows({{true, "abc", -1, 1.1}}));
  }
}

TEST_P(QueryTest, CanExecuteBasicSelectStatement) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT t.thread_id, t.starred "
                    "FROM users, threads t "
                    "WHERE users.user_id = t.user_id "
                    "AND users.user_id=1 AND t.thread_id=4"),
              IsOkAndHoldsRows({{4, false}}));
}

TEST_P(QueryTest, CanExecuteNestedSelectStatement) {
  // Spanner PG dialect doesn't support STRUCT and array subquery expressions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  PopulateDatabase();

  using StructType = std::tuple<std::pair<std::string, std::string>>;
  std::vector<StructType> struct_arr{
      StructType{{"subject", "Re: a code review"}},
      StructType{{"subject", "a code review"}}};

  EXPECT_THAT(
      Query("SELECT threads.thread_id, threads.starred,"
            "       ARRAY(SELECT AS STRUCT messages.subject "
            "             FROM messages"
            "             WHERE messages.user_id = threads.user_id AND"
            "                   messages.thread_id = threads.thread_id "
            "             ORDER BY messages.subject ASC) messages "
            "FROM users JOIN threads ON users.user_id = threads.user_id "
            "WHERE users.user_id=1 AND threads.thread_id=1 "
            "ORDER BY threads.thread_id, threads.starred"),
      IsOkAndHoldsRows({{1, true, Value(struct_arr)}}));
}

TEST_P(QueryTest, CanExecuteEmptySelectStatement) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT t.thread_id, t.starred "
                    "FROM users, threads t "
                    "WHERE users.user_id = t.user_id "
                    "AND users.user_id=10 AND t.thread_id=40"),
              IsOkAndHoldsRows({}));
}

TEST_P(QueryTest, CannotExecuteInvalidSelectStatement) {
  PopulateDatabase();

  EXPECT_THAT(Query("SELECT invalid-identifier "
                    "FROM users, threads t "
                    "WHERE users.user_id = t.user_id "
                    "AND users.user_id=10 AND t.thread_id=40"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_P(QueryTest, InvalidSelectStructColumns) {
  // Spanner PG dialect doesn't support STRUCT and array subquery expressions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  PopulateDatabase();

  EXPECT_THAT(
      Query("SELECT STRUCT< int64_f INT64 > (100) AS expr0 "
            "FROM users "
            "WHERE users.user_id = 10 "),
      StatusIs(absl::StatusCode::kUnimplemented,
               testing::HasSubstr(
                   "A struct value cannot be returned as a column value.")));
}

TEST_P(QueryTest, HashFunctions) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    const char hash[] = {'\xa5', '\x91', '\xa6', '\xd4', '\x0b', '\xf4', ' ',
                         '@',    'J',    '\x01', '\x17', '3',    '\xcf', '\xb7',
                         '\xb1', '\x90', '\xd6', ',',    'e',    '\xbf', '\x0b',
                         '\xcd', '\xa3', '+',    'W',    '\xb2', 'w',    '\xd9',
                         '\xad', '\x9f', '\x14', 'n'};
    EXPECT_THAT(Query("SELECT sha256('Hello World'::bytea) as sha256_hash"),
                IsOkAndHoldsRow({Value(Bytes(hash))}));
  } else {
    const char hash[] = {'\xb1', '\n', '\x8d', '\xb1', 'd',    '\xe0',
                         'u',    'A',  '\x05', '\xb7', '\xa9', '\x9b',
                         '\xe7', '.',  '?',    '\xe5'};
    EXPECT_THAT(Query("SELECT MD5(\"Hello World\") as md5"),
                IsOkAndHoldsRow({Value(Bytes(hash))}));
  }
}

TEST_P(QueryTest, JSONFunctions) {
  // NOTE that more comprehensive testing for JSON functions is done in
  // pg_functions_test.cc. The tests here are trying to match the equivalent
  // JSON functions for GSQL below as much as possible.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        Query(R"(SELECT jsonb_object_field_text('{"a":"str", "b":2}', 'a'))"),
        IsOkAndHoldsRow({"str"}));
    EXPECT_THAT(Query(R"(SELECT jsonb_object_field_text('{"a":null}', 'a'))"),
                IsOkAndHoldsRow({Null<std::string>()}));
    EXPECT_THAT(Query(R"(SELECT jsonb_object_field_text('{"a":null}', 'b'))"),
                IsOkAndHoldsRow({Null<std::string>()}));

    EXPECT_THAT(
        Query(R"(SELECT to_jsonb('{"a":[1,2],"b":"str"}'::jsonb))"),
        IsOkAndHoldsRow({Value(JsonB(R"({"a": [1, 2], "b": "str"})"))}));
    EXPECT_THAT(Query(absl::Substitute(R"(SELECT to_jsonb('{"id":$0}'::jsonb))",
                                       absl::StrCat(MaxNumericString(), "1"))),
                // TODO: Remove check once the errors are
                // consistent between the two environments.
                StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                       : absl::StatusCode::kOutOfRange));

    EXPECT_THAT(Query(R"(SELECT to_jsonb(123))"),
                IsOkAndHoldsRow({Value(JsonB("123"))}));
    EXPECT_THAT(Query(R"(SELECT to_jsonb(12345678901234567))"),
                IsOkAndHoldsRow({Value(JsonB("12345678901234567"))}));
    EXPECT_THAT(Query(absl::Substitute(R"(SELECT to_jsonb($0))",
                                       absl::StrCat(MaxNumericString(), "1"))),
                // TODO: Remove check once the errors are
                // consistent between the two environments.
                StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                       : absl::StatusCode::kOutOfRange));

    EXPECT_THAT(Query(R"(SELECT CAST('{"a":"str", "b":2}'::jsonb AS text))"),
                IsOkAndHoldsRow({R"({"a": "str", "b": 2})"}));
    EXPECT_THAT(Query(R"(SELECT CAST('123'::jsonb AS text))"),
                IsOkAndHoldsRow({"123"}));

    EXPECT_THAT(Query(R"(SELECT '{"a":"str", "b":[1,2,3]}'::jsonb -> 'b')"),
                IsOkAndHoldsRow({Value(JsonB(R"([1, 2, 3])"))}));
    EXPECT_THAT(Query(
                    R"(SELECT jsonb_array_element_text(
               '{"a":"str", "b":[1,2,3]}'::jsonb -> 'b', 1))"),
                IsOkAndHoldsRow({"2"}));

    // Returns output as jsonb.
    EXPECT_THAT(Query(R"(SELECT '{"a":"str", "b":2}'::jsonb -> 'a')"),
                IsOkAndHoldsRow({Value(JsonB(R"("str")"))}));
    EXPECT_THAT(Query(R"(SELECT '{"a":null}'::jsonb -> 'a')"),
                IsOkAndHoldsRow({Value(JsonB("null"))}));
    EXPECT_THAT(Query(R"(SELECT '{"a":null}'::jsonb -> 'b')"),
                IsOkAndHoldsRow({Null<JsonB>()}));

    // Returns output as text.
    EXPECT_THAT(Query(R"(SELECT '{"a":"str", "b":2}'::jsonb ->> 'a')"),
                IsOkAndHoldsRow({"str"}));
    EXPECT_THAT(Query(R"(SELECT '{"a":null}'::jsonb ->> 'a')"),
                IsOkAndHoldsRow({Null<std::string>()}));
    EXPECT_THAT(Query(R"(SELECT '{"a":null}'::jsonb ->> 'b')"),
                IsOkAndHoldsRow({Null<std::string>()}));

  } else {
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
                StatusIs(absl::StatusCode::kOutOfRange));
    EXPECT_THAT(Query(R"(
      SELECT PARSE_JSON('{"id":123456789012345678901}', wide_number_mode=>'exact')
    )"),
                StatusIs(absl::StatusCode::kOutOfRange));
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
    EXPECT_THAT(
        Query(R"(SELECT TO_JSON_STRING('{"a":"str", "b":2}'))"),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            testing::HasSubstr(
                "TO_JSON_STRING is not supported on values of type STRING")));
    EXPECT_THAT(Query(R"(SELECT TO_JSON_STRING(JSON '123'))"),
                IsOkAndHoldsRow({R"(123)"}));
    EXPECT_THAT(
        Query(R"(SELECT TO_JSON_STRING(123))"),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            testing::HasSubstr(
                "TO_JSON_STRING is not supported on values of type INT64")));

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
}

TEST_P(QueryTest, FormatFunction) {
  // Spanner PG dialect doesn't support the PG format function.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(Query(R"(SELECT FORMAT('%s %s', 'hello', 'world'))"),
              IsOkAndHoldsRow({Value("hello world")}));

  EXPECT_THAT(Query(R"(SELECT SAFE.FORMAT('%s %s', 'hello', 'world'))"),
              IsOkAndHoldsRow({Value("hello world")}));
}

TEST_P(QueryTest, DateTimestampArithmeticFunctions) {
  // Spanner PG dialect doesn't support these date/time interval related
  // functions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

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

TEST_P(QueryTest, NETFunctions) {
  // Spanner PG dialect doesn't support the NET functions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
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

TEST_P(QueryTest, CanReturnArrayOfStructTypedColumns) {
  // Spanner PG dialect doesn't support STRUCT and array subquery expressions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  using EmptyStruct = std::tuple<>;
  EXPECT_THAT(
      Query("SELECT ARRAY(SELECT STRUCT<>())"),
      IsOkAndHoldsRow({Value(std::vector<EmptyStruct>{EmptyStruct{}})}));

  using SimpleStruct = std::tuple<int64_t>;
  EXPECT_THAT(
      Query("SELECT ARRAY(SELECT STRUCT<INT64>(1))"),
      IsOkAndHoldsRow({Value(std::vector<SimpleStruct>{SimpleStruct{1}})}));
}

TEST_P(QueryTest, CannotQueryArrayOfEmptyStruct) {
  // Spanner PG dialect doesn't support STRUCT.
  if (GetParam() != database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        Query("SELECT ARRAY<STRUCT<int64_val INT64>>[]"),
        StatusIs(
            absl::StatusCode::kUnimplemented,
            "Unsupported query shape: Spanner does not support array "
            "constructor "
            "syntax for an empty array where array elements are Structs."));
  }

  std::string query = "SELECT ARRAY<INT64>[]";
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = "SELECT '{}'::bigint[]";
  }
  EXPECT_THAT(Query(query), IsOkAndHoldsRow({Value(std::vector<int64_t>{})}));
}

TEST_P(QueryTest, QueryColumnCannotBeStruct) {
  // Spanner PG dialect doesn't support STRUCT.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(Query("SELECT STRUCT<INT64>(1)"),
              StatusIs(absl::StatusCode::kUnimplemented));
}

TEST_P(QueryTest, CharLengthFunctionAliasesAreAvailable) {
  auto query = Query("SELECT CHARACTER_LENGTH('abc')");
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = Query("SELECT length('abc')");
  }
  EXPECT_THAT(query, IsOkAndHoldsRow({3}));

  if (GetParam() != database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query("SELECT CHAR_LENGTH('abc')"), IsOkAndHoldsRow({3}));
  }
}

TEST_P(QueryTest, PowerFunctionAliasesAreAvailable) {
  auto query = Query("SELECT POWER(2,2)");
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = Query("SELECT power('2.0'::float8, '2.0'::float8)");
  }
  EXPECT_THAT(query, IsOkAndHoldsRow({4.0}));

  query = Query("SELECT POWER(2,2)");
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = Query("SELECT pow('2.0'::float8, '2.0'::float8)");
  }
  EXPECT_THAT(query, IsOkAndHoldsRow({4.0}));
}

TEST_P(QueryTest, CeilingFunctionAliasesAreAvailable) {
  if (GetParam() != database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query("SELECT CEILING(1.6)"), IsOkAndHoldsRow({2.0}));
  }

  auto query = Query("SELECT CEIL(1.6)");
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    query = Query("SELECT ceil('1.6'::float8)");
  }
  EXPECT_THAT(query, IsOkAndHoldsRow({2.0}));
}

TEST_P(QueryTest, DefaultTimeZoneIsPacificTime) {
  // Spanner PG doesn't support timestamp without explicitly specifying the time
  // zone.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(Query("SELECT TIMESTAMP '2020-12-01'"),
              IsOkAndHoldsRow({Value(MakeTimestamp(absl::ToChronoTime(
                  absl::FromCivil(absl::CivilDay(2020, 12, 1), time_zone))))}));
}

TEST_P(QueryTest, CheckQuerySizeLimitsAreEnforced) {
  // Check that the query size limits enforcement is in place.
  auto many_joins_query = [](int num_joins) {
    std::string join_query = "SELECT t0.user_id FROM users AS t0";
    for (int i = 1; i <= num_joins; ++i) {
      join_query =
          join_query + "\n" +
          absl::StrFormat("JOIN users AS t%d ON t%d.user_id = t%d.age ", i,
                          i - 1, i);
    }
    return join_query;
  };
  EXPECT_THAT(Query(many_joins_query(/*num_joins=*/21)),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("joins exceeds")));
}

TEST_P(QueryTest, QueryStringSizeLimit) {
  auto query = absl::Substitute("SELECT '$0'", std::string(1024 * 1024, 'a'));
  EXPECT_THAT(Query(query),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Query string length")));
}

TEST_P(QueryTest, NumericKey) {
  // TODO: b/294026608 - Unskip after PG.NUMERIC indexing is supported.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  PopulateDatabase();

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key < 0"),
              IsOkAndHoldsRows({{-1}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key = 0"),
              IsOkAndHoldsRows({{0}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key > 0"),
              IsOkAndHoldsRows({{1}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key IS NOT NULL "
                    "ORDER BY t.key ASC"),
              IsOkAndHoldsRows({{-1}, {0}, {1}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key IS NULL"),
              IsOkAndHoldsRows({{Null<std::int64_t>()}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key < -12.1 OR "
                    "t.key > 12.2 ORDER BY t.key ASC"),
              IsOkAndHoldsRows({{-1}, {1}}));
}

TEST_P(QueryTest, PgNumericType) {
  // TODO: b/294026608 - Remove test after PG.NUMERIC indexing is supported and
  // this test can be combined with the NumericKey test above.
  if (GetParam() == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    GTEST_SKIP();
  }

  PopulateDatabase();

  EXPECT_THAT(
      Query("SELECT t.val FROM numeric_table t WHERE t.key < 0"),
      IsOkAndHoldsRows({{Null<PgNumeric>()}, {*MakePgNumeric("-12.3")}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key = 0"),
              IsOkAndHoldsRows({{*MakePgNumeric("0")}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.key > 0"),
              IsOkAndHoldsRows({{*MakePgNumeric("12.3")}}));

  EXPECT_THAT(Query("SELECT t.val FROM numeric_table t WHERE t.val IS NOT NULL "
                    "ORDER BY t.key ASC"),
              IsOkAndHoldsRows({{*MakePgNumeric("-12.3")},
                                {*MakePgNumeric("0")},
                                {*MakePgNumeric("12.3")}}));

  EXPECT_THAT(Query("SELECT t.key FROM numeric_table t WHERE t.val IS NULL"),
              IsOkAndHoldsRows({{-2}}));

  EXPECT_THAT(Query("SELECT t.key FROM numeric_table t WHERE t.val < -12.1 OR "
                    "t.val > 12.2 ORDER BY t.key ASC"),
              IsOkAndHoldsRows({{-1}, {1}}));
}

TEST_P(QueryTest, SelectStarExcept) {
  // PostgreSQL doesn't support select all columns except some.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  PopulateDatabase();
  EXPECT_THAT(Query("SELECT * EXCEPT (bool_val, bytes_val, date_val, "
                    "float_val, string_val, "
                    "                 numeric_val, timestamp_val, json_val)"
                    "FROM scalar_types_table ORDER BY int_val"),
              IsOkAndHoldsRows({{0}, {1}}));
}

TEST_P(QueryTest, StddevAndVariance) {
  // Spanner PG doesn't support standard deviation and variance functions.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  PopulateDatabase();
  EXPECT_THAT(Query(R"(SELECT stddev(int_val) > .6 FROM scalar_types_table)"),
              IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(
      Query(R"(SELECT stddev_samp(int_val) > .6 FROM scalar_types_table)"),
      IsOkAndHoldsRows({{true}}));

  EXPECT_THAT(
      Query(R"(SELECT var_samp(int_val) > 0.4 FROM scalar_types_table)"),
      IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(
      Query(R"(SELECT variance(int_val) > 0.4 FROM scalar_types_table)"),
      IsOkAndHoldsRows({{true}}));
}

TEST_P(QueryTest, RegexString) {
  // Spanner PG doesn't support something equivalent from PG like regexp_like.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  EXPECT_THAT(Query(R"_(SELECT SAFE.REGEXP_CONTAINS("s", ")"))_"),
              IsOkAndHoldsRows({{Null<bool>()}}));
}

TEST_P(QueryTest, NamedArguments) {
  // Spanner PG doesn't yet support function named parameters.
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }

  EXPECT_THAT(
      Query(
          R"_(SELECT PARSE_JSON('{"id":123}', wide_number_mode => 'exact'))_"),
      IsOkAndHoldsRows({{Json("{\"id\":123}")}}));
}

TEST_P(QueryTest, GetInternalSequenceStateFunction) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(Query("SELECT GET_INTERNAL_SEQUENCE_STATE(SEQUENCE MySeq)"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Sequence not found: MySeq")));
}

TEST_P(QueryTest, GetNextSequenceValueFunction) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    GTEST_SKIP();
  }
  EXPECT_THAT(Query("SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE MySeq)"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       testing::HasSubstr("Sequence not found: MySeq")));
}

TEST_P(QueryTest, BitReverseFunction) {
  if (GetParam() == database_api::DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(Query("SELECT SPANNER.BIT_REVERSE(1, null)"),
                IsOkAndHoldsRows({{Null<int64_t>()}}));

    EXPECT_THAT(Query("SELECT SPANNER.BIT_REVERSE(null, true)"),
                IsOkAndHoldsRows({{Null<int64_t>()}}));

    EXPECT_THAT(Query("SELECT SPANNER.BIT_REVERSE(4611686018427387904, true)"),
                IsOkAndHoldsRows({{1}}));
  } else {
    EXPECT_THAT(Query("SELECT BIT_REVERSE(1, cast(null as bool))"),
                IsOkAndHoldsRows({{Null<int64_t>()}}));

    EXPECT_THAT(Query("SELECT BIT_REVERSE(4611686018427387904, true)"),
                IsOkAndHoldsRows({{1}}));
  }
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
