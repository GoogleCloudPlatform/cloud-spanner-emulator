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
#include <limits>
#include <string>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/json.h"
#include "google/cloud/spanner/numeric.h"
#include "tests/common/scoped_feature_flags_setter.h"
#include "tests/conformance/common/database_test_base.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using cloud::spanner::JsonB;
using cloud::spanner::MakePgNumeric;
using cloud::spanner::PgNumeric;
using postgres_translator::spangres::datatypes::common::MaxNumericString;
using postgres_translator::spangres::datatypes::common::MinNumericString;
using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

class PGFunctionsTest : public DatabaseTest {
 public:
  PGFunctionsTest() : feature_flags_({.enable_postgresql_interface = true}) {}

  void SetUp() override {
    dialect_ = database_api::DatabaseDialect::POSTGRESQL;
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    return SetSchemaFromFile("pg_functions.test");
  }

 protected:
  void PopulateDatabase(bool insert_nan = false) {
    ZETASQL_EXPECT_OK(MultiInsert(
        "values", {"id", "int_value", "double_value", "numeric_value"},
        {{1, 1, 1.0, *MakePgNumeric("123.0")},
         {2, 0, 2.0, *MakePgNumeric("12.0")},
         {3, 5, 3.0, *MakePgNumeric("3.0")},
         {4, Null<int64_t>(), Null<double>(), Null<PgNumeric>()}}));

    if (insert_nan) {
      // Only inserting a NaN value if requested by a test since inserting a NaN
      // more generally prevents us from testing the correctness of sums.
      ZETASQL_EXPECT_OK(Insert("values",
                       {"id", "int_value", "double_value", "numeric_value"},
                       {5, 2, 5.0, *MakePgNumeric("NaN")}));
    }
  }

  // Testing this in an awkward way because MakePgNumeric strips trailing zeros
  // and somehow the output of Query has trailing zeros at least in the context
  // of being compared by IsOkAndHoldsRows. So we get the query output, convert
  // it into the version of PgNumeric with the trailing zeros removed and then
  // compare the result.
  void VerifySingleNumericRowValue(const std::string& query,
                                   const std::string& exp) {
    auto rows = Query(query);
    ZETASQL_EXPECT_OK(rows);
    EXPECT_THAT(rows->size(), 1);
    auto row = (*rows)[0];
    EXPECT_THAT(row.values().size(), 1);
    EXPECT_THAT(row.values()[0].get<PgNumeric>(), *MakePgNumeric(exp));
  }

 private:
  test::ScopedEmulatorFeatureFlagsSetter feature_flags_;
};

TEST_F(PGFunctionsTest, CastToDate) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT CAST(col1 AS date) AS date
          FROM (SELECT 'jan 1, 2000' AS col1) subquery)sql"),
              IsOkAndHoldsRows({Date(2000, 1, 1)}));
}

TEST_F(PGFunctionsTest, CastToDateUnsupportedDate) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT CAST(col1 AS date) AS date
          FROM (SELECT 'jan 1, 0000' AS col1) subquery)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(PGFunctionsTest, CastToTimestamp) {
  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT CAST(col1 AS timestamptz) AS timestamptz
          FROM (SELECT
            '2000/1/1 01:02:03 America/Los_Angeles' AS col1) subquery)sql"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(2000, 1, 1, 1, 2, 3), time_zone)))}));
}

TEST_F(PGFunctionsTest, CastToTimestampUnsupportedTimestamp) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT CAST(col1 AS timestamptz) AS timestamptz
          FROM (SELECT
            '0000/1/1 01:02:03 America/Los_Angeles' AS col1) subquery)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
}

TEST_F(PGFunctionsTest, TimestamptzAdd) {
  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(
      Query(
          R"sql(SELECT spanner.timestamptz_add('2000-01-01 02:03:04',
                    '3 minutes'))sql"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(2000, 1, 1, 2, 6, 4), time_zone)))}));
}

TEST_F(PGFunctionsTest, TimestamptzSubtract) {
  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(
      Query(
          R"sql(SELECT spanner.timestamptz_subtract('2000-01-01 02:03:04',
                    '3 minutes'))sql"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(2000, 1, 1, 2, 0, 4), time_zone)))}));
}

TEST_F(PGFunctionsTest, TimestamptzBin) {
  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(
      Query(
          R"sql(SELECT spanner.date_bin('1 day'::text,
                    '2000-01-01 18:19:20'::timestamptz,
                    '1970-01-01 16:01:02'::timestamptz))sql"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(2000, 1, 1, 16, 1, 2), time_zone)))}));
}

TEST_F(PGFunctionsTest, TimestamptzTrunc) {
  absl::TimeZone time_zone;
  ASSERT_TRUE(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  EXPECT_THAT(
      Query(
          R"sql(SELECT date_trunc('month',
                    '2000-01-01 18:19:20'::timestamptz))sql"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(2000, 1, 1, 0, 0, 0), time_zone)))}));
}

TEST_F(PGFunctionsTest, TimestamptzTruncTimezone) {
  EXPECT_THAT(
      Query(R"sql(SELECT date_trunc('day', '2000-01-01 18:19:20', 'zulu'))sql"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(2000, 1, 2, 0, 0, 0), absl::UTCTimeZone())))}));
}

TEST_F(PGFunctionsTest, TimestamptzExtract) {
  EXPECT_THAT(
      Query(
          R"sql(SELECT extract(hour from '2000-01-01 18:19:20'::timestamptz)
               )sql"),
      IsOkAndHoldsRows({*MakePgNumeric("18")}));
}

TEST_F(PGFunctionsTest, DateExtract) {
  EXPECT_THAT(Query(R"sql(SELECT extract(year from '2023-01-01'::date))sql"),
              IsOkAndHoldsRows({*MakePgNumeric("2023")}));
}

TEST_F(PGFunctionsTest, MapDoubleToInt) {
  EXPECT_THAT(Query("SELECT 'NaN'::float = 'NaN'::float"),
              IsOkAndHoldsRows({true}));
}

TEST_F(PGFunctionsTest, LeastInteger) {
  EXPECT_THAT(Query("SELECT LEAST(2, 5, NULL, 1)"), IsOkAndHoldsRows({{1}}));
}

TEST_F(PGFunctionsTest, GreatestInteger) {
  EXPECT_THAT(Query("SELECT GREATEST(3, 7, NULL, 2)"), IsOkAndHoldsRows({{7}}));
}

TEST_F(PGFunctionsTest, LeastDoubles) {
  EXPECT_THAT(Query("SELECT LEAST(2.1, 5.5, 'NaN'::float, NULL)"),
              IsOkAndHoldsRows({{2.1}}));
}

TEST_F(PGFunctionsTest, GreatestDoubles) {
  EXPECT_THAT(Query("SELECT GREATEST(2.1, 5.5, 'NaN'::float, NULL)"),
              IsOkAndHoldsRows({{std::numeric_limits<double>::quiet_NaN()}}));
}

// MIN for the double type uses a different aggregator function than for other
// types so we test for doubles and not doubles.
TEST_F(PGFunctionsTest, MinDoubles) {
  EXPECT_THAT(Query("SELECT MIN('NaN'::float)"),
              IsOkAndHoldsRows({{std::numeric_limits<double>::quiet_NaN()}}));
}

TEST_F(PGFunctionsTest, MinDoublesFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT MIN(double_value) FROM values"),
              IsOkAndHoldsRows({{1.0}}));
}

TEST_F(PGFunctionsTest, MinNotDoubles) {
  EXPECT_THAT(Query("SELECT MIN(12345)"), IsOkAndHoldsRows({{12345}}));
}

TEST_F(PGFunctionsTest, MinNotDoublesFromTable) {
  EXPECT_THAT(Query("SELECT MIN(int_value) FROM values"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));
}

// Tests for pg.min with PG.NUMERIC.
TEST_F(PGFunctionsTest, MinNumeric) {
  EXPECT_THAT(Query("SELECT min(1.1)"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));
}

TEST_F(PGFunctionsTest, MinNumericFromEmptyTable) {
  EXPECT_THAT(Query("SELECT min(numeric_value) FROM values"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, MinNumericFromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT min(numeric_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, MinNumericFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT min(numeric_value) FROM values"),
              IsOkAndHoldsRows({{*MakePgNumeric("3.0")}}));
  EXPECT_THAT(
      Query("SELECT min(numeric_value) + min(numeric_value) FROM values"),
      IsOkAndHoldsRows({{*MakePgNumeric("6.0")}}));
}

// Tests for pg.max with PG.NUMERIC.
TEST_F(PGFunctionsTest, MaxNumeric) {
  EXPECT_THAT(Query("SELECT max(1.1)"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));
}

TEST_F(PGFunctionsTest, MaxNumericFromEmptyTable) {
  EXPECT_THAT(Query("SELECT max(numeric_value) FROM values"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, MaxNumericFromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT max(numeric_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, MaxNumericFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT max(numeric_value) FROM values"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.0")}}));
  EXPECT_THAT(
      Query("SELECT max(numeric_value) + max(numeric_value) FROM values"),
      IsOkAndHoldsRows({{*MakePgNumeric("246.0")}}));
}

// Tests for pg.sum with Int64.
TEST_F(PGFunctionsTest, SumInt64) {
  EXPECT_THAT(Query("SELECT sum(1)"),
              IsOkAndHoldsRows({{*MakePgNumeric("1")}}));
}

TEST_F(PGFunctionsTest, SumInt64OnEmptyTable) {
  EXPECT_THAT(Query("SELECT sum(int_value) FROM values"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, SumInt64FromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT sum(int_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, SumInt64FromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT sum(int_value) FROM values"),
              IsOkAndHoldsRows({{*MakePgNumeric("6")}}));
  EXPECT_THAT(Query("SELECT sum(int_value) + sum(int_value) FROM values"),
              IsOkAndHoldsRows({{*MakePgNumeric("12")}}));
}

// Tests for pg.sum with Double.
TEST_F(PGFunctionsTest, SumDouble) {
  EXPECT_THAT(Query("SELECT sum(1.0::float8)"), IsOkAndHoldsRows({{1.0}}));
}

// This is a known issue where despite this column being of type float8, the
// output is PG.NUMERIC because the evaluator doesn't know the type of the
// return value. But the GSQL reference implementation detects this and throws
// an error.
// TODO: Figure out how to return a double value.
TEST_F(PGFunctionsTest, DISABLED_SumDoubleOnEmptyTable) {
  EXPECT_THAT(
      Query("SELECT sum(double_value) FROM values"),
      IsOkAndHoldsRows({{in_prod_env() ? Null<double>() : Null<PgNumeric>()}}));
}

// This is a known issue where despite this column being of type float8, the
// output is PG.NUMERIC because the evaluator doesn't know the type of the
// return value. But the GSQL reference implementation detects this and throws
// an error.
// TODO: Figure out how to return a double value.
TEST_F(PGFunctionsTest, DISABLED_SumDoubleFromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT sum(double_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<double>()}}));
}

TEST_F(PGFunctionsTest, SumDoubleFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT sum(double_value) FROM values"),
              IsOkAndHoldsRows({{6.0}}));
}

// Tests for pg.sum with PG.NUMERIC.
TEST_F(PGFunctionsTest, SumNumeric) {
  EXPECT_THAT(Query("SELECT sum(1.1)"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));
}

TEST_F(PGFunctionsTest, SumNumericOnEmptyTable) {
  EXPECT_THAT(Query("SELECT sum(numeric_value) FROM values"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, SumNumericFromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT sum(numeric_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, SumNumericFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT sum(numeric_value) FROM values"),
              IsOkAndHoldsRows({{*MakePgNumeric("138.0")}}));
  EXPECT_THAT(
      Query("SELECT sum(numeric_value) + sum(numeric_value) FROM values"),
      IsOkAndHoldsRows({{*MakePgNumeric("276.0")}}));
}

// Tests for pg.avg with Int64.
TEST_F(PGFunctionsTest, AvgInt64) {
  VerifySingleNumericRowValue("SELECT avg(1)", "1");
}

TEST_F(PGFunctionsTest, AvgInt64OnEmptyTable) {
  EXPECT_THAT(Query("SELECT avg(int_value) FROM values"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, AvgInt64FromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT avg(int_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, AvgInt64FromTable) {
  PopulateDatabase();
  VerifySingleNumericRowValue("SELECT avg(int_value) FROM values", "2");
  VerifySingleNumericRowValue(
      "SELECT avg(int_value) + avg(int_value) FROM values", "4");
}

// Tests for pg.avg with Double.
TEST_F(PGFunctionsTest, AvgDouble) {
  EXPECT_THAT(Query("SELECT avg(1.0::float8)"), IsOkAndHoldsRows({{1.0}}));
}

// This is a known issue where despite this column being of type float8, the
// output is PG.NUMERIC because the evaluator doesn't know the type of the
// return value. But the GSQL reference implementation detects this and throws
// an error.
// TODO: Figure out how to return a double value.
TEST_F(PGFunctionsTest, DISABLED_AvgDoubleOnEmptyTable) {
  EXPECT_THAT(
      Query("SELECT avg(double_value) FROM values"),
      IsOkAndHoldsRows({{in_prod_env() ? Null<double>() : Null<PgNumeric>()}}));
}

// This is a known issue where despite this column being of type float8, the
// output is PG.NUMERIC because the evaluator doesn't know the type of the
// return value. But the GSQL reference implementation detects this and throws
// an error.
// TODO: Figure out how to return a double value.
TEST_F(PGFunctionsTest, DISABLED_AvgDoubleFromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT avg(double_value) FROM values WHERE id=5"),
              IsOkAndHoldsRows({{Null<double>()}}));
}

TEST_F(PGFunctionsTest, AvgDoubleFromTable) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT avg(double_value) FROM values"),
              IsOkAndHoldsRows({{2.0}}));
}

// Tests for pg.avg with PG.NUMERIC.
TEST_F(PGFunctionsTest, AvgNumeric) {
  VerifySingleNumericRowValue("SELECT avg(1.0)", "1");
}

TEST_F(PGFunctionsTest, AvgNumericOnEmptyTable) {
  EXPECT_THAT(Query("SELECT avg(numeric_value) FROM values"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, AvgNumericFromTableWithOnlyNull) {
  PopulateDatabase();
  EXPECT_THAT(Query("SELECT avg(numeric_value) FROM values WHERE id=4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, AvgNumericFromTable) {
  PopulateDatabase();
  VerifySingleNumericRowValue("SELECT avg(numeric_value) FROM values", "46");
  VerifySingleNumericRowValue(
      "SELECT avg(numeric_value) + avg(numeric_value) FROM values", "92");
}

TEST_F(PGFunctionsTest, NumericAbs) {
  EXPECT_THAT(Query("SELECT abs(123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT abs(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT abs(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_abs(123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT numeric_abs(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT numeric_abs(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
}

TEST_F(PGFunctionsTest, NumericAdd) {
  EXPECT_THAT(Query("SELECT 123.45 + 3.4"),
              IsOkAndHoldsRows({{*MakePgNumeric("126.85")}}));
  EXPECT_THAT(Query("SELECT -123.45 + 3.4"),
              IsOkAndHoldsRows({{*MakePgNumeric("-120.05")}}));
  EXPECT_THAT(Query("SELECT 'NaN'::numeric + 3.4"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT 3.4 + 'NaN'::numeric"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT 3.4 + NULL::numeric"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_add(123.45, 3.4)"),
              IsOkAndHoldsRows({{*MakePgNumeric("126.85")}}));
  EXPECT_THAT(Query("SELECT numeric_add(-123.45, 3.4)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-120.05")}}));
  EXPECT_THAT(Query("SELECT numeric_add('NaN'::numeric, 3.4)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_add(3.4, 'NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_add(3.4, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 + 0.1", MaxNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 + (-0.1)", MinNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query("SELECT numeric_add(1.0, 2.0, 3.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericCeil) {
  EXPECT_THAT(Query("SELECT ceil(123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("124")}}));
  EXPECT_THAT(Query("SELECT ceil(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-123")}}));
  EXPECT_THAT(Query("SELECT ceil('NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT ceil(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT ceil($0)", MaxNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query("SELECT ceil(1.0, 2.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericCeiling) {
  EXPECT_THAT(Query("SELECT ceiling(123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("124")}}));
  EXPECT_THAT(Query("SELECT ceiling(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-123")}}));
  EXPECT_THAT(Query("SELECT ceiling('NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT ceiling(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT ceiling($0)", MaxNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query("SELECT ceiling(1.0, 2.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericDivTrunc) {
  EXPECT_THAT(Query("SELECT div(123.0, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("61")}}));
  EXPECT_THAT(Query("SELECT div(-123.0, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-61")}}));
  EXPECT_THAT(Query("SELECT div('NaN'::numeric, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT div(2.0, 'NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT div(NULL::numeric, 2.0)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT div(2.0, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_div_trunc(123.0, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("61")}}));
  EXPECT_THAT(Query("SELECT numeric_div_trunc(-123.0, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-61")}}));
  EXPECT_THAT(Query("SELECT numeric_div_trunc('NaN'::numeric, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_div_trunc(2.0, 'NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_div_trunc(NULL::numeric, 2.0)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_div_trunc(2.0, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(
      Query(absl::Substitute("SELECT div($0, 0.1)", MaxNumericString())),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("value overflows numeric format")));
  EXPECT_THAT(
      Query(absl::Substitute("SELECT div($0, 0.1)", MinNumericString())),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("value overflows numeric format")));
  EXPECT_THAT(
      Query("SELECT div(1.0, 0.0)"),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(in_prod_env() ? "divide by 0" : "division by zero")));
  EXPECT_THAT(Query("SELECT div(1.0, 2.0, 3.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericDivide) {
  VerifySingleNumericRowValue("SELECT 123.4 / 2.0", "61.7");
  VerifySingleNumericRowValue("SELECT -123.4 / 2.0", "-61.7");
  EXPECT_THAT(Query("SELECT 'NaN'::numeric / 2.0"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT 2.0 / 'NaN'::numeric"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT NULL::numeric / 2.0"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT 2.0 / NULL::numeric"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  VerifySingleNumericRowValue("SELECT numeric_div(123.4, 2.0)", "61.7");
  VerifySingleNumericRowValue("SELECT numeric_div(-123.4, 2.0)", "-61.7");
  EXPECT_THAT(Query("SELECT numeric_div('NaN'::numeric, 2.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_div(2.0, 'NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_div(NULL::numeric, 2.0)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_div(2.0, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 / 0.1", MaxNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 / 0.1", MinNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(
      Query("SELECT 1.0 / 0.0"),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(in_prod_env() ? "divide by 0" : "division by zero")));
  EXPECT_THAT(Query("SELECT numeric_div(1.0, 2.0, 3.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericFloor) {
  EXPECT_THAT(Query("SELECT floor(123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123")}}));
  EXPECT_THAT(Query("SELECT floor(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-124")}}));
  EXPECT_THAT(Query("SELECT floor(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT floor($0)", MinNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query("SELECT floor(1.0, 2.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericMod) {
  EXPECT_THAT(Query("SELECT 123.45 % 10.0"),
              IsOkAndHoldsRows({{*MakePgNumeric("3.45")}}));
  EXPECT_THAT(Query("SELECT -123.45 % 10.0"),
              IsOkAndHoldsRows({{*MakePgNumeric("-3.45")}}));
  EXPECT_THAT(Query("SELECT 'NaN'::numeric % 2.0"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT 2.0 % 'NaN'::numeric"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT NULL::numeric % 2.0"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT 2.0 % NULL::numeric"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT mod(123.45, 10.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("3.45")}}));
  EXPECT_THAT(Query("SELECT mod(-123.45, 10.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-3.45")}}));
  EXPECT_THAT(Query("SELECT mod(NULL::numeric, 10.0)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT mod(10.0, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT mod(123.45, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT mod(NULL::numeric, 123.45)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_mod(123.45, 10.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("3.45")}}));
  EXPECT_THAT(Query("SELECT numeric_mod(-123.45, 10.0)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-3.45")}}));
  EXPECT_THAT(Query("SELECT numeric_mod(NULL::numeric, 10.0)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_mod(10.0, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_mod(123.45, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_mod(NULL::numeric, 123.45)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(
      Query("SELECT 1.0 % 0.0"),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr(in_prod_env() ? "mod by 0" : "division by zero")));
  EXPECT_THAT(Query("SELECT numeric_mod(1.0, 2.0, 3.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericMultiply) {
  EXPECT_THAT(Query("SELECT 123.4 * 2.2"),
              IsOkAndHoldsRows({{*MakePgNumeric("271.48")}}));
  EXPECT_THAT(Query("SELECT -123.4 * 2.2"),
              IsOkAndHoldsRows({{*MakePgNumeric("-271.48")}}));
  EXPECT_THAT(Query("SELECT 'NaN'::numeric * 2.2"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT 2.2 * 'NaN'::numeric"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT NULL::numeric * 2.2"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT 2.2 * NULL::numeric"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_mul(123.4, 2.2)"),
              IsOkAndHoldsRows({{*MakePgNumeric("271.48")}}));
  EXPECT_THAT(Query("SELECT numeric_mul(-123.4, 2.2)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-271.48")}}));
  EXPECT_THAT(Query("SELECT numeric_mul('NaN'::numeric, 2.2)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_mul(2.2, 'NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_mul(NULL::numeric, 2.2)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_mul(2.2, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 * 1.1", MaxNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 * 1.1", MinNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query("SELECT numeric_mul(1.0, 2.0, 3.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericSubtract) {
  EXPECT_THAT(Query("SELECT 123.45 - 3.4"),
              IsOkAndHoldsRows({{*MakePgNumeric("120.05")}}));
  EXPECT_THAT(Query("SELECT -123.45 - 3.4"),
              IsOkAndHoldsRows({{*MakePgNumeric("-126.85")}}));
  EXPECT_THAT(Query("SELECT 'NaN'::numeric - 3.4"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT 3.4 - 'NaN'::numeric"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT NULL::numeric - 3.4"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT 3.4 - NULL::numeric"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_sub(123.45, 3.4)"),
              IsOkAndHoldsRows({{*MakePgNumeric("120.05")}}));
  EXPECT_THAT(Query("SELECT numeric_sub(-123.45, 3.4)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-126.85")}}));
  EXPECT_THAT(Query("SELECT numeric_sub('NaN'::numeric, 3.4)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_sub(3.4, 'NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_sub(NULL::numeric, 3.4)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT numeric_sub(3.4, NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 - (-0.1)", MaxNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query(absl::Substitute("SELECT $0 - 0.1", MinNumericString())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("value overflows numeric format")));
  EXPECT_THAT(Query("SELECT numeric_sub(1.0, 2.0, 3.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericTrunc) {
  EXPECT_THAT(Query("SELECT trunc(123.45, 1)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.4")}}));
  EXPECT_THAT(Query("SELECT trunc(123.45, -1)"),
              IsOkAndHoldsRows({{*MakePgNumeric("120")}}));
  EXPECT_THAT(Query("SELECT trunc(NULL::numeric, -1)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));
  EXPECT_THAT(Query("SELECT trunc(123.45, NULL::int8)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  // TODO: remove once trunc with one argument is supported.
  if (!in_prod_env()) {
    EXPECT_THAT(Query("SELECT trunc(123.45)"),
                StatusIs(absl::StatusCode::kUnimplemented,
                         HasSubstr("is not supported")));
  }
}

TEST_F(PGFunctionsTest, NumericUminus) {
  EXPECT_THAT(Query("SELECT -123.45"),
              IsOkAndHoldsRows({{*MakePgNumeric("-123.45")}}));
  EXPECT_THAT(Query("SELECT -(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT -('NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT -(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  EXPECT_THAT(Query("SELECT numeric_uminus(123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("-123.45")}}));
  EXPECT_THAT(Query("SELECT numeric_uminus(-123.45)"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT numeric_uminus('NaN'::numeric)"),
              IsOkAndHoldsRows({{*MakePgNumeric("NaN")}}));
  EXPECT_THAT(Query("SELECT numeric_uminus(NULL::numeric)"),
              IsOkAndHoldsRows({{Null<PgNumeric>()}}));

  // Error cases
  EXPECT_THAT(Query("SELECT numeric_uminus(1.0, 2.0)"),
              // TODO: Remove check if the two environments ever
              // become consistent.
              StatusIs(in_prod_env() ? absl::StatusCode::kInvalidArgument
                                     : absl::StatusCode::kNotFound,
                       HasSubstr("does not exist")));
}

TEST_F(PGFunctionsTest, NumericCastToInt64) {
  EXPECT_THAT(Query("SELECT 0.1::int8"), IsOkAndHoldsRows({{0}}));
  EXPECT_THAT(Query("SELECT 0.49::int8"), IsOkAndHoldsRows({{0}}));
  EXPECT_THAT(Query("SELECT 0.5::int8"), IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(Query("SELECT -1.49::int8"), IsOkAndHoldsRows({{-1}}));
  EXPECT_THAT(Query("SELECT -1.5::int8"), IsOkAndHoldsRows({{-2}}));
  EXPECT_THAT(Query(absl::Substitute("SELECT $0::numeric::int8",
                                     std::numeric_limits<int64_t>().max())),
              IsOkAndHoldsRows({{std::numeric_limits<int64_t>().max()}}));
  // Both PROD and the emulator support int64_t min + 1 as the lowest bound
  EXPECT_THAT(Query(absl::Substitute("SELECT ($0 + 1)::numeric::int8",
                                     std::numeric_limits<int64_t>().min())),
              IsOkAndHoldsRows({{std::numeric_limits<int64_t>().min() + 1}}));

  // Error cases
  // TODO: b/308517728 - Normalize the error message
  EXPECT_THAT(Query(absl::Substitute("SELECT ($0::numeric + 1)::int8",
                                     std::numeric_limits<int64_t>().max())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       in_prod_env() ? HasSubstr("Cannot cast to INT64")
                                     : HasSubstr("out of range")));
  EXPECT_THAT(Query(absl::Substitute("SELECT ($0::numeric - 1)::int8",
                                     std::numeric_limits<int64_t>().min())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       in_prod_env() ? HasSubstr("Cannot cast to INT64")
                                     : HasSubstr("out of range")));
  // TODO: b/308518029 - Normalize the error status / message
  EXPECT_THAT(Query("SELECT 'NaN'::numeric::int8"),
              StatusIs(in_prod_env() ? absl::StatusCode::kOutOfRange
                                     : absl::StatusCode::kUnimplemented,
                       in_prod_env() ? HasSubstr("Cannot cast from NaN")
                                     : HasSubstr("cannot convert NaN")));
}

TEST_F(PGFunctionsTest, NumericCastToDouble) {
  EXPECT_THAT(Query("SELECT 0.2999999999999997::float8"),
              IsOkAndHoldsRows({{(double)0.2999999999999997}}));
  EXPECT_THAT(Query("SELECT 2999999999999997::float8"),
              IsOkAndHoldsRows({{(double)2999999999999997}}));
  EXPECT_THAT(Query("SELECT -2999999999999997::float8"),
              IsOkAndHoldsRows({{(double)-2999999999999997}}));
  EXPECT_THAT(Query("SELECT -0.2999999999999997::float8"),
              IsOkAndHoldsRows({{(double)-0.2999999999999997}}));
  EXPECT_THAT(Query("SELECT 'NaN'::numeric::float8"),
              IsOkAndHoldsRows({{std::numeric_limits<double>::quiet_NaN()}}));
  EXPECT_THAT(Query("SELECT NULL::numeric::float8"),
              IsOkAndHoldsRows({{Null<double>()}}));
  EXPECT_THAT(*Query(absl::Substitute("SELECT $0::numeric::float8",
                                      std::numeric_limits<double>().min()))
                   ->at(0)
                   .values()
                   .at(0)
                   .get<double>(),
              testing::DoubleNear(std::numeric_limits<double>().min(), 0.001));

  // Error cases
  EXPECT_THAT(Query(absl::Substitute("SELECT (($0)::numeric * 10)::float8",
                                     std::numeric_limits<double>().max())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Cannot cast to double")));
  EXPECT_THAT(Query(absl::Substitute("SELECT (($0)::numeric * 10)::float8",
                                     std::numeric_limits<double>().lowest())),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("Cannot cast to double")));
}

TEST_F(PGFunctionsTest, NumericCastToString) {
  EXPECT_THAT(Query("SELECT 'NaN'::numeric::text"),
              IsOkAndHoldsRows({{"NaN"}}));
  EXPECT_THAT(Query("SELECT NULL::numeric::text"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(
      Query(absl::Substitute("SELECT $0::numeric::text", MaxNumericString())),
      IsOkAndHoldsRows({{std::string(MaxNumericString())}}));
  EXPECT_THAT(
      Query(absl::Substitute("SELECT ($0)::numeric::text", MinNumericString())),
      IsOkAndHoldsRows({{std::string(MinNumericString())}}));
}

TEST_F(PGFunctionsTest, NumericEquals) {
  PopulateDatabase(/*insert_nan = */ true);
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value = 123.0)sql"),
              IsOkAndHoldsRows({{1, 1}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value = NULL)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value IS NULL)sql"),
              IsOkAndHoldsRows({{4, Null<int64_t>()}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value = 135.1)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value = 'NaN'::numeric)sql"),
              IsOkAndHoldsRows({{5, 2}}));
}

TEST_F(PGFunctionsTest, NumericNotEquals) {
  PopulateDatabase(/*insert_nan = */ true);
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value != 123.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{2, 0}, {3, 5}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value != NULL
          ORDER BY id)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value IS NOT NULL
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {3, 5}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value != 135.1
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {3, 5}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value != 'NaN'::numeric
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {3, 5}}));
}

TEST_F(PGFunctionsTest, NumericLessThan) {
  PopulateDatabase(/*insert_nan = */ true);
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value < 123.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{2, 0}, {3, 5}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value < 12.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{3, 5}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value < NULL
          ORDER BY id)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value < 'NaN'::numeric
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {3, 5}}));
}

TEST_F(PGFunctionsTest, NumericLessThanEquals) {
  PopulateDatabase(/*insert_nan = */ true);
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value <= 123.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {3, 5}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value <= 12.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{2, 0}, {3, 5}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value <= NULL
          ORDER BY id)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value <= 'NaN'::numeric
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {3, 5}, {5, 2}}));
}

TEST_F(PGFunctionsTest, NumericGreaterThan) {
  PopulateDatabase(/*insert_nan = */ true);
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value > 120.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value > 11.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value > NULL
          ORDER BY id)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value > 'NaN'::numeric
          ORDER BY id)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(PGFunctionsTest, NumericGreaterThanEquals) {
  PopulateDatabase(/*insert_nan = */ true);
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value >= 123.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value >= 12.0
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{1, 1}, {2, 0}, {5, 2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value >= NULL
          ORDER BY id)sql"),
              IsOkAndHoldsRows({}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT id, int_value
          FROM values
          WHERE numeric_value >= 'NaN'::numeric
          ORDER BY id)sql"),
              IsOkAndHoldsRows({{5, 2}}));
}

TEST_F(PGFunctionsTest, ArrayLength) {
  EXPECT_THAT(Query("SELECT array_length(ARRAY[true, false, true, false], 1)"),
              IsOkAndHoldsRows({{4}}));
  EXPECT_THAT(
      Query("SELECT array_length(ARRAY['bytes1'::bytea, 'bytes2'::bytea], 1)"),
      IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(Query("SELECT array_length(ARRAY['1970-01-01'::date, "
                    "'1969-01-01'::date, '1968-01-01'::date], 1)"),
              IsOkAndHoldsRows({{3}}));
  EXPECT_THAT(Query("SELECT array_length(ARRAY[1::float8, 2::float8, "
                    "3::float8, 4::float8, 5::float8], 1)"),
              IsOkAndHoldsRows({{5}}));
  EXPECT_THAT(Query("SELECT array_length(ARRAY[0,1,2,3,4,5,6,7,8,9], 1)"),
              IsOkAndHoldsRows({{10}}));
  EXPECT_THAT(
      Query("SELECT array_length(ARRAY['test1'::text, 'test2'::text], 1)"),
      IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(
      Query("SELECT array_length(ARRAY['1970-01-01 02:03:04'::timestamptz], "
            "1)"),
      IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(Query("SELECT array_length(ARRAY[to_jsonb('hello'::text)], 1)"),
              IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(Query("SELECT array_length(ARRAY[1::numeric], 1)"),
              IsOkAndHoldsRows({{1}}));

  // Returns null for empty array
  EXPECT_THAT(Query("SELECT array_length(ARRAY[]::bigint[], 1)"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));

  // Returns null for dimensions <= 0
  EXPECT_THAT(Query("SELECT array_length(ARRAY[1,2,3], 0)"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));
  EXPECT_THAT(Query("SELECT array_length(ARRAY[1,2,3], -1)"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));

  // Error case
  EXPECT_THAT(
      Query("SELECT array_length(ARRAY[1,2,3], 2)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("multi-dimensional arrays are not supported")));

  // Should fail for GSQL's array_length signature.
  // GSQL's signature is array_length(<array>) while PG's is
  // array_length(<array>, <dimension>).
  EXPECT_THAT(
      Query("SELECT array_length(ARRAY[1,2,3])"),
      // TODO: Remove check if the two environments ever
      // become consistent.
      StatusIs(
          in_prod_env() ? absl::StatusCode::kInvalidArgument
                        : absl::StatusCode::kNotFound,
          HasSubstr("No function matches the given name and argument types")));
}

TEST_F(PGFunctionsTest, ArrayUpper) {
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[true, false, true, false], 1)"),
              IsOkAndHoldsRows({{4}}));
  EXPECT_THAT(
      Query("SELECT array_upper(ARRAY['bytes1'::bytea, 'bytes2'::bytea], 1)"),
      IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(Query("SELECT array_upper(ARRAY['1970-01-01'::date, "
                    "'1969-01-01'::date, '1968-01-01'::date], 1)"),
              IsOkAndHoldsRows({{3}}));
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[1::float8, 2::float8, 3::float8, "
                    "4::float8, 5::float8], 1)"),
              IsOkAndHoldsRows({{5}}));
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[0,1,2,3,4,5,6,7,8,9], 1)"),
              IsOkAndHoldsRows({{10}}));
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[0.1, 1.1, 2.3, 3.9], 1)"),
              IsOkAndHoldsRows({{4}}));
  EXPECT_THAT(
      Query("SELECT array_upper(ARRAY['test1'::text, 'test2'::text], 1)"),
      IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(
      Query("SELECT array_upper(ARRAY['1970-01-01 02:03:04'::timestamptz], 1)"),
      IsOkAndHoldsRows({{1}}));

  // Returns null for empty array
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[]::bigint[], 1)"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));

  // Returns null for dimensions <= 0
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[1,2,3], 0)"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));
  EXPECT_THAT(Query("SELECT array_upper(ARRAY[1,2,3], -1)"),
              IsOkAndHoldsRows({{Null<int64_t>()}}));

  // Error cases
  EXPECT_THAT(
      Query("SELECT array_upper(ARRAY[1,2,3], 2)"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("multi-dimensional arrays are not supported")));
}

TEST_F(PGFunctionsTest, Textregexne) {
  EXPECT_THAT(Query("SELECT textregexne('abcdefg', 'bb.*')"),
              IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(Query("SELECT 'abcdefg' !~ 'bb.*'"), IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(Query("SELECT textregexne('abcdefg', 'bc.*')"),
              IsOkAndHoldsRows({{false}}));
  EXPECT_THAT(Query("SELECT 'abcdefg' !~ 'bc.*'"), IsOkAndHoldsRows({{false}}));

  // Error cases
  EXPECT_THAT(Query("SELECT textregexne('abcd', '(a.c')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
  EXPECT_THAT(Query(absl::StrCat("SELECT '", std::string(10000, 'a'), "' !~ '",
                                 std::string(20000, '('),
                                 std::string(20000, ')'), "'")),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("regular expression is too complex")));
}

TEST_F(PGFunctionsTest, DateMi) {
  EXPECT_THAT(Query("SELECT date_mi('2001-02-01', '2001-01-01')"),
              IsOkAndHoldsRows({{31}}));
  EXPECT_THAT(Query("SELECT '2001-02-01'::date - '2001-01-01'::date"),
              IsOkAndHoldsRows({{31}}));
  EXPECT_THAT(Query("SELECT date_mi('2001-01-01', '2001-01-01')"),
              IsOkAndHoldsRows({{0}}));
  EXPECT_THAT(Query("SELECT '2001-01-01'::date - '2001-01-01'::date"),
              IsOkAndHoldsRows({{0}}));
  EXPECT_THAT(Query("SELECT date_mi('2001-01-01', '2001-02-01')"),
              IsOkAndHoldsRows({{-31}}));
  EXPECT_THAT(Query("SELECT '2001-01-01'::date - '2001-02-01'::date"),
              IsOkAndHoldsRows({{-31}}));
}

TEST_F(PGFunctionsTest, DateMii) {
  EXPECT_THAT(Query("SELECT date_mii('2001-01-01', 365)"),
              IsOkAndHoldsRows({Date(2000, 1, 2)}));  // Leap year
  EXPECT_THAT(Query("SELECT '2001-01-01'::date - 365"),
              IsOkAndHoldsRows({Date(2000, 1, 2)}));

  // Error cases
  EXPECT_THAT(Query("SELECT '0001-01-01'::date - 1"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(PGFunctionsTest, DatePli) {
  EXPECT_THAT(Query("SELECT date_pli('2001-01-01', 365)"),
              IsOkAndHoldsRows({Date(2002, 1, 1)}));
  EXPECT_THAT(Query("SELECT '2001-01-01'::date + 365"),
              IsOkAndHoldsRows({Date(2002, 1, 1)}));

  // Error cases
  EXPECT_THAT(Query("SELECT '9999-12-31'::date + 1"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(PGFunctionsTest, ToDate) {
  EXPECT_THAT(Query("SELECT to_date('01 Jan 1970', 'DD Mon YYYY')"),
              IsOkAndHoldsRows({Date(1970, 1, 1)}));

  // Error cases
  EXPECT_THAT(Query("SELECT to_date('0000-02-01', 'YYYY-MM-DD')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Date is out of supported range")));
}

TEST_F(PGFunctionsTest, ToTimestamp) {
  EXPECT_THAT(
      Query("SELECT to_timestamp('01 Jan 1970 17 03 04 +00', 'DD "
            "Mon YYYY HH24 MI SS TZH')"),
      IsOkAndHoldsRows({MakeTimestamp(absl::ToChronoTime(absl::FromCivil(
          absl::CivilSecond(1970, 1, 1, 17, 3, 4), absl::UTCTimeZone())))}));

  // Error cases
  EXPECT_THAT(Query("SELECT to_timestamp('1997 BC 11 16', 'YYYY BC MM DD')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Timestamp is out of supported range")));
  EXPECT_THAT(
      Query("SELECT to_timestamp('2011-12-18 11:38 PST', 'YYYY-MM-DD HH12:MI "
            "TZ')"),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("formatting field \"TZ\" is only supported in to_char")));
}

TEST_F(PGFunctionsTest, ToNumber) {
  EXPECT_THAT(Query("SELECT to_number('123', '9999')"),
              IsOkAndHoldsRows({{*MakePgNumeric("123")}}));
  EXPECT_THAT(Query("SELECT to_number('123', '9')"),
              IsOkAndHoldsRows({{*MakePgNumeric("1")}}));
  EXPECT_THAT(Query("SELECT to_number('123', '0000')"),
              IsOkAndHoldsRows({{*MakePgNumeric("0123")}}));
  EXPECT_THAT(Query("SELECT to_number('123.456', '999.99')"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT to_number('123,456', '999,999')"),
              IsOkAndHoldsRows({{*MakePgNumeric("123456")}}));
  EXPECT_THAT(Query("SELECT to_number('<123>', '999PR')"),
              IsOkAndHoldsRows({{*MakePgNumeric("-123")}}));
  EXPECT_THAT(Query("SELECT to_number('$123.45', 'L999.99')"),
              IsOkAndHoldsRows({{*MakePgNumeric("123.45")}}));
  EXPECT_THAT(Query("SELECT to_number('3rd', '9th')"),
              IsOkAndHoldsRows({{*MakePgNumeric("3")}}));
  EXPECT_THAT(
      Query("SELECT to_number('Good number: 123', '\"Good number:\" 999')"),
      IsOkAndHoldsRows({{*MakePgNumeric("123")}}));

  // Error cases
  EXPECT_THAT(Query("SELECT to_number('1.23+e05', '9.99EEEE')"),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("\"EEEE\" not supported for input")));
  EXPECT_THAT(Query("SELECT to_number('NaN', '999')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));
}

TEST_F(PGFunctionsTest, ToChar) {
  EXPECT_THAT(Query("SELECT to_char(-123, '999PR')"),
              IsOkAndHoldsRows({{"<123>"}}));
  EXPECT_THAT(Query("SELECT to_char(-123::float8, '999.99PR')"),
              IsOkAndHoldsRows({{"<123.00>"}}));
  EXPECT_THAT(Query("SELECT to_char(123.45, '999')"),
              IsOkAndHoldsRows({{" 123"}}));
  EXPECT_THAT(Query("SELECT to_char('1970-01-01 02:03:04'::timestamptz, "
                    "'YYYY-MM-DD HH24 MI SS')"),
              IsOkAndHoldsRows({{"1970-01-01 02 03 04"}}));

  // Error cases
  EXPECT_THAT(
      Query("SELECT to_char(9, '9.9V9')"),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot use \"V\" and decimal point together")));
  EXPECT_THAT(Query("SELECT to_char(-9, '9PR.9')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("\"9\" must be ahead of \"PR\"")));
}

TEST_F(PGFunctionsTest, QuoteIdent) {
  EXPECT_THAT(Query("SELECT quote_ident('test')"),
              IsOkAndHoldsRows({{"\"test\""}}));
}

TEST_F(PGFunctionsTest, Substring) {
  EXPECT_THAT(Query("SELECT substring('abcdefg', 'a(b.)')"),
              IsOkAndHoldsRows({{"bc"}}));

  // Error cases
  EXPECT_THAT(Query("SELECT substring('abcd', '(a.c')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
}

TEST_F(PGFunctionsTest, RegexpMatch) {
  EXPECT_THAT(Query("SELECT regexp_match('abcdefg', 'b.')"),
              IsOkAndHoldsRows({Array<std::string>({"bc"})}));
  EXPECT_THAT(Query("SELECT regexp_match('aBcdefg', 'b.', 'i')"),
              IsOkAndHoldsRows({Array<std::string>({"Bc"})}));

  // Error cases
  EXPECT_THAT(Query("SELECT regexp_match('abcd', '(a.c')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
}

TEST_F(PGFunctionsTest, RegexpSplitToArray) {
  EXPECT_THAT(Query("SELECT regexp_split_to_array('a1b2c3d', '[0-9]')"),
              IsOkAndHoldsRows({Array<std::string>({"a", "b", "c", "d"})}));
  EXPECT_THAT(Query("SELECT regexp_split_to_array('1A2b3C4', '[a-z]', 'i')"),
              IsOkAndHoldsRows({Array<std::string>({"1", "2", "3", "4"})}));

  // Error cases
  EXPECT_THAT(Query("SELECT regexp_split_to_array('abcd', '(a.c')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid regular expression")));
}

TEST_F(PGFunctionsTest, JsonBObjectFieldText) {
  EXPECT_THAT(Query("select jsonb_object_field_text('{\"a\":1}', 'a')"),
              IsOkAndHoldsRows({{"1"}}));
  EXPECT_THAT(
      Query("select jsonb_object_field_text('{\"a\":{\"b\": 1}}', 'a')"),
      IsOkAndHoldsRows({{"{\"b\": 1}"}}));
  // Pretty-prints the object using a single space for field separation
  EXPECT_THAT(
      Query("select jsonb_object_field_text('{\"a\":{\"b\":        1}}', 'a')"),
      IsOkAndHoldsRows({{"{\"b\": 1}"}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('{\"a\":1}', '')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('1', 'a')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('1.0', 'a')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('\"a\"', 'a')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('{\"a\":1}', 'b')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('null', 'a')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text(NULL::jsonb, 'a')"),
              IsOkAndHoldsRows({{Null<std::string>()}}));
  EXPECT_THAT(Query("select jsonb_object_field_text('{\"a\":1}', NULL::text)"),
              IsOkAndHoldsRows({{Null<std::string>()}}));

  EXPECT_THAT(Query("select jsonb_object_field_text('', 'a')"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax")));
}

TEST_F(PGFunctionsTest, JsonBArrayElementText) {
  EXPECT_THAT(Query("select jsonb_array_element_text('[1,2]', 0)"),
              IsOkAndHoldsRows({"1"}));
  EXPECT_THAT(Query("select jsonb_array_element_text('[1,2]', 1)"),
              IsOkAndHoldsRows({"2"}));

  EXPECT_THAT(Query("select jsonb_array_element_text('[]', 0)"),
              IsOkAndHoldsRows({Null<std::string>()}));
  EXPECT_THAT(Query("select jsonb_array_element_text('[1,2]', 2)"),
              IsOkAndHoldsRows({Null<std::string>()}));
  EXPECT_THAT(Query("select jsonb_array_element_text('[1,2]', -1)"),
              IsOkAndHoldsRows({Null<std::string>()}));
  EXPECT_THAT(Query("select jsonb_array_element_text('null', 0)"),
              IsOkAndHoldsRows({Null<std::string>()}));
  EXPECT_THAT(Query("select jsonb_array_element_text('{\"a\":1}', 0)"),
              IsOkAndHoldsRows({Null<std::string>()}));
  EXPECT_THAT(Query("select jsonb_array_element_text(NULL::jsonb, 0)"),
              IsOkAndHoldsRows({Null<std::string>()}));
  EXPECT_THAT(Query("select jsonb_array_element_text('[]', NULL)"),
              IsOkAndHoldsRows({Null<std::string>()}));

  EXPECT_THAT(Query("select jsonb_array_element_text('', 0)"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax")));
}

TEST_F(PGFunctionsTest, ToJsonB) {
  EXPECT_THAT(Query(R"(select to_jsonb(null::bigint))"),
              IsOkAndHoldsRows({Null<JsonB>()}));
  EXPECT_THAT(Query(R"(select to_jsonb(4))"), IsOkAndHoldsRows({JsonB("4")}));
  EXPECT_THAT(Query(R"(select to_jsonb(fAlSe))"),
              IsOkAndHoldsRows({JsonB("false")}));
  EXPECT_THAT(Query(R"(select to_jsonb(10419.85))"),
              IsOkAndHoldsRows({JsonB("10419.85")}));
  EXPECT_THAT(Query(R"(select to_jsonb('this is a string'::text))"),
              IsOkAndHoldsRows({JsonB("\"this is a string\"")}));
  EXPECT_THAT(Query(R"(select to_jsonb('hello'::bytea))"),
              IsOkAndHoldsRows({JsonB("\"\\\\x68656c6c6f\"")}));
  EXPECT_THAT(Query(R"(select to_jsonb('1999-01-08'::date))"),
              IsOkAndHoldsRows({JsonB("\"1999-01-08\"")}));
  EXPECT_THAT(Query(R"(select to_jsonb('1986-01-01T00:00:01Z'::timestamptz))"),
              IsOkAndHoldsRows({JsonB("\"1986-01-01T00:00:01+00:00\"")}));
  EXPECT_THAT(Query(R"(select to_jsonb('{" ", "ab"}'::bytea[]))"),
              IsOkAndHoldsRows({JsonB("[\"\\\\x20\", \"\\\\x6162\"]")}));
  EXPECT_THAT(Query(R"(select to_jsonb('{"b":[1e0],"a":[20e-1]}'::jsonb))"),
              IsOkAndHoldsRows({JsonB(R"({"a": [2.0], "b": [1]})")}));
  EXPECT_THAT(
      Query(R"(select to_jsonb('-15e1500'::numeric))"),
      IsOkAndHoldsRows({JsonB(std::string("-15" + std::string(1500, '0')))}));
}

TEST_F(PGFunctionsTest, ExtendedTypeCastTest) {
  // Cast <TYPE> to PG.NUMERIC.
  EXPECT_THAT(Query(R"(SELECT CAST(1.1 AS numeric))"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));
  EXPECT_THAT(Query(R"(SELECT CAST(1 AS numeric))"),
              IsOkAndHoldsRows({{*MakePgNumeric("1")}}));
  EXPECT_THAT(Query(R"(SELECT CAST(1.1::float AS numeric))"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));
  EXPECT_THAT(Query(R"(SELECT CAST('1.1' AS numeric))"),
              IsOkAndHoldsRows({{*MakePgNumeric("1.1")}}));

  // Cast PG.NUMERIC to <TYPE>.
  EXPECT_THAT(Query(R"(SELECT CAST('1.1'::numeric AS bigint))"),
              IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(Query(R"(SELECT CAST('1.1'::numeric AS float))"),
              IsOkAndHoldsRows({{1.1}}));
  EXPECT_THAT(Query(R"(SELECT CAST('1.1'::numeric AS text))"),
              IsOkAndHoldsRows({{"1.1"}}));

  // Cast <TYPE> to PG.JSONB.
  EXPECT_THAT(Query(R"(SELECT CAST('{"b":[1e0],"a":[20e-1]}' AS jsonb))"),
              IsOkAndHoldsRows({{JsonB(R"({"a": [2.0], "b": [1]})")}}));

  // Cast PG.JSONB to <TYPE>.
  EXPECT_THAT(Query(R"(SELECT CAST('true'::jsonb AS boolean))"),
              IsOkAndHoldsRows({{true}}));
  EXPECT_THAT(Query(R"(SELECT CAST('1.1'::jsonb AS float))"),
              IsOkAndHoldsRows({{1.1}}));
  EXPECT_THAT(Query(R"(SELECT CAST('1'::jsonb AS bigint))"),
              IsOkAndHoldsRows({{1}}));
  EXPECT_THAT(Query(R"(SELECT CAST('"hello"'::jsonb AS text))"),
              IsOkAndHoldsRows({{"\"hello\""}}));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
