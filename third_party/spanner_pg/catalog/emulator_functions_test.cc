//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/emulator_functions.h"

#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/time/clock.h"
#include "absl/types/span.h"

namespace postgres_translator {
namespace {

using testing::HasSubstr;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

class EmulatorFunctionsTest : public ::testing::Test {
 protected:
  EmulatorFunctionsTest() {
    SpannerPGFunctions spanner_pg_functions =
        GetSpannerPGFunctions("TestCatalog");

    for (auto& function : spanner_pg_functions) {
      functions_[function->Name()] = std::move(function);
    }
  }

  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions_;
  zetasql::FunctionEvaluator evaluator_;
};

struct PGScalarFunctionTestCase {
  std::string function_name;
  std::vector<zetasql::Value> function_arguments;
  zetasql::Value expected_result;
};

using PGScalarFunctionsTest =
    ::testing::TestWithParam<PGScalarFunctionTestCase>;

TEST_P(PGScalarFunctionsTest, ExecutesFunctionsSuccessfully) {
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");

  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }
  const PGScalarFunctionTestCase& param = GetParam();
  const zetasql::Function* function = functions[param.function_name].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::FunctionEvaluator evaluator,
                       (function->GetFunctionEvaluatorFactory())(
                           function->signatures().front()));

  EXPECT_THAT(evaluator(absl::MakeConstSpan(param.function_arguments)),
              IsOkAndHolds(param.expected_result));
}
INSTANTIATE_TEST_SUITE_P(
    PGScalarFunctionTests, PGScalarFunctionsTest,
    ::testing::Values(
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::BoolArray({true}), zetasql::values::Int64(1)},
            zetasql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {zetasql::values::BytesArray({"1", "2"}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::Array(zetasql::types::DateArrayType(),
                                      {zetasql::values::Date(0),
                                       zetasql::values::Date(1)}),
             zetasql::values::Int64(1)},
            zetasql::Value::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {zetasql::values::DoubleArray({1.0}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {zetasql::values::Int64Array({1, 2}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {zetasql::values::StringArray({"a", "b"}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::TimestampArray({absl::Now()}),
             zetasql::values::Int64(1)},
            zetasql::values::Int64(1)},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::Int64Array({1}), zetasql::values::Int64(0)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::Int64Array({1}), zetasql::values::Int64(-1)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::Int64Array({}), zetasql::values::Int64(1)},
            zetasql::values::NullInt64()},

        PGScalarFunctionTestCase{kPGTextregexneFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("bb.*")},
                                 zetasql::values::Bool(true)},
        PGScalarFunctionTestCase{kPGTextregexneFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("ab.*")},
                                 zetasql::values::Bool(false)},

        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {zetasql::values::Date(0), zetasql::values::Date(1)},
            zetasql::values::Int64(-1)},
        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {zetasql::values::Date(0), zetasql::values::Int64(1)},
            zetasql::values::Date(-1)},
        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {zetasql::values::Date(0), zetasql::values::Int64(1)},
            zetasql::values::Date(1)},

        PGScalarFunctionTestCase{kPGToDateFunctionName,
                                 {zetasql::values::String("01 Jan 1970"),
                                  zetasql::values::String("DD Mon YYYY")},
                                 zetasql::values::Date(0)},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {zetasql::values::String("01 Jan 1970 00:00:00+00"),
             zetasql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            zetasql::values::Timestamp(absl::UnixEpoch())},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {zetasql::values::Int64(-123),
                                  zetasql::values::String("999PR")},
                                 zetasql::values::String("<123>")},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {zetasql::values::Timestamp(absl::UnixEpoch()),
             zetasql::values::String("YYYY-MM-DD HH24:MI:SSTZH")},
            zetasql::values::String("1969-12-31 16:00:00-08")},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {zetasql::values::Timestamp(absl::UnixEpoch()),
             zetasql::values::String("")},
            zetasql::values::NullString()},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {zetasql::values::Double(-123.45),
                                  zetasql::values::String("999.999PR")},
                                 zetasql::values::String("<123.450>")},

        PGScalarFunctionTestCase{kPGQuoteIdentFunctionName,
                                 {zetasql::values::String("test")},
                                 zetasql::values::String("\"test\"")},
        PGScalarFunctionTestCase{kPGSubstringFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("a(b.)")},
                                 zetasql::values::String("bc")},
        PGScalarFunctionTestCase{kPGSubstringFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("(h.)?")},
                                 zetasql::values::NullString()},
        PGScalarFunctionTestCase{kPGRegexpMatchFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("b.")},
                                 zetasql::values::StringArray({"bc"})},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcdefg"),
             zetasql::values::String("h.")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcDefg"),
             zetasql::values::String("b.*"), zetasql::values::String("i")},
            zetasql::values::StringArray({"bcDefg"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("a1b2c3d"),
             zetasql::values::String("[0-9]")},
            zetasql::values::StringArray({"a", "b", "c", "d"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"),
             zetasql::values::String("[a-z]"),
             zetasql::values::String("i")},
            zetasql::values::StringArray({"1", "2", "3", "4"})}));

TEST_F(EmulatorFunctionsTest,
       RegexpMatchReturnsNullElementForUnmatchedOptionalCapturingGroups) {
  const zetasql::Function* function =
      functions_[kPGRegexpMatchFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Value expected,
      zetasql::Value::MakeArray(
          zetasql::types::StringArrayType(),
          {zetasql::values::String("bc"), zetasql::values::NullString()}));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::values::String("abcdefg"),
                                      zetasql::values::String("(b.)(h.)?")})),
      IsOkAndHolds(expected));
}

TEST_F(EmulatorFunctionsTest,
       ArrayUpperReturnsErrorWhenDimensionIsGreaterThanOne) {
  const zetasql::Function* function =
      functions_[kPGArrayUpperFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({zetasql::values::StringArray({"a", "b"}),
                               zetasql::values::Int64(2)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("multi-dimensional arrays are not supported")));
}

class EvalCastToDateTest : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGCastToDateFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalCastToDateTest, SuccessfulCast) {
  std::vector<zetasql::Value> args = {
      zetasql::values::String("1999-01-08")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::IsOkAndHolds(zetasql::Value::Date(
                  absl::CivilDay(1999, 1, 8) - absl::CivilDay(1970, 1, 1))));
}

TEST_F(EvalCastToDateTest, UnsupportedDate) {
  std::vector<zetasql::Value> args = {
      zetasql::values::String("January 8 04:05:06 1999 PST")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(EvalCastToDateTest, InvalidArgsCount) {
  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

class EvalCastToTimestampTest : public EmulatorFunctionsTest {
 protected:
  EvalCastToTimestampTest() {
    ABSL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &default_timezone_));
  }

  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGCastToTimestampFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }

  absl::TimeZone default_timezone_;
};

TEST_F(EvalCastToTimestampTest, SuccessfulCast) {
  std::vector<zetasql::Value> args = {
      zetasql::values::String("January 8 04:05:06 1999")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::IsOkAndHolds(zetasql::values::Timestamp(
                  absl::FromCivil(absl::CivilSecond(1999, 1, 8, 4, 5, 6),
                                  default_timezone_))));
}

TEST_F(EvalCastToTimestampTest, UnsupportedTime) {
  std::vector<zetasql::Value> args = {
      zetasql::values::String("January 8 04:05:06 1999 PST")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(EvalCastToTimestampTest, InvalidArgsCount) {
  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

class EvalMapDoubleToIntTest : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGMapDoubleToIntFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }

  void VerifyEquality(const absl::Span<const double> values) {
    ASSERT_GT(values.size(), 1);
    for (int i = 1; i < values.size(); i++) {
      std::vector<zetasql::Value> args1 = {
          zetasql::values::Double(values[i - 1])};
      std::vector<zetasql::Value> args2 = {
          zetasql::values::Double(values[i])};
      ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value res1,
                           evaluator_(absl::MakeConstSpan(args1)));
      ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value res2,
                           evaluator_(absl::MakeConstSpan(args2)));
      EXPECT_EQ(res1.int64_value(), res2.int64_value());
    }
  }

  void VerifyGivenOrder(const absl::Span<const double> values) {
    ASSERT_GT(values.size(), 1);
    for (int i = 1; i < values.size(); i++) {
      std::vector<zetasql::Value> args1 = {
          zetasql::values::Double(values[i - 1])};
      std::vector<zetasql::Value> args2 = {
          zetasql::values::Double(values[i])};
      ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value res1,
                           evaluator_(absl::MakeConstSpan(args1)));
      ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value res2,
                           evaluator_(absl::MakeConstSpan(args2)));
      EXPECT_LT(res1.int64_value(), res2.int64_value());
    }
  }

  std::string RandomString() {
    absl::BitGen gen;
    return std::to_string(
        absl::Uniform<int64_t>(gen, 0, std::numeric_limits<int64_t>::max()));
  }
};

// Verifies that all Nans are mapped to the same value.
TEST_F(EvalMapDoubleToIntTest, NansEquality) {
  VerifyEquality({std::numeric_limits<double>::quiet_NaN(),
                  -std::numeric_limits<double>::quiet_NaN(),
                  std::numeric_limits<double>::signaling_NaN(),
                  -std::numeric_limits<double>::signaling_NaN(), -std::nan(""),
                  -std::nan(RandomString().c_str())});
}

// Verifies that all Zeros are mapped to the same value.
TEST_F(EvalMapDoubleToIntTest, ZerosEquality) { VerifyEquality({0.0, -0.0}); }

// Verifies that outputs follow PostgreSQL FLOAT8 order rules for inputs.
TEST_F(EvalMapDoubleToIntTest, FixedOrder) {
  VerifyGivenOrder({-std::numeric_limits<double>::infinity(),
                    std::numeric_limits<double>::lowest(), -1.03,
                    -std::numeric_limits<double>::min(), 0,
                    std::numeric_limits<double>::min(), 1,
                    std::numeric_limits<double>::max(),
                    std::numeric_limits<double>::infinity(),
                    std::numeric_limits<double>::quiet_NaN()});
}

TEST_F(EvalMapDoubleToIntTest, RandomOrder) {
  // Add at least two distrinct values, so we never end up with one value after
  // dedup.
  std::vector<double> values{std::numeric_limits<double>::min(), 0};
  absl::BitGen gen;
  for (int i = 0; i < 10; i++) {
    values.push_back(
        absl::Uniform<double>(absl::IntervalClosedClosed, gen,
                              -std::numeric_limits<double>::infinity(),
                              std::numeric_limits<double>::infinity()));
  }
  std::sort(values.begin(), values.end());

  // Dedup.
  values.erase(std::unique(values.begin(), values.end()), values.end());

  // Verification.
  VerifyGivenOrder(values);
}

TEST_F(EvalMapDoubleToIntTest, InvalidArgsCount) {
  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

struct PgLeastGreatestTestCase {
  std::string test_name;
  std::vector<zetasql::Value> args;
  std::string type_name;
  size_t expected_least_index;
  size_t expected_greatest_index;
  absl::StatusCode status_code;
};

using EvalLeastGreatestTest = ::testing::TestWithParam<PgLeastGreatestTestCase>;

TEST_P(EvalLeastGreatestTest, TestEvalLeastGreatest) {
  // Setup
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  const zetasql::Function* least_function =
      functions[kPGLeastFunctionName].get();
  const zetasql::Function* greatest_function =
      functions[kPGGreatestFunctionName].get();

  const std::vector<const zetasql::Type*> types = {
      zetasql::types::DoubleType(),   zetasql::types::Int64Type(),
      zetasql::types::BoolType(),     zetasql::types::BytesType(),
      zetasql::types::StringType(),   zetasql::types::DateType(),
      zetasql::types::TimestampType()};

  absl::flat_hash_map<std::string, zetasql::FunctionEvaluator>
      least_evaluators;
  absl::flat_hash_map<std::string, zetasql::FunctionEvaluator>
      greatest_evaluators;

  least_evaluators.reserve(types.size());
  greatest_evaluators.reserve(types.size());
  for (auto type : types) {
    zetasql::FunctionSignature signature(
        type, {type, {type, zetasql::FunctionArgumentType::REPEATED}},
        nullptr);

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        least_evaluators[type->DebugString()],
        (least_function->GetFunctionEvaluatorFactory())(signature));

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        greatest_evaluators[type->DebugString()],
        (greatest_function->GetFunctionEvaluatorFactory())(signature));
  }

  // Test
  const PgLeastGreatestTestCase& test_case = GetParam();

  if (test_case.status_code == absl::StatusCode::kOk) {
    EXPECT_THAT(least_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                zetasql_base::testing::IsOkAndHolds(
                    test_case.args[test_case.expected_least_index]));
    EXPECT_THAT(greatest_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                zetasql_base::testing::IsOkAndHolds(
                    test_case.args[test_case.expected_greatest_index]));
  } else {
    EXPECT_THAT(least_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                zetasql_base::testing::StatusIs(test_case.status_code));
    EXPECT_THAT(greatest_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                zetasql_base::testing::StatusIs(test_case.status_code));
  }
}

INSTANTIATE_TEST_SUITE_P(
    EvalLeastGreatestTests, EvalLeastGreatestTest,
    ::testing::ValuesIn<PgLeastGreatestTestCase>(
        {{"DoubleResultsInMid",
          {zetasql::values::Double(-12),
           zetasql::values::Double(-87980.125),
           zetasql::values::Double(100), zetasql::values::Double(-7)},
          zetasql::types::DoubleType()->DebugString(),
          1,
          2,
          absl::StatusCode::kOk},
         {"DoubleAscending",
          {zetasql::values::Double(-10000.123),
           zetasql::values::Double(-12), zetasql::values::Double(-7),
           zetasql::values::Double(100)},
          zetasql::types::DoubleType()->DebugString(),
          0,
          3,
          absl::StatusCode::kOk},
         {"DoubleDescending",
          {zetasql::values::Double(100), zetasql::values::Double(-7),
           zetasql::values::Double(-12), zetasql::values::Double(-879.125)},
          zetasql::types::DoubleType()->DebugString(),
          3,
          0,
          absl::StatusCode::kOk},
         {"DoubleWithNaN",
          {zetasql::values::Double(std::numeric_limits<double>::quiet_NaN()),
           zetasql::values::Double(-12), zetasql::values::Double(-5),
           zetasql::values::Double(-7)},
          zetasql::types::DoubleType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"DoubleWithNegativeNaN",
          {zetasql::values::Double(-std::numeric_limits<double>::quiet_NaN()),
           zetasql::values::Double(-12), zetasql::values::Double(-5),
           zetasql::values::Double(-7)},
          zetasql::types::DoubleType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"DoubleSingleValue",
          {zetasql::values::Double(-87980.125)},
          zetasql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"DoubleWithInfinitiesAndNaNAndNull",
          {zetasql::values::Double(87980.125),
           zetasql::values::Double(std::numeric_limits<double>::infinity()),
           zetasql::values::Double(std::numeric_limits<double>::quiet_NaN()),
           zetasql::values::NullDouble(),
           zetasql::values::Double(-std::numeric_limits<double>::infinity())},
          zetasql::types::DoubleType()->DebugString(),
          4,
          2,
          absl::StatusCode::kOk},
         {"DoubleAllNaNs",
          {zetasql::values::Double(std::numeric_limits<double>::quiet_NaN()),
           zetasql::values::Double(std::numeric_limits<double>::quiet_NaN())},
          zetasql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"DoubleAllNulls",
          {zetasql::values::NullDouble(), zetasql::values::NullDouble()},
          zetasql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"DoubleSkipNullFirst",
          {zetasql::values::NullDouble(), zetasql::values::Double(100)},
          zetasql::types::DoubleType()->DebugString(),
          1,
          1,
          absl::StatusCode::kOk},
         {"DoubleSkipNullLast",
          {zetasql::values::Double(200), zetasql::values::NullDouble()},
          zetasql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"StringWithDuplicates",
          {zetasql::values::String("aaaaa"),
           zetasql::values::String("aaaab"),
           zetasql::values::String("aaaab"),
           zetasql::values::String("aaaaa")},
          zetasql::types::StringType()->DebugString(),
          0,
          1,
          absl::StatusCode::kOk},
         {"Int64SmallVals",
          {zetasql::values::Int64(0), zetasql::values::Int64(12),
           zetasql::values::Int64(-5), zetasql::values::Int64(7)},
          zetasql::types::Int64Type()->DebugString(),
          2,
          1,
          absl::StatusCode::kOk},
         {"Int64MinMaxVals",
          {zetasql::values::Int64(0), zetasql::values::Int64(12),
           zetasql::values::Int64(-5),
           zetasql::values::Int64(std::numeric_limits<int64_t>::max()),
           zetasql::values::Int64(std::numeric_limits<int64_t>::min()),
           zetasql::values::Int64(-14)},
          zetasql::types::Int64Type()->DebugString(),
          4,
          3,
          absl::StatusCode::kOk},
         {"BoolVals",
          {zetasql::values::Bool(true), zetasql::values::Bool(false),
           zetasql::values::Bool(true), zetasql::values::Bool(false)},
          zetasql::types::BoolType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"BytesWithDuplicates",
          {zetasql::values::Bytes("aaaaa"), zetasql::values::Bytes("aaaab"),
           zetasql::values::Bytes("aaaab"),
           zetasql::values::Bytes("aaaaa")},
          zetasql::types::BytesType()->DebugString(),
          0,
          1,
          absl::StatusCode::kOk},
         {"DateValues",
          {zetasql::values::Date(absl::CivilDay(1999, 1, 8) -
                                   absl::CivilDay(1970, 1, 1)),
           zetasql::values::Date(0), zetasql::values::Date(-1),
           zetasql::values::Date(1000)},
          zetasql::types::DateType()->DebugString(),
          2,
          0,
          absl::StatusCode::kOk},
         {"TimestampValues",
          {zetasql::values::Timestamp(absl::UnixEpoch()),
           zetasql::values::Timestamp(absl::Now() + absl::Hours(20)),
           zetasql::values::Timestamp(absl::Now())},
          zetasql::types::TimestampType()->DebugString(),
          0,
          1,
          absl::StatusCode::kOk},
         {"InvalidArgsCount",
          {},
          zetasql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInternal},
         {"InvalidSingleArgument",
          {zetasql::Value()},
          zetasql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInvalidArgument},
         {"InvalidMidArgument",
          {zetasql::values::Int64(0), zetasql::Value(),
           zetasql::values::Int64(12)},
          zetasql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInvalidArgument},
         {"MismatchedTypes",
          {zetasql::values::Int64(0), zetasql::Value(),
           zetasql::values::Int64(12)},
          zetasql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInvalidArgument}}),
    [](const ::testing::TestParamInfo<EvalLeastGreatestTest::ParamType>& info) {
      return info.param.test_name;
    });

TEST(EvalLeastGreatestInvalidTest, InvalidType) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  zetasql::FunctionSignature signature(
      zetasql::types::Int32Type(),
      {zetasql::types::Int32Type(),
       {zetasql::types::Int32Type(),
        zetasql::FunctionArgumentType::REPEATED}},
      nullptr);

  const zetasql::Function* least_function =
      functions[kPGLeastFunctionName].get();
  EXPECT_THAT((least_function->GetFunctionEvaluatorFactory())(signature),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));

  const zetasql::Function* greatest_function =
      functions[kPGGreatestFunctionName].get();
  EXPECT_THAT((greatest_function->GetFunctionEvaluatorFactory())(signature),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(EvalMinSignatureTest, MinOnlyForDoubleType) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }
  const zetasql::Function* function = functions[kPGMinFunctionName].get();
  const std::vector<zetasql::FunctionSignature>& signatures =
      function->signatures();
  EXPECT_THAT(signatures.size(), 1);
  EXPECT_TRUE(signatures.front().result_type().type()->IsDouble());
  EXPECT_THAT(signatures.front().arguments().size(), 1);
  EXPECT_TRUE(signatures.front().arguments().front().type()->IsDouble());
}

struct EvalMinTestCase {
  std::string test_name;
  std::vector<const zetasql::Value*> args;
  zetasql::Value expected_value;
  absl::StatusCode expected_status_code;
};

using EvalMinTest = ::testing::TestWithParam<EvalMinTestCase>;

TEST_P(EvalMinTest, TestMin) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }
  zetasql::FunctionSignature signature(zetasql::types::DoubleType(),
                                         {zetasql::types::DoubleType()},
                                         nullptr);
  const zetasql::Function* min_function = functions[kPGMinFunctionName].get();
  auto callback = min_function->GetAggregateFunctionEvaluatorFactory();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::AggregateFunctionEvaluator> evaluator,
      callback(signature));

  bool stop_acc = false;
  const EvalMinTestCase& test_case = GetParam();

  // We have to make a copy here because GetParam() returns a const value but
  // the accumulate interface doesn't want a const span.
  std::vector<const zetasql::Value*> args = test_case.args;
  if (test_case.expected_status_code == absl::StatusCode::kOk) {
    int i = 0;
    while (!stop_acc) {
      ZETASQL_EXPECT_OK(
          evaluator->Accumulate(absl::MakeSpan(args).subspan(i), &stop_acc));
      ++i;
    }
    ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value result, evaluator->GetFinalResult());
    EXPECT_THAT(result, test_case.expected_value);
  } else {
    absl::Status status = absl::OkStatus();
    int i = 0;
    while (!stop_acc && status.ok()) {
      status =
          evaluator->Accumulate(absl::MakeSpan(args).subspan(i), &stop_acc);
      ++i;
    }
    EXPECT_THAT(status,
                zetasql_base::testing::StatusIs(test_case.expected_status_code));
  }
}

const zetasql::Value kNullValue = zetasql::values::NullDouble();
const zetasql::Value kDoubleValue = zetasql::values::Double(1.0);
const zetasql::Value kPosInfValue =
    zetasql::values::Double(std::numeric_limits<double>::infinity());
const zetasql::Value kNegInfValue =
    zetasql::values::Double(-1 * std::numeric_limits<double>::infinity());
const zetasql::Value kNaNValue =
    zetasql::values::Double(std::numeric_limits<double>::quiet_NaN());
const zetasql::Value kInvalidValue = zetasql::values::Int64(1);

INSTANTIATE_TEST_SUITE_P(
    EvalMinTests, EvalMinTest,
    ::testing::ValuesIn<EvalMinTestCase>({
        {"OneNullArg", {&kNullValue}, kNullValue, absl::StatusCode::kOk},
        {"EmptyArgs", {}, kNullValue, absl::StatusCode::kOk},
        {"OneDoubleArg", {&kDoubleValue}, kDoubleValue, absl::StatusCode::kOk},
        {"OneDoubleArgOneNullArg",
         {&kDoubleValue, &kNullValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"OneDoubleArgOnePosInfArg",
         {&kDoubleValue, &kPosInfValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"OneDoubleArgOneNegInfArg",
         {&kDoubleValue, &kNegInfValue},
         kNegInfValue,
         absl::StatusCode::kOk},
        {"OnePosInfArgOneNegInfArg",
         {&kPosInfValue, &kNegInfValue},
         kNegInfValue,
         absl::StatusCode::kOk},
        {"OnePosInfArgOneNegInfArg",
         {&kPosInfValue, &kNegInfValue},
         kNegInfValue,
         absl::StatusCode::kOk},
        {"OneNanArg", {&kNaNValue}, kNaNValue, absl::StatusCode::kOk},
        {"OneNullArgOneNanArg",
         {&kNullValue, &kNaNValue},
         kNaNValue,
         absl::StatusCode::kOk},
        {"OneDoubleArgOneNanArg",
         {&kDoubleValue, &kNaNValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"OneNegInfArgOneNanArg",
         {&kNegInfValue, &kNaNValue},
         kNegInfValue,
         absl::StatusCode::kOk},
        {"OnePosInfArgOneNanArg",
         {&kPosInfValue, &kNaNValue},
         kPosInfValue,
         absl::StatusCode::kOk},
    }));

INSTANTIATE_TEST_SUITE_P(EvalMinFailureTests, EvalMinTest,
                         ::testing::ValuesIn<EvalMinTestCase>({
                             {"OneInvalidArg",
                              {&kInvalidValue},
                              kNullValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneValidArgOneInvalidArg",
                              {&kDoubleValue, &kInvalidValue},
                              kNullValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                         }));

}  // namespace
}  // namespace postgres_translator

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
