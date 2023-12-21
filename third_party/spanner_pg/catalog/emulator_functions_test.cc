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

#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/functions/date_time_util.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/random/random.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/civil_time.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace {
using spangres::datatypes::CreatePgJsonbValueWithMemoryContext;
using spangres::datatypes::CreatePgNumericValueWithMemoryContext;
using spangres::datatypes::common::kMaxPGNumericFractionalDigits;
using spangres::datatypes::common::kMaxPGNumericWholeDigits;
using spangres::datatypes::common::MaxNumericString;
using spangres::datatypes::common::MinNumericString;
using testing::HasSubstr;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

static zetasql::Value CreatePgJsonBNullValue() {
  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  return zetasql::values::Null(gsql_pg_jsonb);
}

static zetasql::Value CreatePgNumericNullValue() {
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  return zetasql::values::Null(gsql_pg_numeric);
}

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

// Performs equality with the memory arena initialized. This is necessary for pg
// types that call internal functions in order to convert values into a
// comparable representation (e.g. pg numeric, which uses `numeric_in`).
MATCHER_P(EqPG, result,
          absl::StrCat("EqualPostgreSQLValue(", result.DebugString(), ")")) {
  auto pg_arena = postgres_translator::interfaces::CreatePGArena(nullptr);
  if (!pg_arena.ok()) {
    *result_listener << "pg memory arena could not be initialized "
                     << pg_arena.status();
    return false;
  }
  return arg == result;
}

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
              IsOkAndHolds(EqPG(param.expected_result)));
}

const zetasql::Value kNullDoubleValue = zetasql::values::NullDouble();
const zetasql::Value kDoubleValue = zetasql::values::Double(1.0);
const zetasql::Value kPosInfDoubleValue =
    zetasql::values::Double(std::numeric_limits<double>::infinity());
const zetasql::Value kNegInfDoubleValue =
    zetasql::values::Double(-1 * std::numeric_limits<double>::infinity());
const zetasql::Value kDoubleNaNValue =
    zetasql::values::Double(std::numeric_limits<double>::quiet_NaN());
const zetasql::Value kDoubleMaxValue =
    zetasql::values::Double(std::numeric_limits<double>::max());
const zetasql::Value kDoubleMinValue =
    zetasql::values::Double(std::numeric_limits<double>::min());
const zetasql::Value kDoubleLowestValue =
    zetasql::values::Double(std::numeric_limits<double>::lowest());

const zetasql::Value kNullInt64Value = zetasql::values::NullInt64();
const zetasql::Value kInt64Value = zetasql::values::Int64(1);
const zetasql::Value kInt64MaxValue =
    zetasql::values::Int64(std::numeric_limits<int64_t>::max());
const zetasql::Value kInt64MinValue =
    zetasql::values::Int64(std::numeric_limits<int64_t>::min());

const zetasql::Value kNullPGNumericValue =
    zetasql::values::Null(spangres::datatypes::GetPgNumericType());
const zetasql::Value kPGNumericValue =
    *CreatePgNumericValueWithMemoryContext("1.0");
const zetasql::Value kPGNumericNaNValue =
    *CreatePgNumericValueWithMemoryContext("NaN");
const zetasql::Value kPGNumericMaxValue =
    *CreatePgNumericValueWithMemoryContext(MaxNumericString());
const zetasql::Value kPGNumericMinValue =
    *CreatePgNumericValueWithMemoryContext(MinNumericString());
const zetasql::Value kPGNumericMaxDoubleValueRetainingFirst15Digits =
    *CreatePgNumericValueWithMemoryContext(
        absl::StrCat("179769313486232", std::string(294, '0')));
const zetasql::Value kPGNumericLowestDoubleValueRetainingFirst15Digits =
    *CreatePgNumericValueWithMemoryContext(
        absl::StrCat("-179769313486232", std::string(294, '0')));
const zetasql::Value kPGNumericMinDoubleValueRetainingLast15Digits =
    *CreatePgNumericValueWithMemoryContext(
        absl::StrCat("0.", std::string(307, '0'), "22250738585072"));

const zetasql::Value kNullStringValue = zetasql::values::NullString();
absl::TimeZone default_timezone() {
  absl::TimeZone timezone;
  ABSL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &timezone));
  return timezone;
}
absl::TimeZone timezone = default_timezone();

INSTANTIATE_TEST_SUITE_P(
    PGScalarFunctionTests, PGScalarFunctionsTest,
    ::testing::Values(
        PGScalarFunctionTestCase{
            kPGTimestamptzAddFunctionName,
            {zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             zetasql::values::String("3 months 8 days 20 seconds")},
            zetasql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2005, 4, 10, 3, 4, 25), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             zetasql::values::String("2 years 1 hour")},
            zetasql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2003, 1, 2, 2, 4, 5), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzBinFunctionName,
            {zetasql::values::String("10 seconds"),
             zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2001, 1, 1, 0, 0, 0), timezone))},
            zetasql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2020, 2, 11, 15, 44, 10), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {zetasql::values::String("day"),
             zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone))},
            zetasql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2020, 2, 11, 0, 0, 0), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {zetasql::values::String("day"),
             zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             zetasql::values::String("Australia/Sydney")},
            zetasql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2020, 2, 11, 5, 0, 0), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {zetasql::values::String("second"),
             zetasql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone))},
            *CreatePgNumericValueWithMemoryContext("17"),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {zetasql::values::String("month"), zetasql::values::Date(45)},
            *CreatePgNumericValueWithMemoryContext("2"),
        },
        // pg.jsonb_array_element
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             zetasql::Value::Int64(0)},
            *CreatePgJsonbValueWithMemoryContext("null")},
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([1.00, "string val"])"),
             zetasql::Value::Int64(1)},
            *CreatePgJsonbValueWithMemoryContext(R"("string val")")},
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             zetasql::Value::Int64(2)},
            CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             zetasql::Value::Int64(-1)},
            CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {CreatePgJsonBNullValue(), zetasql::Value::Int64(0)},
            CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             zetasql::Value::NullInt64()},
            CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{
            kPGJsonBArrayElementFunctionName,
            {CreatePgJsonBNullValue(), zetasql::Value::NullInt64()},
            CreatePgJsonBNullValue()},

        // pg.jsonb_object_field
        PGScalarFunctionTestCase{
            kPGJsonBObjectFieldFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
             zetasql::Value::String("a")},
            *CreatePgJsonbValueWithMemoryContext(R"("string val")")},
        PGScalarFunctionTestCase{
            kPGJsonBObjectFieldFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                 R"({"a": {"b": "string_val"}})"),
             zetasql::Value::String("a")},
            *CreatePgJsonbValueWithMemoryContext(R"({"b": "string_val"})")},
        PGScalarFunctionTestCase{kPGJsonBObjectFieldFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                      R"({"a": {"b": "string_val"}})"),
                                  zetasql::Value::String("c")},
                                 CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{kPGJsonBObjectFieldFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                      R"({"a": {"b": "string_val"}})"),
                                  zetasql::Value::String("no match")},
                                 CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{
            kPGJsonBObjectFieldFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"),
             zetasql::Value::String("a")},
            *CreatePgJsonbValueWithMemoryContext(R"("")")},
        PGScalarFunctionTestCase{
            kPGJsonBObjectFieldFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"),
             kNullStringValue},
            CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{kPGJsonBObjectFieldFunctionName,
                                 {CreatePgJsonBNullValue(), kNullStringValue},
                                 CreatePgJsonBNullValue()},
        PGScalarFunctionTestCase{
            kPGJsonBObjectFieldFunctionName,
            {CreatePgJsonBNullValue(), zetasql::Value::String("a")},
            CreatePgJsonBNullValue()},

        // pg.jsonb_typeof
        PGScalarFunctionTestCase{kPGJsonBTypeofFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext("null")},
                                 zetasql::Value::String("null")},
        PGScalarFunctionTestCase{
            kPGJsonBTypeofFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1,2,3.56]")},
            zetasql::Value::String("array")},
        PGScalarFunctionTestCase{
            kPGJsonBTypeofFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"("hello")")},
            zetasql::Value::String("string")},
        PGScalarFunctionTestCase{
            kPGJsonBTypeofFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                R"({ "a" : { "b" : [null, 3.5, -214215, true] } })")},
            zetasql::Value::String("object")},
        PGScalarFunctionTestCase{kPGJsonBTypeofFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                     "-18446744073709551615124125")},
                                 zetasql::Value::String("number")},
        PGScalarFunctionTestCase{kPGJsonBTypeofFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                     "18446744073709551615124125")},
                                 zetasql::Value::String("number")},
        PGScalarFunctionTestCase{
            kPGJsonBTypeofFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                spangres::datatypes::common::MaxJsonBNumericString())},
            zetasql::Value::String("number")},
        PGScalarFunctionTestCase{
            kPGJsonBTypeofFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                spangres::datatypes::common::MinJsonBNumericString())},
            zetasql::Value::String("number")},
        PGScalarFunctionTestCase{kPGJsonBTypeofFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext("true")},
                                 zetasql::Value::String("bool")},
        PGScalarFunctionTestCase{
            kPGJsonBTypeofFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("false")},
            zetasql::Value::String("bool")},

        PGScalarFunctionTestCase{
            kPGNumericAddFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kPGNumericAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("126.90")},
        PGScalarFunctionTestCase{
            kPGNumericAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("-120")},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::BoolArray({true}), zetasql::values::Int64(1)},
            zetasql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {zetasql::values::BytesArray({"1", "2"}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::Array(zetasql::types::DateArrayType(),
                                      {zetasql::values::Date(0),
                                       zetasql::values::Date(1)}),
             zetasql::values::Int64(1)},
            zetasql::Value::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {zetasql::values::DoubleArray({1.0}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {zetasql::values::Int64Array({1, 2}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {zetasql::values::StringArray({"a", "b"}),
                                  zetasql::values::Int64(1)},
                                 zetasql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::TimestampArray({absl::Now()}),
             zetasql::values::Int64(1)},
            zetasql::values::Int64(1)},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::Int64Array({1}), zetasql::values::Int64(0)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::Int64Array({1}), zetasql::values::Int64(-1)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::Int64Array({}), zetasql::values::Int64(1)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {zetasql::values::Int64Array({1}),
                                  zetasql::values::NullInt64()},
                                 zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {zetasql::values::Null(zetasql::types::Int64ArrayType()),
             zetasql::values::Int64(1)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGNumericSubtractFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kPGNumericSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("120.00")},
        PGScalarFunctionTestCase{
            kPGNumericSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("-126.90")},

        PGScalarFunctionTestCase{
            kPGNumericMultiplyFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kPGNumericMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("246.90")},
        PGScalarFunctionTestCase{
            kPGNumericMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("-246.90")},

        PGScalarFunctionTestCase{
            kPGNumericDivideFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kPGNumericDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("61.725")},
        PGScalarFunctionTestCase{
            kPGNumericDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("-61.725")},

        PGScalarFunctionTestCase{
            kPGNumericDivTruncFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kPGNumericDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("61")},
        PGScalarFunctionTestCase{
            kPGNumericDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("-61")},

        PGScalarFunctionTestCase{
            kPGNumericAbsFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("123.45")},
        PGScalarFunctionTestCase{
            kPGNumericAbsFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("123.45")},
        PGScalarFunctionTestCase{kPGNumericAbsFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kPGNumericCeilFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("124")},
        PGScalarFunctionTestCase{
            kPGNumericCeilFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("-123")},
        PGScalarFunctionTestCase{kPGNumericCeilFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kPGNumericCeilingFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("124")},
        PGScalarFunctionTestCase{
            kPGNumericCeilingFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("-123")},
        PGScalarFunctionTestCase{kPGNumericCeilingFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kPGNumericFloorFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("123")},
        PGScalarFunctionTestCase{
            kPGNumericFloorFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("-124")},
        PGScalarFunctionTestCase{kPGNumericFloorFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kPGNumericModFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("10")},
            *CreatePgNumericValueWithMemoryContext("3.45")},
        PGScalarFunctionTestCase{
            kPGNumericModFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("10")},
            *CreatePgNumericValueWithMemoryContext("-3.45")},
        PGScalarFunctionTestCase{kPGNumericModFunctionName,
                                 {CreatePgNumericNullValue(),
                                  *CreatePgNumericValueWithMemoryContext("10")},
                                 CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericModFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kPGNumericTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             zetasql::values::Int64(1)},
            *CreatePgNumericValueWithMemoryContext("123.4")},
        PGScalarFunctionTestCase{
            kPGNumericTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             zetasql::values::Int64(-1)},
            *CreatePgNumericValueWithMemoryContext("120")},
        PGScalarFunctionTestCase{
            kPGNumericTruncFunctionName,
            {CreatePgNumericNullValue(), zetasql::values::Int64(-1)},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kPGNumericTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             zetasql::values::NullInt64()},
            CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kPGNumericUminusFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("-123.45")},
        PGScalarFunctionTestCase{
            kPGNumericUminusFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("123.45")},
        PGScalarFunctionTestCase{kPGNumericUminusFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{kPGNumericCastToInt64FunctionName,
                                 {*CreatePgNumericValueWithMemoryContext("0")},
                                 zetasql::Value::Int64(0)},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.00000001")},
            zetasql::Value::Int64(0)},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.49999999")},
            zetasql::Value::Int64(0)},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.5")},
            zetasql::Value::Int64(1)},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.49999999")},
            zetasql::Value::Int64(-1)},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.5")},
            zetasql::Value::Int64(-2)},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext(
                absl::StrCat(std::numeric_limits<int64_t>::max()))},
            zetasql::Value::Int64(std::numeric_limits<int64_t>::max())},
        PGScalarFunctionTestCase{
            kPGNumericCastToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext(
                absl::StrCat(std::numeric_limits<int64_t>::lowest()))},
            zetasql::Value::Int64(std::numeric_limits<int64_t>::lowest())},
        PGScalarFunctionTestCase{kPGNumericCastToInt64FunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullInt64Value},

        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.000001")},
            zetasql::Value::Double(1.000001)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.299999999999997")},
            zetasql::Value::Double(0.299999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.2999999999999997")},
            zetasql::Value::Double(0.2999999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("299999999999997")},
            zetasql::Value::Double(299999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("2999999999999997")},
            zetasql::Value::Double(2999999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.299999999999997")},
            zetasql::Value::Double(-0.299999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.2999999999999997")},
            zetasql::Value::Double(-0.2999999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-299999999999997")},
            zetasql::Value::Double(-299999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-2999999999999997")},
            zetasql::Value::Double(-2999999999999997)},
        PGScalarFunctionTestCase{
            kPGNumericCastToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN")},
            kDoubleNaNValue},
        PGScalarFunctionTestCase{kPGNumericCastToDoubleFunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullDoubleValue},

        PGScalarFunctionTestCase{kPGNumericCastToStringFunctionName,
                                 {kPGNumericNaNValue},
                                 zetasql::Value::String("NaN")},
        PGScalarFunctionTestCase{kPGNumericCastToStringFunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullStringValue},
        PGScalarFunctionTestCase{kPGNumericCastToStringFunctionName,
                                 {kPGNumericMinValue},
                                 zetasql::Value::String(MinNumericString())},
        PGScalarFunctionTestCase{kPGNumericCastToStringFunctionName,
                                 {kPGNumericMaxValue},
                                 zetasql::Value::String(MaxNumericString())},
        PGScalarFunctionTestCase{
            kPGNumericCastToStringFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.1")},
            zetasql::Value::String("0.1")},

        // CAST_TO_NUMERIC for INT64
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kInt64MaxValue},
            *CreatePgNumericValueWithMemoryContext(
                absl::StrCat(kInt64MaxValue.int64_value()))},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kInt64MinValue},
            *CreatePgNumericValueWithMemoryContext(
                absl::StrCat(kInt64MinValue.int64_value()))},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kInt64Value, kInt64Value},
                                 *CreatePgNumericValueWithMemoryContext("1")},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kNullInt64Value, kInt64Value, kInt64Value},
                                 kNullPGNumericValue},

        // CAST_TO_NUMERIC for DOUBLE
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kDoubleMaxValue},
            kPGNumericMaxDoubleValueRetainingFirst15Digits},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kDoubleLowestValue},
            kPGNumericLowestDoubleValueRetainingFirst15Digits},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kDoubleMinValue},
                                 kPGNumericMinDoubleValueRetainingLast15Digits},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {zetasql::Value::Double(-123.123), zetasql::Value::Int64(4),
             kInt64Value},
            *CreatePgNumericValueWithMemoryContext("-123.1")},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kNullDoubleValue, kInt64Value, kInt64Value},
                                 kNullPGNumericValue},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kDoubleNaNValue, kInt64Value},
                                 kPGNumericNaNValue},

        // CAST_TO_NUMERIC for STRING
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {zetasql::Value::String("123.123"), zetasql::Value::Int64(5),
             zetasql::Value::Int64(2)},
            *CreatePgNumericValueWithMemoryContext("123.12")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {zetasql::Value::String("NaN"), zetasql::Value::Int64(5),
             zetasql::Value::Int64(2)},
            kPGNumericNaNValue},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kNullStringValue, kInt64Value, kInt64Value},
                                 kNullPGNumericValue},

        // CAST_TO_NUMERIC for PG.NUMERIC
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("12.345"),
             zetasql::Value::Int64(4), zetasql::Value::Int64(2)},
            *CreatePgNumericValueWithMemoryContext("12.35")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("12.345"),
             zetasql::Value::Int64(4), kInt64Value},
            *CreatePgNumericValueWithMemoryContext("12.3")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("12.345"),
             zetasql::Value::Int64(4)},
            *CreatePgNumericValueWithMemoryContext("12")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             zetasql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("123")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000001"),
             zetasql::Value::Int64(13), zetasql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("123.0000000001")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000001"),
             zetasql::Value::Int64(13), zetasql::Value::Int64(9)},
            *CreatePgNumericValueWithMemoryContext("123.0000000000")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000001"),
             zetasql::Value::Int64(15), zetasql::Value::Int64(12)},
            *CreatePgNumericValueWithMemoryContext("123.000000000100")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000009"),
             zetasql::Value::Int64(13), zetasql::Value::Int64(9)},
            *CreatePgNumericValueWithMemoryContext("123.000000001")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1"),
             zetasql::Value::Int64(1000), zetasql::Value::Int64(999)},
            *CreatePgNumericValueWithMemoryContext(
                absl::StrCat("1.", std::string(999, '0')))},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext(
                 absl::StrCat("1", std::string(999, '0'))),
             zetasql::Value::Int64(1000), zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext(
                absl::StrCat("1", std::string(999, '0')))},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.5"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("2")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.499999999"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("1")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.5"), kInt64Value,
             zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("-2")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.499999999"),
             kInt64Value, zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("-1")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.5"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("2")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.0000000009"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("-0.0000000009")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.0000000009"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(9)},
            *CreatePgNumericValueWithMemoryContext("-0.000000001")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.000"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("0")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0"),
             zetasql::Value::Int64(10), zetasql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("0.0000000000")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("9.99"),
             zetasql::Value::Int64(3), kInt64Value},
            *CreatePgNumericValueWithMemoryContext("10.0")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.99"), kInt64Value,
             zetasql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("1")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.3"),
             zetasql::Value::Int64(3), zetasql::Value::Int64(3)},
            *CreatePgNumericValueWithMemoryContext("0.300")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kPGNumericNaNValue, zetasql::Value::Int64(5),
             zetasql::Value::Int64(3)},
            kPGNumericNaNValue},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kNullPGNumericValue, zetasql::Value::Int64(5),
             zetasql::Value::Int64(3)},
            kNullPGNumericValue},

        // PG.NUMERIC equals
        PGScalarFunctionTestCase{kPGNumericEqualsFunctionName,
                                 {kNullPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::False()},

        // PG.NUMERIC not equals
        PGScalarFunctionTestCase{kPGNumericNotEqualsFunctionName,
                                 {kNullPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericNotEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericNaNValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericNotEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericNotEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericNotEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericNotEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::True()},

        // PG.NUMERIC less than
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kNullPGNumericValue, kPGNumericMinValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kNullPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericMaxValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericNaNValue, kPGNumericValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericValue, kPGNumericValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericNaNValue, kPGNumericMaxValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericLessThanFunctionName,
                                 {kPGNumericMinValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},

        // PG.NUMERIC less than equals
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericMinValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericMaxValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kNullPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericMaxValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericLessThanEqualsFunctionName,
                                 {kPGNumericMinValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},

        // PG.NUMERIC greater than
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericMinValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericNaNValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericNaNValue, kPGNumericValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericNaNValue, kPGNumericMaxValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericValue, kPGNumericValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kPGNumericMaxValue, kPGNumericNaNValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanFunctionName,
                                 {kNullPGNumericValue, kPGNumericMinValue},
                                 zetasql::values::NullBool()},

        // PG.NUMERIC greater than equals
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericMinValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericMaxValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kNullPGNumericValue, kNullPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericValue},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericValue, kPGNumericNaNValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericNaNValue, kPGNumericNaNValue},
                                 zetasql::values::True()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kPGNumericMaxValue, kPGNumericNaNValue},
                                 zetasql::values::False()},
        PGScalarFunctionTestCase{kPGNumericGreaterThanEqualsFunctionName,
                                 {kNullPGNumericValue, kPGNumericMinValue},
                                 zetasql::values::NullBool()},

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
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {zetasql::values::Int64Array({1}),
                                  zetasql::values::NullInt64()},
                                 zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {zetasql::values::Null(zetasql::types::Int64ArrayType()),
             zetasql::values::Int64(1)},
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
            kPGTextregexneFunctionName,
            {kNullStringValue, zetasql::values::String("ab.*")},
            zetasql::values::NullBool()},
        PGScalarFunctionTestCase{
            kPGTextregexneFunctionName,
            {zetasql::values::String("abcdefg"), kNullStringValue},
            zetasql::values::NullBool()},

        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {zetasql::values::Date(0), zetasql::values::Date(1)},
            zetasql::values::Int64(-1)},
        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {zetasql::values::NullDate(), zetasql::values::Date(1)},
            zetasql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {zetasql::values::Date(0), zetasql::values::NullDate()},
            zetasql::values::NullInt64()},

        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {zetasql::values::Date(0), zetasql::values::Int64(1)},
            zetasql::values::Date(-1)},
        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {zetasql::values::NullDate(), zetasql::values::Int64(1)},
            zetasql::values::NullDate()},
        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {zetasql::values::Date(0), zetasql::values::NullInt64()},
            zetasql::values::NullDate()},

        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {zetasql::values::Date(0), zetasql::values::Int64(1)},
            zetasql::values::Date(1)},
        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {zetasql::values::NullDate(), zetasql::values::Int64(1)},
            zetasql::values::NullDate()},
        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {zetasql::values::Date(0), zetasql::values::NullInt64()},
            zetasql::values::NullDate()},

        PGScalarFunctionTestCase{kPGToDateFunctionName,
                                 {zetasql::values::String("01 Jan 1970"),
                                  zetasql::values::String("DD Mon YYYY")},
                                 zetasql::values::Date(0)},
        PGScalarFunctionTestCase{
            kPGToDateFunctionName,
            {kNullStringValue, zetasql::values::String("DD Mon YYYY")},
            zetasql::values::NullDate()},
        PGScalarFunctionTestCase{
            kPGToDateFunctionName,
            {zetasql::values::String("01 Jan 1970"), kNullStringValue},
            zetasql::values::NullDate()},

        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {zetasql::values::String("01 Jan 1970 00:00:00+00"),
             zetasql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            zetasql::values::Timestamp(absl::UnixEpoch())},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {kNullStringValue,
             zetasql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            zetasql::values::NullTimestamp()},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {zetasql::values::String("01 Jan 1970 00:00:00+00"),
             kNullStringValue},
            zetasql::values::NullTimestamp()},

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
            kNullStringValue},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {zetasql::values::Double(-123.45),
                                  zetasql::values::String("999.999PR")},
                                 zetasql::values::String("<123.450>")},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {CreatePgNumericValueWithMemoryContext("123.45").value(),
             zetasql::values::String("999")},
            zetasql::values::String(" 123")},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {zetasql::values::NullDouble(),
                                  zetasql::values::String("999.999PR")},
                                 kNullStringValue},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {zetasql::values::Double(-123.45), kNullStringValue},
            kNullStringValue},

        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {zetasql::values::String("-12,345,678"),
             zetasql::values::String("99G999G999")},
            *CreatePgNumericValueWithMemoryContext("-12345678")},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {zetasql::values::String("<123.456>"),
             zetasql::values::String("999.999PR")},
            *CreatePgNumericValueWithMemoryContext("-123.456")},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {zetasql::values::String("$123.45-"),
             zetasql::values::String("L999.99S")},
            *CreatePgNumericValueWithMemoryContext("-123.45")},
        PGScalarFunctionTestCase{kPGToNumberFunctionName,
                                 {zetasql::values::String("42nd"),
                                  zetasql::values::String("99th")},
                                 *CreatePgNumericValueWithMemoryContext("42")},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {kNullStringValue, zetasql::values::String("999")},
            zetasql::values::Null(spangres::datatypes::GetPgNumericType())},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {zetasql::values::String("123"), kNullStringValue},
            zetasql::values::Null(spangres::datatypes::GetPgNumericType())},

        PGScalarFunctionTestCase{kPGQuoteIdentFunctionName,
                                 {zetasql::values::String("test")},
                                 zetasql::values::String("\"test\"")},
        PGScalarFunctionTestCase{
            kPGQuoteIdentFunctionName, {kNullStringValue}, kNullStringValue},

        PGScalarFunctionTestCase{kPGSubstringFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("a(b.)")},
                                 zetasql::values::String("bc")},
        PGScalarFunctionTestCase{kPGSubstringFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("(h.)?")},
                                 kNullStringValue},

        PGScalarFunctionTestCase{kPGRegexpMatchFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::String("b.")},
                                 zetasql::values::StringArray({"bc"})},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {kNullStringValue, zetasql::values::String("b.")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcdefg"), kNullStringValue},
            zetasql::values::Null(zetasql::types::StringArrayType())},
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
            kPGRegexpMatchFunctionName,
            {kNullStringValue, zetasql::values::String("b.*"),
             zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcDefg"), kNullStringValue,
             zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcDefg"),
             zetasql::values::String("b.*"), kNullStringValue},
            zetasql::values::Null(zetasql::types::StringArrayType())},

        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("a1b2c3d"),
             zetasql::values::String("[0-9]")},
            zetasql::values::StringArray({"a", "b", "c", "d"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {kNullStringValue, zetasql::values::String("[0-9]")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("a1b2c3d"), kNullStringValue},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"),
             zetasql::values::String("[a-z]"),
             zetasql::values::String("i")},
            zetasql::values::StringArray({"1", "2", "3", "4"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {kNullStringValue, zetasql::values::String("[a-z]"),
             zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"), kNullStringValue,
             zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"),
             zetasql::values::String("[a-z]"), kNullStringValue},
            zetasql::values::Null(zetasql::types::StringArrayType())}));

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
          {zetasql::values::String("bc"), kNullStringValue}));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::values::String("abcdefg"),
                                      zetasql::values::String("(b.)(h.)?")})),
      IsOkAndHolds(expected));
}

// Tested separately from the parameterized tests as we need a memory context
// before creating a PG.JSONB value.
TEST_F(EmulatorFunctionsTest, ArrayUpperWithPGJsonb) {
  const zetasql::Function* function =
      functions_[kPGArrayUpperFunctionName].get();
  zetasql::FunctionSignature signature(
      zetasql::types::Int64Type(),
      {postgres_translator::spangres::datatypes::GetPgJsonbArrayType(),
       zetasql::types::Int64Type()},
      /*context_ptr=*/nullptr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (function->GetFunctionEvaluatorFactory())(signature));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto pg_arena, interfaces::CreatePGArena(nullptr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto pg_jsonb_array,
      zetasql::Value::MakeArray(
          spangres::datatypes::GetPgJsonbArrayType(),
          {spangres::datatypes::CreatePgJsonbValue("{\"a\": \"b\"}").value(),
           spangres::datatypes::CreatePgJsonbValue("null").value(),
           spangres::datatypes::CreatePgJsonbValue("[1, 2, 3]").value()}));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, zetasql::values::Int64(1)})),
              IsOkAndHolds(zetasql::values::Int64(3)));
}

// Tested separately from the parameterized tests as we need a memory context
// before creating a PG.NUMERIC value.
TEST_F(EmulatorFunctionsTest, ArrayUpperWithPGNumeric) {
  const zetasql::Function* function =
      functions_[kPGArrayUpperFunctionName].get();
  zetasql::FunctionSignature signature(
      zetasql::types::Int64Type(),
      {postgres_translator::spangres::datatypes::GetPgNumericArrayType(),
       zetasql::types::Int64Type()},
      /*context_ptr=*/nullptr);
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (function->GetFunctionEvaluatorFactory())(signature));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto pg_arena, interfaces::CreatePGArena(nullptr));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto pg_numeric_array,
      zetasql::Value::MakeArray(
          spangres::datatypes::GetPgNumericArrayType(),
          {spangres::datatypes::CreatePgNumericValue("1.3").value(),
           spangres::datatypes::CreatePgNumericValue("0.1").value()}));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_numeric_array, zetasql::values::Int64(1)})),
              IsOkAndHolds(zetasql::values::Int64(2)));
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

TEST_F(EmulatorFunctionsTest, ToCharReturnsErrorWhenTypeUnsupported) {
  const zetasql::Function* function = functions_[kPGToCharFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("3.14").value(),
                   zetasql::values::String("999")})),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("to_char(PG.JSONB, text)")));
}

TEST_F(EmulatorFunctionsTest, AddReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericAddFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, AddReturnsErrorWhenResultIsOverflow) {
  const zetasql::Function* function =
      functions_[kPGNumericAddFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("1")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-1")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, SubtractReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericSubtractFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, SubtractReturnsErrorWhenResultIsOverflow) {
  const zetasql::Function* function =
      functions_[kPGNumericSubtractFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-1")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("1")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, MultiplyReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericMultiplyFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, MultiplyReturnsErrorWhenResultIsOverflow) {
  const zetasql::Function* function =
      functions_[kPGNumericMultiplyFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("2.0")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-2.0")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivideReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericDivideFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, DivideReturnsErrorWhenDividingByZero) {
  const zetasql::Function* function =
      functions_[kPGNumericDivideFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.00")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.0")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivideReturnsErrorWhenResultIsOverflow) {
  const zetasql::Function* function =
      functions_[kPGNumericDivideFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.5")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-0.5")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivTruncReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericDivTruncFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, DivTruncReturnsErrorWhenDividingByZero) {
  const zetasql::Function* function =
      functions_[kPGNumericDivTruncFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.00")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.0")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivTruncReturnsErrorWhenResultIsOverflow) {
  const zetasql::Function* function =
      functions_[kPGNumericDivTruncFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.5")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-0.5")})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, UminusReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericUminusFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToDoubleReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericCastToDoubleFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Value too small to be represented by a double.
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {*CreatePgNumericValueWithMemoryContext("-1.79769313486232e+308")})),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Cannot cast to double")));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToStringReturnsErrorWhenArgumentsAreInvalid) {
  const zetasql::Function* function =
      functions_[kPGNumericCastToStringFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInvalidArgumentSizeError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  std::vector<zetasql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kPGNumericValue, kInt64Value, kInt64Value, kInt64Value})),
              zetasql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithNullPrecisionScaleError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Null precision or scale
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kInt64Value, kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kDoubleValue, kNullInt64Value, kInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String("1.0"), kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kPGNumericValue, kInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));

  // Test that null precision and scale is checked first when value is special
  // (NaN/NULL).
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({kDoubleNaNValue, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kPosInfDoubleValue, kNullInt64Value, kInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kNegInfDoubleValue, kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kNullDoubleValue, kInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kNullStringValue, kNullInt64Value, kInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({kPGNumericNaNValue, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kNullPGNumericValue, kInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithOutOfRangePrecisionScaleError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Out of range precision and scale
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kPGNumericValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kPGNumericValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              zetasql::Value::Int64(1001),
                                              zetasql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              zetasql::Value::Int64(-1),
                                              zetasql::Value::Int64(-2)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              zetasql::Value::Int64(-1),
                                              zetasql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("1.0"),
                                      zetasql::Value::Int64(2),
                                      zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));

  // Test that out-of-range precision and scale is checked first when value is
  // special (NaN/NULL).
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNullPGNumericValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kPGNumericNaNValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNullDoubleValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kDoubleNaNValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kPosInfDoubleValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNegInfDoubleValue,
                                              zetasql::Value::Int64(1001),
                                              zetasql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullInt64Value,
                                              zetasql::Value::Int64(-1),
                                              zetasql::Value::Int64(-2)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullStringValue,
                                              zetasql::Value::Int64(-1),
                                              zetasql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNullStringValue, zetasql::Value::Int64(2),
                               zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::String("-Inf"),
                                              zetasql::Value::Int64(10001),
                                              zetasql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithTooSmallPrecisionError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::Int64(1000), zetasql::Value::Int64(2)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::Double(99), kInt64Value})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext("1234.987654321"),
                   zetasql::Value::Int64(5), zetasql::Value::Int64(2)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String("1234.987654321"),
                   zetasql::Value::Int64(3), zetasql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String("-1e1000"),
                   zetasql::Value::Int64(3), zetasql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInfinityDoubleError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Infinity double value
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNegInfDoubleValue})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPosInfDoubleValue})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));

  // Infinity double value with valid precision and scale: expect same error as
  // when there are no precision and scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNegInfDoubleValue,
                                              zetasql::Value::Int64(1000),
                                              zetasql::Value::Int64(100)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({kPosInfDoubleValue, kInt64Value})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Cannot cast infinity to PG.NUMERIC")));

  // Infinity double value with out of range precision and scale: expect error
  // regarding invalid precision/scale
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNegInfDoubleValue, zetasql::Value::Int64(100),
                               zetasql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kPosInfDoubleValue, zetasql::Value::Int64(1001)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kPosInfDoubleValue, zetasql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNegInfDoubleValue, zetasql::Value::Int64(100),
                               zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));

  // Infinity double value with null precision and scale: expect error regarding
  // null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kPosInfDoubleValue, zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNegInfDoubleValue, zetasql::Value::Int64(100),
                               zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

// The following test is known to produce different error messages compared to
// Spanner PROD.
TEST_F(
    EmulatorFunctionsTest,
    DISABLED_CastToNumericWithInfinityStringError_KnownProdEmulatorErrorMessageMismatches) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Infinity string value with or without space padding with valid precision
  // and scale: expect same error as when there are no precision and scale

  // Emulator returns error "[ERROR] numeric field overflow Detail: A field with
  // precision 1000, scale 100 cannot hold an infinite value."
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-infinity"),
                                      zetasql::Value::Int64(1000),
                                      zetasql::Value::Int64(100)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1, scale 0 cannot hold an infinite value."
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String(" INFinity "), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1000, scale 100 cannot hold an infinite value."
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+INFINITY "),
                                      zetasql::Value::Int64(1000),
                                      zetasql::Value::Int64(100)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1, scale 0 cannot hold an infinite value."
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String("-iNf"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1000, scale 100 cannot hold an infinite value."
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::String("inf"),
                                              zetasql::Value::Int64(1000),
                                              zetasql::Value::Int64(100)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1, scale 0 cannot hold an infinite value.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String(" +INF"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInfinityStringError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Infinity string value with or without space padding
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-infinity")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(" INFinity ")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+INFINITY ")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-iNf")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("inf")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(" +INF")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));

  // Infinity string value with or without space padding with invalid precision
  // and scale: expect error regarding invalid precision/scale
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-infinity"),
                                      zetasql::Value::Int64(100),
                                      zetasql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(" INFinity "),
                                      zetasql::Value::Int64(1001)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+INFINITY "),
                                      zetasql::Value::Int64(0)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-iNf"),
                                      zetasql::Value::Int64(100),
                                      zetasql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::String("inf"),
                                              zetasql::Value::Int64(1001)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::String(" +INF"),
                                              zetasql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));

  // Infinity string value with or without space padding with null precision and
  // scale: expect error regarding null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-infinity"),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(" INFinity "),
                                      zetasql::Value::Int64(100),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+INFINITY "),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("-iNf"),
                                      zetasql::Value::Int64(100),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String("inf"), zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(" +INF"),
                                      zetasql::Value::Int64(100),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInvalidStringError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Invalid string
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("invalid")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("- iNf")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+ Infinity")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric")));

  // Invalid string with valid precision and scale: expect same error as
  // when there are no precision and scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String("invalid"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::String("- iNf"),
                                              zetasql::Value::Int64(1000),
                                              kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String("+ Infinity"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));

  // Invalid string with invalid precision and scale: expect error regarding
  // invalid precision/scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::String("NULL"),
                                              zetasql::Value::Int64(1000000),
                                              kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("Inf"),
                                      zetasql::Value::Int64(1000),
                                      zetasql::Value::Int64(10000)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+ Infinity"),
                                      zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));

  // Invalid string with null precision and scale: expect error regarding null
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({zetasql::Value::String("NULL"),
                               zetasql::Value::NullInt64(), kInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("Inf"),
                                      zetasql::Value::Int64(1000),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("+ Infinity"),
                                      zetasql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

// The following test is known to produce different errors/results compared to
// Spanner PROD.
TEST_F(
    EmulatorFunctionsTest,
    DISABLED_CastToNumericWithTooLargeStringExponentError_KnownProdEmulatorMismatches) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Emulator returns "numeric field overflow Detail: A field with precision
  // 1000, scale 0 must round to an absolute value less than 10^1000"
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("1e1000000"),
                                      zetasql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("exponent that is too large")));

  // Emulator returns 0
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String("1e-100000"), zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("fractional component of NUMERIC")));

  // Emulator returns 0
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String("1e-10000"), zetasql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("must round to an absolute value less than")));
}

TEST_F(EmulatorFunctionsTest,
       FAILEDCastToNumericWithTooLargeStringExponentError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Exponent values are too large for string to be represented as a numeric
  // value but precision and scale are invalid: expect error regarding invalid
  // precision/scale
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("1e1000000"),
                                      zetasql::Value::Int64(1001)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("1e-100000"),
                                      zetasql::Value::Int64(3),
                                      zetasql::Value::Int64(100)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String("1e-10000"), zetasql::Value::Int64(0)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));

  // Exponent values are too large for string to be represented as a numeric
  // value but precision and scale are null: expect error regarding null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String("1e1000000"), kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({zetasql::Value::String("1e-100000"),
                               zetasql::Value::Int64(3), kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String("1e-10000"),
                                      kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

// The following test is known to produce different error messages compared to
// Spanner PROD.
TEST_F(
    EmulatorFunctionsTest,
    DISABLED_CastToNumericWithTooLargeStringValueError_KnownProdEmulatorErrorMessageMismatches) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Values are too large to be represented as a numeric value
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(
          std::string(kMaxPGNumericWholeDigits + 1, '9'))})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value: whole component of NUMERIC")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {zetasql::Value::String(std::string(147466, '9'))})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(absl::StrCat(
          "0.", std::string(kMaxPGNumericFractionalDigits + 1, '9')))})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Invalid NUMERIC value: fractional component of NUMERIC")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(
          absl::StrCat(std::string(131073, '9'), ".",
                       std::string(kMaxPGNumericFractionalDigits + 1, '9')))})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithTooLargeStringValueError) {
  const zetasql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Values are too large to be represented as a numeric value but precision and
  // scale are invalid: expect error regarding invalid precision/scale
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({zetasql::Value::String(std::string(
                                   kMaxPGNumericWholeDigits + 1, '9')),
                               kInt64Value, zetasql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String(std::string(
               kMaxPGNumericWholeDigits + kMaxPGNumericFractionalDigits + 1,
               '9')),
           zetasql::Value::Int64(1001)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String(absl::StrCat(
               "0.", std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           zetasql::Value::Int64(0)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String(absl::StrCat(
               std::string(kMaxPGNumericWholeDigits + 1, '9'), ".",
               std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           kInt64Value, zetasql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));

  // Values are too large to be represented as a numeric value but precision and
  // scale are null: expect error regarding null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({zetasql::Value::String(std::string(
                                          kMaxPGNumericWholeDigits + 1, '9')),
                                      kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String(std::string(
               kMaxPGNumericWholeDigits + kMaxPGNumericFractionalDigits + 1,
               '9')),
           kNullInt64Value, zetasql::Value::Int64(0)})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String(absl::StrCat(
               "0.", std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           kInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::Value::String(absl::StrCat(
               std::string(kMaxPGNumericWholeDigits + 1, '9'), ".",
               std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

TEST_F(EmulatorFunctionsTest, CastNumericToInt64ReturnsErrorForNaN) {
  const zetasql::Function* function =
      functions_[kPGNumericCastToInt64FunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext("NaN")})),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("cannot convert NaN to bigint")));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToInt64ReturnsErrorForOverflowAndUnderflow) {
  const zetasql::Function* function =
      functions_[kPGNumericCastToInt64FunctionName].get();
  ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {*CreatePgNumericValueWithMemoryContext(MaxNumericString())})),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("bigint out of range")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {*CreatePgNumericValueWithMemoryContext(MinNumericString())})),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("bigint out of range")));
}

class EvalToJsonBTest : public EmulatorFunctionsTest {
 protected:
  const std::string kMaxPgJsonbNumericWholeDigitStr = std::string(
      spangres::datatypes::common::kMaxPGJSONBNumericWholeDigits, '9');
  const std::string kMaxPgJsonbNumericFractionalDigitStr = std::string(
      spangres::datatypes::common::kMaxPGJSONBNumericFractionalDigits, '9');
  const std::string kMaxPgJsonbNumericDigitStr =
      std::string(kMaxPgJsonbNumericWholeDigitStr + "." +
                  kMaxPgJsonbNumericFractionalDigitStr);

  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGToJsonBFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

MATCHER_P(NullToJsonB, input, "") {
  EXPECT_THAT(arg(absl::MakeConstSpan({input})),
              zetasql_base::testing::IsOkAndHolds(zetasql::values::Null(
                  spangres::datatypes::GetPgJsonbType())));
  return true;
}

TEST_F(EvalToJsonBTest, NullValueInput) {
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullBool()));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullInt64()));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullDouble()));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullDate()));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullTimestamp()));
  EXPECT_THAT(evaluator_, NullToJsonB(kNullStringValue));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullBytes()));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::Null(
                              spangres::datatypes::GetPgJsonbType())));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::Null(
                              spangres::datatypes::GetPgNumericType())));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::Null(
                              zetasql::types::StringArrayType())));
  EXPECT_THAT(
      evaluator_,
      NullToJsonB(zetasql::values::Null(zetasql::types::Int64ArrayType())));
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::Null(
                              zetasql::types::DoubleArrayType())));
  EXPECT_THAT(
      evaluator_,
      NullToJsonB(zetasql::values::Null(zetasql::types::BytesArrayType())));
}

MATCHER_P2(TimestampToJsonB, input, expected_string, "") {
  absl::Time timestamp;
  absl::Status status = zetasql::functions::ConvertStringToTimestamp(
      input, absl::UTCTimeZone(),
      zetasql::functions::TimestampScale::kNanoseconds,
      /*allow_tz_in_str=*/true, &timestamp);
  if (!status.ok()) {
    *result_listener << "\nFailed to convert string to timestamp: " << status;
    return false;
  }
  EXPECT_THAT(
      arg(absl::MakeConstSpan({zetasql::values::Timestamp(timestamp)})),
      zetasql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, TimestampInput) {
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01Z",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01.0Z",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01.1Z",
                                           "\"1986-01-01T00:00:01.1+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01.01Z",
                                           "\"1986-01-01T00:00:01.01+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonB("1986-01-01T00:00:01.001Z",
                               "\"1986-01-01T00:00:01.001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonB("1986-01-01T00:00:01.0001Z",
                               "\"1986-01-01T00:00:01.0001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonB("1986-01-01T00:00:01.00001Z",
                               "\"1986-01-01T00:00:01.00001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonB("1986-01-01T00:00:01.000100Z",
                               "\"1986-01-01T00:00:01.0001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonB("1986-01-01T00:00:01.000101Z",
                               "\"1986-01-01T00:00:01.000101+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonB("1986-01-01T00:00:01.001001100Z",
                               "\"1986-01-01T00:00:01.001001+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01 00:00:01Z",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01 00:00:01",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01+5:30",
                                           "\"1985-12-31T18:30:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonB("1986-01-01T00:00:01+5:30",
                                           "\"1985-12-31T18:30:01+00:00\""));
}

MATCHER_P2(BoolToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({zetasql::values::Bool(input)})),
      zetasql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, BoolInput) {
  EXPECT_THAT(evaluator_, BoolToJsonB(true, "true"));
  EXPECT_THAT(evaluator_, BoolToJsonB(false, "false"));
}

MATCHER_P2(Int64ToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({zetasql::values::Int64(input)})),
      zetasql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, Int64Input) {
  EXPECT_THAT(evaluator_, Int64ToJsonB(10, "10"));
  EXPECT_THAT(evaluator_, Int64ToJsonB(std::numeric_limits<int64_t>::max(),
                                       "9223372036854775807"));
  EXPECT_THAT(evaluator_, Int64ToJsonB(std::numeric_limits<int64_t>::min(),
                                       "-9223372036854775808"));
}

MATCHER_P2(DoubleToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({zetasql::values::Double(input)})),
      zetasql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, DoubleInput) {
  EXPECT_THAT(evaluator_, DoubleToJsonB(0.0, "0"));
  EXPECT_THAT(evaluator_, DoubleToJsonB(3.14, "3.14"));
  EXPECT_THAT(evaluator_, DoubleToJsonB(3.14000000, "3.14"));
  EXPECT_THAT(evaluator_,
              DoubleToJsonB(3.14567897543568997764, "3.14567897543569"));
  EXPECT_THAT(evaluator_,
              DoubleToJsonB(3.14567897543562524102, "3.1456789754356254"));
  EXPECT_THAT(evaluator_, DoubleToJsonB(-33.1234954500, "-33.12349545"));
  EXPECT_THAT(evaluator_, DoubleToJsonB(0.0000134200, "0.00001342"));
  EXPECT_THAT(evaluator_, DoubleToJsonB(0.0000000000000000000100000000000000001,
                                        "0.00000000000000000001"));
  EXPECT_THAT(evaluator_,
              DoubleToJsonB(0.000000000000000000010000000000000001,
                            "0.000000000000000000010000000000000001"));
  EXPECT_THAT(evaluator_, DoubleToJsonB(NAN, "\"NaN\""));
  EXPECT_THAT(evaluator_, DoubleToJsonB(-INFINITY, "\"-Infinity\""));
  EXPECT_THAT(evaluator_, DoubleToJsonB(+INFINITY, "\"Infinity\""));
  EXPECT_THAT(evaluator_, DoubleToJsonB(std::numeric_limits<double>::max(),
                                        absl::StrCat("17976931348623157",
                                                     std::string(292, '0'))));
  EXPECT_THAT(evaluator_,
              DoubleToJsonB(std::numeric_limits<double>::min(),
                            absl::StrCat("0.", std::string(307, '0'),
                                         "22250738585072014")));
  EXPECT_THAT(evaluator_, DoubleToJsonB(std::numeric_limits<double>::lowest(),
                                        absl::StrCat("-17976931348623157",
                                                     std::string(292, '0'))));
}

MATCHER_P2(DateToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan(
          {zetasql::values::Date(input - absl::CivilDay(1970, 1, 1))})),
      zetasql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, DateInput) {
  EXPECT_THAT(evaluator_,
              DateToJsonB(absl::CivilDay(1970, 1, 1), "\"1970-01-01\""));
  EXPECT_THAT(evaluator_,
              DateToJsonB(absl::CivilDay(1971, 1, 1), "\"1971-01-01\""));
  EXPECT_THAT(evaluator_,
              DateToJsonB(absl::CivilDay(1971, 1, 1), "\"1971-01-01\""));
}

MATCHER_P2(StringToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({zetasql::values::String(input)})),
      zetasql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, StringInput) {
  EXPECT_THAT(evaluator_, StringToJsonB("hello", "\"hello\""));
  EXPECT_THAT(evaluator_,
              StringToJsonB("special characters(', \", \r, \n)",
                            "\"special characters(', \\\", \\r, \\n)\""));
  EXPECT_THAT(evaluator_,
              StringToJsonB("non ascii characters(ß, Д, \u0001)",
                            "\"non ascii characters(ß, Д, \\u0001)\""));
  EXPECT_THAT(evaluator_, StringToJsonB("", "\"\""));
  EXPECT_THAT(evaluator_, StringToJsonB("例子", R"("例子")"));
  EXPECT_THAT(evaluator_,
              StringToJsonB("{\"a\":      1}", "\"{\\\"a\\\":      1}\""));
}

MATCHER_P2(BytesToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({zetasql::values::Bytes(input)})),
      zetasql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, BytesInput) {
  EXPECT_THAT(evaluator_, BytesToJsonB(" ", "\"\\\\x20\""));
  EXPECT_THAT(evaluator_, BytesToJsonB("hello", "\"\\\\x68656c6c6f\""));
  EXPECT_THAT(evaluator_, BytesToJsonB("special characters(', \\\", \\r, \\n)",
                                       "\"\\\\x7370656369616c206368617261637465"
                                       "727328272c205c222c205c722c205c6e29\""));
  EXPECT_THAT(evaluator_, BytesToJsonB("non ascii characters(ß, Д, \u0001)",
                                       "\"\\\\x6e6f6e20617363696920636861726163"
                                       "7465727328c39f2c20d0942c200129\""));
  EXPECT_THAT(evaluator_, BytesToJsonB("", "\"\\\\x\""));
  EXPECT_THAT(evaluator_, BytesToJsonB("例子", "\"\\\\xe4be8be5ad90\""));
}

MATCHER_P2(JsonBToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan(
          {CreatePgJsonbValueWithMemoryContext(input).value()})),
      zetasql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, JsonBInput) {
  EXPECT_THAT(evaluator_, JsonBToJsonB(R"({"a":1.0, "b" : null})",
                                       R"({"a": 1.0, "b": null})"));
  EXPECT_THAT(evaluator_,
              JsonBToJsonB(R"({"a"  :[ "b" , "c" ]})", R"({"a": ["b", "c"]})"));
  EXPECT_THAT(evaluator_, JsonBToJsonB("  1.0 ", "1.0"));
  EXPECT_THAT(evaluator_, JsonBToJsonB(R"(   "abcd"  )", R"("abcd")"));
  EXPECT_THAT(evaluator_, JsonBToJsonB("[1,2,  3,   4]", "[1, 2, 3, 4]"));

  // Test normalization of PG.NUMERIC and PG.JSONB
  EXPECT_THAT(evaluator_,
              JsonBToJsonB(R"({"a":[2],"a":[1]})", R"({"a": [1]})"));
  EXPECT_THAT(evaluator_, JsonBToJsonB(R"({"b":[1e0],"a":[2]})",
                                       R"({"a": [2], "b": [1]})"));
}

MATCHER_P2(NumericToJsonB, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan(
          {CreatePgNumericValueWithMemoryContext(input).value()})),
      zetasql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, NumericInput) {
  EXPECT_THAT(evaluator_, NumericToJsonB("0  ", "0"));
  EXPECT_THAT(evaluator_,
              NumericToJsonB(absl::StrCat(" -", kMaxPgJsonbNumericDigitStr),
                             absl::StrCat("-", kMaxPgJsonbNumericDigitStr)));
  EXPECT_THAT(evaluator_, NumericToJsonB(kMaxPgJsonbNumericDigitStr,
                                         kMaxPgJsonbNumericDigitStr));
  EXPECT_THAT(evaluator_,
              NumericToJsonB(" 0.0000000001230 ", "0.0000000001230"));
  EXPECT_THAT(evaluator_, NumericToJsonB("  NaN", "\"NaN\""));
}

MATCHER_P2(ArrayToJsonB, array_input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({array_input})),
      zetasql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonBTest, ArrayInput) {
  EXPECT_THAT(evaluator_,
              ArrayToJsonB(zetasql::values::Int64Array({1, 9007199254740993}),
                           "[1, 9007199254740993]"));
  EXPECT_THAT(
      evaluator_,
      ArrayToJsonB(zetasql::Value::MakeArray(
                       zetasql::types::StringArrayType(),
                       {zetasql::values::String("a"), kNullStringValue})
                       .value(),
                   "[\"a\", null]"));
  EXPECT_THAT(evaluator_,
              ArrayToJsonB(zetasql::values::BytesArray({" ", "ab"}),
                           "[\"\\\\x20\", \"\\\\x6162\"]"));
  EXPECT_THAT(evaluator_,
              ArrayToJsonB(zetasql::Value::MakeArray(
                               spangres::datatypes::GetPgNumericArrayType(),
                               {CreatePgNumericValueWithMemoryContext(
                                    absl::StrCat(kMaxPgJsonbNumericDigitStr))
                                    .value()})
                               .value(),
                           absl::StrCat("[", kMaxPgJsonbNumericDigitStr, "]")));
  EXPECT_THAT(evaluator_,
              ArrayToJsonB(zetasql::values::DoubleArray({}), "[]"));
}

class EvalJsonBSubscriptText : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGJsonBSubscriptTextFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

MATCHER_P3(JsonBArrayElementText, jsonb, element_index, expected_string_value,
           "") {
  EXPECT_THAT(arg(absl::MakeConstSpan(
                  {jsonb.value(), zetasql::values::Int64(element_index)})),
              zetasql_base::testing::IsOkAndHolds(expected_string_value));
  return true;
}

MATCHER_P3(JsonBObjectFieldText, jsonb, object_field, expected_string_value,
           "") {
  EXPECT_THAT(arg(absl::MakeConstSpan(
                  {jsonb.value(), zetasql::values::String(object_field)})),
              zetasql_base::testing::IsOkAndHolds(expected_string_value));
  return true;
}

TEST_F(EvalJsonBSubscriptText, ElementIndexInput) {
  EXPECT_THAT(evaluator_,
              JsonBArrayElementText(CreatePgJsonbValueWithMemoryContext(
                                        R"([null, "string val"])"),
                                    0, kNullStringValue));
  EXPECT_THAT(
      evaluator_,
      JsonBArrayElementText(
          CreatePgJsonbValueWithMemoryContext(R"([1.00, "string val"])"), 1,
          zetasql::values::String("string val")));
  EXPECT_THAT(evaluator_,
              JsonBArrayElementText(CreatePgJsonbValueWithMemoryContext(
                                        R"([null, "string val"])"),
                                    2, kNullStringValue));
  EXPECT_THAT(evaluator_,
              JsonBArrayElementText(CreatePgJsonbValueWithMemoryContext(
                                        R"([null, "string val"])"),
                                    -1, kNullStringValue));
  EXPECT_THAT(evaluator_,
              JsonBArrayElementText(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  0, kNullStringValue));

  // Following are 3 test cases when any NULL value occurs in the arguments.
  // There is no error in these cases and the results are just NULL.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[1,2]").value(),
                   zetasql::values::NullInt64()})),
              zetasql_base::testing::IsOkAndHolds(kNullStringValue));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
           zetasql::values::Int64(-1)})),
      zetasql_base::testing::IsOkAndHolds(kNullStringValue));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
           zetasql::values::NullInt64()})),
      zetasql_base::testing::IsOkAndHolds(kNullStringValue));
}

TEST_F(EvalJsonBSubscriptText, ObjectFieldInput) {
  EXPECT_THAT(evaluator_,
              JsonBObjectFieldText(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  "a", zetasql::values::String("string val")));
  EXPECT_THAT(
      evaluator_,
      JsonBObjectFieldText(
          CreatePgJsonbValueWithMemoryContext(R"({"a": {"b": "string_val"}})"),
          "a", zetasql::values::String(R"({"b": "string_val"})")));
  EXPECT_THAT(evaluator_,
              JsonBObjectFieldText(CreatePgJsonbValueWithMemoryContext(
                                       R"([1.00, "string val"])"),
                                   "a", kNullStringValue));
  EXPECT_THAT(evaluator_,
              JsonBObjectFieldText(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  "no match", kNullStringValue));
  EXPECT_THAT(
      evaluator_,
      JsonBObjectFieldText(CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"),
                           "a", zetasql::values::String("")));

  // Following is a test case when STRING argument is NULL. There is no error
  // and the result is just NULL.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
                   kNullStringValue})),
              zetasql_base::testing::IsOkAndHolds(kNullStringValue));
}

TEST_F(EvalJsonBSubscriptText, ErrorCases) {
  // More than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   zetasql::values::Int64(1), zetasql::values::Int64(2)})),
              StatusIs(absl::StatusCode::kInternal));

  // Less than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value()})),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   zetasql::values::NullBool()})),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class EvalJsonBArrayElement : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGJsonBArrayElementFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalJsonBArrayElement, ErrorCases) {
  // More than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   zetasql::values::Int64(1), zetasql::values::Int64(2)})),
              StatusIs(absl::StatusCode::kInternal));

  // Less than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value()})),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   zetasql::values::NullBool()})),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class EvalJsonBObjectField : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGJsonBObjectFieldFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalJsonBObjectField, ErrorCases) {
  // More than 2 arguments
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
           zetasql::values::String("a"), zetasql::values::String("b")})),
      StatusIs(absl::StatusCode::kInternal));

  // Less than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value()})),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
                   zetasql::values::NullBool()})),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class EvalJsonBTypeof : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGJsonBTypeofFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalJsonBTypeof, ErrorCases) {
  // More than 1 argument
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgJsonbValueWithMemoryContext("[1,2,3.56]"),
                   *CreatePgJsonbValueWithMemoryContext("3.14")})),
              StatusIs(absl::StatusCode::kInternal));

  // Less than 1 argument
  std::vector<zetasql::Value> args = {};

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({zetasql::Value::Double(3.14)})),
              StatusIs(absl::StatusCode::kInternal));
}

struct EvalCastFromJsonbTestCase {
  std::string test_name;
  zetasql::Value arg;
  zetasql::Value expected_value;
  absl::StatusCode expected_status_code;
};

using EvalCastFromJsonbTest =
    ::testing::TestWithParam<EvalCastFromJsonbTestCase>;

TEST_P(EvalCastFromJsonbTest, TestEvalCastFromJsonb) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  static const zetasql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  absl::flat_hash_map<zetasql::TypeKind, zetasql::FunctionSignature>
      signature_map = {
          {zetasql::TYPE_BOOL,
           {zetasql::types::BoolType(), {gsql_pg_jsonb}, nullptr}},
          {zetasql::TYPE_INT64,
           {zetasql::types::Int64Type(), {gsql_pg_jsonb}, nullptr}},
          {zetasql::TYPE_DOUBLE,
           {zetasql::types::DoubleType(), {gsql_pg_jsonb}, nullptr}},
          {zetasql::TYPE_EXTENDED,
           {gsql_pg_numeric, {gsql_pg_jsonb}, nullptr}},
          {zetasql::TYPE_STRING,
           {zetasql::types::StringType(), {gsql_pg_jsonb}, nullptr}},
          // To trigger an invalid cast.
          {zetasql::TYPE_TIMESTAMP,
           {zetasql::types::TimestampType(), {gsql_pg_jsonb}, nullptr}},
      };

  const EvalCastFromJsonbTestCase& test_case = GetParam();

  const zetasql::Function* function =
      functions[kPGCastFromJsonBFunctionName].get();
  auto callback = function->GetFunctionEvaluatorFactory();

  auto iter = signature_map.find(test_case.expected_value.type_kind());
  ASSERT_NE(iter, signature_map.end());
  if (test_case.expected_value.type_kind() == zetasql::TYPE_TIMESTAMP) {
    // This test is attempting to trigger an invalid cast.
    EXPECT_THAT(callback(iter->second),
                StatusIs(test_case.expected_status_code));
    return;
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto evaluator, callback(iter->second));

  if (test_case.expected_status_code == absl::StatusCode::kOk) {
    EXPECT_THAT(evaluator({test_case.arg}),
                IsOkAndHolds(EqPG(test_case.expected_value)));
  } else {
    EXPECT_THAT(evaluator({test_case.arg}),
                StatusIs(test_case.expected_status_code));
  }
}

INSTANTIATE_TEST_SUITE_P(
    EvalCastFromJsonbTests, EvalCastFromJsonbTest,
    ::testing::ValuesIn<EvalCastFromJsonbTestCase>({
        // PG.JSONB -> BOOL
        {"CastNullJsonbToNullBool",
         zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
         zetasql::Value::NullBool(), absl::StatusCode::kOk},
        {"CastTrueJsonbToBool", *CreatePgJsonbValueWithMemoryContext("true"),
         zetasql::values::True(), absl::StatusCode::kOk},
        {"CastFalseJsonbToBool", *CreatePgJsonbValueWithMemoryContext("false"),
         zetasql::values::False(), absl::StatusCode::kOk},
        {"CastInvalidValueToBoolFails",
         *CreatePgJsonbValueWithMemoryContext("1.0"),
         zetasql::Value::NullBool(),  // unused
         absl::StatusCode::kInvalidArgument},

        // PG.JSONB -> DOUBLE
        {"CastNullJsonbToNullDouble",
         zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
         zetasql::Value::NullDouble(), absl::StatusCode::kOk},
        {"CastNumberJsonbToDouble", *CreatePgJsonbValueWithMemoryContext("1.5"),
         zetasql::Value::Double(1.5), absl::StatusCode::kOk},
        {"CastInvalidValueToDoubleFails",
         *CreatePgJsonbValueWithMemoryContext("true"),
         zetasql::Value::NullDouble(),  // unused
         absl::StatusCode::kInvalidArgument},

        // PG.JSONB -> INT64
        {"CastNullJsonbToNullInt64",
         zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
         zetasql::Value::NullInt64(), absl::StatusCode::kOk},
        {"CastNumberJsonbToInt64", *CreatePgJsonbValueWithMemoryContext("500"),
         zetasql::Value::Int64(500), absl::StatusCode::kOk},
        {"CastNumberWithDecimalPointJsonbToInt64",
         *CreatePgJsonbValueWithMemoryContext("1.5"),
         zetasql::Value::Int64(2), absl::StatusCode::kOk},
        {"CastInvalidValueToInt64Fails",
         *CreatePgJsonbValueWithMemoryContext("true"),
         zetasql::Value::NullInt64(),  // unused
         absl::StatusCode::kInvalidArgument},

        // PG.JSONB -> STRING
        {"CastNullJsonbToNullString",
         zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
         zetasql::Value::NullString(), absl::StatusCode::kOk},
        {"CastNumberJsonbToString", *CreatePgJsonbValueWithMemoryContext("500"),
         zetasql::Value::String("500"), absl::StatusCode::kOk},
        {"CastStringJsonbToString",
         *CreatePgJsonbValueWithMemoryContext("\"hello\""),
         zetasql::Value::String("\"hello\""), absl::StatusCode::kOk},

        // PG.JSONB -> <INVALID TYPE>
        {"CastTimestampJsonbToTimestampIsInvalid",
         *CreatePgJsonbValueWithMemoryContext("\"01 Jan 1970 00:00:00+00\""),
         zetasql::values::Timestamp(absl::UnixEpoch()),
         absl::StatusCode::kInvalidArgument},
    }));

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

struct EvalAggregatorTestCase {
  std::string test_name;
  std::string function_name;
  std::vector<const zetasql::Value*> args;
  zetasql::Value expected_value;
  absl::StatusCode expected_status_code;
};

using EvalMinTest = ::testing::TestWithParam<EvalAggregatorTestCase>;

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
  const EvalAggregatorTestCase& test_case = GetParam();
  const zetasql::Function* min_function =
      functions[test_case.function_name].get();
  auto callback = min_function->GetAggregateFunctionEvaluatorFactory();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::AggregateFunctionEvaluator> evaluator,
      callback(signature));

  bool stop_acc = false;
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

INSTANTIATE_TEST_SUITE_P(EvalMinTests, EvalMinTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneNullArg",
                              kPGMinFunctionName,
                              {&kNullDoubleValue},
                              kNullDoubleValue,
                              absl::StatusCode::kOk},
                             {"EmptyArgs",
                              kPGMinFunctionName,
                              {},
                              kNullDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleArg",
                              kPGMinFunctionName,
                              {&kDoubleValue},
                              kDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleArgOneNullArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kNullDoubleValue},
                              kDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleArgOnePosInfArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kPosInfDoubleValue},
                              kDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kNegInfDoubleValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OnePosInfArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kPosInfDoubleValue, &kNegInfDoubleValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OnePosInfArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kPosInfDoubleValue, &kNegInfDoubleValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneNanArg",
                              kPGMinFunctionName,
                              {&kDoubleNaNValue},
                              kDoubleNaNValue,
                              absl::StatusCode::kOk},
                             {"OneNullArgOneNanArg",
                              kPGMinFunctionName,
                              {&kNullDoubleValue, &kDoubleNaNValue},
                              kDoubleNaNValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleArgOneNanArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kDoubleNaNValue},
                              kDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneNegInfArgOneNanArg",
                              kPGMinFunctionName,
                              {&kNegInfDoubleValue, &kDoubleNaNValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OnePosInfArgOneNanArg",
                              kPGMinFunctionName,
                              {&kPosInfDoubleValue, &kDoubleNaNValue},
                              kPosInfDoubleValue,
                              absl::StatusCode::kOk},
                         }));

INSTANTIATE_TEST_SUITE_P(EvalMinFailureTests, EvalMinTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneInvalidArg",
                              kPGMinFunctionName,
                              {&kInt64Value},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneValidArgOneInvalidArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kInt64Value},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                         }));

using EvalNumericMinMaxTest = ::testing::TestWithParam<EvalAggregatorTestCase>;

TEST_P(EvalNumericMinMaxTest, TestNumericMinMax) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  zetasql::FunctionSignature signature(gsql_pg_numeric, {gsql_pg_numeric},
                                         nullptr);

  bool stop_acc = false;
  const EvalAggregatorTestCase& test_case = GetParam();

  const zetasql::Function* function =
      functions[test_case.function_name].get();
  auto callback = function->GetAggregateFunctionEvaluatorFactory();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::AggregateFunctionEvaluator> evaluator,
      callback(signature));

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

    EXPECT_THAT(evaluator->GetFinalResult(),
                IsOkAndHolds(EqPG(test_case.expected_value)));
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

INSTANTIATE_TEST_SUITE_P(
    EvalNumericMinMaxTests, EvalNumericMinMaxTest,
    ::testing::ValuesIn<EvalAggregatorTestCase>({
        // pg.numeric_max test cases.
        {"OneNullArg",
         kPGNumericMaxFunctionName,
         {&kNullPGNumericValue},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"EmptyArgs",
         kPGNumericMaxFunctionName,
         {},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OneNumericArg",
         kPGNumericMaxFunctionName,
         {&kNullPGNumericValue},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OneNumericArgOneNullArg",
         kPGNumericMaxFunctionName,
         {&kPGNumericValue, &kNullPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"MaxNumericArg",
         kPGNumericMaxFunctionName,
         {&kPGNumericValue, &kPGNumericMaxValue, &kPGNumericMinValue},
         kPGNumericMaxValue,
         absl::StatusCode::kOk},
        {"OneNanArg",
         kPGNumericMaxFunctionName,
         {&kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"OneNullArgOneNanArg",
         kPGNumericMaxFunctionName,
         {&kNullPGNumericValue, &kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"OneNumericArgOneNanArg",
         kPGNumericMaxFunctionName,
         {&kPGNumericValue, &kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},

        // pg.numeric_min test cases.
        {"OneNullArg",
         kPGNumericMinFunctionName,
         {&kNullPGNumericValue},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"EmptyArgs",
         kPGNumericMinFunctionName,
         {},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OneNumericArg",
         kPGNumericMinFunctionName,
         {&kNullPGNumericValue},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OneNumericArgOneNullArg",
         kPGNumericMinFunctionName,
         {&kPGNumericValue, &kNullPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"MinNumericArg",
         kPGNumericMinFunctionName,
         {&kPGNumericValue, &kPGNumericMaxValue, &kPGNumericMinValue},
         kPGNumericMinValue,
         absl::StatusCode::kOk},
        {"OneNanArg",
         kPGNumericMinFunctionName,
         {&kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"OneNullArgOneNanArg",
         kPGNumericMinFunctionName,
         {&kNullPGNumericValue, &kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"OneNumericArgOneNanArg",
         kPGNumericMinFunctionName,
         {&kPGNumericValue, &kPGNumericNaNValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
    }));

using EvalSumAvgTest = ::testing::TestWithParam<EvalAggregatorTestCase>;

TEST_P(EvalSumAvgTest, TestSumAvgAggregator) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<zetasql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  static const zetasql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  absl::flat_hash_map<zetasql::TypeKind, zetasql::FunctionSignature>
      signature_map = {
          {zetasql::TYPE_INT64,
           {gsql_pg_numeric, {zetasql::types::Int64Type()}, nullptr}},
          {zetasql::TYPE_DOUBLE,
           {zetasql::types::DoubleType(),
            {zetasql::types::DoubleType()},
            nullptr}},
          {zetasql::TYPE_EXTENDED,
           {gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      };

  bool stop_acc = false;
  const EvalAggregatorTestCase& test_case = GetParam();

  const zetasql::Function* function =
      functions[test_case.function_name].get();
  auto callback = function->GetAggregateFunctionEvaluatorFactory();

  // In these test cases, we don't know what the input type is if we don't have
  // any test args so we assume it's an INT64 input that returns a PG.NUMERIC
  // output.
  zetasql::TypeKind type_kind = test_case.args.empty()
      ? zetasql::TYPE_INT64 : test_case.args[0]->type_kind();
  auto iter = signature_map.find(type_kind);
  ASSERT_NE(iter, signature_map.end());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::AggregateFunctionEvaluator> evaluator,
      callback(iter->second));

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
    EXPECT_THAT(evaluator->GetFinalResult(),
                IsOkAndHolds(EqPG(test_case.expected_value)));
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

INSTANTIATE_TEST_SUITE_P(
    EvalSumAvgTests, EvalSumAvgTest,
    ::testing::ValuesIn<EvalAggregatorTestCase>({
        // Tests for pg.sum with no args
        {"NoArgs",
         kPGSumFunctionName,
         {},
         kNullPGNumericValue,
         absl::StatusCode::kOk},

        // Tests for pg.sum of INT64
        {"OneNullInt64Arg",
         kPGSumFunctionName,
         {&kNullInt64Value},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OneInt64Arg",
         kPGSumFunctionName,
         {&kInt64Value},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"ManyInt64Args",
         kPGSumFunctionName,
         {&kInt64Value, &kInt64Value, &kInt64Value},
         *CreatePgNumericValueWithMemoryContext("3.0"),
         absl::StatusCode::kOk},
        {"NullInt64ArgFirst",
         kPGSumFunctionName,
         {&kNullInt64Value, &kInt64Value, &kInt64Value},
         *CreatePgNumericValueWithMemoryContext("2.0"),
         absl::StatusCode::kOk},
        {"NullInt64ArgsBeforeInt64Values",
         kPGSumFunctionName,
         {&kNullInt64Value, &kNullInt64Value, &kInt64Value, &kInt64Value},
         *CreatePgNumericValueWithMemoryContext("2.0"),
         absl::StatusCode::kOk},
        {"NullInt64ArgsElsewhere",
         kPGSumFunctionName,
         {&kInt64Value, &kNullInt64Value, &kInt64Value, &kNullInt64Value},
         *CreatePgNumericValueWithMemoryContext("2.0"),
         absl::StatusCode::kOk},
        {"SumMinAndMaxInt64",
         kPGSumFunctionName,
         {&kInt64MinValue, &kInt64MaxValue},
         *CreatePgNumericValueWithMemoryContext("-1.0"),
         absl::StatusCode::kOk},
        {"SumInt64MaxWithInt64Max",
         kPGSumFunctionName,
         {&kInt64MaxValue, &kInt64MaxValue},
         *CreatePgNumericValueWithMemoryContext("18446744073709551614"),
         absl::StatusCode::kOk},
        {"SumInt64MinWithInt64Min",
         kPGSumFunctionName,
         {&kInt64MinValue, &kInt64MinValue},
         *CreatePgNumericValueWithMemoryContext("-18446744073709551616"),
         absl::StatusCode::kOk},

        // Tests for pg.sum of DOUBLE
        {"OneNullDoubleArg",
         kPGSumFunctionName,
         {&kNullDoubleValue},
         kNullDoubleValue,
         absl::StatusCode::kOk},
        {"OneDoubleArg",
         kPGSumFunctionName,
         {&kDoubleValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"ManyDoubleArgs",
         kPGSumFunctionName,
         {&kDoubleValue, &kDoubleValue, &kDoubleValue},
         zetasql::values::Double(3.0),
         absl::StatusCode::kOk},
        {"NullDoubleArgFirst",
         kPGSumFunctionName,
         {&kNullDoubleValue, &kDoubleValue, &kDoubleValue},
         zetasql::values::Double(2.0),
         absl::StatusCode::kOk},
        {"NullDoubleArgsBeforeDoubleValues",
         kPGSumFunctionName,
         {&kNullDoubleValue, &kNullDoubleValue, &kDoubleValue, &kDoubleValue},
         zetasql::values::Double(2.0),
         absl::StatusCode::kOk},
        {"NullDoubleArgsElsewhere",
         kPGSumFunctionName,
         {&kDoubleValue, &kNullDoubleValue, &kDoubleValue, &kNullDoubleValue},
         zetasql::values::Double(2.0),
         absl::StatusCode::kOk},
        {"OneNanDoubleArg",
         kPGSumFunctionName,
         {&kDoubleNaNValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"ManyNanDoubleArgs",
         kPGSumFunctionName,
         {&kDoubleValue, &kDoubleNaNValue, &kDoubleValue, &kDoubleNaNValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"OneInfinityDoubleArg",
         kPGSumFunctionName,
         {&kPosInfDoubleValue},
         kPosInfDoubleValue,
         absl::StatusCode::kOk},
        {"ManyInfinityDoubleArgs",
         kPGSumFunctionName,
         {&kPosInfDoubleValue, &kPosInfDoubleValue, &kPosInfDoubleValue},
         kPosInfDoubleValue,
         absl::StatusCode::kOk},
        {"PosAndNegInfinityMakesNaN",
         kPGSumFunctionName,
         {&kPosInfDoubleValue, &kNegInfDoubleValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},

        // Tests for pg.sum of PG.NUMERIC
        {"OneNullPGNumericArg",
         kPGSumFunctionName,
         {&kNullPGNumericValue},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OnePGNumericArg",
         kPGSumFunctionName,
         {&kPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"ManyPGNumericArgs",
         kPGSumFunctionName,
         {&kPGNumericValue, &kPGNumericValue, &kPGNumericValue},
         *CreatePgNumericValueWithMemoryContext("3.0"),
         absl::StatusCode::kOk},
        {"NullPGNumericArgFirst",
         kPGSumFunctionName,
         {&kNullPGNumericValue, &kPGNumericValue, &kPGNumericValue},
         *CreatePgNumericValueWithMemoryContext("2.0"),
         absl::StatusCode::kOk},
        {"NullPGNumericArgsBeforePGNumericValues",
         kPGSumFunctionName,
         {&kNullPGNumericValue, &kNullPGNumericValue, &kPGNumericValue,
          &kPGNumericValue},
         *CreatePgNumericValueWithMemoryContext("2.0"),
         absl::StatusCode::kOk},
        {"NullPGNumericArgsElsewhere",
         kPGSumFunctionName,
         {&kPGNumericValue, &kNullPGNumericValue, &kPGNumericValue,
          &kNullPGNumericValue},
         *CreatePgNumericValueWithMemoryContext("2.0"),
         absl::StatusCode::kOk},
        {"OneNanPGNumericArg",
         kPGSumFunctionName,
         {&kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"ManyNanPGNumericArgs",
         kPGSumFunctionName,
         {&kPGNumericValue, &kPGNumericNaNValue, &kPGNumericValue,
          &kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},

        // Tests for pg.avg with no args
        {"NoArgs",
         kPGAvgFunctionName,
         {},
         kNullPGNumericValue,
         absl::StatusCode::kOk},

        // Tests for pg.avg of INT64
        {"OneNullInt64Arg",
         kPGAvgFunctionName,
         {&kNullInt64Value},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OneInt64Arg",
         kPGAvgFunctionName,
         {&kInt64Value},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"MultipleInt64Args",
         kPGAvgFunctionName,
         {&kInt64Value, &kInt64Value},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"NullInt64ArgFirst",
         kPGAvgFunctionName,
         {&kNullInt64Value, &kInt64Value, &kInt64Value},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"NullInt64ArgsBeforeInt64Values",
         kPGAvgFunctionName,
         {&kNullInt64Value, &kNullInt64Value, &kInt64Value, &kInt64Value},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"NullInt64ArgsElsewhere",
         kPGAvgFunctionName,
         {&kInt64Value, &kNullInt64Value, &kInt64Value, &kNullInt64Value},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"AvgMinAndMaxInt64",
         kPGAvgFunctionName,
         {&kInt64MinValue, &kInt64MaxValue},
         *CreatePgNumericValueWithMemoryContext("-0.5"),
         absl::StatusCode::kOk},
        {"AvgInt64MaxWithInt64Max",
         kPGAvgFunctionName,
         {&kInt64MaxValue, &kInt64MaxValue},
         *CreatePgNumericValueWithMemoryContext(
             absl::StrCat(kInt64MaxValue.int64_value())),
         absl::StatusCode::kOk},
        {"AvgInt64MinWithInt64Min",
         kPGAvgFunctionName,
         {&kInt64MinValue, &kInt64MinValue},
         *CreatePgNumericValueWithMemoryContext(
             absl::StrCat(kInt64MinValue.int64_value())),
         absl::StatusCode::kOk},

        // Tests for pg.avg of DOUBLE
        {"OneNullDoubleArg",
         kPGAvgFunctionName,
         {&kNullDoubleValue},
         kNullDoubleValue,
         absl::StatusCode::kOk},
        {"OneDoubleArg",
         kPGAvgFunctionName,
         {&kDoubleValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"ManyDoubleArgs",
         kPGAvgFunctionName,
         {&kDoubleValue, &kDoubleValue, &kDoubleValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"NullDoubleArgFirst",
         kPGAvgFunctionName,
         {&kNullDoubleValue, &kDoubleValue, &kDoubleValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"NullDoubleArgsBeforeDoubleValues",
         kPGAvgFunctionName,
         {&kNullDoubleValue, &kNullDoubleValue, &kDoubleValue, &kDoubleValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"NullDoubleArgsElsewhere",
         kPGAvgFunctionName,
         {&kDoubleValue, &kNullDoubleValue, &kDoubleValue, &kNullDoubleValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"OneNanDoubleArg",
         kPGAvgFunctionName,
         {&kDoubleNaNValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"ManyNanDoubleArgs",
         kPGAvgFunctionName,
         {&kDoubleValue, &kDoubleNaNValue, &kDoubleValue, &kDoubleNaNValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"OneInfinityDoubleArg",
         kPGAvgFunctionName,
         {&kPosInfDoubleValue},
         kPosInfDoubleValue,
         absl::StatusCode::kOk},
        {"ManyInfinityDoubleArgs",
         kPGAvgFunctionName,
         {&kPosInfDoubleValue, &kPosInfDoubleValue, &kPosInfDoubleValue},
         kPosInfDoubleValue,
         absl::StatusCode::kOk},
        {"PosAndNegInfinityMakesNaN",
         kPGAvgFunctionName,
         {&kPosInfDoubleValue, &kNegInfDoubleValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"AvgMinAndMaxDouble",
         kPGAvgFunctionName,
         {&kDoubleMinValue, &kDoubleMaxValue},
         zetasql::values::Double((std::numeric_limits<double>::min() +
                                    std::numeric_limits<double>::max()) /
                                   2.0),
         absl::StatusCode::kOk},

        // Tests for pg.avg of PG.NUMERIC
        {"OneNullPGNumericArg",
         kPGAvgFunctionName,
         {&kNullPGNumericValue},
         kNullPGNumericValue,
         absl::StatusCode::kOk},
        {"OnePGNumericArg",
         kPGAvgFunctionName,
         {&kPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"ManyPGNumericArgs",
         kPGAvgFunctionName,
         {&kPGNumericValue, &kPGNumericValue, &kPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"NullPGNumericArgFirst",
         kPGAvgFunctionName,
         {&kNullPGNumericValue, &kPGNumericValue, &kPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"NullPGNumericArgsBeforePGNumericValues",
         kPGAvgFunctionName,
         {&kNullPGNumericValue, &kNullPGNumericValue, &kPGNumericValue,
          &kPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"NullPGNumericArgsElsewhere",
         kPGAvgFunctionName,
         {&kPGNumericValue, &kNullPGNumericValue, &kPGNumericValue,
          &kNullPGNumericValue},
         kPGNumericValue,
         absl::StatusCode::kOk},
        {"OneNanPGNumericArg",
         kPGAvgFunctionName,
         {&kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"ManyNanPGNumericArgs",
         kPGAvgFunctionName,
         {&kPGNumericValue, &kPGNumericNaNValue, &kPGNumericValue,
          &kPGNumericNaNValue},
         kPGNumericNaNValue,
         absl::StatusCode::kOk},
        {"AvgMinAndMaxPGNumeric",
         kPGAvgFunctionName,
         {&kPGNumericMinValue, &kPGNumericMaxValue},
         *CreatePgNumericValueWithMemoryContext("0.0"),
         absl::StatusCode::kOk},
    }));

INSTANTIATE_TEST_SUITE_P(EvalSumAvgFailureTests, EvalSumAvgTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"SumInt64WithInconsistentTypes",
                              kPGSumFunctionName,
                              {&kInt64Value, &kDoubleValue},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"SumDoubleWithInconsistentTypes",
                              kPGSumFunctionName,
                              {&kDoubleValue, &kInt64Value},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"SumDoubleWithOverflow",
                              kPGSumFunctionName,
                              {&kDoubleMaxValue, &kDoubleMaxValue},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kOutOfRange},
                             {"SumPGNumericWithInconsistentTypes",
                              kPGSumFunctionName,
                              {&kPGNumericValue, &kDoubleValue},
                              kNullPGNumericValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"SumPGNumericWithOverflow",
                              kPGSumFunctionName,
                              {&kPGNumericMaxValue, &kPGNumericValue},
                              kNullPGNumericValue,  // ignored
                              absl::StatusCode::kOutOfRange},

                             {"AvgInt64WithInconsistentTypes",
                              kPGAvgFunctionName,
                              {&kInt64Value, &kDoubleValue},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"AvgDoubleWithInconsistentTypes",
                              kPGAvgFunctionName,
                              {&kDoubleValue, &kInt64Value},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"AvgDoubleWithOverflow",
                              kPGAvgFunctionName,
                              {&kDoubleMaxValue, &kDoubleMaxValue},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kOutOfRange},
                             {"AvgPGNumericWithInconsistentTypes",
                              kPGAvgFunctionName,
                              {&kPGNumericValue, &kDoubleValue},
                              kNullPGNumericValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"AvgPGNumericWithOverflow",
                              kPGAvgFunctionName,
                              {&kPGNumericMaxValue, &kPGNumericValue},
                              kNullPGNumericValue,  // ignored
                              absl::StatusCode::kOutOfRange},
                         }));

}  // namespace
}  // namespace postgres_translator

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
