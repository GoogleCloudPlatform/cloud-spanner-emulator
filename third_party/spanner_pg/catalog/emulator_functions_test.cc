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
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "googlesql/public/function.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/functions/date_time_util.h"
#include "googlesql/public/interval_value.h"
#include "googlesql/public/types/timestamp_util.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/uuid_value.h"
#include "googlesql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "absl/algorithm/container.h"
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
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"

namespace postgres_translator {
namespace {
using spangres::datatypes::CreatePgJsonbValueWithMemoryContext;
using spangres::datatypes::CreatePgNumericValueWithMemoryContext;

googlesql::Value UnwrapValue(absl::StatusOr<googlesql::Value> status_or) {
  ABSL_CHECK(status_or.ok()) << status_or.status();
  return status_or.value();
}
using spangres::datatypes::CreatePgOidValue;
using spangres::datatypes::common::kMaxPGNumericFractionalDigits;
using spangres::datatypes::common::kMaxPGNumericWholeDigits;
using spangres::datatypes::common::MaxNumericString;
using spangres::datatypes::common::MinNumericString;
using testing::HasSubstr;
using googlesql_base::testing::IsOkAndHolds;
using googlesql_base::testing::StatusIs;

static googlesql::Value CreatePgJsonbNullValue() {
  static const googlesql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  return googlesql::values::Null(gsql_pg_jsonb);
}

static googlesql::Value CreatePgNumericNullValue() {
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();
  return googlesql::values::Null(gsql_pg_numeric);
}

static googlesql::Value CreateIntervalValue(absl::string_view interval_string) {
  return googlesql::values::Interval(
      *googlesql::IntervalValue::ParseFromString(interval_string,
                                                 /*allow_nanos=*/true));
}

static googlesql::Value CreateUuidValue(absl::string_view uuid_string) {
  absl::StatusOr<googlesql::UuidValue> uuid_value =
      googlesql::UuidValue::FromString(uuid_string);
  ABSL_CHECK_OK(uuid_value) << "Failed to parse UUID string: " << uuid_string;
  return googlesql::values::Uuid(*uuid_value);
}

class EmulatorFunctionsTest : public ::testing::Test {
 protected:
  EmulatorFunctionsTest() {
    SpannerPGFunctions spanner_pg_functions =
        GetSpannerPGFunctions("TestCatalog");

    for (auto& function : spanner_pg_functions) {
      // Add entry for function alias.
      if (!function->alias_name().empty()) {
        auto alias_function = std::make_unique<googlesql::Function>(
            function->Name(), function->GetGroup(), function->mode(),
            function->signatures(), function->function_options());
        functions_[function->alias_name()] = std::move(alias_function);
      }
      functions_[function->Name()] = std::move(function);
    }
  }

  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions_;
  googlesql::FunctionEvaluator evaluator_;
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
  std::vector<googlesql::Value> function_arguments;
  googlesql::Value expected_result;
  absl::StatusCode expected_status_code = absl::StatusCode::kOk;
  std::string expected_error_message = "";
};

using PGScalarFunctionsTest =
    ::testing::TestWithParam<PGScalarFunctionTestCase>;

TEST_P(PGScalarFunctionsTest, ExecutesFunctionsSuccessfully) {
  const PGScalarFunctionTestCase& param = GetParam();
  std::vector<std::string> arg_strings;
  arg_strings.reserve(param.function_arguments.size());
  for (const googlesql::Value& value : param.function_arguments) {
    arg_strings.push_back(value.DebugString());
  }

  SCOPED_TRACE(absl::StrCat("Function: ", param.function_name,
                            "\n Args: ", absl::StrJoin(arg_strings, ", ")));
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");

  for (auto& function : spanner_pg_functions) {
    // Add entry for function alias.
    if (!function->alias_name().empty()) {
      auto alias_function = std::make_unique<googlesql::Function>(
          function->Name(), function->GetGroup(), function->mode(),
          function->signatures(), function->function_options());
      functions[function->alias_name()] = std::move(alias_function);
    }
    functions[function->Name()] = std::move(function);
  }

  const googlesql::Function* function = functions[param.function_name].get();
  ASSERT_NE(function, nullptr) << "Function not found: " << param.function_name;
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::FunctionEvaluator evaluator,
      (function->GetFunctionEvaluatorFactory())(
          // This test case ExecutesFunctionsSuccessfully assumes that a
          // function can be found, so we do not check if c_find_if cannot find
          // any function.
          *absl::c_find_if(
              function->signatures(),
              [&param](const googlesql::FunctionSignature& signature) {
                return signature.result_type().type() ==
                       param.expected_result.type();
              })));
  if (param.expected_status_code == absl::StatusCode::kOk) {
    EXPECT_THAT(evaluator(absl::MakeConstSpan(param.function_arguments)),
                IsOkAndHolds(EqPG(param.expected_result)));
  } else {
    EXPECT_THAT(evaluator(absl::MakeConstSpan(param.function_arguments)),
                StatusIs(param.expected_status_code,
                         HasSubstr(param.expected_error_message)));
  }
}

const googlesql::Value kNullDoubleValue = googlesql::values::NullDouble();
const googlesql::Value kDoubleValue = googlesql::values::Double(1.0);
const googlesql::Value kPosInfDoubleValue =
    googlesql::values::Double(std::numeric_limits<double>::infinity());
const googlesql::Value kNegInfDoubleValue =
    googlesql::values::Double(-1 * std::numeric_limits<double>::infinity());
const googlesql::Value kDoubleNaNValue =
    googlesql::values::Double(std::numeric_limits<double>::quiet_NaN());
const googlesql::Value kDoubleMaxValue =
    googlesql::values::Double(std::numeric_limits<double>::max());
const googlesql::Value kDoubleMinValue =
    googlesql::values::Double(std::numeric_limits<double>::min());
const googlesql::Value kDoubleLowestValue =
    googlesql::values::Double(std::numeric_limits<double>::lowest());

const googlesql::Value kNullFloatValue = googlesql::values::NullFloat();
const googlesql::Value kFloatValue = googlesql::values::Float(1.0);
const googlesql::Value kPosInfFloatValue =
    googlesql::values::Float(std::numeric_limits<float>::infinity());
const googlesql::Value kNegInfFloatValue =
    googlesql::values::Float(-1 * std::numeric_limits<float>::infinity());
const googlesql::Value kFloatNaNValue =
    googlesql::values::Float(std::numeric_limits<float>::quiet_NaN());
const googlesql::Value kFloatMaxValue =
    googlesql::values::Float(std::numeric_limits<float>::max());
const googlesql::Value kFloatMinValue =
    googlesql::values::Float(std::numeric_limits<float>::min());
const googlesql::Value kFloatLowestValue =
    googlesql::values::Float(std::numeric_limits<float>::lowest());

const googlesql::Value kNullInt64Value = googlesql::values::NullInt64();
const googlesql::Value kInt64Value = googlesql::values::Int64(1);
const googlesql::Value kInt64MaxValue =
    googlesql::values::Int64(std::numeric_limits<int64_t>::max());
const googlesql::Value kInt64MinValue =
    googlesql::values::Int64(std::numeric_limits<int64_t>::min());

const googlesql::Value kNullPGNumericValue =
    googlesql::values::Null(spangres::datatypes::GetPgNumericType());
const googlesql::Value kPGNumericValue =
    *CreatePgNumericValueWithMemoryContext("1.0");
const googlesql::Value kPGNumericNaNValue =
    *CreatePgNumericValueWithMemoryContext("NaN");
const googlesql::Value kPGNumericMaxValue =
    *CreatePgNumericValueWithMemoryContext(MaxNumericString());
const googlesql::Value kPGNumericMinValue =
    *CreatePgNumericValueWithMemoryContext(MinNumericString());
const googlesql::Value kPGNumericMaxDoubleValueRetainingFirst15Digits =
    *CreatePgNumericValueWithMemoryContext(
        absl::StrCat("179769313486232", std::string(294, '0')));
const googlesql::Value kPGNumericLowestDoubleValueRetainingFirst15Digits =
    *CreatePgNumericValueWithMemoryContext(
        absl::StrCat("-179769313486232", std::string(294, '0')));
const googlesql::Value kPGNumericMinDoubleValueRetainingLast15Digits =
    *CreatePgNumericValueWithMemoryContext(
        absl::StrCat("0.", std::string(307, '0'), "22250738585072"));

const googlesql::Value kNullPGOidValue =
    googlesql::values::Null(spangres::datatypes::GetPgOidType());
const googlesql::Value kPGOidValue = *CreatePgOidValue(1);
const googlesql::Value kPGOidMaxValue =
    *CreatePgOidValue(std::numeric_limits<uint32_t>::max());
const googlesql::Value kPGOidMinValue =
    *CreatePgOidValue(std::numeric_limits<uint32_t>::min());

const googlesql::Value kNullIntervalValue = googlesql::Value::NullInterval();
const googlesql::Value kIntervalValue =
    googlesql::values::Interval(*googlesql::IntervalValue::FromDays(1));
const googlesql::Value kIntervalMaxValue =
    googlesql::values::Interval(googlesql::IntervalValue::MaxValue());
const googlesql::Value kIntervalMinValue =
    googlesql::values::Interval(googlesql::IntervalValue::MinValue());

const googlesql::Value kNullUuidValue = googlesql::Value::NullUuid();
const googlesql::Value kUuidValue = googlesql::values::Uuid(
    *googlesql::UuidValue::FromString("11111111-1111-1111-1111-111111111111"));
const googlesql::Value kUuidMaxValue =
    googlesql::values::Uuid(googlesql::UuidValue::MaxValue());
const googlesql::Value kUuidMinValue =
    googlesql::values::Uuid(googlesql::UuidValue::MinValue());
const googlesql::Value kNullUuidArrayValue =
    googlesql::values::Null(googlesql::types::UuidArrayType());
const googlesql::Value kUuidArrayValue = googlesql::values::Array(
    googlesql::types::UuidArrayType(), {kUuidValue, kUuidMaxValue});

const googlesql::Value kMaxTimestampValue =
    googlesql::values::Timestamp(googlesql::types::TimestampMaxBaseTime());
const googlesql::Value kMinTimestampValue =
    googlesql::values::Timestamp(googlesql::types::TimestampMinBaseTime());

const googlesql::Value kNullStringValue = googlesql::values::NullString();
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
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             googlesql::values::String("3 months 8 days 20 seconds")},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2005, 4, 10, 3, 4, 25), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzAddFunctionName,
            {googlesql::values::NullTimestamp(),
             googlesql::values::String("3 months 8 days 20 seconds")},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzAddFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             googlesql::values::NullString()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzAddFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             CreateIntervalValue("0-3 8 0:0:20")},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2005, 4, 10, 3, 4, 25), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzAddFunctionName,
            {googlesql::values::NullTimestamp(),
             CreateIntervalValue("0-3 8 0:0:20")},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzAddFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             googlesql::values::NullInterval()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             googlesql::values::String("2 years 1 hour")},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2003, 1, 2, 2, 4, 5), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {googlesql::values::NullTimestamp(),
             googlesql::values::String("2 years 1 hour")},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             googlesql::values::NullString()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             CreateIntervalValue("2-0 0 1:0:0")},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2003, 1, 2, 2, 4, 5), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {googlesql::values::NullTimestamp(),
             CreateIntervalValue("2-0 0 1:0:0")},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractFunctionName,
            {googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2005, 1, 2, 3, 4, 5), timezone)),
             googlesql::values::NullInterval()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzBinFunctionName,
            {googlesql::values::String("10 seconds"),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2001, 1, 1, 0, 0, 0), timezone))},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2020, 2, 11, 15, 44, 10), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzBinFunctionName,
            {googlesql::values::NullString(),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2001, 1, 1, 0, 0, 0), timezone))},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzBinFunctionName,
            {googlesql::values::String("10 seconds"),
             googlesql::values::NullTimestamp(),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2001, 1, 1, 0, 0, 0), timezone))},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzBinFunctionName,
            {googlesql::values::String("10 seconds"),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             googlesql::values::NullTimestamp()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::String("day"),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone))},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2020, 2, 11, 0, 0, 0), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::NullString(),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone))},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::String("day"),
             googlesql::values::NullTimestamp()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::String("day"),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             googlesql::values::String("Australia/Sydney")},
            googlesql::values::Timestamp(absl::FromCivil(
                absl::CivilSecond(2020, 2, 11, 5, 0, 0), timezone)),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::NullString(),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             googlesql::values::String("Australia/Sydney")},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::String("day"),
             googlesql::values::NullTimestamp(),
             googlesql::values::String("Australia/Sydney")},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGTimestamptzTruncFunctionName,
            {googlesql::values::String("day"),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone)),
             googlesql::values::NullString()},
            googlesql::values::NullTimestamp(),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {googlesql::values::String("second"),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone))},
            *CreatePgNumericValueWithMemoryContext("17"),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {googlesql::values::NullString(),
             googlesql::values::Timestamp(absl::FromCivil(
                 absl::CivilSecond(2020, 2, 11, 15, 44, 17), timezone))},
            CreatePgNumericNullValue(),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {googlesql::values::String("second"),
             googlesql::values::NullTimestamp()},
            CreatePgNumericNullValue(),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {googlesql::values::String("month"), googlesql::values::NullDate()},
            CreatePgNumericNullValue(),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {googlesql::values::NullString(), googlesql::values::Date(45)},
            CreatePgNumericNullValue(),
        },
        PGScalarFunctionTestCase{
            kPGExtractFunctionName,
            {googlesql::values::String("month"), googlesql::values::Date(45)},
            *CreatePgNumericValueWithMemoryContext("2"),
        },
        // pg.jsonb_array_element
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             googlesql::Value::Int64(0)},
            *CreatePgJsonbValueWithMemoryContext("null")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([1.00, "string val"])"),
             googlesql::Value::Int64(1)},
            *CreatePgJsonbValueWithMemoryContext(R"("string val")")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             googlesql::Value::Int64(2)},
            CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             googlesql::Value::Int64(-1)},
            *CreatePgJsonbValueWithMemoryContext(R"("string val")")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {CreatePgJsonbNullValue(), googlesql::Value::Int64(0)},
            CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"),
             googlesql::Value::NullInt64()},
            CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {CreatePgJsonbNullValue(), googlesql::Value::NullInt64()},
            CreatePgJsonbNullValue()},

        // pg.jsonb_object_field
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
             googlesql::Value::String("a")},
            *CreatePgJsonbValueWithMemoryContext(R"("string val")")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                 R"({"a": {"b": "string_val"}})"),
             googlesql::Value::String("a")},
            *CreatePgJsonbValueWithMemoryContext(R"({"b": "string_val"})")},
        PGScalarFunctionTestCase{kGoogleSQLSubscriptFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                      R"({"a": {"b": "string_val"}})"),
                                  googlesql::Value::String("c")},
                                 CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{kGoogleSQLSubscriptFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                      R"({"a": {"b": "string_val"}})"),
                                  googlesql::Value::String("no match")},
                                 CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"),
             googlesql::Value::String("a")},
            *CreatePgJsonbValueWithMemoryContext(R"("")")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"),
             kNullStringValue},
            CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{kGoogleSQLSubscriptFunctionName,
                                 {CreatePgJsonbNullValue(), kNullStringValue},
                                 CreatePgJsonbNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubscriptFunctionName,
            {CreatePgJsonbNullValue(), googlesql::Value::String("a")},
            CreatePgJsonbNullValue()},

        // pg.jsonb_typeof
        PGScalarFunctionTestCase{kGoogleSQLJsonTypeFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext("null")},
                                 googlesql::Value::String("null")},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonTypeFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1,2,3.56]")},
            googlesql::Value::String("array")},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonTypeFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(R"("hello")")},
            googlesql::Value::String("string")},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonTypeFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                R"({ "a" : { "b" : [null, 3.5, -214215, true] } })")},
            googlesql::Value::String("object")},
        PGScalarFunctionTestCase{kGoogleSQLJsonTypeFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                     "-18446744073709551615124125")},
                                 googlesql::Value::String("number")},
        PGScalarFunctionTestCase{kGoogleSQLJsonTypeFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext(
                                     "18446744073709551615124125")},
                                 googlesql::Value::String("number")},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonTypeFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                spangres::datatypes::common::MaxJsonbNumericString())},
            googlesql::Value::String("number")},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonTypeFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                spangres::datatypes::common::MinJsonbNumericString())},
            googlesql::Value::String("number")},
        PGScalarFunctionTestCase{kGoogleSQLJsonTypeFunctionName,
                                 {*CreatePgJsonbValueWithMemoryContext("true")},
                                 googlesql::Value::String("boolean")},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonTypeFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("false")},
            googlesql::Value::String("boolean")},
        // pg.jsonb_query_array
        PGScalarFunctionTestCase{
            kGoogleSQLJsonQueryArrayFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1, 2, 3]")},
            *googlesql::Value::MakeArray(
                spangres::datatypes::GetPgJsonbArrayType(),
                {*CreatePgJsonbValueWithMemoryContext("1"),
                 *CreatePgJsonbValueWithMemoryContext("2"),
                 *CreatePgJsonbValueWithMemoryContext("3")})},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonQueryArrayFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"abc\", \"def\"]")},
            *googlesql::Value::MakeArray(
                spangres::datatypes::GetPgJsonbArrayType(),
                {*CreatePgJsonbValueWithMemoryContext("\"abc\""),
                 *CreatePgJsonbValueWithMemoryContext("\"def\"")})},
        PGScalarFunctionTestCase{
            kGoogleSQLJsonQueryArrayFunctionName,
            {CreatePgJsonbNullValue()},
            googlesql::values::Null(
                spangres::datatypes::GetPgJsonbArrayType())},
        // pg.jsonb_build_array
        PGScalarFunctionTestCase{kPGJsonbBuildArrayFunctionName,
                                 {},
                                 *CreatePgJsonbValueWithMemoryContext("[]")},
        PGScalarFunctionTestCase{kPGJsonbBuildArrayFunctionName,
                                 {googlesql::Value::Int64(1)},
                                 *CreatePgJsonbValueWithMemoryContext("[1]")},
        PGScalarFunctionTestCase{
            kPGJsonbBuildArrayFunctionName,
            {googlesql::Value::Int64(1), googlesql::Value::Int64(2)},
            *CreatePgJsonbValueWithMemoryContext("[1, 2]")},
        PGScalarFunctionTestCase{
            kPGJsonbBuildArrayFunctionName,
            {kNullStringValue},
            *CreatePgJsonbValueWithMemoryContext("[null]")},

        // pg.jsonb_build_object
        PGScalarFunctionTestCase{kPGJsonbBuildObjectFunctionName,
                                 {},
                                 *CreatePgJsonbValueWithMemoryContext("{}")},
        PGScalarFunctionTestCase{
            kPGJsonbBuildObjectFunctionName,
            {googlesql::Value::String("a"), googlesql::Value::Int64(1)},
            *CreatePgJsonbValueWithMemoryContext("{\"a\": 1}")},
        PGScalarFunctionTestCase{
            kPGJsonbBuildObjectFunctionName,
            {googlesql::Value::String("a"), googlesql::Value::Int64(1),
             googlesql::Value::String("b"), googlesql::Value::Int64(2)},
            *CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": 2}")},
        PGScalarFunctionTestCase{
            kPGJsonbBuildObjectFunctionName,
            {googlesql::Value::String("key"), kNullStringValue},
            *CreatePgJsonbValueWithMemoryContext("{\"key\": null}")},

        // pg.jsonb_contains
        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": -2.0}"),
             *CreatePgJsonbValueWithMemoryContext("{\"b\": -2}")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": true}"),
             *CreatePgJsonbValueWithMemoryContext("{\"a\": true}")},
            googlesql::Value::Bool(false)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                 "[{\"a\": true, \"b\": [2, 3.3]}]"),
             *CreatePgJsonbValueWithMemoryContext("{\"b\": [2]}")},
            googlesql::Value::Bool(false)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext(
                 "[{\"a\": true, \"b\": [2, 3.3]}]"),
             *CreatePgJsonbValueWithMemoryContext("[{\"b\": [3.30]}]")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbContainedFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1, 1.340, 3]"),
             *CreatePgJsonbValueWithMemoryContext("2")},
            googlesql::Value::Bool(false)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1, 1.340, 3]"),
             *CreatePgJsonbValueWithMemoryContext("3.0")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1, 1.340, 3]"),
             *CreatePgJsonbValueWithMemoryContext("[3.0]")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1, 1.340, [3, 4]]"),
             *CreatePgJsonbValueWithMemoryContext("[3.0]")},
            googlesql::Value::Bool(false)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[1, 1.340, [3, 4]]"),
             *CreatePgJsonbValueWithMemoryContext("[[3.0]]")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": [2, 3]}"),
             *CreatePgJsonbValueWithMemoryContext("{\"b\": 2}")},
            googlesql::Value::Bool(false)},

        PGScalarFunctionTestCase{
            kPGJsonbContainsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": [2, 3]}"),
             *CreatePgJsonbValueWithMemoryContext("{\"b\": [2]}")},
            googlesql::Value::Bool(true)},

        // pg.jsonb_contained
        PGScalarFunctionTestCase{
            kPGJsonbContainedFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("1.340000"),
             *CreatePgJsonbValueWithMemoryContext("[1, 1.340, 3]")},
            googlesql::Value::Bool(true)},

        // pg.jsonb_exists
        PGScalarFunctionTestCase{
            kPGJsonbExistsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": -2.0}"),
             googlesql::Value::String("a")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbExistsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"a\", 1, \"b\"]"),
             googlesql::Value::String("b")},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbExistsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": -2.0}"),
             googlesql::Value::String("c")},
            googlesql::Value::Bool(false)},

        PGScalarFunctionTestCase{
            kPGJsonbExistsFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"a\", 1, \"b\"]"),
             googlesql::Value::String("c")},
            googlesql::Value::Bool(false)},

        // pg.jsonb_exists_any
        PGScalarFunctionTestCase{
            kPGJsonbExistsAnyFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"a\", 1, \"b\"]"),
             googlesql::Value::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value::String("c"),
                                      googlesql::Value::NullString(),
                                      googlesql::Value::String("b")})},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbExistsAnyFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"a\", 1, \"b\"]"),
             googlesql::Value::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value::String("c"),
                                      googlesql::Value::NullString(),
                                      googlesql::Value::String("d")})},
            googlesql::Value::Bool(false)},

        // pg.jsonb_exists_all
        PGScalarFunctionTestCase{
            kPGJsonbExistsAllFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"a\", 1, \"b\"]"),
             googlesql::Value::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value::String("a"),
                                      googlesql::Value::NullString(),
                                      googlesql::Value::String("b")})},
            googlesql::Value::Bool(true)},

        PGScalarFunctionTestCase{
            kPGJsonbExistsAllFunctionName,
            {*CreatePgJsonbValueWithMemoryContext("[\"a\", 1, \"b\"]"),
             googlesql::Value::Array(googlesql::types::StringArrayType(),
                                     {googlesql::Value::String("a"),
                                      googlesql::Value::NullString(),
                                      googlesql::Value::String("c")})},
            googlesql::Value::Bool(false)},

        // pg.generate_array (generate_series)
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(3)},
            googlesql::values::Int64Array({1, 2, 3})},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(5),
             googlesql::values::Int64(2)},
            googlesql::values::Int64Array({1, 3, 5})},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {UnwrapValue(CreatePgNumericValueWithMemoryContext("1.0")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("3.0")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("1.0"))},
            googlesql::values::Array(
                spangres::datatypes::GetPgNumericArrayType(),
                {UnwrapValue(CreatePgNumericValueWithMemoryContext("1")),
                 UnwrapValue(CreatePgNumericValueWithMemoryContext("2")),
                 UnwrapValue(CreatePgNumericValueWithMemoryContext("3"))})},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {googlesql::values::NullInt64(), googlesql::values::Int64(3)},
            googlesql::values::Null(googlesql::types::Int64ArrayType())},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {googlesql::values::Int64(1), googlesql::values::NullInt64()},
            googlesql::values::Null(googlesql::types::Int64ArrayType())},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(3),
             googlesql::values::NullInt64()},
            googlesql::values::Null(googlesql::types::Int64ArrayType())},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {CreatePgNumericNullValue(),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("3.0"))},
            googlesql::values::Null(
                spangres::datatypes::GetPgNumericArrayType())},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {UnwrapValue(CreatePgNumericValueWithMemoryContext("1.0")),
             CreatePgNumericNullValue()},
            googlesql::values::Null(
                spangres::datatypes::GetPgNumericArrayType())},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {UnwrapValue(CreatePgNumericValueWithMemoryContext("1.0")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("3.0")),
             CreatePgNumericNullValue()},
            googlesql::values::Null(
                spangres::datatypes::GetPgNumericArrayType())},

        // Failure cases for generate_series
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(3),
             googlesql::values::Int64(0)},
            googlesql::values::Null(googlesql::types::Int64ArrayType()),
            absl::StatusCode::kOutOfRange},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {UnwrapValue(CreatePgNumericValueWithMemoryContext("1.0")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("3.0")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("0.0"))},
            googlesql::values::Null(
                spangres::datatypes::GetPgNumericArrayType()),
            absl::StatusCode::kOutOfRange},
        PGScalarFunctionTestCase{
            kPGGenerateArrayFunctionName,
            {UnwrapValue(CreatePgNumericValueWithMemoryContext("NaN")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("3.0")),
             UnwrapValue(CreatePgNumericValueWithMemoryContext("1.0"))},
            googlesql::values::Null(
                spangres::datatypes::GetPgNumericArrayType()),
            absl::StatusCode::kOutOfRange},

        PGScalarFunctionTestCase{
            kGoogleSQLAddFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kGoogleSQLAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("126.90")},
        PGScalarFunctionTestCase{
            kGoogleSQLAddFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("-120")},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::BoolArray({true}), googlesql::values::Int64(1)},
            googlesql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {googlesql::values::BytesArray({"1", "2"}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::Array(googlesql::types::DateArrayType(),
                                      {googlesql::values::Date(0),
                                       googlesql::values::Date(1)}),
             googlesql::values::Int64(1)},
            googlesql::Value::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {googlesql::values::DoubleArray({1.0}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {googlesql::values::Int64Array({1, 2}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {googlesql::values::StringArray({"a", "b"}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::TimestampArray({absl::Now()}),
             googlesql::values::Int64(1)},
            googlesql::values::Int64(1)},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::Int64Array({1}), googlesql::values::Int64(0)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::Int64Array({1}), googlesql::values::Int64(-1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::Int64Array({}), googlesql::values::Int64(1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {googlesql::values::Int64Array({1}),
                                  googlesql::values::NullInt64()},
                                 googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::Null(googlesql::types::Int64ArrayType()),
             googlesql::values::Int64(1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayLengthFunctionName,
            {googlesql::values::Array(
                 googlesql::types::IntervalArrayType(),
                 {kNullIntervalValue,
                  googlesql::values::Interval(
                      googlesql::IntervalValue::MaxValue())}),
             googlesql::values::Int64(1)},
            googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayLengthFunctionName,
                                 {kUuidArrayValue, googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kGoogleSQLSubtractFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("120.00")},
        PGScalarFunctionTestCase{
            kGoogleSQLSubtractFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("-126.90")},

        PGScalarFunctionTestCase{
            kGoogleSQLMultiplyFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kGoogleSQLMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("246.90")},
        PGScalarFunctionTestCase{
            kGoogleSQLMultiplyFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("-246.90")},

        PGScalarFunctionTestCase{
            kGoogleSQLDivideFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kGoogleSQLDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("61.725")},
        PGScalarFunctionTestCase{
            kGoogleSQLDivideFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("-61.725")},

        PGScalarFunctionTestCase{
            kGoogleSQLDivTruncFunctionName,
            {CreatePgNumericNullValue(),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("3.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN"),
             *CreatePgNumericValueWithMemoryContext("3.45")},
            *CreatePgNumericValueWithMemoryContext("NaN")},
        PGScalarFunctionTestCase{
            kGoogleSQLDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("61")},
        PGScalarFunctionTestCase{
            kGoogleSQLDivTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("2.0")},
            *CreatePgNumericValueWithMemoryContext("-61")},

        PGScalarFunctionTestCase{
            kGoogleSQLAbsFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("123.45")},
        PGScalarFunctionTestCase{
            kGoogleSQLAbsFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("123.45")},
        PGScalarFunctionTestCase{kGoogleSQLAbsFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kGoogleSQLCeilFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("124")},
        PGScalarFunctionTestCase{
            kGoogleSQLCeilFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("-123")},
        PGScalarFunctionTestCase{kGoogleSQLCeilFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kGoogleSQLCeilingFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("124")},
        PGScalarFunctionTestCase{
            kGoogleSQLCeilingFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("-123")},
        PGScalarFunctionTestCase{kGoogleSQLCeilingFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kGoogleSQLFloorFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("123")},
        PGScalarFunctionTestCase{
            kGoogleSQLFloorFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("-124")},
        PGScalarFunctionTestCase{kGoogleSQLFloorFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kGoogleSQLModFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             *CreatePgNumericValueWithMemoryContext("10")},
            *CreatePgNumericValueWithMemoryContext("3.45")},
        PGScalarFunctionTestCase{
            kGoogleSQLModFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45"),
             *CreatePgNumericValueWithMemoryContext("10")},
            *CreatePgNumericValueWithMemoryContext("-3.45")},
        PGScalarFunctionTestCase{kGoogleSQLModFunctionName,
                                 {CreatePgNumericNullValue(),
                                  *CreatePgNumericValueWithMemoryContext("10")},
                                 CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLModFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             CreatePgNumericNullValue()},
            CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kGoogleSQLTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             googlesql::values::Int64(1)},
            *CreatePgNumericValueWithMemoryContext("123.4")},
        PGScalarFunctionTestCase{
            kGoogleSQLTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             googlesql::values::Int64(-1)},
            *CreatePgNumericValueWithMemoryContext("120")},
        PGScalarFunctionTestCase{
            kGoogleSQLTruncFunctionName,
            {CreatePgNumericNullValue(), googlesql::values::Int64(-1)},
            CreatePgNumericNullValue()},
        PGScalarFunctionTestCase{
            kGoogleSQLTruncFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             googlesql::values::NullInt64()},
            CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{
            kGoogleSQLUminusFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45")},
            *CreatePgNumericValueWithMemoryContext("-123.45")},
        PGScalarFunctionTestCase{
            kGoogleSQLUminusFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-123.45")},
            *CreatePgNumericValueWithMemoryContext("123.45")},
        PGScalarFunctionTestCase{kGoogleSQLUminusFunctionName,
                                 {CreatePgNumericNullValue()},
                                 CreatePgNumericNullValue()},

        PGScalarFunctionTestCase{kPGCastNumericToInt64FunctionName,
                                 {*CreatePgNumericValueWithMemoryContext("0")},
                                 googlesql::Value::Int64(0)},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.00000001")},
            googlesql::Value::Int64(0)},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.49999999")},
            googlesql::Value::Int64(0)},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.5")},
            googlesql::Value::Int64(1)},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.49999999")},
            googlesql::Value::Int64(-1)},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.5")},
            googlesql::Value::Int64(-2)},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext(
                absl::StrCat(std::numeric_limits<int64_t>::max()))},
            googlesql::Value::Int64(std::numeric_limits<int64_t>::max())},
        PGScalarFunctionTestCase{
            kPGCastNumericToInt64FunctionName,
            {*CreatePgNumericValueWithMemoryContext(
                absl::StrCat(std::numeric_limits<int64_t>::lowest()))},
            googlesql::Value::Int64(std::numeric_limits<int64_t>::lowest())},
        PGScalarFunctionTestCase{kPGCastNumericToInt64FunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullInt64Value},

        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.000001")},
            googlesql::Value::Double(1.000001)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.299999999999997")},
            googlesql::Value::Double(0.299999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.2999999999999997")},
            googlesql::Value::Double(0.2999999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("299999999999997")},
            googlesql::Value::Double(299999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("2999999999999997")},
            googlesql::Value::Double(2999999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.299999999999997")},
            googlesql::Value::Double(-0.299999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.2999999999999997")},
            googlesql::Value::Double(-0.2999999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-299999999999997")},
            googlesql::Value::Double(-299999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-2999999999999997")},
            googlesql::Value::Double(-2999999999999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToDoubleFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN")},
            kDoubleNaNValue},
        PGScalarFunctionTestCase{kPGCastNumericToDoubleFunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullDoubleValue},

        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.00001")},
            googlesql::Value::Float(1.00001f)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.299997")},
            googlesql::Value::Float(0.299997f)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.2999997")},
            googlesql::Value::Float(0.2999997f)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("299997")},
            googlesql::Value::Float(299997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("2999997")},
            googlesql::Value::Float(2999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.2999997")},
            googlesql::Value::Float(-0.2999997f)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.29999997")},
            googlesql::Value::Float(-0.29999997f)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-2999997")},
            googlesql::Value::Float(-2999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-29999997")},
            googlesql::Value::Float(-29999997)},
        PGScalarFunctionTestCase{
            kPGCastNumericToFloatFunctionName,
            {*CreatePgNumericValueWithMemoryContext("NaN")},
            kFloatNaNValue},
        PGScalarFunctionTestCase{kPGCastNumericToFloatFunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullFloatValue},

        PGScalarFunctionTestCase{kPGCastToStringFunctionName,
                                 {kPGNumericNaNValue},
                                 googlesql::Value::String("NaN")},
        PGScalarFunctionTestCase{kPGCastToStringFunctionName,
                                 {CreatePgNumericNullValue()},
                                 kNullStringValue},
        PGScalarFunctionTestCase{kPGCastToStringFunctionName,
                                 {kPGNumericMinValue},
                                 googlesql::Value::String(MinNumericString())},
        PGScalarFunctionTestCase{kPGCastToStringFunctionName,
                                 {kPGNumericMaxValue},
                                 googlesql::Value::String(MaxNumericString())},
        PGScalarFunctionTestCase{
            kPGCastToStringFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.1")},
            googlesql::Value::String("0.1")},

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
            {googlesql::Value::Double(-123.123), googlesql::Value::Int64(4),
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
            {googlesql::Value::String("123.123"), googlesql::Value::Int64(5),
             googlesql::Value::Int64(2)},
            *CreatePgNumericValueWithMemoryContext("123.12")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {googlesql::Value::String("294"), googlesql::Value::Int64(2),
             googlesql::Value::Int64(-1)},
            *CreatePgNumericValueWithMemoryContext("290")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {googlesql::Value::String("NaN"), googlesql::Value::Int64(5),
             googlesql::Value::Int64(2)},
            kPGNumericNaNValue},
        PGScalarFunctionTestCase{kPGCastToNumericFunctionName,
                                 {kNullStringValue, kInt64Value, kInt64Value},
                                 kNullPGNumericValue},

        // CAST_TO_NUMERIC for PG.NUMERIC
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("12.345"),
             googlesql::Value::Int64(4), googlesql::Value::Int64(2)},
            *CreatePgNumericValueWithMemoryContext("12.35")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("12.345"),
             googlesql::Value::Int64(4), kInt64Value},
            *CreatePgNumericValueWithMemoryContext("12.3")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("12.345"),
             googlesql::Value::Int64(4)},
            *CreatePgNumericValueWithMemoryContext("12")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.45"),
             googlesql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("123")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000001"),
             googlesql::Value::Int64(13), googlesql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("123.0000000001")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000001"),
             googlesql::Value::Int64(13), googlesql::Value::Int64(9)},
            *CreatePgNumericValueWithMemoryContext("123.0000000000")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000001"),
             googlesql::Value::Int64(15), googlesql::Value::Int64(12)},
            *CreatePgNumericValueWithMemoryContext("123.000000000100")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("123.0000000009"),
             googlesql::Value::Int64(13), googlesql::Value::Int64(9)},
            *CreatePgNumericValueWithMemoryContext("123.000000001")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1"),
             googlesql::Value::Int64(1000), googlesql::Value::Int64(999)},
            *CreatePgNumericValueWithMemoryContext(
                absl::StrCat("1.", std::string(999, '0')))},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext(
                 absl::StrCat("1", std::string(999, '0'))),
             googlesql::Value::Int64(1000), googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext(
                absl::StrCat("1", std::string(999, '0')))},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.5"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("2")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.499999999"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("1")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.5"), kInt64Value,
             googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("-2")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-1.499999999"),
             kInt64Value, googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("-1")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("1.5"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("2")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.0000000009"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("-0.0000000009")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("-0.0000000009"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(9)},
            *CreatePgNumericValueWithMemoryContext("-0.000000001")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.000"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("0")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0"),
             googlesql::Value::Int64(10), googlesql::Value::Int64(10)},
            *CreatePgNumericValueWithMemoryContext("0.0000000000")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("9.99"),
             googlesql::Value::Int64(3), kInt64Value},
            *CreatePgNumericValueWithMemoryContext("10.0")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.99"), kInt64Value,
             googlesql::Value::Int64(0)},
            *CreatePgNumericValueWithMemoryContext("1")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {*CreatePgNumericValueWithMemoryContext("0.3"),
             googlesql::Value::Int64(3), googlesql::Value::Int64(3)},
            *CreatePgNumericValueWithMemoryContext("0.300")},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kPGNumericNaNValue, googlesql::Value::Int64(5),
             googlesql::Value::Int64(3)},
            kPGNumericNaNValue},
        PGScalarFunctionTestCase{
            kPGCastToNumericFunctionName,
            {kNullPGNumericValue, googlesql::Value::Int64(5),
             googlesql::Value::Int64(3)},
            kNullPGNumericValue},

        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::BoolArray({true}), googlesql::values::Int64(1)},
            googlesql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {googlesql::values::BytesArray({"1", "2"}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::Array(googlesql::types::DateArrayType(),
                                      {googlesql::values::Date(0),
                                       googlesql::values::Date(1)}),
             googlesql::values::Int64(1)},
            googlesql::Value::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {googlesql::values::DoubleArray({1.0}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(1)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {googlesql::values::Int64Array({1, 2}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {googlesql::values::StringArray({"a", "b"}),
                                  googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::TimestampArray({absl::Now()}),
             googlesql::values::Int64(1)},
            googlesql::values::Int64(1)},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::Int64Array({1}), googlesql::values::Int64(0)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::Int64Array({1}), googlesql::values::Int64(-1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::Int64Array({}), googlesql::values::Int64(1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {googlesql::values::Int64Array({1}),
                                  googlesql::values::NullInt64()},
                                 googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::Null(googlesql::types::Int64ArrayType()),
             googlesql::values::Int64(1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGArrayUpperFunctionName,
            {googlesql::values::Array(
                 googlesql::types::IntervalArrayType(),
                 {kNullIntervalValue,
                  googlesql::values::Interval(
                      googlesql::IntervalValue::MaxValue())}),
             googlesql::values::Int64(1)},
            googlesql::values::Int64(2)},
        PGScalarFunctionTestCase{kPGArrayUpperFunctionName,
                                 {kUuidArrayValue, googlesql::values::Int64(1)},
                                 googlesql::values::Int64(2)},

        PGScalarFunctionTestCase{kPGTextregexneFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("bb.*")},
                                 googlesql::values::Bool(true)},
        PGScalarFunctionTestCase{kPGTextregexneFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("ab.*")},
                                 googlesql::values::Bool(false)},
        PGScalarFunctionTestCase{
            kPGTextregexneFunctionName,
            {kNullStringValue, googlesql::values::String("ab.*")},
            googlesql::values::NullBool()},
        PGScalarFunctionTestCase{
            kPGTextregexneFunctionName,
            {googlesql::values::String("abcdefg"), kNullStringValue},
            googlesql::values::NullBool()},

        PGScalarFunctionTestCase{kPGILikeFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("ABC%")},
                                 googlesql::values::Bool(true)},
        PGScalarFunctionTestCase{kPGILikeFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("abc")},
                                 googlesql::values::Bool(false)},
        PGScalarFunctionTestCase{kPGILikeFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("A_C%")},
                                 googlesql::values::Bool(true)},
        PGScalarFunctionTestCase{
            kPGILikeFunctionName,
            {kNullStringValue, googlesql::values::String("abc%")},
            googlesql::values::NullBool()},
        PGScalarFunctionTestCase{
            kPGILikeFunctionName,
            {googlesql::values::String("abcdefg"), kNullStringValue},
            googlesql::values::NullBool()},

        PGScalarFunctionTestCase{kPGNotILikeFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("ABC%")},
                                 googlesql::values::Bool(false)},
        PGScalarFunctionTestCase{kPGNotILikeFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("abc")},
                                 googlesql::values::Bool(true)},
        PGScalarFunctionTestCase{
            kPGNotILikeFunctionName,
            {kNullStringValue, googlesql::values::String("abc%")},
            googlesql::values::NullBool()},
        PGScalarFunctionTestCase{
            kPGNotILikeFunctionName,
            {googlesql::values::String("abcdefg"), kNullStringValue},
            googlesql::values::NullBool()},

        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {googlesql::values::Date(0), googlesql::values::Date(1)},
            googlesql::values::Int64(-1)},
        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {googlesql::values::NullDate(), googlesql::values::Date(1)},
            googlesql::values::NullInt64()},
        PGScalarFunctionTestCase{
            kPGDateMiFunctionName,
            {googlesql::values::Date(0), googlesql::values::NullDate()},
            googlesql::values::NullInt64()},

        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {googlesql::values::Date(0), googlesql::values::Int64(1)},
            googlesql::values::Date(-1)},
        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {googlesql::values::NullDate(), googlesql::values::Int64(1)},
            googlesql::values::NullDate()},
        PGScalarFunctionTestCase{
            kPGDateMiiFunctionName,
            {googlesql::values::Date(0), googlesql::values::NullInt64()},
            googlesql::values::NullDate()},

        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {googlesql::values::Date(0), googlesql::values::Int64(1)},
            googlesql::values::Date(1)},
        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {googlesql::values::NullDate(), googlesql::values::Int64(1)},
            googlesql::values::NullDate()},
        PGScalarFunctionTestCase{
            kPGDatePliFunctionName,
            {googlesql::values::Date(0), googlesql::values::NullInt64()},
            googlesql::values::NullDate()},

        PGScalarFunctionTestCase{kPGToDateFunctionName,
                                 {googlesql::values::String("01 Jan 1970"),
                                  googlesql::values::String("DD Mon YYYY")},
                                 googlesql::values::Date(0)},
        PGScalarFunctionTestCase{
            kPGToDateFunctionName,
            {kNullStringValue, googlesql::values::String("DD Mon YYYY")},
            googlesql::values::NullDate()},
        PGScalarFunctionTestCase{
            kPGToDateFunctionName,
            {googlesql::values::String("01 Jan 1970"), kNullStringValue},
            googlesql::values::NullDate()},

        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {googlesql::values::String("01 Jan 1970 00:00:00+00"),
             googlesql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            googlesql::values::Timestamp(absl::UnixEpoch())},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {kNullStringValue,
             googlesql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            googlesql::values::NullTimestamp()},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {googlesql::values::String("01 Jan 1970 00:00:00+00"),
             kNullStringValue},
            googlesql::values::NullTimestamp()},

        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {googlesql::values::Int64(-123),
                                  googlesql::values::String("999PR")},
                                 googlesql::values::String("<123>")},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {googlesql::values::Timestamp(absl::UnixEpoch()),
             googlesql::values::String("YYYY-MM-DD HH24:MI:SSTZH")},
            googlesql::values::String("1969-12-31 16:00:00-08")},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {googlesql::values::Timestamp(absl::UnixEpoch()),
             googlesql::values::String("")},
            kNullStringValue},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {googlesql::values::Double(-123.45),
                                  googlesql::values::String("999.999PR")},
                                 googlesql::values::String("<123.450>")},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {CreatePgNumericValueWithMemoryContext("123.45").value(),
             googlesql::values::String("999")},
            googlesql::values::String(" 123")},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {googlesql::values::NullDouble(),
                                  googlesql::values::String("999.999PR")},
                                 kNullStringValue},
        PGScalarFunctionTestCase{
            kPGToCharFunctionName,
            {googlesql::values::Double(-123.45), kNullStringValue},
            kNullStringValue},

        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {googlesql::values::String("-12,345,678"),
             googlesql::values::String("99G999G999")},
            *CreatePgNumericValueWithMemoryContext("-12345678")},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {googlesql::values::String("<123.456>"),
             googlesql::values::String("999.999PR")},
            *CreatePgNumericValueWithMemoryContext("-123.456")},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {googlesql::values::String("$123.45-"),
             googlesql::values::String("L999.99S")},
            *CreatePgNumericValueWithMemoryContext("-123.45")},
        PGScalarFunctionTestCase{kPGToNumberFunctionName,
                                 {googlesql::values::String("42nd"),
                                  googlesql::values::String("99th")},
                                 *CreatePgNumericValueWithMemoryContext("42")},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {kNullStringValue, googlesql::values::String("999")},
            googlesql::values::Null(spangres::datatypes::GetPgNumericType())},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {googlesql::values::String("123"), kNullStringValue},
            googlesql::values::Null(spangres::datatypes::GetPgNumericType())},

        PGScalarFunctionTestCase{kPGQuoteIdentFunctionName,
                                 {googlesql::values::String("test")},
                                 googlesql::values::String("\"test\"")},
        PGScalarFunctionTestCase{
            kPGQuoteIdentFunctionName, {kNullStringValue}, kNullStringValue},

        PGScalarFunctionTestCase{kPGSubstringFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("a(b.)")},
                                 googlesql::values::String("bc")},
        PGScalarFunctionTestCase{kPGSubstringFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("(h.)?")},
                                 kNullStringValue},

        PGScalarFunctionTestCase{kPGRegexpMatchFunctionName,
                                 {googlesql::values::String("abcdefg"),
                                  googlesql::values::String("b.")},
                                 googlesql::values::StringArray({"bc"})},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {kNullStringValue, googlesql::values::String("b.")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {googlesql::values::String("abcdefg"), kNullStringValue},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {googlesql::values::String("abcdefg"),
             googlesql::values::String("h.")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {googlesql::values::String("abcDefg"),
             googlesql::values::String("b.*"), googlesql::values::String("i")},
            googlesql::values::StringArray({"bcDefg"})},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {kNullStringValue, googlesql::values::String("b.*"),
             googlesql::values::String("i")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {googlesql::values::String("abcDefg"), kNullStringValue,
             googlesql::values::String("i")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {googlesql::values::String("abcDefg"),
             googlesql::values::String("b.*"), kNullStringValue},
            googlesql::values::Null(googlesql::types::StringArrayType())},

        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {googlesql::values::String("a1b2c3d"),
             googlesql::values::String("[0-9]")},
            googlesql::values::StringArray({"a", "b", "c", "d"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {kNullStringValue, googlesql::values::String("[0-9]")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {googlesql::values::String("a1b2c3d"), kNullStringValue},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {googlesql::values::String("1A2b3C4"),
             googlesql::values::String("[a-z]"),
             googlesql::values::String("i")},
            googlesql::values::StringArray({"1", "2", "3", "4"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {kNullStringValue, googlesql::values::String("[a-z]"),
             googlesql::values::String("i")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {googlesql::values::String("1A2b3C4"), kNullStringValue,
             googlesql::values::String("i")},
            googlesql::values::Null(googlesql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {googlesql::values::String("1A2b3C4"),
             googlesql::values::String("[a-z]"), kNullStringValue},
            googlesql::values::Null(googlesql::types::StringArrayType())},

        // Cast to PG.OID
        PGScalarFunctionTestCase{
            kPGCastToOidFunctionName, {kInt64Value}, kPGOidValue},
        PGScalarFunctionTestCase{
            kPGCastToOidFunctionName,
            {googlesql::Value::Int64(std::numeric_limits<uint32_t>::min())},
            kPGOidMinValue},
        PGScalarFunctionTestCase{
            kPGCastToOidFunctionName,
            {googlesql::Value::Int64(std::numeric_limits<uint32_t>::max())},
            kPGOidMaxValue},
        PGScalarFunctionTestCase{
            kPGCastToOidFunctionName, {kNullInt64Value}, kNullPGOidValue},

        PGScalarFunctionTestCase{kPGCastToOidFunctionName,
                                 {googlesql::Value::String("1")},
                                 kPGOidValue},
        PGScalarFunctionTestCase{
            kPGCastToOidFunctionName,
            {googlesql::Value::String(
                absl::StrCat(std::numeric_limits<int32_t>::min()))},
            *CreatePgOidValue(
                static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1)},
        PGScalarFunctionTestCase{kPGCastToOidFunctionName,
                                 {googlesql::Value::String(absl::StrCat(
                                     std::numeric_limits<uint32_t>::max()))},
                                 kPGOidMaxValue},
        PGScalarFunctionTestCase{
            kPGCastToOidFunctionName, {kNullStringValue}, kNullPGOidValue},

        // Cast from PG.OID
        PGScalarFunctionTestCase{
            kPGCastFromOidFunctionName, {kPGOidValue}, kInt64Value},
        PGScalarFunctionTestCase{
            kPGCastFromOidFunctionName,
            {kPGOidMinValue},
            googlesql::Value::Int64(std::numeric_limits<uint32_t>::min())},
        PGScalarFunctionTestCase{
            kPGCastFromOidFunctionName,
            {kPGOidMaxValue},
            googlesql::Value::Int64(std::numeric_limits<uint32_t>::max())},
        PGScalarFunctionTestCase{
            kPGCastFromOidFunctionName, {kNullPGOidValue}, kNullInt64Value},

        PGScalarFunctionTestCase{kPGCastFromOidFunctionName,
                                 {kPGOidValue},
                                 googlesql::Value::String("1")},
        PGScalarFunctionTestCase{kPGCastFromOidFunctionName,
                                 {kPGOidMinValue},
                                 googlesql::Value::String(absl::StrCat(
                                     std::numeric_limits<uint32_t>::min()))},
        PGScalarFunctionTestCase{kPGCastFromOidFunctionName,
                                 {kPGOidMaxValue},
                                 googlesql::Value::String(absl::StrCat(
                                     std::numeric_limits<uint32_t>::max()))},
        PGScalarFunctionTestCase{
            kPGCastFromOidFunctionName, {kNullPGOidValue}, kNullStringValue},

        // PG.FLOAT_ADD
        PGScalarFunctionTestCase{kPGFloatAddFunctionName,
                                 {kNullFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatAddFunctionName,
                                 {kNullFloatValue, kFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatAddFunctionName,
                                 {kFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatAddFunctionName,
                                 {kFloatValue, kFloatValue},
                                 googlesql::values::Float(2.0f)},

        // PG.FLOAT_SUBTRACT
        PGScalarFunctionTestCase{kPGFloatSubtractFunctionName,
                                 {kNullFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatSubtractFunctionName,
                                 {kNullFloatValue, kFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatSubtractFunctionName,
                                 {kFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatSubtractFunctionName,
                                 {kFloatValue, kFloatValue},
                                 googlesql::values::Float(0)},
        PGScalarFunctionTestCase{kPGFloatSubtractFunctionName,
                                 {kFloatValue, googlesql::values::Float(2.0)},
                                 googlesql::values::Float(-1.0f)},
        PGScalarFunctionTestCase{kPGFloatSubtractFunctionName,
                                 {googlesql::values::Float(2.0), kFloatValue},
                                 googlesql::values::Float(1.0f)},

        // PG.FLOAT_MULTIPLY
        PGScalarFunctionTestCase{kPGFloatMultiplyFunctionName,
                                 {kNullFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatMultiplyFunctionName,
                                 {kNullFloatValue, kFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatMultiplyFunctionName,
                                 {kFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatMultiplyFunctionName,
                                 {kFloatValue, kFloatValue},
                                 googlesql::values::Float(1.0f)},
        PGScalarFunctionTestCase{kPGFloatMultiplyFunctionName,
                                 {kFloatValue, googlesql::values::Float(-2.0f)},
                                 googlesql::values::Float(-2.0f)},
        PGScalarFunctionTestCase{kPGFloatMultiplyFunctionName,
                                 {kFloatValue, googlesql::values::Float(0)},
                                 googlesql::values::Float(0)},

        // PG.FLOAT_DIVIDE
        PGScalarFunctionTestCase{kPGFloatDivideFunctionName,
                                 {kNullFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatDivideFunctionName,
                                 {kNullFloatValue, kFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatDivideFunctionName,
                                 {kFloatValue, kNullFloatValue},
                                 googlesql::values::NullFloat()},
        PGScalarFunctionTestCase{kPGFloatDivideFunctionName,
                                 {kFloatValue, kFloatValue},
                                 googlesql::values::Float(1.0f)},
        PGScalarFunctionTestCase{kPGFloatDivideFunctionName,
                                 {kFloatValue, googlesql::values::Float(2.0)},
                                 googlesql::values::Float(0.5f)},
        PGScalarFunctionTestCase{kPGFloatDivideFunctionName,
                                 {googlesql::values::Float(2.0), kFloatValue},
                                 googlesql::values::Float(2.0f)},
        // Interval Unary minus
        PGScalarFunctionTestCase{kPGIntervalUnaryMinusFunctionName,
                                 {kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalUnaryMinusFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456789")},
                                 CreateIntervalValue("-1-1 -8 -1:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalUnaryMinusFunctionName,
            {CreateIntervalValue("-1-1 -8 -1:1:1.123456789")},
            CreateIntervalValue("1-1 8 1:1:1.123457")},

        // interval + interval -> interval
        PGScalarFunctionTestCase{kPGIntervalAddFunctionName,
                                 {kNullIntervalValue, kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalAddFunctionName,
                                 {kNullIntervalValue, kIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalAddFunctionName,
                                 {kIntervalValue, kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalAddFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456789"),
                                  CreateIntervalValue("1-1 8 1:1:1.123456789")},
                                 CreateIntervalValue("2-2 16 2:2:2.246914")},
        PGScalarFunctionTestCase{kPGIntervalAddFunctionName,
                                 {kIntervalMaxValue, kIntervalMaxValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kOutOfRange},
        PGScalarFunctionTestCase{kPGIntervalAddFunctionName,
                                 {kIntervalMinValue, kIntervalMinValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kOutOfRange},

        // interval - interval -> interval
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {kNullIntervalValue, kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {kNullIntervalValue, kIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {kIntervalValue, kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456789"),
                                  CreateIntervalValue("1-1 8 1:1:1.123456789")},
                                 CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{
            kPGIntervalSubtractFunctionName,
            {CreateIntervalValue("-1-1 -8 -1:1:1.123456489"),
             CreateIntervalValue("1-1 8 1:1:1.123456489")},
            CreateIntervalValue("-2-2 -16 -2:2:2.246912")},
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456"),
                                  CreateIntervalValue("-1-1 -8 -1:1:1.123456")},
                                 CreateIntervalValue("2-2 16 2:2:2.246912")},
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {kIntervalMinValue, kIntervalMaxValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kOutOfRange},
        PGScalarFunctionTestCase{kPGIntervalSubtractFunctionName,
                                 {kIntervalMaxValue, kIntervalMinValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kOutOfRange},

        // interval * double -> interval
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kNullIntervalValue, kNullDoubleValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kNullIntervalValue, kDoubleValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kIntervalValue, kNullDoubleValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456"),
                                  googlesql::values::Double(1.0)},
                                 CreateIntervalValue("1-1 8 1:1:1.123456")},
        PGScalarFunctionTestCase{
            kPGIntervalMultiplyFunctionName,
            {CreateIntervalValue("1-1 8 1:1:1.123456"),
             googlesql::values::Double(-2.5)},
            CreateIntervalValue("-2-8 -35 -2:32:32.808640")},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456"),
                                  googlesql::values::Double(0)},
                                 CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kIntervalMaxValue, kDoubleMaxValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kIntervalMinValue, kDoubleMaxValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kIntervalMaxValue, kDoubleMinValue},
                                 CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{kPGIntervalMultiplyFunctionName,
                                 {kIntervalMinValue, kDoubleMinValue},
                                 CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{
            kPGIntervalMultiplyFunctionName,
            {kIntervalMinValue, googlesql::values::Double(
                                    std::numeric_limits<double>::infinity())},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{
            kPGIntervalMultiplyFunctionName,
            {kIntervalMaxValue, googlesql::values::Double(
                                    std::numeric_limits<double>::infinity())},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{
            kPGIntervalMultiplyFunctionName,
            {kIntervalMinValue, googlesql::values::Double(
                                    -std::numeric_limits<double>::infinity())},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{
            kPGIntervalMultiplyFunctionName,
            {kIntervalMaxValue, googlesql::values::Double(
                                    -std::numeric_limits<double>::infinity())},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument},

        // interval / double -> interval
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kNullIntervalValue, kNullDoubleValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kNullIntervalValue, kDoubleValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kIntervalValue, kNullDoubleValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456"),
                                  googlesql::values::Double(1.0)},
                                 CreateIntervalValue("1-1 8 1:1:1.123456")},
        PGScalarFunctionTestCase{
            kPGIntervalDivideFunctionName,
            {CreateIntervalValue("-0-5 -9 -5:12:24.449382"),
             googlesql::values::Double(-2.5)},
            CreateIntervalValue("0-2 3 16:28:57.779753")},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456"),
                                  googlesql::values::Double(0)},
                                 kNullIntervalValue,
                                 absl::StatusCode::kOutOfRange,
                                 "division by zero"},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kIntervalMaxValue, kDoubleMaxValue},
                                 CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kIntervalMinValue, kDoubleMaxValue},
                                 CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kIntervalMaxValue, kDoubleMinValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{kPGIntervalDivideFunctionName,
                                 {kIntervalMinValue, kDoubleMinValue},
                                 kNullIntervalValue,
                                 absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{
            kPGIntervalDivideFunctionName,
            {kIntervalMinValue, googlesql::values::Double(
                                    std::numeric_limits<double>::infinity())},
            CreateIntervalValue("0-0 0 0:0:0")},

        // make_interval
        PGScalarFunctionTestCase{
            kPGIntervalMakeIntervalFunctionName,
            {googlesql::values::Int64(0), googlesql::values::Int64(0),
             googlesql::values::Int64(0), googlesql::values::Int64(0),
             googlesql::values::Int64(0), googlesql::values::Int64(0),
             kNullDoubleValue},
            kNullIntervalValue},
        PGScalarFunctionTestCase{
            kPGIntervalMakeIntervalFunctionName,
            {googlesql::values::Int64(0), googlesql::values::Int64(0),
             googlesql::values::Int64(0), googlesql::values::Int64(0),
             googlesql::values::Int64(0), googlesql::values::Int64(0),
             googlesql::values::Double(0.0)},
            CreateIntervalValue("0-0 0 0:0:0")},
        PGScalarFunctionTestCase{
            kPGIntervalMakeIntervalFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(2),
             googlesql::values::Int64(3), googlesql::values::Int64(4),
             googlesql::values::Int64(5), googlesql::values::Int64(6),
             googlesql::values::Double(10000.12345643)},
            CreateIntervalValue("1-2 25 7:52:40.123456")},
        PGScalarFunctionTestCase{
            kPGIntervalMakeIntervalFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(-2),
             googlesql::values::Int64(3), googlesql::values::Int64(-4),
             googlesql::values::Int64(5), googlesql::values::Int64(-6),
             googlesql::values::Double(10000.123456789)},
            CreateIntervalValue("0-10 17 7:40:40.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalMakeIntervalFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(-2),
             googlesql::values::Int64(3), googlesql::values::Int64(-4),
             googlesql::values::Int64(5), googlesql::values::Int64(-6),
             googlesql::values::Double(
                 std::numeric_limits<double>::infinity())},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument},
        PGScalarFunctionTestCase{
            kPGIntervalMakeIntervalFunctionName,
            {googlesql::values::Int64(1), googlesql::values::Int64(-2),
             googlesql::values::Int64(3), googlesql::values::Int64(-4),
             googlesql::values::Int64(5), googlesql::values::Int64(-6),
             googlesql::values::Double(
                 std::numeric_limits<double>::quiet_NaN())},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument},

        // justify_interval
        PGScalarFunctionTestCase{kPGIntervalJustifyIntervalFunctionName,
                                 {kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyIntervalFunctionName,
            {CreateIntervalValue("1-1 8 26:1:1.123456589")},
            CreateIntervalValue("1-1 9 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyIntervalFunctionName,
            {CreateIntervalValue("1-1 32 2:1:1.123456589")},
            CreateIntervalValue("1-2 2 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyIntervalFunctionName,
            {CreateIntervalValue("1-1 29 50:1:1.123456589")},
            CreateIntervalValue("1-2 1 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyIntervalFunctionName,
            {CreateIntervalValue("-1-1 -8 -26:1:1.123456589")},
            CreateIntervalValue("-1-1 -9 -2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyIntervalFunctionName,
            {CreateIntervalValue("-1-1 -32 -2:1:1.123456589")},
            CreateIntervalValue("-1-2 -2 -2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyIntervalFunctionName,
            {CreateIntervalValue("-1-1 -29 -50:1:1.123456589")},
            CreateIntervalValue("-1-2 -1 -2:1:1.123457")},

        // justify_days
        PGScalarFunctionTestCase{kPGIntervalJustifyDaysFunctionName,
                                 {kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalJustifyDaysFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456789")},
                                 CreateIntervalValue("1-1 8 1:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyDaysFunctionName,
            {CreateIntervalValue("1-1 8 26:1:1.123456589")},
            CreateIntervalValue("1-1 8 26:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyDaysFunctionName,
            {CreateIntervalValue("1-1 32 2:1:1.123456589")},
            CreateIntervalValue("1-2 2 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyDaysFunctionName,
            {CreateIntervalValue("1-1 30 50:1:1.123456589")},
            CreateIntervalValue("1-2 0 50:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyDaysFunctionName,
            {CreateIntervalValue("-1-1 -8 -26:1:1.123456589")},
            CreateIntervalValue("-1-1 -8 -26:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyDaysFunctionName,
            {CreateIntervalValue("-1-1 -32 -2:1:1.123456589")},
            CreateIntervalValue("-1-2 -2 -2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyDaysFunctionName,
            {CreateIntervalValue("-1-1 -30 -50:1:1.123456589")},
            CreateIntervalValue("-1-2 0 -50:1:1.123457")},

        // justify_hours
        PGScalarFunctionTestCase{kPGIntervalJustifyHoursFunctionName,
                                 {kNullIntervalValue},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{kPGIntervalJustifyHoursFunctionName,
                                 {CreateIntervalValue("1-1 8 1:1:1.123456789")},
                                 CreateIntervalValue("1-1 8 1:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyHoursFunctionName,
            {CreateIntervalValue("1-1 8 26:1:1.123456589")},
            CreateIntervalValue("1-1 9 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyHoursFunctionName,
            {CreateIntervalValue("1-1 32 2:1:1.123456589")},
            CreateIntervalValue("1-1 32 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyHoursFunctionName,
            {CreateIntervalValue("1-1 29 50:1:1.123456589")},
            CreateIntervalValue("1-1 31 2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyHoursFunctionName,
            {CreateIntervalValue("-1-1 -8 -26:1:1.123456589")},
            CreateIntervalValue("-1-1 -9 -2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyHoursFunctionName,
            {CreateIntervalValue("-1-1 -32 -2:1:1.123456589")},
            CreateIntervalValue("-1-2 -2 -2:1:1.123457")},
        PGScalarFunctionTestCase{
            kPGIntervalJustifyHoursFunctionName,
            {CreateIntervalValue("-1-1 -29 -50:1:1.123456589")},
            CreateIntervalValue("-1-1 -31 -2:1:1.123457")},

        // cast to string
        PGScalarFunctionTestCase{kPGCastToStringFunctionName,
                                 {kNullIntervalValue},
                                 googlesql::values::NullString()},
        PGScalarFunctionTestCase{
            kPGCastToStringFunctionName,
            {CreateIntervalValue("1-1 8 1:1:1.123456")},
            googlesql::Value::String("1 year 1 mon 8 days 01:01:01.123456")},
        PGScalarFunctionTestCase{
            kPGCastToStringFunctionName,
            {CreateIntervalValue("0-1 -8 1:7:1.123456897")},
            googlesql::Value::String("1 mon -8 days +01:07:01.123457")},
        PGScalarFunctionTestCase{
            kPGCastToStringFunctionName,
            {CreateIntervalValue("-1-1 68 1:56:30.123456")},
            googlesql::Value::String(
                "-1 years -1 mons +68 days 01:56:30.123456")},

        // cast to interval
        PGScalarFunctionTestCase{kPGCastToIntervalFunctionName,
                                 {googlesql::values::NullString()},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{
            kPGCastToIntervalFunctionName,
            {googlesql::values::String("1 year 1 mon 8 days 01:01:01.123456")},
            CreateIntervalValue("1-1 8 1:1:1.123456")},
        PGScalarFunctionTestCase{
            kPGCastToIntervalFunctionName,
            {googlesql::values::String("P1Y1M8DT1:1:1.123456")},
            CreateIntervalValue("1-1 8 1:1:1.123456")},
        PGScalarFunctionTestCase{
            kPGCastToIntervalFunctionName,
            {googlesql::values::String("P1Y1M8DT1H1M1.123456S")},
            CreateIntervalValue("1-1 8 1:1:1.123456")},
        PGScalarFunctionTestCase{
            kPGCastToIntervalFunctionName,
            {googlesql::values::String("abc")},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument,
            "invalid input syntax for type interval: \"abc\""},

        // extract(string, interval) -> numeric
        PGScalarFunctionTestCase{
            kPGIntervalExtractFunctionName,
            {googlesql::values::String("seconds"),
             CreateIntervalValue("1-1 8 1:1:1.123456")},
            *CreatePgNumericValueWithMemoryContext("1.123456"),
        },
        PGScalarFunctionTestCase{
            kPGIntervalExtractFunctionName,
            {googlesql::values::String("years"),
             CreateIntervalValue("1-1 8 1:1:1.123456")},
            *CreatePgNumericValueWithMemoryContext("1"),
        },
        PGScalarFunctionTestCase{
            kPGIntervalExtractFunctionName,
            {kNullStringValue, CreateIntervalValue("1-1 8 1:1:1.123456")},
            kNullPGNumericValue,
        },
        PGScalarFunctionTestCase{
            kPGIntervalExtractFunctionName,
            {googlesql::values::String("abc"), kNullIntervalValue},
            kNullPGNumericValue,
        },
        PGScalarFunctionTestCase{
            kPGIntervalExtractFunctionName,
            {googlesql::values::String("abc"),
             CreateIntervalValue("1-1 8 1:1:1.123456")},
            kNullIntervalValue,
            absl::StatusCode::kInvalidArgument,
            "unit \"abc\" not recognized for type interval"},

        // timestamptz - timestamptz -> interval
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::values::NullTimestamp(),
             googlesql::Value::Timestamp(absl::FromUnixNanos(223456789))},
            kNullIntervalValue},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::Value::Timestamp(absl::FromUnixNanos(223456789)),
             googlesql::values::NullTimestamp()},
            kNullIntervalValue},
        PGScalarFunctionTestCase{kPGTimestamptzSubtractTimestamptzFunctionName,
                                 {googlesql::values::NullTimestamp(),
                                  googlesql::values::NullTimestamp()},
                                 kNullIntervalValue},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::Value::Timestamp(absl::FromUnixNanos(123456789)),
             googlesql::Value::Timestamp(absl::FromUnixNanos(223456789))},
            CreateIntervalValue("0-0 0 -0:0:0.100")},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::Value::Timestamp(absl::FromUnixNanos(123456789)),
             googlesql::Value::Timestamp(absl::FromUnixNanos(133457789))},
            CreateIntervalValue("0-0 0 -0:0:0.010001")},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::Value::Timestamp(absl::FromUnixNanos(123456789)),
             googlesql::Value::Timestamp(absl::FromUnixNanos(123456788))},
            CreateIntervalValue("0-0 0 0:0:0.0")},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::Value::Timestamp(absl::FromUnixNanos(123456789)),
             googlesql::Value::Timestamp(absl::FromUnixNanos(123456679))},
            CreateIntervalValue("0-0 0 -0:0:0.0")},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {googlesql::Value::Timestamp(absl::FromUnixNanos(223456789)),
             googlesql::Value::Timestamp(absl::FromUnixNanos(123456079))},
            CreateIntervalValue("0-0 0 0:0:0.1")},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {kMaxTimestampValue, kMinTimestampValue},
            CreateIntervalValue("0-0 0 87649415:59:59.999999")},
        PGScalarFunctionTestCase{
            kPGTimestamptzSubtractTimestamptzFunctionName,
            {kMinTimestampValue, kMaxTimestampValue},
            CreateIntervalValue("0-0 0 -87649415:59:59.999999")}),
    [](const testing::TestParamInfo<PGScalarFunctionTestCase>& info) {
      std::string name = absl::StrCat(
          "idx_", info.index, "_", info.param.function_name, "_",
          absl::StrJoin(info.param.function_arguments, "_",
                        [](std::string* out, googlesql::Value v) {
                          absl::StrAppend(
                              out, absl::StrCat(v.type()->DebugString(), "_",
                                                // Limit number of chars.
                                                v.DebugString().substr(0, 10)));
                        }));
      absl::c_replace_if(name, [](char c) { return !std::isalnum(c); }, '_');
      return name;
    });

TEST_F(EmulatorFunctionsTest,
       RegexpMatchReturnsNullElementForUnmatchedOptionalCapturingGroups) {
  const googlesql::Function* function =
      functions_[kPGRegexpMatchFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::Value expected,
      googlesql::Value::MakeArray(
          googlesql::types::StringArrayType(),
          {googlesql::values::String("bc"), kNullStringValue}));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::values::String("abcdefg"),
                                      googlesql::values::String("(b.)(h.)?")})),
      IsOkAndHolds(expected));
}

// Tested separately from the parameterized tests as we need a memory context
// before creating a PG.JSONB value.
TEST_F(EmulatorFunctionsTest, ArrayUpperWithPGJsonb) {
  const googlesql::Function* function =
      functions_[kPGArrayUpperFunctionName].get();
  googlesql::FunctionSignature signature(
      googlesql::types::Int64Type(),
      {postgres_translator::spangres::datatypes::GetPgJsonbArrayType(),
       googlesql::types::Int64Type()},
      /*context_ptr=*/nullptr);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (function->GetFunctionEvaluatorFactory())(signature));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto pg_arena, interfaces::CreatePGArena(nullptr));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto pg_jsonb_array,
      googlesql::Value::MakeArray(
          spangres::datatypes::GetPgJsonbArrayType(),
          {spangres::datatypes::CreatePgJsonbValue("{\"a\": \"b\"}").value(),
           spangres::datatypes::CreatePgJsonbValue("null").value(),
           spangres::datatypes::CreatePgJsonbValue("[1, 2, 3]").value()}));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::Int64(1)})),
              IsOkAndHolds(googlesql::values::Int64(3)));
}

TEST_F(EmulatorFunctionsTest, PGJsonbObjectKeysFunction) {
  const googlesql::Function* function =
      functions_[kPGJsonbObjectKeysFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value pg_jsonb_object,
                       CreatePgJsonbValueWithMemoryContext("{\"a\": \"b\"}"));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan({pg_jsonb_object})),
              IsOkAndHolds(googlesql::values::StringArray({"a"})));
}

TEST_F(EmulatorFunctionsTest, PGJsonbMutatorFunctions) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::Value pg_jsonb_array,
      googlesql::Value::MakeArray(
          googlesql::types::StringArrayType(),
          {googlesql::values::String("a"), googlesql::values::String("b")}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::Value pg_jsonb_object,
      CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": 2}"));

  googlesql::Value jsonb_typed_null =
      googlesql::values::Null(spangres::datatypes::GetPgJsonbType());

  const googlesql::Function* jsonb_delete_function =
      functions_[kPGJsonbDeleteFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (jsonb_delete_function->GetFunctionEvaluatorFactory())(
                           jsonb_delete_function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::String("a")})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::String("c")})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"a\", \"b\"]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::Int64(0)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"b\"]")));
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      googlesql::Value string_array,
      googlesql::Value::MakeArray(
          googlesql::types::StringArrayType(),
          {googlesql::values::String("a"), googlesql::values::String("b")}));

  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({pg_jsonb_array, string_array})),
      StatusIs(
          absl::StatusCode::kUnimplemented,
          HasSubstr("jsonb_delete(jsonb, array) is currently not supported")));

  const googlesql::Function* jsonb_delete_path_function =
      functions_[kPGJsonbDeletePathFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      evaluator_, (jsonb_delete_path_function->GetFunctionEvaluatorFactory())(
                      jsonb_delete_path_function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"0"})})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::StringArray({"3"})})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"a\", \"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::StringArray({"a"})})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("path element at position 1 is not an integer: \"a\"")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_object, googlesql::values::StringArray({"a"})})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("{\"b\": 2}")));

  const googlesql::Function* jsonb_set_function =
      functions_[kPGJsonbSetFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (jsonb_set_function->GetFunctionEvaluatorFactory())(
                           jsonb_set_function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"0"}),
                   pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "[[\"a\", \"b\"], \"b\"]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"-4"}),
                   pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "[[\"a\", \"b\"], \"a\", \"b\"]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_object, googlesql::values::StringArray({"a"}),
                   pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "{\"a\": [\"a\", \"b\"], \"b\": 2}")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_object, googlesql::values::StringArray({"a"}),
                   pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "{\"a\": [\"a\", \"b\"], \"b\": 2}")));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_object, googlesql::values::StringArray({"c"}),
                   pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "{\"a\": 1, \"b\": 2, \"c\": [\"a\", \"b\"]}")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_object, googlesql::values::StringArray({"c", "0"}),
                   pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "{\"a\": 1, \"b\": 2}")));

  const googlesql::Function* jsonb_set_lax_function =
      functions_[kPGJsonbSetLaxFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (jsonb_set_lax_function->GetFunctionEvaluatorFactory())(
                           jsonb_set_lax_function->signatures().front()));

  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::StringArray({"0"}),
           jsonb_typed_null, googlesql::values::Bool(true),
           googlesql::values::String("use_json_null")})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[null, \"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::StringArray({"0"}),
           jsonb_typed_null, googlesql::values::Bool(true),
           googlesql::values::String("return_target")})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"a\", \"b\"]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"0"}),
                   jsonb_typed_null, googlesql::values::Bool(true),
                   googlesql::values::String("delete_key")})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"b\"]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"0"}),
                   jsonb_typed_null, googlesql::values::Bool(true),
                   googlesql::values::String("raise_exception")})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("JSON value must not be null")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"0"}),
                   jsonb_typed_null, googlesql::values::Bool(true),
                   googlesql::values::String("invalid_value")})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("null_value_treatment must be")));

  const googlesql::Function* jsonb_concat_function =
      functions_[kPGJsonbConcatFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (jsonb_concat_function->GetFunctionEvaluatorFactory())(
                           jsonb_concat_function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan({pg_jsonb_array, pg_jsonb_array})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "[\"a\", \"b\", \"a\", \"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({pg_jsonb_object, pg_jsonb_object})),
      IsOkAndHolds(
          *CreatePgJsonbValueWithMemoryContext("{\"a\": 1, \"b\": 2}")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({pg_jsonb_array, pg_jsonb_object})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
          "[\"a\", \"b\", {\"a\": 1, \"b\": 2}]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "[\"a\", \"b\", true]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_object, googlesql::values::Bool(true)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "[{\"a\": 1, \"b\": 2}, true]")));

  const googlesql::Function* jsonb_insert_function =
      functions_[kPGJsonbInsertFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (jsonb_insert_function->GetFunctionEvaluatorFactory())(
                           jsonb_insert_function->signatures().front()));

  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::StringArray({"0"}),
           googlesql::values::String("\"c\""), googlesql::values::Bool(true)})),
      IsOkAndHolds(
          *CreatePgJsonbValueWithMemoryContext("[\"a\", \"c\", \"b\"]")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_jsonb_array, googlesql::values::StringArray({"0"}),
                   googlesql::values::String("\"c\""),
                   googlesql::values::Bool(false)})),
              IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
                  "[\"c\", \"a\", \"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_array, googlesql::values::StringArray({"-4"}),
           googlesql::values::String("\"c\""), googlesql::values::Bool(true)})),
      IsOkAndHolds(
          *CreatePgJsonbValueWithMemoryContext("[\"c\", \"a\", \"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_object, googlesql::values::StringArray({"c"}),
           googlesql::values::Int64(3), googlesql::values::Bool(true)})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
          "{\"a\": 1, \"b\": 2, \"c\": 3}")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {pg_jsonb_object, googlesql::values::StringArray({"a"}),
           googlesql::values::Int64(3), googlesql::values::Bool(false)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("cannot replace existing key")));

  const googlesql::Function* jsonb_strip_nulls_function =
      functions_[kPGJsonbStripNullsFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      evaluator_, (jsonb_strip_nulls_function->GetFunctionEvaluatorFactory())(
                      jsonb_strip_nulls_function->signatures().front()));

  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({pg_jsonb_array})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext("[\"a\", \"b\"]")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({*CreatePgJsonbValueWithMemoryContext(
          "{\"a\": null, \"b\": 2, \"c\": [null, 2], \"d\": {\"e\": null, "
          "\"f\": 2}}")})),
      IsOkAndHolds(*CreatePgJsonbValueWithMemoryContext(
          "{\"b\": 2, \"c\": [null, 2], \"d\": {\"f\": 2}}")));
}

// Tested separately from the parameterized tests as we need a memory context
// before creating a PG.NUMERIC value.
TEST_F(EmulatorFunctionsTest, ArrayUpperWithPGNumeric) {
  const googlesql::Function* function =
      functions_[kPGArrayUpperFunctionName].get();
  googlesql::FunctionSignature signature(
      googlesql::types::Int64Type(),
      {postgres_translator::spangres::datatypes::GetPgNumericArrayType(),
       googlesql::types::Int64Type()},
      /*context_ptr=*/nullptr);
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_,
                       (function->GetFunctionEvaluatorFactory())(signature));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto pg_arena, interfaces::CreatePGArena(nullptr));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      auto pg_numeric_array,
      googlesql::Value::MakeArray(
          spangres::datatypes::GetPgNumericArrayType(),
          {spangres::datatypes::CreatePgNumericValue("1.3").value(),
           spangres::datatypes::CreatePgNumericValue("0.1").value()}));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {pg_numeric_array, googlesql::values::Int64(1)})),
              IsOkAndHolds(googlesql::values::Int64(2)));
}

TEST_F(EmulatorFunctionsTest,
       ArrayUpperReturnsErrorWhenDimensionIsGreaterThanOne) {
  const googlesql::Function* function =
      functions_[kPGArrayUpperFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({googlesql::values::StringArray({"a", "b"}),
                               googlesql::values::Int64(2)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("multi-dimensional arrays are not supported")));
}

TEST_F(EmulatorFunctionsTest, ToCharReturnsErrorWhenTypeUnsupported) {
  const googlesql::Function* function = functions_[kPGToCharFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("3.14").value(),
                   googlesql::values::String("999")})),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("to_char(PG.JSONB, text)")));
}

TEST_F(EmulatorFunctionsTest, AddReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kGoogleSQLAddFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, AddReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kGoogleSQLAddFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("1")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-1")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, SubtractReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kGoogleSQLSubtractFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, SubtractReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kGoogleSQLSubtractFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-1")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("1")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, MultiplyReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kGoogleSQLMultiplyFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, MultiplyReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kGoogleSQLMultiplyFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("2.0")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-2.0")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivideReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kGoogleSQLDivideFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, DivideReturnsErrorWhenDividingByZero) {
  const googlesql::Function* function =
      functions_[kGoogleSQLDivideFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.00")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.0")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivideReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kGoogleSQLDivideFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.5")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-0.5")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivTruncReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kGoogleSQLDivTruncFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, DivTruncReturnsErrorWhenDividingByZero) {
  const googlesql::Function* function =
      functions_[kGoogleSQLDivTruncFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.00")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.0")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, DivTruncReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kGoogleSQLDivTruncFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MaxNumericString()),
                   *CreatePgNumericValueWithMemoryContext("0.5")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext(MinNumericString()),
                   *CreatePgNumericValueWithMemoryContext("-0.5")})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, UminusReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kGoogleSQLUminusFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, FloatAddReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGFloatAddFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, FloatAddReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kPGFloatAddFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kFloatMaxValue, kFloatMaxValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({kFloatLowestValue, kFloatLowestValue})),
      googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest,
       FloatSubtractReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGFloatSubtractFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, FloatSubtractReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kPGFloatSubtractFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatLowestValue, kFloatMaxValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatMaxValue, kFloatLowestValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest,
       FloatMultiplyReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGFloatMultiplyFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, FloatMultiplyReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kPGFloatMultiplyFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kFloatMaxValue, kFloatMaxValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatLowestValue, kFloatLowestValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, FloatDivideReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGFloatDivideFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, FloatDivideReturnsErrorWhenDividingByZero) {
  const googlesql::Function* function =
      functions_[kPGFloatDivideFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatMaxValue, googlesql::Value::Float(0.0f)})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatMinValue, googlesql::Value::Float(0.0f)})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest, FloatDivideReturnsErrorWhenResultIsOverflow) {
  const googlesql::Function* function =
      functions_[kPGFloatDivideFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatMaxValue, kFloatMinValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kFloatLowestValue, kFloatMinValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest,
       CastOidToInt64ReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGCastFromOidFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(std::vector<googlesql::Value>())),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Too many arguments.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGOidMinValue, kPGOidMaxValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Invalid argument.
  std::vector<googlesql::Value> args_invalid = {googlesql::Value::Int64(0)};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args_invalid)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest,
       CastOidToStringReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGCastFromOidFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(std::vector<googlesql::Value>())),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Too many arguments.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGOidMinValue, kPGOidMaxValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Invalid argument.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::Int64(0)})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, CastToOidReturnsErrorWhenArgumentAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGCastToOidFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(std::vector<googlesql::Value>())),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Too many arguments.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kInt64Value, kNullStringValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Invalid argument.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kDoubleValue})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("invalid")})),
      googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
  // Argument too small.
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::Int64(
          static_cast<int64_t>(std::numeric_limits<uint32_t>::min()) - 1)})),
      googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(absl::StrCat(
          static_cast<int64_t>(std::numeric_limits<int32_t>::min()) - 1))})),
      googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
  // Argument too large.
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::Int64(
          static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 1)})),
      googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(absl::StrCat(
          static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 1))})),
      googlesql_base::testing::StatusIs(absl::StatusCode::kOutOfRange));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToDoubleReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGCastNumericToDoubleFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Value too small to be represented by a double.
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {*CreatePgNumericValueWithMemoryContext("-1.79769313486232e+308")})),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Cannot cast to double")));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToFloatReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGCastNumericToFloatFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  // Value too small to be represented by a float.
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {*CreatePgNumericValueWithMemoryContext("-3.4028238e+38")})),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("Cannot cast to float")));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToStringReturnsErrorWhenArgumentsAreInvalid) {
  const googlesql::Function* function =
      functions_[kPGCastToStringFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  // Insufficient arguments.
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInvalidArgumentSizeError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kPGNumericValue, kInt64Value, kInt64Value, kInt64Value})),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithNullPrecisionScaleError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
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
          {googlesql::Value::String("1.0"), kNullInt64Value})),
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
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Out of range precision and scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              googlesql::Value::Int64(2),
                                              googlesql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("numeric field overflow")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              googlesql::Value::Int64(1001),
                                              googlesql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              googlesql::Value::Int64(-1),
                                              googlesql::Value::Int64(-2)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPGNumericValue,
                                              googlesql::Value::Int64(-1),
                                              googlesql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("1.0"),
                                              googlesql::Value::Int64(2),
                                              googlesql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("numeric field overflow")));

  // Test that out-of-range precision and scale is checked first when value is
  // special (NaN/NULL).
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullPGNumericValue,
                                              googlesql::Value::Int64(2),
                                              googlesql::Value::Int64(3)})),
              googlesql_base::testing::IsOkAndHolds(kNullPGNumericValue));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullDoubleValue,
                                              googlesql::Value::Int64(2),
                                              googlesql::Value::Int64(-1)})),
              googlesql_base::testing::IsOkAndHolds(kNullPGNumericValue));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kPosInfDoubleValue,
                                              googlesql::Value::Int64(2),
                                              googlesql::Value::Int64(-1)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNegInfDoubleValue,
                                              googlesql::Value::Int64(1001),
                                              googlesql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullInt64Value,
                                              googlesql::Value::Int64(-1),
                                              googlesql::Value::Int64(-2)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullStringValue,
                                              googlesql::Value::Int64(-1),
                                              googlesql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNullStringValue,
                                              googlesql::Value::Int64(2),
                                              googlesql::Value::Int64(3)})),
              googlesql_base::testing::IsOkAndHolds(kNullPGNumericValue));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("-Inf"),
                                              googlesql::Value::Int64(10001),
                                              googlesql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithTooSmallPrecisionError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::Int64(1000), googlesql::Value::Int64(2)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::Double(99), kInt64Value})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext("1234.987654321"),
                   googlesql::Value::Int64(5), googlesql::Value::Int64(2)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String("1234.987654321"),
                   googlesql::Value::Int64(3), googlesql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String("-1e1000"),
                   googlesql::Value::Int64(3), googlesql::Value::Int64(3)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("must round to an absolute value less than")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInfinityDoubleError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
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
                                              googlesql::Value::Int64(1000),
                                              googlesql::Value::Int64(100)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({kPosInfDoubleValue, kInt64Value})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Cannot cast infinity to PG.NUMERIC")));

  // Infinity double value with out of range precision and scale: expect error
  // regarding invalid precision/scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNegInfDoubleValue,
                                              googlesql::Value::Int64(100),
                                              googlesql::Value::Int64(1000)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kPosInfDoubleValue, googlesql::Value::Int64(1001)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {kPosInfDoubleValue, googlesql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({kNegInfDoubleValue,
                                              googlesql::Value::Int64(100),
                                              googlesql::Value::Int64(-1)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Cannot cast infinity to PG.NUMERIC")));

  // Infinity double value with null precision and scale: expect error regarding
  // null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {kPosInfDoubleValue, googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({kNegInfDoubleValue, googlesql::Value::Int64(100),
                               googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

// The following test is known to produce different error messages compared to
// Spanner PROD.
TEST_F(EmulatorFunctionsTest,
       DISABLED_CastToNumericWithInfinityStringError_KnownProdMismatch) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Infinity string value with or without space padding with valid precision
  // and scale: expect same error as when there are no precision and scale

  // Emulator returns error "[ERROR] numeric field overflow Detail: A field with
  // precision 1000, scale 100 cannot hold an infinite value."
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("-infinity"),
                                      googlesql::Value::Int64(1000),
                                      googlesql::Value::Int64(100)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1, scale 0 cannot hold an infinite value."
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String(" INFinity "), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1000, scale 100 cannot hold an infinite value."
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+INFINITY "),
                                      googlesql::Value::Int64(1000),
                                      googlesql::Value::Int64(100)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1, scale 0 cannot hold an infinite value."
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String("-iNf"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1000, scale 100 cannot hold an infinite value."
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("inf"),
                                              googlesql::Value::Int64(1000),
                                              googlesql::Value::Int64(100)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));

  // Emulator returns error "numeric field overflow Detail: A field with
  // precision 1, scale 0 cannot hold an infinite value.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String(" +INF"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInfinityStringError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Infinity string value with or without space padding
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("-infinity")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(" INFinity ")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+INFINITY ")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("-iNf")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("inf")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(" +INF")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));

  // Infinity string value with or without space padding with invalid precision
  // and scale: expect error regarding invalid precision/scale
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("-infinity"),
                                      googlesql::Value::Int64(100),
                                      googlesql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kOutOfRange,
               HasSubstr("numeric field overflow")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(" INFinity "),
                                      googlesql::Value::Int64(1001)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+INFINITY "),
                                      googlesql::Value::Int64(0)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("-iNf"),
                                              googlesql::Value::Int64(100),
                                              googlesql::Value::Int64(1000)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("numeric field overflow")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("inf"),
                                              googlesql::Value::Int64(1001)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String(" +INF"),
                                              googlesql::Value::Int64(0)})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));

  // Infinity string value with or without space padding with null precision and
  // scale: expect error regarding null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("-infinity"),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(" INFinity "),
                                      googlesql::Value::Int64(100),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+INFINITY "),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("-iNf"),
                                      googlesql::Value::Int64(100),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String("inf"), googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(" +INF"),
                                      googlesql::Value::Int64(100),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithInvalidStringError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Invalid string
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("invalid")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("- iNf")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+ Infinity")})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("invalid input syntax for type numeric")));

  // Invalid string with valid precision and scale: expect same error as
  // when there are no precision and scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String("invalid"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("- iNf"),
                                              googlesql::Value::Int64(1000),
                                              kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String("+ Infinity"), kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type numeric")));

  // Invalid string with invalid precision and scale: expect error regarding
  // invalid precision/scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::String("NULL"),
                                              googlesql::Value::Int64(1000000),
                                              kInt64Value})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("Inf"),
                                      googlesql::Value::Int64(1000),
                                      googlesql::Value::Int64(10000)})),
      StatusIs(absl::StatusCode::kInvalidArgument, HasSubstr("NUMERIC scale")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+ Infinity"),
                                      googlesql::Value::Int64(-1)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));

  // Invalid string with null precision and scale: expect error regarding null
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({googlesql::Value::String("NULL"),
                               googlesql::Value::NullInt64(), kInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("Inf"),
                                      googlesql::Value::Int64(1000),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("+ Infinity"),
                                      googlesql::Value::NullInt64()})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

// The following test is known to produce different errors/results compared to
// Spanner PROD.
TEST_F(EmulatorFunctionsTest,
       DISABLED_CastToNumericWithTooLargeStringExponentError_Mismatch) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Emulator returns "numeric field overflow Detail: A field with precision
  // 1000, scale 0 must round to an absolute value less than 10^1000"
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("1e1000000"),
                                      googlesql::Value::Int64(1000)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("exponent that is too large")));

  // Emulator returns 0
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String("1e-100000"), googlesql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("fractional component of NUMERIC")));

  // Emulator returns 0
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String("1e-10000"), googlesql::Value::Int64(3)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("must round to an absolute value less than")));
}

TEST_F(EmulatorFunctionsTest,
       FAILEDCastToNumericWithTooLargeStringExponentError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Exponent values are too large for string to be represented as a numeric
  // value but precision and scale are invalid: expect error regarding invalid
  // precision/scale
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("1e1000000"),
                                      googlesql::Value::Int64(1001)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String("1e-10000"), googlesql::Value::Int64(0)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));

  // Exponent values are too large for string to be represented as a numeric
  // value but precision and scale are null: expect error regarding null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String("1e1000000"), kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(
          absl::MakeConstSpan({googlesql::Value::String("1e-100000"),
                               googlesql::Value::Int64(3), kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String("1e-10000"),
                                      kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

// The following test is known to produce different error messages compared to
// Spanner PROD.
TEST_F(EmulatorFunctionsTest,
       DISABLED_CastToNumericWithTooLargeStringValueError_Mismatch) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Values are too large to be represented as a numeric value
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(
          std::string(kMaxPGNumericWholeDigits + 1, '9'))})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value: whole component of NUMERIC")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String(std::string(147466, '9'))})),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid NUMERIC value")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(absl::StrCat(
          "0.", std::string(kMaxPGNumericFractionalDigits + 1, '9')))})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("Invalid NUMERIC value: fractional component of NUMERIC")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(
          absl::StrCat(std::string(131073, '9'), ".",
                       std::string(kMaxPGNumericFractionalDigits + 1, '9')))})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("Invalid NUMERIC value")));
}

TEST_F(EmulatorFunctionsTest, CastToNumericWithTooLargeStringValueError) {
  const googlesql::Function* function =
      functions_[kPGCastToNumericFunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  // Values are too large to be represented as a numeric value but precision and
  // scale are invalid: expect error regarding invalid precision/scale
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String(
                       std::string(kMaxPGNumericWholeDigits + 1, '9')),
                   kInt64Value, googlesql::Value::Int64(1000)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("numeric field overflow")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String(std::string(
               kMaxPGNumericWholeDigits + kMaxPGNumericFractionalDigits + 1,
               '9')),
           googlesql::Value::Int64(1001)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String(absl::StrCat(
               "0.", std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           googlesql::Value::Int64(0)})),
      StatusIs(absl::StatusCode::kInvalidArgument,
               HasSubstr("NUMERIC precision")));
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {googlesql::Value::String(absl::StrCat(
                       std::string(kMaxPGNumericWholeDigits + 1, '9'), ".",
                       std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
                   kInt64Value, googlesql::Value::Int64(-1)})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("numeric field overflow")));

  // Values are too large to be represented as a numeric value but precision and
  // scale are null: expect error regarding null
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan({googlesql::Value::String(std::string(
                                          kMaxPGNumericWholeDigits + 1, '9')),
                                      kNullInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String(std::string(
               kMaxPGNumericWholeDigits + kMaxPGNumericFractionalDigits + 1,
               '9')),
           kNullInt64Value, googlesql::Value::Int64(0)})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String(absl::StrCat(
               "0.", std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           kInt64Value, kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::Value::String(absl::StrCat(
               std::string(kMaxPGNumericWholeDigits + 1, '9'), ".",
               std::string(kMaxPGNumericFractionalDigits + 1, '9'))),
           kNullInt64Value})),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr("type modifiers must be simple constants or identifiers")));
}

TEST_F(EmulatorFunctionsTest, CastNumericToInt64ReturnsErrorForNaN) {
  const googlesql::Function* function =
      functions_[kPGCastNumericToInt64FunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                       function->signatures().front()));

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgNumericValueWithMemoryContext("NaN")})),
              StatusIs(absl::StatusCode::kOutOfRange,
                       HasSubstr("cannot convert NaN to bigint")));
}

TEST_F(EmulatorFunctionsTest,
       CastNumericToInt64ReturnsErrorForOverflowAndUnderflow) {
  const googlesql::Function* function =
      functions_[kPGCastNumericToInt64FunctionName].get();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
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

class EvalToJsonbTest : public EmulatorFunctionsTest {
 protected:
  const std::string kMaxPgJsonbNumericWholeDigitStr = std::string(
      spangres::datatypes::common::kMaxPGJSONBNumericWholeDigits, '9');
  const std::string kMaxPgJsonbNumericFractionalDigitStr = std::string(
      spangres::datatypes::common::kMaxPGJSONBNumericFractionalDigits, '9');
  const std::string kMaxPgJsonbNumericDigitStr =
      std::string(kMaxPgJsonbNumericWholeDigitStr + "." +
                  kMaxPgJsonbNumericFractionalDigitStr);

  void SetUp() override {
    const googlesql::Function* function =
        functions_[kPGToJsonbFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

MATCHER_P(NullToJsonb, input, "") {
  EXPECT_THAT(arg(absl::MakeConstSpan({input})),
              googlesql_base::testing::IsOkAndHolds(googlesql::values::Null(
                  spangres::datatypes::GetPgJsonbType())));
  return true;
}

TEST_F(EvalToJsonbTest, NullValueInput) {
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::NullBool()));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::NullInt64()));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::NullDouble()));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::NullDate()));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::NullTimestamp()));
  EXPECT_THAT(evaluator_, NullToJsonb(kNullStringValue));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::NullBytes()));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::Null(
                              spangres::datatypes::GetPgJsonbType())));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::Null(
                              spangres::datatypes::GetPgNumericType())));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::Null(
                              googlesql::types::StringArrayType())));
  EXPECT_THAT(
      evaluator_,
      NullToJsonb(googlesql::values::Null(googlesql::types::Int64ArrayType())));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::Null(
                              googlesql::types::DoubleArrayType())));
  EXPECT_THAT(
      evaluator_,
      NullToJsonb(googlesql::values::Null(googlesql::types::BytesArrayType())));

  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::Value::NullInterval()));
  EXPECT_THAT(evaluator_, NullToJsonb(googlesql::values::Null(
                              googlesql::types::IntervalArrayType())));
  EXPECT_THAT(evaluator_, NullToJsonb(kNullUuidValue));
  EXPECT_THAT(evaluator_, NullToJsonb(kNullUuidArrayValue));
}

MATCHER_P2(TimestampToJsonb, input, expected_string, "") {
  absl::Time timestamp;
  absl::Status status = googlesql::functions::ConvertStringToTimestamp(
      input, absl::UTCTimeZone(),
      googlesql::functions::TimestampScale::kNanoseconds,
      /*allow_tz_in_str=*/true, &timestamp);
  if (!status.ok()) {
    *result_listener << "\nFailed to convert string to timestamp: " << status;
    return false;
  }
  EXPECT_THAT(
      arg(absl::MakeConstSpan({googlesql::values::Timestamp(timestamp)})),
      googlesql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, TimestampInput) {
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01Z",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01.0Z",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01.1Z",
                                           "\"1986-01-01T00:00:01.1+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01.01Z",
                                           "\"1986-01-01T00:00:01.01+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonb("1986-01-01T00:00:01.001Z",
                               "\"1986-01-01T00:00:01.001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonb("1986-01-01T00:00:01.0001Z",
                               "\"1986-01-01T00:00:01.0001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonb("1986-01-01T00:00:01.00001Z",
                               "\"1986-01-01T00:00:01.00001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonb("1986-01-01T00:00:01.000100Z",
                               "\"1986-01-01T00:00:01.0001+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonb("1986-01-01T00:00:01.000101Z",
                               "\"1986-01-01T00:00:01.000101+00:00\""));
  EXPECT_THAT(evaluator_,
              TimestampToJsonb("1986-01-01T00:00:01.001001100Z",
                               "\"1986-01-01T00:00:01.001001+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01 00:00:01Z",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01 00:00:01",
                                           "\"1986-01-01T00:00:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01+5:30",
                                           "\"1985-12-31T18:30:01+00:00\""));
  EXPECT_THAT(evaluator_, TimestampToJsonb("1986-01-01T00:00:01+5:30",
                                           "\"1985-12-31T18:30:01+00:00\""));
}

MATCHER_P2(BoolToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({googlesql::values::Bool(input)})),
      googlesql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, BoolInput) {
  EXPECT_THAT(evaluator_, BoolToJsonb(true, "true"));
  EXPECT_THAT(evaluator_, BoolToJsonb(false, "false"));
}

MATCHER_P2(Int64ToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({googlesql::values::Int64(input)})),
      googlesql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, Int64Input) {
  EXPECT_THAT(evaluator_, Int64ToJsonb(10, "10"));
  EXPECT_THAT(evaluator_, Int64ToJsonb(std::numeric_limits<int64_t>::max(),
                                       "9223372036854775807"));
  EXPECT_THAT(evaluator_, Int64ToJsonb(std::numeric_limits<int64_t>::min(),
                                       "-9223372036854775808"));
}

MATCHER_P2(DoubleToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({googlesql::values::Double(input)})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, DoubleInput) {
  EXPECT_THAT(evaluator_, DoubleToJsonb(0.0, "0"));
  EXPECT_THAT(evaluator_, DoubleToJsonb(3.14, "3.14"));
  EXPECT_THAT(evaluator_, DoubleToJsonb(3.14000000, "3.14"));
  EXPECT_THAT(evaluator_,
              DoubleToJsonb(3.14567897543568997764, "3.14567897543569"));
  EXPECT_THAT(evaluator_,
              DoubleToJsonb(3.14567897543562524102, "3.1456789754356254"));
  EXPECT_THAT(evaluator_, DoubleToJsonb(-33.1234954500, "-33.12349545"));
  EXPECT_THAT(evaluator_, DoubleToJsonb(0.0000134200, "0.00001342"));
  EXPECT_THAT(evaluator_, DoubleToJsonb(0.0000000000000000000100000000000000001,
                                        "0.00000000000000000001"));
  EXPECT_THAT(evaluator_,
              DoubleToJsonb(0.000000000000000000010000000000000001,
                            "0.000000000000000000010000000000000001"));
  EXPECT_THAT(evaluator_, DoubleToJsonb(NAN, "\"NaN\""));
  EXPECT_THAT(evaluator_, DoubleToJsonb(-INFINITY, "\"-Infinity\""));
  EXPECT_THAT(evaluator_, DoubleToJsonb(+INFINITY, "\"Infinity\""));
  EXPECT_THAT(evaluator_, DoubleToJsonb(std::numeric_limits<double>::max(),
                                        absl::StrCat("17976931348623157",
                                                     std::string(292, '0'))));
  EXPECT_THAT(evaluator_,
              DoubleToJsonb(std::numeric_limits<double>::min(),
                            absl::StrCat("0.", std::string(307, '0'),
                                         "22250738585072014")));
  EXPECT_THAT(evaluator_, DoubleToJsonb(std::numeric_limits<double>::lowest(),
                                        absl::StrCat("-17976931348623157",
                                                     std::string(292, '0'))));
}

MATCHER_P2(DateToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan(
          {googlesql::values::Date(input - absl::CivilDay(1970, 1, 1))})),
      googlesql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, DateInput) {
  EXPECT_THAT(evaluator_,
              DateToJsonb(absl::CivilDay(1970, 1, 1), "\"1970-01-01\""));
  EXPECT_THAT(evaluator_,
              DateToJsonb(absl::CivilDay(1971, 1, 1), "\"1971-01-01\""));
  EXPECT_THAT(evaluator_,
              DateToJsonb(absl::CivilDay(1971, 1, 1), "\"1971-01-01\""));
}

MATCHER_P2(StringToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({googlesql::values::String(input)})),
      googlesql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, StringInput) {
  EXPECT_THAT(evaluator_, StringToJsonb("hello", "\"hello\""));
  EXPECT_THAT(evaluator_,
              StringToJsonb("special characters(', \", \r, \n)",
                            "\"special characters(', \\\", \\r, \\n)\""));
  EXPECT_THAT(evaluator_,
              StringToJsonb("non ascii characters(ß, Д, \u0001)",
                            "\"non ascii characters(ß, Д, \\u0001)\""));
  EXPECT_THAT(evaluator_, StringToJsonb("", "\"\""));
  EXPECT_THAT(evaluator_, StringToJsonb("例子", R"("例子")"));
  EXPECT_THAT(evaluator_,
              StringToJsonb("{\"a\":      1}", "\"{\\\"a\\\":      1}\""));
}

MATCHER_P2(BytesToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({googlesql::values::Bytes(input)})),
      googlesql_base::testing::IsOkAndHolds(
          spangres::datatypes::CreatePgJsonbValue(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, BytesInput) {
  EXPECT_THAT(evaluator_, BytesToJsonb(" ", "\"\\\\x20\""));
  EXPECT_THAT(evaluator_, BytesToJsonb("hello", "\"\\\\x68656c6c6f\""));
  EXPECT_THAT(evaluator_, BytesToJsonb("special characters(', \\\", \\r, \\n)",
                                       "\"\\\\x7370656369616c206368617261637465"
                                       "727328272c205c222c205c722c205c6e29\""));
  EXPECT_THAT(evaluator_, BytesToJsonb("non ascii characters(ß, Д, \u0001)",
                                       "\"\\\\x6e6f6e20617363696920636861726163"
                                       "7465727328c39f2c20d0942c200129\""));
  EXPECT_THAT(evaluator_, BytesToJsonb("", "\"\\\\x\""));
  EXPECT_THAT(evaluator_, BytesToJsonb("例子", "\"\\\\xe4be8be5ad90\""));
}

MATCHER_P2(JsonbToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan(
          {CreatePgJsonbValueWithMemoryContext(input).value()})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, JsonbInput) {
  EXPECT_THAT(evaluator_, JsonbToJsonb(R"({"a":1.0, "b" : null})",
                                       R"({"a": 1.0, "b": null})"));
  EXPECT_THAT(evaluator_,
              JsonbToJsonb(R"({"a"  :[ "b" , "c" ]})", R"({"a": ["b", "c"]})"));
  EXPECT_THAT(evaluator_, JsonbToJsonb("  1.0 ", "1.0"));
  EXPECT_THAT(evaluator_, JsonbToJsonb(R"(   "abcd"  )", R"("abcd")"));
  EXPECT_THAT(evaluator_, JsonbToJsonb("[1,2,  3,   4]", "[1, 2, 3, 4]"));

  // Test normalization of PG.NUMERIC and PG.JSONB
  EXPECT_THAT(evaluator_,
              JsonbToJsonb(R"({"a":[2],"a":[1]})", R"({"a": [1]})"));
  EXPECT_THAT(evaluator_, JsonbToJsonb(R"({"b":[1e0],"a":[2]})",
                                       R"({"a": [2], "b": [1]})"));
}

MATCHER_P2(NumericToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan(
          {CreatePgNumericValueWithMemoryContext(input).value()})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, NumericInput) {
  EXPECT_THAT(evaluator_, NumericToJsonb("0  ", "0"));
  EXPECT_THAT(evaluator_,
              NumericToJsonb(absl::StrCat(" -", kMaxPgJsonbNumericDigitStr),
                             absl::StrCat("-", kMaxPgJsonbNumericDigitStr)));
  EXPECT_THAT(evaluator_, NumericToJsonb(kMaxPgJsonbNumericDigitStr,
                                         kMaxPgJsonbNumericDigitStr));
  EXPECT_THAT(evaluator_,
              NumericToJsonb(" 0.0000000001230 ", "0.0000000001230"));
  EXPECT_THAT(evaluator_, NumericToJsonb("  NaN", "\"NaN\""));
}

MATCHER_P2(OidToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({CreatePgOidValue(input).value()})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, OidInput) {
  EXPECT_THAT(evaluator_, OidToJsonb(123456, "123456"));
  EXPECT_THAT(evaluator_, OidToJsonb(std::numeric_limits<uint32_t>::max(),
                                     "4294967295"));
  EXPECT_THAT(evaluator_, OidToJsonb(0, "0"));
}

MATCHER_P2(IntervalToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({CreateIntervalValue(input)})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, IntervalInput) {
  EXPECT_THAT(evaluator_, IntervalToJsonb("0-0 0 0:0:0", R"("00:00:00")"));
  EXPECT_THAT(evaluator_,
              IntervalToJsonb("1-2 4 5:6:7.123456789",
                              "\"1 year 2 mons 4 days 05:06:07.123457\""));
  EXPECT_THAT(evaluator_,
              IntervalToJsonb("1-2 -4 5:6:7.123456789",
                              "\"1 year 2 mons -4 days +05:06:07.123457\""));
  EXPECT_THAT(evaluator_, IntervalToJsonb("12-0 0 0:0:0.0", "\"12 years\""));
  EXPECT_THAT(evaluator_, IntervalToJsonb("0-5 -9 -1:2:3.001",
                                          "\"5 mons -9 days -01:02:03.001\""));
  EXPECT_THAT(evaluator_,
              IntervalToJsonb("-1-5 8 -12:78:24",
                              "\"-1 years -5 mons +8 days -13:18:24\""));
}

MATCHER_P2(UuidToJsonb, input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({CreateUuidValue(input)})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, UuidInput) {
  EXPECT_THAT(evaluator_,
              UuidToJsonb("00000000-0000-0000-0000-000000000000",
                          R"("00000000-0000-0000-0000-000000000000")"));
  EXPECT_THAT(evaluator_,
              UuidToJsonb("11111111-1111-1111-1111-111111111111",
                          R"("11111111-1111-1111-1111-111111111111")"));
  EXPECT_THAT(evaluator_,
              UuidToJsonb("9a31411b-caca-4ff1-86e9-39fbd2bc3f39",
                          R"("9a31411b-caca-4ff1-86e9-39fbd2bc3f39")"));
  EXPECT_THAT(evaluator_,
              UuidToJsonb("ffffffff-ffff-ffff-ffff-ffffffffffff",
                          R"("ffffffff-ffff-ffff-ffff-ffffffffffff")"));
}

MATCHER_P2(ArrayToJsonb, array_input, expected_string, "") {
  EXPECT_THAT(
      arg(absl::MakeConstSpan({array_input})),
      googlesql_base::testing::IsOkAndHolds(
          CreatePgJsonbValueWithMemoryContext(expected_string).value()));
  return true;
}

TEST_F(EvalToJsonbTest, ArrayInput) {
  EXPECT_THAT(evaluator_,
              ArrayToJsonb(googlesql::values::Int64Array({1, 9007199254740993}),
                           "[1, 9007199254740993]"));
  EXPECT_THAT(
      evaluator_,
      ArrayToJsonb(googlesql::Value::MakeArray(
                       googlesql::types::StringArrayType(),
                       {googlesql::values::String("a"), kNullStringValue})
                       .value(),
                   "[\"a\", null]"));
  EXPECT_THAT(evaluator_,
              ArrayToJsonb(googlesql::values::BytesArray({" ", "ab"}),
                           "[\"\\\\x20\", \"\\\\x6162\"]"));
  EXPECT_THAT(evaluator_,
              ArrayToJsonb(googlesql::Value::MakeArray(
                               spangres::datatypes::GetPgNumericArrayType(),
                               {CreatePgNumericValueWithMemoryContext(
                                    absl::StrCat(kMaxPgJsonbNumericDigitStr))
                                    .value()})
                               .value(),
                           absl::StrCat("[", kMaxPgJsonbNumericDigitStr, "]")));
  EXPECT_THAT(evaluator_,
              ArrayToJsonb(googlesql::values::DoubleArray({}), "[]"));

  EXPECT_THAT(evaluator_,
              ArrayToJsonb(googlesql::Value::MakeArray(
                               spangres::datatypes::GetPgOidArrayType(),
                            {CreatePgOidValue(0).value(),
                             CreatePgOidValue(
                                 std::numeric_limits<uint32_t>::max()).value()
                            }).value(), "[0, 4294967295]"));
}

class EvalJsonbSubscriptText : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const googlesql::Function* function =
        functions_[kPGJsonbSubscriptTextFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

MATCHER_P3(JsonbArrayElementText, jsonb, element_index, expected_string_value,
           "") {
  EXPECT_THAT(arg(absl::MakeConstSpan(
                  {jsonb.value(), googlesql::values::Int64(element_index)})),
              googlesql_base::testing::IsOkAndHolds(expected_string_value));
  return true;
}

MATCHER_P3(JsonbObjectFieldText, jsonb, object_field, expected_string_value,
           "") {
  EXPECT_THAT(arg(absl::MakeConstSpan(
                  {jsonb.value(), googlesql::values::String(object_field)})),
              googlesql_base::testing::IsOkAndHolds(expected_string_value));
  return true;
}

TEST_F(EvalJsonbSubscriptText, ElementIndexInput) {
  EXPECT_THAT(evaluator_,
              JsonbArrayElementText(CreatePgJsonbValueWithMemoryContext(
                                        R"([null, "string val"])"),
                                    0, kNullStringValue));
  EXPECT_THAT(
      evaluator_,
      JsonbArrayElementText(
          CreatePgJsonbValueWithMemoryContext(R"([1.00, "string val"])"), 1,
          googlesql::values::String("string val")));
  EXPECT_THAT(evaluator_,
              JsonbArrayElementText(CreatePgJsonbValueWithMemoryContext(
                                        R"([null, "string val"])"),
                                    2, kNullStringValue));
  EXPECT_THAT(
      evaluator_,
      JsonbArrayElementText(
          CreatePgJsonbValueWithMemoryContext(R"([null, "string val"])"), -1,
          googlesql::values::String("string val")));
  EXPECT_THAT(evaluator_,
              JsonbArrayElementText(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  0, kNullStringValue));

  // Following are 3 test cases when any NULL value occurs in the arguments.
  // There is no error in these cases and the results are just NULL.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[1,2]").value(),
                   googlesql::values::NullInt64()})),
              googlesql_base::testing::IsOkAndHolds(kNullStringValue));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::values::Null(spangres::datatypes::GetPgJsonbType()),
           googlesql::values::Int64(-1)})),
      googlesql_base::testing::IsOkAndHolds(kNullStringValue));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {googlesql::values::Null(spangres::datatypes::GetPgJsonbType()),
           googlesql::values::NullInt64()})),
      googlesql_base::testing::IsOkAndHolds(kNullStringValue));
}

TEST_F(EvalJsonbSubscriptText, ObjectFieldInput) {
  EXPECT_THAT(evaluator_,
              JsonbObjectFieldText(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  "a", googlesql::values::String("string val")));
  EXPECT_THAT(
      evaluator_,
      JsonbObjectFieldText(
          CreatePgJsonbValueWithMemoryContext(R"({"a": {"b": "string_val"}})"),
          "a", googlesql::values::String(R"({"b": "string_val"})")));
  EXPECT_THAT(evaluator_,
              JsonbObjectFieldText(CreatePgJsonbValueWithMemoryContext(
                                       R"([1.00, "string val"])"),
                                   "a", kNullStringValue));
  EXPECT_THAT(evaluator_,
              JsonbObjectFieldText(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  "no match", kNullStringValue));
  EXPECT_THAT(
      evaluator_,
      JsonbObjectFieldText(CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"),
                           "a", googlesql::values::String("")));

  // Following is a test case when STRING argument is NULL. There is no error
  // and the result is just NULL.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
                   kNullStringValue})),
              googlesql_base::testing::IsOkAndHolds(kNullStringValue));
}

TEST_F(EvalJsonbSubscriptText, ErrorCases) {
  // More than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   googlesql::values::Int64(1), googlesql::values::Int64(2)})),
              StatusIs(absl::StatusCode::kInternal));

  // Less than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value()})),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   googlesql::values::NullBool()})),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class EvalJsonbArrayElement : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const googlesql::Function* function =
        functions_[kGoogleSQLSubscriptFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalJsonbArrayElement, ErrorCases) {
  // More than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   googlesql::values::Int64(1), googlesql::values::Int64(2)})),
              StatusIs(absl::StatusCode::kInternal));

  // Less than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value()})),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[null]").value(),
                   googlesql::values::NullBool()})),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class EvalJsonbObjectField : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const googlesql::Function* function =
        functions_[kGoogleSQLSubscriptFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalJsonbObjectField, ErrorCases) {
  // More than 2 arguments
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
           googlesql::values::String("a"), googlesql::values::String("b")})),
      StatusIs(absl::StatusCode::kInternal));

  // Less than 2 arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value()})),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
                   googlesql::values::NullBool()})),
              StatusIs(absl::StatusCode::kUnimplemented));
}

class EvalJsonbTypeof : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const googlesql::Function* function =
        functions_[kGoogleSQLJsonTypeFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalJsonbTypeof, ErrorCases) {
  // More than 1 argument
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {*CreatePgJsonbValueWithMemoryContext("[1,2,3.56]"),
                   *CreatePgJsonbValueWithMemoryContext("3.14")})),
              StatusIs(absl::StatusCode::kInternal));

  // Less than 1 argument
  std::vector<googlesql::Value> args = {};

  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              StatusIs(absl::StatusCode::kInternal));

  // Invalid arguments
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({googlesql::Value::Double(3.14)})),
              StatusIs(absl::StatusCode::kInternal));
}

struct EvalCastFromJsonbTestCase {
  std::string test_name;
  googlesql::Value arg;
  googlesql::Value expected_value;
  absl::StatusCode expected_status_code;
};

using EvalCastFromJsonbTest =
    ::testing::TestWithParam<EvalCastFromJsonbTestCase>;

TEST_P(EvalCastFromJsonbTest, TestEvalCastFromJsonb) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    // Add function alias if it exists.
    if (!function->function_options().alias_name.empty()) {
      googlesql::FunctionOptions function_options =
          function->function_options();
      std::string alias_name = function_options.alias_name;
      function_options.set_alias_name("");
      auto alias_function = std::make_unique<googlesql::Function>(
        function->Name(), function->GetGroup(),
        function->mode(), function->signatures(),
          function_options);
      functions[alias_name] = std::move(alias_function);
    }
    functions[function->Name()] = std::move(function);
  }

  static const googlesql::Type* gsql_pg_jsonb =
      spangres::datatypes::GetPgJsonbType();
  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  absl::flat_hash_map<googlesql::TypeKind, googlesql::FunctionSignature>
      signature_map = {
          {googlesql::TYPE_BOOL,
           {googlesql::types::BoolType(), {gsql_pg_jsonb}, nullptr}},
          {googlesql::TYPE_INT64,
           {googlesql::types::Int64Type(), {gsql_pg_jsonb}, nullptr}},
          {googlesql::TYPE_DOUBLE,
           {googlesql::types::DoubleType(), {gsql_pg_jsonb}, nullptr}},
          {googlesql::TYPE_EXTENDED,
           {gsql_pg_numeric, {gsql_pg_jsonb}, nullptr}},
          {googlesql::TYPE_STRING,
           {googlesql::types::StringType(), {gsql_pg_jsonb}, nullptr}},
          // To trigger an invalid cast.
          {googlesql::TYPE_TIMESTAMP,
           {googlesql::types::TimestampType(), {gsql_pg_jsonb}, nullptr}},
      };

  const EvalCastFromJsonbTestCase& test_case = GetParam();

  const googlesql::Function* function =
      functions[kPGCastFromJsonbFunctionName].get();
  auto callback = function->GetFunctionEvaluatorFactory();

  auto iter = signature_map.find(test_case.expected_value.type_kind());
  ASSERT_NE(iter, signature_map.end());
  if (test_case.expected_value.type_kind() == googlesql::TYPE_TIMESTAMP) {
    // This test is attempting to trigger an invalid cast.
    EXPECT_THAT(callback(iter->second),
                StatusIs(test_case.expected_status_code));
    return;
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(auto evaluator, callback(iter->second));

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
         googlesql::values::Null(spangres::datatypes::GetPgJsonbType()),
         googlesql::Value::NullBool(), absl::StatusCode::kOk},
        {"CastTrueJsonbToBool", *CreatePgJsonbValueWithMemoryContext("true"),
         googlesql::values::True(), absl::StatusCode::kOk},
        {"CastFalseJsonbToBool", *CreatePgJsonbValueWithMemoryContext("false"),
         googlesql::values::False(), absl::StatusCode::kOk},
        {"CastInvalidValueToBoolFails",
         *CreatePgJsonbValueWithMemoryContext("1.0"),
         googlesql::Value::NullBool(),  // unused
         absl::StatusCode::kInvalidArgument},

        // PG.JSONB -> DOUBLE
        {"CastNullJsonbToNullDouble",
         googlesql::values::Null(spangres::datatypes::GetPgJsonbType()),
         googlesql::Value::NullDouble(), absl::StatusCode::kOk},
        {"CastNumberJsonbToDouble", *CreatePgJsonbValueWithMemoryContext("1.5"),
         googlesql::Value::Double(1.5), absl::StatusCode::kOk},
        {"CastInvalidValueToDoubleFails",
         *CreatePgJsonbValueWithMemoryContext("true"),
         googlesql::Value::NullDouble(),  // unused
         absl::StatusCode::kInvalidArgument},

        // PG.JSONB -> INT64
        {"CastNullJsonbToNullInt64",
         googlesql::values::Null(spangres::datatypes::GetPgJsonbType()),
         googlesql::Value::NullInt64(), absl::StatusCode::kOk},
        {"CastNumberJsonbToInt64", *CreatePgJsonbValueWithMemoryContext("500"),
         googlesql::Value::Int64(500), absl::StatusCode::kOk},
        {"CastNumberWithDecimalPointJsonbToInt64",
         *CreatePgJsonbValueWithMemoryContext("1.5"),
         googlesql::Value::Int64(2), absl::StatusCode::kOk},
        {"CastInvalidValueToInt64Fails",
         *CreatePgJsonbValueWithMemoryContext("true"),
         googlesql::Value::NullInt64(),  // unused
         absl::StatusCode::kInvalidArgument},

        // PG.JSONB -> STRING
        {"CastNullJsonbToNullString",
         googlesql::values::Null(spangres::datatypes::GetPgJsonbType()),
         googlesql::Value::NullString(), absl::StatusCode::kOk},
        {"CastNumberJsonbToString", *CreatePgJsonbValueWithMemoryContext("500"),
         googlesql::Value::String("500"), absl::StatusCode::kOk},
        {"CastStringJsonbToString",
         *CreatePgJsonbValueWithMemoryContext("\"hello\""),
         googlesql::Value::String("\"hello\""), absl::StatusCode::kOk},

        // PG.JSONB -> <INVALID TYPE>
        {"CastTimestampJsonbToTimestampIsInvalid",
         *CreatePgJsonbValueWithMemoryContext("\"01 Jan 1970 00:00:00+00\""),
         googlesql::values::Timestamp(absl::UnixEpoch()),
         absl::StatusCode::kInvalidArgument},
    }));

class EvalCastToDateTest : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const googlesql::Function* function =
        functions_[kPGCastToDateFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

TEST_F(EvalCastToDateTest, SuccessfulCast) {
  std::vector<googlesql::Value> args = {
      googlesql::values::String("1999-01-08")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::IsOkAndHolds(googlesql::Value::Date(
                  absl::CivilDay(1999, 1, 8) - absl::CivilDay(1970, 1, 1))));
}

TEST_F(EvalCastToDateTest, NullValue) {
  auto arg = googlesql::Value::MakeNull<std::string>();
  EXPECT_THAT(evaluator_(absl::MakeConstSpan({arg})),
              googlesql_base::testing::IsOkAndHolds(googlesql::Value::NullDate()));
}

TEST_F(EvalCastToDateTest, UnsupportedDate) {
  std::vector<googlesql::Value> args = {
      googlesql::values::String("January 8 04:05:06 1999 PST")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(EvalCastToDateTest, InvalidArgsCount) {
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

class EvalCastToTimestampTest : public EmulatorFunctionsTest {
 protected:
  EvalCastToTimestampTest() {
    ABSL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &default_timezone_));
  }

  void SetUp() override {
    const googlesql::Function* function =
        functions_[kPGCastToTimestampFunctionName].get();
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }

  absl::TimeZone default_timezone_;
};

TEST_F(EvalCastToTimestampTest, SuccessfulCast) {
  std::vector<googlesql::Value> args = {
      googlesql::values::String("January 8 04:05:06 1999")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::IsOkAndHolds(googlesql::values::Timestamp(
                  absl::FromCivil(absl::CivilSecond(1999, 1, 8, 4, 5, 6),
                                  default_timezone_))));
}

TEST_F(EvalCastToTimestampTest, NullValue) {
  std::vector<googlesql::Value> args = {
      googlesql::values::NullString()};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::IsOkAndHolds(googlesql::Value::NullTimestamp()));
}

TEST_F(EvalCastToTimestampTest, UnsupportedTime) {
  std::vector<googlesql::Value> args = {
      googlesql::values::String("January 8 04:05:06 1999 PST")};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(EvalCastToTimestampTest, InvalidArgsCount) {
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

template <typename T,
            typename = std::enable_if_t<std::is_floating_point_v<T>>>
class EvalMapFloatingPointToIntTest : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const googlesql::Function* function;
    if constexpr (std::is_same_v<T, double>) {
      function = functions_[kPGMapDoubleToIntFunctionName].get();
    } else {
      function = functions_[kPGMapFloatToIntFunctionName].get();
    }
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }

  void VerifyEquality(const absl::Span<const T> values) {
    ASSERT_GT(values.size(), 1);
    for (int i = 1; i < values.size(); i++) {
      std::vector<googlesql::Value> args1 = {
          googlesql::Value::Make<T>(values[i - 1])};
      std::vector<googlesql::Value> args2 = {
          googlesql::Value::Make<T>(values[i])};
      GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value res1,
                           evaluator_(absl::MakeConstSpan(args1)));
      GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value res2,
                           evaluator_(absl::MakeConstSpan(args2)));
      EXPECT_EQ(res1.int64_value(), res2.int64_value());
    }
  }

  void VerifyGivenOrder(const absl::Span<const T> values) {
    ASSERT_GT(values.size(), 1);
    for (int i = 1; i < values.size(); i++) {
      std::vector<googlesql::Value> args1 = {
          googlesql::Value::Make<T>(values[i - 1])};
      std::vector<googlesql::Value> args2 = {
          googlesql::Value::Make<T>(values[i])};
      GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value res1,
                           evaluator_(absl::MakeConstSpan(args1)));
      GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value res2,
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

using FloatTypes = ::testing::Types<double, float>;
TYPED_TEST_SUITE(EvalMapFloatingPointToIntTest, FloatTypes);

// Verifies that all Nans are mapped to the same value.
TYPED_TEST(EvalMapFloatingPointToIntTest, NansEquality) {
  TypeParam nan1;
  TypeParam nan2;

  if constexpr (std::is_same_v<TypeParam, double>) {
    nan1 = -std::nan("");
    nan2 = -std::nan(this->RandomString().c_str());
  } else {
    nan1 = -std::nanf("");
    nan2 = -std::nanf(this->RandomString().c_str());
  }

  this->VerifyEquality({std::numeric_limits<TypeParam>::quiet_NaN(),
                        -std::numeric_limits<TypeParam>::quiet_NaN(),
                        std::numeric_limits<TypeParam>::signaling_NaN(),
                        -std::numeric_limits<TypeParam>::signaling_NaN(), nan1,
                        nan2});
}

// Verifies that all Zeros are mapped to the same value.
TYPED_TEST(EvalMapFloatingPointToIntTest, ZerosEquality) {
  this->VerifyEquality({0.0, -0.0});
}

TYPED_TEST(EvalMapFloatingPointToIntTest, NullInput) {
  auto arg = googlesql::Value::MakeNull<TypeParam>();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value res,
                       this->evaluator_(absl::MakeConstSpan({arg})));
  EXPECT_TRUE(res.type()->IsInt64());
  EXPECT_TRUE(res.is_null());
}

// Verifies that outputs follow PostgreSQL FLOAT8 order rules for inputs.
TYPED_TEST(EvalMapFloatingPointToIntTest, FixedOrder) {
  this->VerifyGivenOrder({-std::numeric_limits<TypeParam>::infinity(),
                            std::numeric_limits<TypeParam>::lowest(), -1.03,
                            -std::numeric_limits<TypeParam>::min(), 0,
                            std::numeric_limits<TypeParam>::min(), 1,
                            std::numeric_limits<TypeParam>::max(),
                            std::numeric_limits<TypeParam>::infinity(),
                            std::numeric_limits<TypeParam>::quiet_NaN()});
}

TYPED_TEST(EvalMapFloatingPointToIntTest, RandomOrder) {
  // Add at least two distrinct values, so we never end up with one value after
  // dedup.
  std::vector<TypeParam> values{std::numeric_limits<TypeParam>::min(), 0};
  absl::BitGen gen;
  for (int i = 0; i < 10; i++) {
    values.push_back(
        absl::Uniform<TypeParam>(absl::IntervalClosedClosed, gen,
                              -std::numeric_limits<TypeParam>::infinity(),
                              std::numeric_limits<TypeParam>::infinity()));
  }
  std::sort(values.begin(), values.end());

  // Dedup.
  values.erase(std::unique(values.begin(), values.end()), values.end());

  // Verification.
  this->VerifyGivenOrder(values);
}

TYPED_TEST(EvalMapFloatingPointToIntTest, InvalidArgsCount) {
  std::vector<googlesql::Value> args = {};
  EXPECT_THAT(this->evaluator_(absl::MakeConstSpan(args)),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInternal));
}

struct PgLeastGreatestTestCase {
  std::string test_name;
  std::vector<googlesql::Value> args;
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
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  const googlesql::Function* least_function =
      functions[kPGLeastFunctionName].get();
  const googlesql::Function* greatest_function =
      functions[kPGGreatestFunctionName].get();

  const std::vector<const googlesql::Type*> types = {
      googlesql::types::DoubleType(), googlesql::types::Int64Type(),
      googlesql::types::BoolType(), googlesql::types::BytesType(),
      googlesql::types::StringType(), googlesql::types::DateType(),
      googlesql::types::FloatType(), googlesql::types::TimestampType(),
      googlesql::types::IntervalType(),
  };

  absl::flat_hash_map<std::string, googlesql::FunctionEvaluator>
      least_evaluators;
  absl::flat_hash_map<std::string, googlesql::FunctionEvaluator>
      greatest_evaluators;

  least_evaluators.reserve(types.size());
  greatest_evaluators.reserve(types.size());
  for (auto type : types) {
    googlesql::FunctionSignature signature(
        type, {type, {type, googlesql::FunctionArgumentType::REPEATED}},
        nullptr);

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        least_evaluators[type->DebugString()],
        (least_function->GetFunctionEvaluatorFactory())(signature));

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(
        greatest_evaluators[type->DebugString()],
        (greatest_function->GetFunctionEvaluatorFactory())(signature));
  }

  // Test
  const PgLeastGreatestTestCase& test_case = GetParam();

  if (test_case.status_code == absl::StatusCode::kOk) {
    EXPECT_THAT(least_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                googlesql_base::testing::IsOkAndHolds(
                    test_case.args[test_case.expected_least_index]));
    EXPECT_THAT(greatest_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                googlesql_base::testing::IsOkAndHolds(
                    test_case.args[test_case.expected_greatest_index]));
  } else {
    EXPECT_THAT(least_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                googlesql_base::testing::StatusIs(test_case.status_code));
    EXPECT_THAT(greatest_evaluators[test_case.type_name](
                    absl::MakeConstSpan(test_case.args)),
                googlesql_base::testing::StatusIs(test_case.status_code));
  }
}

INSTANTIATE_TEST_SUITE_P(
    EvalLeastGreatestTests, EvalLeastGreatestTest,
    ::testing::ValuesIn<PgLeastGreatestTestCase>(
        {{"DoubleResultsInMid",
          {googlesql::values::Double(-12),
           googlesql::values::Double(-87980.125),
           googlesql::values::Double(100), googlesql::values::Double(-7)},
          googlesql::types::DoubleType()->DebugString(),
          1,
          2,
          absl::StatusCode::kOk},
         {"DoubleAscending",
          {googlesql::values::Double(-10000.123),
           googlesql::values::Double(-12), googlesql::values::Double(-7),
           googlesql::values::Double(100)},
          googlesql::types::DoubleType()->DebugString(),
          0,
          3,
          absl::StatusCode::kOk},
         {"DoubleDescending",
          {googlesql::values::Double(100), googlesql::values::Double(-7),
           googlesql::values::Double(-12), googlesql::values::Double(-879.125)},
          googlesql::types::DoubleType()->DebugString(),
          3,
          0,
          absl::StatusCode::kOk},
         {"DoubleWithNaN",
          {googlesql::values::Double(std::numeric_limits<double>::quiet_NaN()),
           googlesql::values::Double(-12), googlesql::values::Double(-5),
           googlesql::values::Double(-7)},
          googlesql::types::DoubleType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"DoubleWithNegativeNaN",
          {googlesql::values::Double(-std::numeric_limits<double>::quiet_NaN()),
           googlesql::values::Double(-12), googlesql::values::Double(-5),
           googlesql::values::Double(-7)},
          googlesql::types::DoubleType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"DoubleSingleValue",
          {googlesql::values::Double(-87980.125)},
          googlesql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"DoubleWithInfinitiesAndNaNAndNull",
          {googlesql::values::Double(87980.125),
           googlesql::values::Double(std::numeric_limits<double>::infinity()),
           googlesql::values::Double(std::numeric_limits<double>::quiet_NaN()),
           googlesql::values::NullDouble(),
           googlesql::values::Double(-std::numeric_limits<double>::infinity())},
          googlesql::types::DoubleType()->DebugString(),
          4,
          2,
          absl::StatusCode::kOk},
         {"DoubleAllNaNs",
          {googlesql::values::Double(std::numeric_limits<double>::quiet_NaN()),
           googlesql::values::Double(std::numeric_limits<double>::quiet_NaN())},
          googlesql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"DoubleAllNulls",
          {googlesql::values::NullDouble(), googlesql::values::NullDouble()},
          googlesql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"DoubleSkipNullFirst",
          {googlesql::values::NullDouble(), googlesql::values::Double(100)},
          googlesql::types::DoubleType()->DebugString(),
          1,
          1,
          absl::StatusCode::kOk},
         {"DoubleSkipNullLast",
          {googlesql::values::Double(200), googlesql::values::NullDouble()},
          googlesql::types::DoubleType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"FloatResultsInMid",
          {googlesql::values::Float(-12), googlesql::values::Float(-87980.125f),
           googlesql::values::Float(100), googlesql::values::Float(-7)},
          googlesql::types::FloatType()->DebugString(),
          1,
          2,
          absl::StatusCode::kOk},
         {"FloatAscending",
          {googlesql::values::Float(-10000.123f), googlesql::values::Float(-12),
           googlesql::values::Float(-7), googlesql::values::Float(100)},
          googlesql::types::FloatType()->DebugString(),
          0,
          3,
          absl::StatusCode::kOk},
         {"FloatDescending",
          {googlesql::values::Float(100), googlesql::values::Float(-7),
           googlesql::values::Float(-12), googlesql::values::Float(-879.125f)},
          googlesql::types::FloatType()->DebugString(),
          3,
          0,
          absl::StatusCode::kOk},
         {"FloatWithNaN",
          {googlesql::values::Float(std::numeric_limits<float>::quiet_NaN()),
           googlesql::values::Float(-12), googlesql::values::Float(-5),
           googlesql::values::Float(-7)},
          googlesql::types::FloatType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"FloatWithNegativeNaN",
          {googlesql::values::Float(-std::numeric_limits<float>::quiet_NaN()),
           googlesql::values::Float(-12), googlesql::values::Float(-5),
           googlesql::values::Float(-7)},
          googlesql::types::FloatType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"FloatSingleValue",
          {googlesql::values::Float(-87980.125f)},
          googlesql::types::FloatType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"FloatWithInfinitiesAndNaNAndNull",
          {googlesql::values::Float(87980.125f),
           googlesql::values::Float(std::numeric_limits<float>::infinity()),
           googlesql::values::Float(std::numeric_limits<float>::quiet_NaN()),
           googlesql::values::NullFloat(),
           googlesql::values::Float(-std::numeric_limits<float>::infinity())},
          googlesql::types::FloatType()->DebugString(),
          4,
          2,
          absl::StatusCode::kOk},
         {"FloatAllNaNs",
          {googlesql::values::Float(std::numeric_limits<float>::quiet_NaN()),
           googlesql::values::Float(std::numeric_limits<float>::quiet_NaN())},
          googlesql::types::FloatType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"FloatAllNulls",
          {googlesql::values::NullFloat(), googlesql::values::NullFloat()},
          googlesql::types::FloatType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"FloatSkipNullFirst",
          {googlesql::values::NullFloat(), googlesql::values::Float(100)},
          googlesql::types::FloatType()->DebugString(),
          1,
          1,
          absl::StatusCode::kOk},
         {"FloatSkipNullLast",
          {googlesql::values::Float(200), googlesql::values::NullFloat()},
          googlesql::types::FloatType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"StringWithDuplicates",
          {googlesql::values::String("aaaaa"),
           googlesql::values::String("aaaab"),
           googlesql::values::String("aaaab"),
           googlesql::values::String("aaaaa")},
          googlesql::types::StringType()->DebugString(),
          0,
          1,
          absl::StatusCode::kOk},
         {"Int64SmallVals",
          {googlesql::values::Int64(0), googlesql::values::Int64(12),
           googlesql::values::Int64(-5), googlesql::values::Int64(7)},
          googlesql::types::Int64Type()->DebugString(),
          2,
          1,
          absl::StatusCode::kOk},
         {"Int64MinMaxVals",
          {googlesql::values::Int64(0), googlesql::values::Int64(12),
           googlesql::values::Int64(-5),
           googlesql::values::Int64(std::numeric_limits<int64_t>::max()),
           googlesql::values::Int64(std::numeric_limits<int64_t>::min()),
           googlesql::values::Int64(-14)},
          googlesql::types::Int64Type()->DebugString(),
          4,
          3,
          absl::StatusCode::kOk},
         {"BoolVals",
          {googlesql::values::Bool(true), googlesql::values::Bool(false),
           googlesql::values::Bool(true), googlesql::values::Bool(false)},
          googlesql::types::BoolType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"BytesWithDuplicates",
          {googlesql::values::Bytes("aaaaa"), googlesql::values::Bytes("aaaab"),
           googlesql::values::Bytes("aaaab"),
           googlesql::values::Bytes("aaaaa")},
          googlesql::types::BytesType()->DebugString(),
          0,
          1,
          absl::StatusCode::kOk},
         {"DateValues",
          {googlesql::values::Date(absl::CivilDay(1999, 1, 8) -
                                   absl::CivilDay(1970, 1, 1)),
           googlesql::values::Date(0), googlesql::values::Date(-1),
           googlesql::values::Date(1000)},
          googlesql::types::DateType()->DebugString(),
          2,
          0,
          absl::StatusCode::kOk},
         {"TimestampValues",
          {googlesql::values::Timestamp(absl::UnixEpoch()),
           googlesql::values::Timestamp(absl::Now() + absl::Hours(20)),
           googlesql::values::Timestamp(absl::Now())},
          googlesql::types::TimestampType()->DebugString(),
          0,
          1,
          absl::StatusCode::kOk},
         {"IntervalValues",
          {kIntervalMaxValue, kIntervalMinValue,
           googlesql::values::Interval(googlesql::IntervalValue())},
          googlesql::types::IntervalType()->DebugString(),
          1,
          0,
          absl::StatusCode::kOk},
         {"IntervalValuesWithDuplicates",
          {kIntervalMaxValue, kIntervalValue, kIntervalMinValue, kIntervalValue,
           googlesql::values::Interval(googlesql::IntervalValue())},
          googlesql::types::IntervalType()->DebugString(),
          2,
          0,
          absl::StatusCode::kOk},
         {"IntervalValuesWithNull",
          {kIntervalMaxValue, kIntervalValue, kIntervalMinValue,
           kNullIntervalValue,
           googlesql::values::Interval(googlesql::IntervalValue())},
          googlesql::types::IntervalType()->DebugString(),
          2,
          0,
          absl::StatusCode::kOk},
         {"IntervalValuesOnlyNull",
          {
              kNullIntervalValue,
          },
          googlesql::types::IntervalType()->DebugString(),
          0,
          0,
          absl::StatusCode::kOk},
         {"InvalidArgsCount",
          {},
          googlesql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInternal},
         {"InvalidSingleArgument",
          {googlesql::Value()},
          googlesql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInvalidArgument},
         {"InvalidMidArgument",
          {googlesql::values::Int64(0), googlesql::Value(),
           googlesql::values::Int64(12)},
          googlesql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInvalidArgument},
         {"MismatchedTypes",
          {googlesql::values::Int64(0), googlesql::Value(),
           googlesql::values::Int64(12)},
          googlesql::types::DoubleType()->DebugString(),
          std::numeric_limits<size_t>::max() /* unused */,
          std::numeric_limits<size_t>::max() /* unused */,
          absl::StatusCode::kInvalidArgument}}),
    [](const ::testing::TestParamInfo<EvalLeastGreatestTest::ParamType>& info) {
      return info.param.test_name;
    });

TEST(EvalLeastGreatestInvalidTest, InvalidType) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  googlesql::FunctionSignature signature(
      googlesql::types::Int32Type(),
      {googlesql::types::Int32Type(),
       {googlesql::types::Int32Type(),
        googlesql::FunctionArgumentType::REPEATED}},
      nullptr);

  const googlesql::Function* least_function =
      functions[kPGLeastFunctionName].get();
  EXPECT_THAT((least_function->GetFunctionEvaluatorFactory())(signature),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));

  const googlesql::Function* greatest_function =
      functions[kPGGreatestFunctionName].get();
  EXPECT_THAT((greatest_function->GetFunctionEvaluatorFactory())(signature),
              googlesql_base::testing::StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST(EvalMinSignatureTest, CustomMinSignatures) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }
  const googlesql::Function* function = functions[kPGMinFunctionName].get();
  const std::vector<googlesql::FunctionSignature>& signatures =
      function->signatures();
  ASSERT_THAT(signatures.size(), 2);
  EXPECT_TRUE(signatures[0].result_type().type()->IsDouble());
  EXPECT_THAT(signatures[0].arguments().size(), 1);
  EXPECT_TRUE(signatures[0].arguments().front().type()->IsDouble());
  EXPECT_TRUE(signatures[1].result_type().type()->IsFloat());
  EXPECT_THAT(signatures[1].arguments().size(), 1);
  EXPECT_TRUE(signatures[1].arguments().front().type()->IsFloat());
}

struct EvalAggregatorTestCase {
  std::string test_name;
  std::string function_name;
  std::vector<const googlesql::Value*> args;
  googlesql::Value expected_value;
  absl::StatusCode expected_status_code;
};

using EvalMinMaxTest = ::testing::TestWithParam<EvalAggregatorTestCase>;

TEST_P(EvalMinMaxTest, TestMin) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }
  const EvalAggregatorTestCase& test_case = GetParam();
  const googlesql::Type* agg_type = test_case.expected_value.type();
  googlesql::FunctionSignature signature(agg_type, {agg_type}, nullptr);
  const googlesql::Function* function =
      functions[test_case.function_name].get();
  auto callback = function->GetAggregateFunctionEvaluatorFactory();
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<googlesql::AggregateFunctionEvaluator> evaluator,
      callback(signature));

  bool stop_acc = false;
  // We have to make a copy here because GetParam() returns a const value but
  // the accumulate interface doesn't want a const span.
  std::vector<const googlesql::Value*> args = test_case.args;
  if (test_case.expected_status_code == absl::StatusCode::kOk) {
    int i = 0;
    while (!stop_acc) {
      GOOGLESQL_EXPECT_OK(
          evaluator->Accumulate(absl::MakeSpan(args).subspan(i), &stop_acc));
      ++i;
    }
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(googlesql::Value result, evaluator->GetFinalResult());
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
                googlesql_base::testing::StatusIs(test_case.expected_status_code));
  }
}

INSTANTIATE_TEST_SUITE_P(EvalPgMinTests, EvalMinMaxTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneDoubleNullArg",
                              kPGMinFunctionName,
                              {&kNullDoubleValue},
                              kNullDoubleValue,
                              absl::StatusCode::kOk},
                             {"EmptyDoubleArgs",
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
                             {"OneDoublePosInfArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kPosInfDoubleValue, &kNegInfDoubleValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoublePosInfArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kPosInfDoubleValue, &kNegInfDoubleValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleNanArg",
                              kPGMinFunctionName,
                              {&kDoubleNaNValue},
                              kDoubleNaNValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleNullArgOneNanArg",
                              kPGMinFunctionName,
                              {&kNullDoubleValue, &kDoubleNaNValue},
                              kDoubleNaNValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleArgOneNanArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kDoubleNaNValue},
                              kDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoubleNegInfArgOneNanArg",
                              kPGMinFunctionName,
                              {&kNegInfDoubleValue, &kDoubleNaNValue},
                              kNegInfDoubleValue,
                              absl::StatusCode::kOk},
                             {"OneDoublePosInfArgOneNanArg",
                              kPGMinFunctionName,
                              {&kPosInfDoubleValue, &kDoubleNaNValue},
                              kPosInfDoubleValue,
                              absl::StatusCode::kOk},

                             {"OneFloatNullArg",
                              kPGMinFunctionName,
                              {&kNullFloatValue},
                              kNullFloatValue,
                              absl::StatusCode::kOk},
                             {"EmptyFloatArgs",
                              kPGMinFunctionName,
                              {},
                              kNullFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatArg",
                              kPGMinFunctionName,
                              {&kFloatValue},
                              kFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatArgOneNullArg",
                              kPGMinFunctionName,
                              {&kFloatValue, &kNullFloatValue},
                              kFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatArgOnePosInfArg",
                              kPGMinFunctionName,
                              {&kFloatValue, &kPosInfFloatValue},
                              kFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kFloatValue, &kNegInfFloatValue},
                              kNegInfFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatPosInfArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kPosInfFloatValue, &kNegInfFloatValue},
                              kNegInfFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatPosInfArgOneNegInfArg",
                              kPGMinFunctionName,
                              {&kPosInfFloatValue, &kNegInfFloatValue},
                              kNegInfFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatNanArg",
                              kPGMinFunctionName,
                              {&kFloatNaNValue},
                              kFloatNaNValue,
                              absl::StatusCode::kOk},
                             {"OneFloatNullArgOneNanArg",
                              kPGMinFunctionName,
                              {&kNullFloatValue, &kFloatNaNValue},
                              kFloatNaNValue,
                              absl::StatusCode::kOk},
                             {"OneFloatArgOneNanArg",
                              kPGMinFunctionName,
                              {&kFloatValue, &kFloatNaNValue},
                              kFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatNegInfArgOneNanArg",
                              kPGMinFunctionName,
                              {&kNegInfFloatValue, &kFloatNaNValue},
                              kNegInfFloatValue,
                              absl::StatusCode::kOk},
                             {"OneFloatPosInfArgOneNanArg",
                              kPGMinFunctionName,
                              {&kPosInfFloatValue, &kFloatNaNValue},
                              kPosInfFloatValue,
                              absl::StatusCode::kOk},
                         }));

INSTANTIATE_TEST_SUITE_P(EvalMinTests, EvalMinMaxTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneOidNullArg",
                              kGoogleSQLMinFunctionName,
                              {&kNullPGOidValue},
                              kNullPGOidValue,
                              absl::StatusCode::kOk},
                             {"EmptyOidArgs",
                              kGoogleSQLMinFunctionName,
                              {},
                              kNullPGOidValue,
                              absl::StatusCode::kOk},
                             {"OneOidArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGOidValue},
                              kPGOidValue,
                              absl::StatusCode::kOk},
                             {"OneOidArgOneNullArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGOidValue, &kNullPGOidValue},
                              kPGOidValue,
                              absl::StatusCode::kOk},

                             {"OneNumericNullArg",
                              kGoogleSQLMinFunctionName,
                              {&kNullPGNumericValue},
                              kNullPGNumericValue,
                              absl::StatusCode::kOk},
                             {"EmptyNumericArgs",
                              kGoogleSQLMinFunctionName,
                              {},
                              kNullPGNumericValue,
                              absl::StatusCode::kOk},
                             {"OneNumericArg",
                              kGoogleSQLMinFunctionName,
                              {&kNullPGNumericValue},
                              kNullPGNumericValue,
                              absl::StatusCode::kOk},
                             {"OneNumericArgOneNullArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGNumericValue, &kNullPGNumericValue},
                              kPGNumericValue,
                              absl::StatusCode::kOk},
                             {"MinNumericArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGNumericValue, &kPGNumericMaxValue,
                               &kPGNumericMinValue},
                              kPGNumericMinValue,
                              absl::StatusCode::kOk},
                             {"OneNumericNanArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGNumericNaNValue},
                              kPGNumericNaNValue,
                              absl::StatusCode::kOk},
                             {"OneNumericNullArgOneNanArg",
                              kGoogleSQLMinFunctionName,
                              {&kNullPGNumericValue, &kPGNumericNaNValue},
                              kPGNumericNaNValue,
                              absl::StatusCode::kOk},
                             {"OneNumericArgOneNanArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGNumericValue, &kPGNumericNaNValue},
                              kPGNumericValue,
                              absl::StatusCode::kOk},
                         }));

INSTANTIATE_TEST_SUITE_P(EvalPgMinFailureTests, EvalMinMaxTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneInt64InvalidArg",
                              kPGMinFunctionName,
                              {&kInt64Value},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneValidArgOneInt64InvalidArg",
                              kPGMinFunctionName,
                              {&kDoubleValue, &kInt64Value},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneFloatInvalidArg",
                              kPGMinFunctionName,
                              {&kInt64Value},
                              kNullFloatValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneFloatValidArgOneInvalidArg",
                              kPGMinFunctionName,
                              {&kFloatValue, &kInt64Value},
                              kNullFloatValue,  // ignored
                              absl::StatusCode::kInvalidArgument},

                             {"OneInvalidDoubleArg",
                              kGoogleSQLMinFunctionName,
                              {&kDoubleValue},
                              kNullPGOidValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneValidArgOneInvalidDoubleArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGOidValue, &kDoubleValue},
                              kNullPGOidValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                         }));

INSTANTIATE_TEST_SUITE_P(EvalMinFailureTests, EvalMinMaxTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneInvalidDoubleArg",
                              kGoogleSQLMinFunctionName,
                              {&kDoubleValue},
                              kNullPGOidValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneValidArgOneInvalidDoubleArg",
                              kGoogleSQLMinFunctionName,
                              {&kPGOidValue, &kDoubleValue},
                              kNullPGOidValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                         }));

TEST(EvalMaxSignatureTest, ExtendedTypeMaxSignatures) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }
  const googlesql::Function* function =
      functions[kGoogleSQLMaxFunctionName].get();
  const std::vector<googlesql::FunctionSignature>& signatures =
      function->signatures();
  ASSERT_THAT(signatures.size(), 2);
  EXPECT_TRUE(signatures[0].result_type().type() ==
              spangres::datatypes::GetPgOidType());
  EXPECT_THAT(signatures[0].arguments().size(), 1);
  EXPECT_TRUE(signatures[0].arguments().front().type() ==
              spangres::datatypes::GetPgOidType());
  EXPECT_TRUE(signatures[1].result_type().type() ==
              spangres::datatypes::GetPgNumericType());
  EXPECT_THAT(signatures[1].arguments().size(), 1);
  EXPECT_TRUE(signatures[1].arguments().front().type() ==
              spangres::datatypes::GetPgNumericType());
}

INSTANTIATE_TEST_SUITE_P(EvalMaxTest, EvalMinMaxTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             {"OneOidNullArg",
                              kGoogleSQLMaxFunctionName,
                              {&kNullPGOidValue},
                              kNullPGOidValue,
                              absl::StatusCode::kOk},
                             {"EmptyOidArgs",
                              kGoogleSQLMaxFunctionName,
                              {},
                              kNullPGOidValue,
                              absl::StatusCode::kOk},
                             {"OneOidArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGOidValue},
                              kPGOidValue,
                              absl::StatusCode::kOk},
                             {"OneOidArgOneNullArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGOidValue, &kNullPGOidValue},
                              kPGOidValue,
                              absl::StatusCode::kOk},
                         }));

INSTANTIATE_TEST_SUITE_P(EvalMaxFailureTests, EvalMinMaxTest,
                         ::testing::ValuesIn<EvalAggregatorTestCase>({
                             // NUMERIC
                             {"OneNumericNullArg",
                              kGoogleSQLMaxFunctionName,
                              {&kNullPGNumericValue},
                              kNullPGNumericValue,
                              absl::StatusCode::kOk},
                             {"EmptyNumericArgs",
                              kGoogleSQLMaxFunctionName,
                              {},
                              kNullPGNumericValue,
                              absl::StatusCode::kOk},
                             {"OneNumericArg",
                              kGoogleSQLMaxFunctionName,
                              {&kNullPGNumericValue},
                              kNullPGNumericValue,
                              absl::StatusCode::kOk},
                             {"OneNumericArgOneNullArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGNumericValue, &kNullPGNumericValue},
                              kPGNumericValue,
                              absl::StatusCode::kOk},
                             {"MaxNumericArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGNumericValue, &kPGNumericMaxValue,
                               &kPGNumericMinValue},
                              kPGNumericMaxValue,
                              absl::StatusCode::kOk},
                             {"OneNumericNanArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGNumericNaNValue},
                              kPGNumericNaNValue,
                              absl::StatusCode::kOk},
                             {"OneNumericNullArgOneNanArg",
                              kGoogleSQLMaxFunctionName,
                              {&kNullPGNumericValue, &kPGNumericNaNValue},
                              kPGNumericNaNValue,
                              absl::StatusCode::kOk},
                             {"OneNumericArgOneNanArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGNumericValue, &kPGNumericNaNValue},
                              kPGNumericNaNValue,
                              absl::StatusCode::kOk},

                             // OID
                             {"OneNumericInvalidArg",
                              kGoogleSQLMaxFunctionName,
                              {&kDoubleValue},
                              kNullPGOidValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"OneValidArgOneInvalidDoubleArg",
                              kGoogleSQLMaxFunctionName,
                              {&kPGOidValue, &kDoubleValue},
                              kNullPGOidValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                         }));

using EvalSumAvgTest = ::testing::TestWithParam<EvalAggregatorTestCase>;

TEST_P(EvalSumAvgTest, TestSumAvgAggregator) {
  SpannerPGFunctions spanner_pg_functions =
      GetSpannerPGFunctions("TestCatalog");
  std::unordered_map<std::string, std::unique_ptr<googlesql::Function>>
      functions;
  for (auto& function : spanner_pg_functions) {
    functions[function->Name()] = std::move(function);
  }

  static const googlesql::Type* gsql_pg_numeric =
      spangres::datatypes::GetPgNumericType();

  const EvalAggregatorTestCase& test_case = GetParam();

  absl::flat_hash_map<googlesql::TypeKind, googlesql::FunctionSignature>
      signature_map = {
          {googlesql::TYPE_INT64,
           {gsql_pg_numeric, {googlesql::types::Int64Type()}, nullptr}},
          {googlesql::TYPE_DOUBLE,
           {googlesql::types::DoubleType(),
            {googlesql::types::DoubleType()},
            nullptr}},
          {googlesql::TYPE_FLOAT,
           // For avg, the result type is double if the input type is float.
           // For sum, the result type is float if the input type is float.
           {test_case.function_name == kPGAvgFunctionName
                ? googlesql::types::DoubleType()
                : googlesql::types::FloatType(),
            {googlesql::types::FloatType()},
            nullptr}},
          {googlesql::TYPE_EXTENDED,
           {gsql_pg_numeric, {gsql_pg_numeric}, nullptr}},
      };

  bool stop_acc = false;

  const googlesql::Function* function =
      functions[test_case.function_name].get();
  auto callback = function->GetAggregateFunctionEvaluatorFactory();

  // In these test cases, we don't know what the input type is if we don't have
  // any test args so we assume it's an INT64 input that returns a PG.NUMERIC
  // output.
  googlesql::TypeKind type_kind = test_case.args.empty()
      ? googlesql::TYPE_INT64 : test_case.args[0]->type_kind();
  auto iter = signature_map.find(type_kind);
  ASSERT_NE(iter, signature_map.end());
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<googlesql::AggregateFunctionEvaluator> evaluator,
      callback(iter->second));

  // We have to make a copy here because GetParam() returns a const value but
  // the accumulate interface doesn't want a const span.
  std::vector<const googlesql::Value*> args = test_case.args;
  if (test_case.expected_status_code == absl::StatusCode::kOk) {
    int i = 0;
    while (!stop_acc) {
      GOOGLESQL_EXPECT_OK(
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
                googlesql_base::testing::StatusIs(test_case.expected_status_code));
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
         googlesql::values::Double(3.0),
         absl::StatusCode::kOk},
        {"NullDoubleArgFirst",
         kPGSumFunctionName,
         {&kNullDoubleValue, &kDoubleValue, &kDoubleValue},
         googlesql::values::Double(2.0),
         absl::StatusCode::kOk},
        {"NullDoubleArgsBeforeDoubleValues",
         kPGSumFunctionName,
         {&kNullDoubleValue, &kNullDoubleValue, &kDoubleValue, &kDoubleValue},
         googlesql::values::Double(2.0),
         absl::StatusCode::kOk},
        {"NullDoubleArgsElsewhere",
         kPGSumFunctionName,
         {&kDoubleValue, &kNullDoubleValue, &kDoubleValue, &kNullDoubleValue},
         googlesql::values::Double(2.0),
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

        // Tests for pg.sum of FLOAT
        {"OneNullFloatArg",
         kPGSumFunctionName,
         {&kNullFloatValue},
         kNullFloatValue,
         absl::StatusCode::kOk},
        {"OneFloatArg",
         kPGSumFunctionName,
         {&kFloatValue},
         kFloatValue,
         absl::StatusCode::kOk},
        {"ManyFloatArgs",
         kPGSumFunctionName,
         {&kFloatValue, &kFloatValue, &kFloatValue},
         googlesql::values::Float(3.0f),
         absl::StatusCode::kOk},
        {"NullFloatArgFirst",
         kPGSumFunctionName,
         {&kNullFloatValue, &kFloatValue, &kFloatValue},
         googlesql::values::Float(2.0f),
         absl::StatusCode::kOk},
        {"NullFloatArgsBeforeFloatValues",
         kPGSumFunctionName,
         {&kNullFloatValue, &kNullFloatValue, &kFloatValue, &kFloatValue},
         googlesql::values::Float(2.0f),
         absl::StatusCode::kOk},
        {"NullFloatArgsElsewhere",
         kPGSumFunctionName,
         {&kFloatValue, &kNullFloatValue, &kFloatValue, &kNullFloatValue},
         googlesql::values::Float(2.0f),
         absl::StatusCode::kOk},
        {"OneNanFloatArg",
         kPGSumFunctionName,
         {&kFloatNaNValue},
         kFloatNaNValue,
         absl::StatusCode::kOk},
        {"ManyNanFloatArgs",
         kPGSumFunctionName,
         {&kFloatValue, &kFloatNaNValue, &kFloatValue, &kFloatNaNValue},
         kFloatNaNValue,
         absl::StatusCode::kOk},
        {"OneInfinityFloatArg",
         kPGSumFunctionName,
         {&kPosInfFloatValue},
         kPosInfFloatValue,
         absl::StatusCode::kOk},
        {"ManyInfinityFloatArgs",
         kPGSumFunctionName,
         {&kPosInfFloatValue, &kPosInfFloatValue, &kPosInfFloatValue},
         kPosInfFloatValue,
         absl::StatusCode::kOk},
        {"PosAndNegInfinityMakesNaN",
         kPGSumFunctionName,
         {&kPosInfFloatValue, &kNegInfFloatValue},
         kFloatNaNValue,
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
         googlesql::values::Double((std::numeric_limits<double>::min() +
                                    std::numeric_limits<double>::max()) /
                                   2.0),
         absl::StatusCode::kOk},

        // Tests for pg.avg of FLOAT
        {"OneNullFloatArg",
         kPGAvgFunctionName,
         {&kNullFloatValue},
         kNullDoubleValue,
         absl::StatusCode::kOk},
        {"OneFloatArg",
         kPGAvgFunctionName,
         {&kFloatValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"ManyFloatArgs",
         kPGAvgFunctionName,
         {&kFloatValue, &kFloatValue, &kFloatValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"NullFloatArgFirst",
         kPGAvgFunctionName,
         {&kNullFloatValue, &kFloatValue, &kFloatValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"NullFloatArgsBeforeFloatValues",
         kPGAvgFunctionName,
         {&kNullFloatValue, &kNullFloatValue, &kFloatValue, &kFloatValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"NullFloatArgsElsewhere",
         kPGAvgFunctionName,
         {&kFloatValue, &kNullFloatValue, &kFloatValue, &kNullFloatValue},
         kDoubleValue,
         absl::StatusCode::kOk},
        {"OneNanFloatArg",
         kPGAvgFunctionName,
         {&kFloatNaNValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"ManyNanFloatArgs",
         kPGAvgFunctionName,
         {&kFloatValue, &kFloatNaNValue, &kFloatValue, &kFloatNaNValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"OneInfinityFloatArg",
         kPGAvgFunctionName,
         {&kPosInfFloatValue},
         kPosInfDoubleValue,
         absl::StatusCode::kOk},
        {"ManyInfinityFloatArgs",
         kPGAvgFunctionName,
         {&kPosInfFloatValue, &kPosInfFloatValue, &kPosInfFloatValue},
         kPosInfDoubleValue,
         absl::StatusCode::kOk},
        {"PosAndNegInfinityMakesNaN",
         kPGAvgFunctionName,
         {&kPosInfFloatValue, &kNegInfFloatValue},
         kDoubleNaNValue,
         absl::StatusCode::kOk},
        {"AvgMinAndMaxFloat",
         kPGAvgFunctionName,
         {&kFloatMinValue, &kFloatMaxValue},
         googlesql::values::Double((std::numeric_limits<float>::min() +
                                    std::numeric_limits<float>::max()) /
                                   2.0),
         absl::StatusCode::kOk},
        {"AvgFloatMaxDoesNotOverflow",
         kPGAvgFunctionName,
         {&kFloatMaxValue, &kFloatMaxValue},
         googlesql::values::Double(std::numeric_limits<float>::max()),
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
                             {"SumFloatWithInconsistentTypes",
                              kPGSumFunctionName,
                              {&kDoubleValue, &kFloatValue},
                              kNullFloatValue,  // ignored
                              absl::StatusCode::kInvalidArgument},
                             {"SumDoubleWithOverflow",
                              kPGSumFunctionName,
                              {&kDoubleMaxValue, &kDoubleMaxValue},
                              kNullDoubleValue,  // ignored
                              absl::StatusCode::kOutOfRange},
                             {"SumFloatWithOverflow",
                              kPGSumFunctionName,
                              {&kFloatMaxValue, &kFloatMaxValue},
                              kNullFloatValue,  // ignored
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
                             {"AvgFloatWithInconsistentTypes",
                              kPGAvgFunctionName,
                              {&kFloatValue, &kInt64Value},
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
