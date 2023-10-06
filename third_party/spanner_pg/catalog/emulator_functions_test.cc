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

using testing::HasSubstr;
using zetasql_base::testing::IsOkAndHolds;
using zetasql_base::testing::StatusIs;

// Create PG.JSONB value in a valid memory context which is required for calling
// PG code.
static absl::StatusOr<zetasql::Value> CreatePgJsonbValueWithMemoryContext(
    absl::string_view jsonb_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return spangres::datatypes::CreatePgJsonbValue(jsonb_string);
}

// Create PG.NUMERIC value in a valid memory context which is required for
// calling PG code.
static absl::StatusOr<zetasql::Value> CreatePgNumericValueWithMemoryContext(
    absl::string_view numeric_string) {
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
      postgres_translator::interfaces::CreatePGArena(nullptr));
  return spangres::datatypes::CreatePgNumericValue(numeric_string);
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
        PGScalarFunctionTestCase{kPGTextregexneFunctionName,
                                 {zetasql::values::NullString(),
                                  zetasql::values::String("ab.*")},
                                 zetasql::values::NullBool()},
        PGScalarFunctionTestCase{kPGTextregexneFunctionName,
                                 {zetasql::values::String("abcdefg"),
                                  zetasql::values::NullString()},
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
        PGScalarFunctionTestCase{kPGToDateFunctionName,
                                 {zetasql::values::NullString(),
                                  zetasql::values::String("DD Mon YYYY")},
                                 zetasql::values::NullDate()},
        PGScalarFunctionTestCase{kPGToDateFunctionName,
                                 {zetasql::values::String("01 Jan 1970"),
                                  zetasql::values::NullString()},
                                 zetasql::values::NullDate()},

        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {zetasql::values::String("01 Jan 1970 00:00:00+00"),
             zetasql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            zetasql::values::Timestamp(absl::UnixEpoch())},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {zetasql::values::NullString(),
             zetasql::values::String("DD Mon YYYY HH24:MI:SSTZH")},
            zetasql::values::NullTimestamp()},
        PGScalarFunctionTestCase{
            kPGToTimestampFunctionName,
            {zetasql::values::String("01 Jan 1970 00:00:00+00"),
             zetasql::values::NullString()},
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
            zetasql::values::NullString()},
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
                                 zetasql::values::NullString()},
        PGScalarFunctionTestCase{kPGToCharFunctionName,
                                 {zetasql::values::Double(-123.45),
                                  zetasql::values::NullString()},
                                 zetasql::values::NullString()},

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
            {zetasql::values::NullString(), zetasql::values::String("999")},
            zetasql::values::Null(spangres::datatypes::GetPgNumericType())},
        PGScalarFunctionTestCase{
            kPGToNumberFunctionName,
            {zetasql::values::String("123"), zetasql::values::NullString()},
            zetasql::values::Null(spangres::datatypes::GetPgNumericType())},

        PGScalarFunctionTestCase{kPGQuoteIdentFunctionName,
                                 {zetasql::values::String("test")},
                                 zetasql::values::String("\"test\"")},
        PGScalarFunctionTestCase{kPGQuoteIdentFunctionName,
                                 {zetasql::values::NullString()},
                                 zetasql::values::NullString()},

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
            {zetasql::values::NullString(), zetasql::values::String("b.")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcdefg"),
             zetasql::values::NullString()},
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
            {zetasql::values::NullString(), zetasql::values::String("b.*"),
             zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcDefg"),
             zetasql::values::NullString(), zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpMatchFunctionName,
            {zetasql::values::String("abcDefg"),
             zetasql::values::String("b.*"), zetasql::values::NullString()},
            zetasql::values::Null(zetasql::types::StringArrayType())},

        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("a1b2c3d"),
             zetasql::values::String("[0-9]")},
            zetasql::values::StringArray({"a", "b", "c", "d"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::NullString(),
             zetasql::values::String("[0-9]")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("a1b2c3d"),
             zetasql::values::NullString()},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"),
             zetasql::values::String("[a-z]"),
             zetasql::values::String("i")},
            zetasql::values::StringArray({"1", "2", "3", "4"})},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::NullString(),
             zetasql::values::String("[a-z]"),
             zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"),
             zetasql::values::NullString(), zetasql::values::String("i")},
            zetasql::values::Null(zetasql::types::StringArrayType())},
        PGScalarFunctionTestCase{
            kPGRegexpSplitToArrayFunctionName,
            {zetasql::values::String("1A2b3C4"),
             zetasql::values::String("[a-z]"),
             zetasql::values::NullString()},
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
          {zetasql::values::String("bc"), zetasql::values::NullString()}));
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
  EXPECT_THAT(evaluator_, NullToJsonB(zetasql::values::NullString()));
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
  EXPECT_THAT(evaluator_, ArrayToJsonB(zetasql::Value::MakeArray(
                                           zetasql::types::StringArrayType(),
                                           {zetasql::values::String("a"),
                                            zetasql::values::NullString()})
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

class EvalToJsonBSubscript : public EmulatorFunctionsTest {
 protected:
  void SetUp() override {
    const zetasql::Function* function =
        functions_[kPGJsonBSubscriptTextFunctionName].get();
    ZETASQL_ASSERT_OK_AND_ASSIGN(evaluator_, (function->GetFunctionEvaluatorFactory())(
                                         function->signatures().front()));
  }
};

MATCHER_P3(JsonBArrayElement, jsonb, element_index, expected_string_value, "") {
  EXPECT_THAT(arg(absl::MakeConstSpan(
                  {jsonb.value(), zetasql::values::Int64(element_index)})),
              zetasql_base::testing::IsOkAndHolds(expected_string_value));
  return true;
}

MATCHER_P3(JsonBObjectField, jsonb, object_field, expected_string_value, "") {
  EXPECT_THAT(arg(absl::MakeConstSpan(
                  {jsonb.value(), zetasql::values::String(object_field)})),
              zetasql_base::testing::IsOkAndHolds(expected_string_value));
  return true;
}

TEST_F(EvalToJsonBSubscript, ElementIndexInput) {
  EXPECT_THAT(evaluator_,
              JsonBArrayElement(CreatePgJsonbValueWithMemoryContext(
                                    R"([null, "string val"])"),
                                0, zetasql::values::NullString()));
  EXPECT_THAT(evaluator_,
              JsonBArrayElement(CreatePgJsonbValueWithMemoryContext(
                                    R"([1.00, "string val"])"),
                                1, zetasql::values::String("string val")));
  EXPECT_THAT(evaluator_,
              JsonBArrayElement(CreatePgJsonbValueWithMemoryContext(
                                    R"([null, "string val"])"),
                                2, zetasql::values::NullString()));
  EXPECT_THAT(evaluator_,
              JsonBArrayElement(CreatePgJsonbValueWithMemoryContext(
                                    R"([null, "string val"])"),
                                -1, zetasql::values::NullString()));
  EXPECT_THAT(evaluator_,
              JsonBArrayElement(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  0, zetasql::values::NullString()));

  // Following are 3 test cases when any NULL value occurs in the arguments.
  // There is no error in these cases and the results are just NULL.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext("[1,2]").value(),
                   zetasql::values::NullInt64()})),
              zetasql_base::testing::IsOkAndHolds(zetasql::values::NullString()));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
           zetasql::values::Int64(-1)})),
      zetasql_base::testing::IsOkAndHolds(zetasql::values::NullString()));
  EXPECT_THAT(
      evaluator_(absl::MakeConstSpan(
          {zetasql::values::Null(spangres::datatypes::GetPgJsonbType()),
           zetasql::values::NullInt64()})),
      zetasql_base::testing::IsOkAndHolds(zetasql::values::NullString()));
}

TEST_F(EvalToJsonBSubscript, ObjectFieldInput) {
  EXPECT_THAT(evaluator_,
              JsonBObjectField(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  "a", zetasql::values::String("string val")));
  EXPECT_THAT(
      evaluator_,
      JsonBObjectField(
          CreatePgJsonbValueWithMemoryContext(R"({"a": {"b": "string_val"}})"),
          "a", zetasql::values::String(R"({"b": "string_val"})")));
  EXPECT_THAT(evaluator_,
              JsonBObjectField(CreatePgJsonbValueWithMemoryContext(
                                   R"([1.00, "string val"])"),
                               "a", zetasql::values::NullString()));
  EXPECT_THAT(evaluator_,
              JsonBObjectField(
                  CreatePgJsonbValueWithMemoryContext(R"({"a": "string val"})"),
                  "no match", zetasql::values::NullString()));
  EXPECT_THAT(
      evaluator_,
      JsonBObjectField(CreatePgJsonbValueWithMemoryContext(R"({"a": ""})"), "a",
                       zetasql::values::String("")));

  // Following is a test case when STRING argument is NULL. There is no error
  // and the result is just NULL.
  EXPECT_THAT(evaluator_(absl::MakeConstSpan(
                  {CreatePgJsonbValueWithMemoryContext(R"({"a":1})").value(),
                   zetasql::values::NullString()})),
              zetasql_base::testing::IsOkAndHolds(zetasql::values::NullString()));
}

TEST_F(EvalToJsonBSubscript, ErrorCases) {
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
