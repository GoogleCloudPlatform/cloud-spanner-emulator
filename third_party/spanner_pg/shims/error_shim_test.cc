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

#include "third_party/spanner_pg/shims/error_shim.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/interface/regexp_evaluators.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {

namespace {

using ::postgres_translator::function_evaluators::CleanupRegexCache;
using ::postgres_translator::test::ValidMemoryContext;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

class ErrorShimTest : public ValidMemoryContext {
  void TearDown() override {
    CleanupRegexCache();
    ValidMemoryContext::TearDown();
  }
};

// Simple demonstration of error shim for checked calls that throw and that do
// not throw. Verify returns are expected and that control always returns to the
// test function.
TEST_F(ErrorShimTest, BasicError) {
  const int err_code = ERRCODE_INTERNAL_ERROR;

  EXPECT_TRUE(TEST_CheckedError(/*throw_error=*/false, err_code).ok());
  // Verify flow resumes and error is reported.
  EXPECT_FALSE(TEST_CheckedError(/*throw_error=*/true, err_code).ok());
}

// Test that FATAL and PANIC errors are correctly caught.
TEST_F(ErrorShimTest, FatalPanic) {
  const bool throw_error = true;
  // Test FATAL and PANIC errors. Ensure control flow returns to this function.
  EXPECT_FALSE(TEST_CheckedFatal(throw_error, ERRCODE_INTERNAL_ERROR).ok());
  EXPECT_FALSE(TEST_CheckedPanic(throw_error, ERRCODE_INTERNAL_ERROR).ok());
}

// Helper function that passes a PostgreSQL error code through the error shim
// and gets the resulting absl error code.
absl::StatusCode PgErrorToCode(int postgres_error_code) {
  return TEST_CheckedError(/*throw_error=*/true, postgres_error_code)
      .status()
      .code();
}

// Test that PostgreSQL error codes get correctly translated to g3 error
// codes.
TEST_F(ErrorShimTest, ErrorCodes) {
  EXPECT_EQ(PgErrorToCode(ERRCODE_INTERNAL_ERROR), absl::StatusCode::kInternal);
  EXPECT_EQ(PgErrorToCode(ERRCODE_FEATURE_NOT_SUPPORTED),
            absl::StatusCode::kUnimplemented);
  EXPECT_EQ(PgErrorToCode(ERRCODE_INSUFFICIENT_RESOURCES),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(PgErrorToCode(ERRCODE_STATEMENT_TOO_COMPLEX),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(PgErrorToCode(ERRCODE_OUT_OF_MEMORY),
            absl::StatusCode::kResourceExhausted);
  EXPECT_EQ(PgErrorToCode(ERRCODE_SYNTAX_ERROR),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(PgErrorToCode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            absl::StatusCode::kPermissionDenied);
  EXPECT_EQ(PgErrorToCode(ERRCODE_INDEX_CORRUPTED),
            absl::StatusCode::kDataLoss);
  EXPECT_EQ(PgErrorToCode(ERRCODE_DATA_CORRUPTED), absl::StatusCode::kDataLoss);
  EXPECT_EQ(PgErrorToCode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
            absl::StatusCode::kOutOfRange);
  EXPECT_EQ(PgErrorToCode(ERRCODE_UNDEFINED_TABLE),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(PgErrorToCode(ERRCODE_UNDEFINED_OBJECT),
            absl::StatusCode::kNotFound);
  EXPECT_EQ(PgErrorToCode(ERRCODE_DATA_EXCEPTION),
            absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(PgErrorToCode(ERRCODE_INVALID_TEXT_REPRESENTATION),
            absl::StatusCode::kInvalidArgument);
}

// Test that the parser works correctly both when it errors and when it runs
// successfully.
TEST_F(ErrorShimTest, ParserError) {
  const char* success_query = "select 1;";
  const char* fail_query = "NOT A VALID QUERY";
  auto success_status = CheckedPgRawParser(success_query);
  EXPECT_TRUE(success_status.ok());
  auto failure_status = CheckedPgRawParser(fail_query);
  EXPECT_FALSE(failure_status.ok());
  EXPECT_EQ(failure_status.status().code(), absl::StatusCode::kInvalidArgument);
  EXPECT_EQ(failure_status.status().message(),
            "[ERROR] syntax error at or near \"NOT\"");
}

// Test that legacy elog() errors are correctly handled.
TEST_F(ErrorShimTest, ElogError) {
  EXPECT_TRUE(TEST_CheckedElogError(/*throw_error=*/false).ok());
  // Verify flow resumes and error is reported.
  EXPECT_FALSE(TEST_CheckedElogError(/*throw_error=*/true).ok());
}

TEST_F(ErrorShimTest, AnalyzerError) {
  const char* success_query = "select 1;";
  const char* fail_query = "select sqrt(\"foo\");";

  ZETASQL_ASSERT_OK_AND_ASSIGN(List * lst, CheckedPgRawParser(success_query));
  for (RawStmt* stmt : StructList<RawStmt*>(lst)) {
    auto success_status =
        CheckedPgParseAnalyze(stmt, success_query, nullptr, 0, nullptr);
    EXPECT_TRUE(success_status.ok());
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(lst, CheckedPgRawParser(fail_query));

  for (RawStmt* stmt : StructList<RawStmt*>(lst)) {
    auto failure_status =
        CheckedPgParseAnalyze(stmt, fail_query, nullptr, 0, nullptr);
    EXPECT_FALSE(failure_status.ok());
    EXPECT_EQ(failure_status.status().code(),
              absl::StatusCode::kInvalidArgument);
  }
}

TEST_F(ErrorShimTest, StringToNode) {
  // `stringToNode` requires a `char*`, NOT a `const char*`.
  // This means we have to make a copy in mutable memory.
  char* invalid_query = strdup("INVALID SERIALIZED STRING");
  auto status = CheckedPgStringToNode(invalid_query);
  free(invalid_query);

  EXPECT_EQ(status.status().message(),
            "[ERROR] unrecognized token: \"INVALID\"");
}

TEST_F(ErrorShimTest, InvalidStringToByteaErrors) {
  std::string invalid_bytes = "b\"\x83NN\xcb\\\xb6\x83\x7f+p\"";
  EXPECT_THAT(CheckedPgStringToDatum(invalid_bytes.c_str(), BYTEAOID),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("invalid input syntax for type bytea")));
}

TEST_F(ErrorShimTest, CheckedPgStringToNameSucceeds) {
  std::string test_string = "test string";
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum datum,
                       CheckedPgStringToDatum(test_string.c_str(), NAMEOID));

  char* new_test_string = DatumGetCString(DirectFunctionCall1(nameout, datum));
  EXPECT_STREQ(new_test_string, test_string.c_str());
}

TEST_F(ErrorShimTest, CheckedPgStringToByteaSucceeds) {
  std::string bytes_string = "\\x01";
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum datum,
                       CheckedPgStringToDatum(bytes_string.c_str(), BYTEAOID));

  char* new_byte_string = DatumGetCString(DirectFunctionCall1(byteaout, datum));
  EXPECT_STREQ(new_byte_string, bytes_string.c_str());
}

TEST_F(ErrorShimTest, TextDatumToCString) {
  std::string test_string = "test string";
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum datum,
                       CheckedPgStringToDatum(test_string.c_str(), TEXTOID));

  ZETASQL_ASSERT_OK_AND_ASSIGN(char* new_test_string,
                       CheckedPgTextDatumGetCString(datum));

  EXPECT_STREQ(new_test_string, test_string.c_str());
}

TEST_F(ErrorShimTest, CheckedOidFunctionCall1) {
  // Non-null result
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_string,
                       CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_EXPECT_OK(CheckedOidFunctionCall1(F_QUOTE_IDENT, input_string));

  // CheckedOidFunctionCall* does not support NULL results
  ZETASQL_ASSERT_OK_AND_ASSIGN(ArrayType * input_array,
                       CheckedPgConstructEmptyArray(TEXTOID));
  Datum input_array_datum = PointerGetDatum(input_array);
  EXPECT_THAT(CheckedOidFunctionCall1(F_ARRAY_DIMS, input_array_datum),
              StatusIs(absl::StatusCode::kUnknown, HasSubstr("returned NULL")));
}

TEST_F(ErrorShimTest, CheckedOidFunctionCall2) {
  // Non-null result
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_string,
                       CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_pattern,
                       CheckedPgStringToDatum("(a)", TEXTOID));
  ZETASQL_EXPECT_OK(CheckedOidFunctionCall2(F_REGEXP_MATCH_TEXT_TEXT, input_string,
                                    input_pattern));

  // CheckedOidFunctionCall* does not support NULL results
  ZETASQL_ASSERT_OK_AND_ASSIGN(input_string, CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(input_pattern, CheckedPgStringToDatum("(b)", TEXTOID));
  EXPECT_THAT(CheckedOidFunctionCall2(F_REGEXP_MATCH_TEXT_TEXT, input_string,
                                      input_pattern),
              StatusIs(absl::StatusCode::kUnknown, HasSubstr("returned NULL")));
}

TEST_F(ErrorShimTest, CheckedOidFunctionCall3) {
  // Non-null result
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_string,
                       CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_pattern,
                       CheckedPgStringToDatum("(a)", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_flags, CheckedPgStringToDatum("i", TEXTOID));
  ZETASQL_EXPECT_OK(CheckedOidFunctionCall3(F_REGEXP_MATCH_TEXT_TEXT, input_string,
                                    input_pattern, input_flags));

  // CheckedOidFunctionCall* does not support NULL results
  ZETASQL_ASSERT_OK_AND_ASSIGN(input_string, CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(input_pattern, CheckedPgStringToDatum("(b)", TEXTOID));
  EXPECT_THAT(CheckedOidFunctionCall3(F_REGEXP_MATCH_TEXT_TEXT, input_string,
                                      input_pattern, input_flags),
              StatusIs(absl::StatusCode::kUnknown, HasSubstr("returned NULL")));
}

TEST_F(ErrorShimTest, CheckedNullableOidFunctionCall1) {
  // Non-null result
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_string,
                       CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_EXPECT_OK(CheckedNullableOidFunctionCall1(F_QUOTE_IDENT, input_string));

  ZETASQL_ASSERT_OK_AND_ASSIGN(ArrayType * input_array,
                       CheckedPgConstructEmptyArray(TEXTOID));
  Datum input_array_datum = PointerGetDatum(input_array);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Datum result_datum,
      CheckedNullableOidFunctionCall1(F_ARRAY_DIMS, input_array_datum));
  EXPECT_EQ(result_datum, NULL_DATUM);
}

TEST_F(ErrorShimTest, CheckedNullableOidFunctionCall2) {
  // Non-null result
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_string,
                       CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_pattern,
                       CheckedPgStringToDatum("(a)", TEXTOID));
  ZETASQL_EXPECT_OK(CheckedNullableOidFunctionCall2(F_REGEXP_MATCH_TEXT_TEXT,
                                            input_string, input_pattern));

  ZETASQL_ASSERT_OK_AND_ASSIGN(input_string, CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(input_pattern, CheckedPgStringToDatum("(b)", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum result_datum, CheckedNullableOidFunctionCall2(
                                               F_REGEXP_MATCH_TEXT_TEXT,
                                               input_string, input_pattern));
  EXPECT_EQ(result_datum, NULL_DATUM);
}

TEST_F(ErrorShimTest, CheckedNullableOidFunctionCall3) {
  // Non-null result
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_string,
                       CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_pattern,
                       CheckedPgStringToDatum("(a)", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Datum input_flags, CheckedPgStringToDatum("i", TEXTOID));
  ZETASQL_EXPECT_OK(CheckedNullableOidFunctionCall3(
      F_REGEXP_MATCH_TEXT_TEXT, input_string, input_pattern, input_flags));

  ZETASQL_ASSERT_OK_AND_ASSIGN(input_string, CheckedPgStringToDatum("a", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(input_pattern, CheckedPgStringToDatum("(b)", TEXTOID));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Datum result_datum,
      CheckedNullableOidFunctionCall3(F_REGEXP_MATCH_TEXT_TEXT, input_string,
                                      input_pattern, input_flags));
  EXPECT_EQ(result_datum, NULL_DATUM);
}

TEST_F(ErrorShimTest, CheckedPgListDeleteNthCell) {
  List* test_list = list_make3_int(1, 2, 3);
  ZETASQL_ASSERT_OK_AND_ASSIGN(test_list, CheckedPgListDeleteNthCell(test_list, 1));
  EXPECT_FALSE(list_member_int(test_list, 2));
  EXPECT_TRUE(list_member_int(test_list, 1));
  EXPECT_TRUE(list_member_int(test_list, 3));
  EXPECT_EQ(test_list->length, 2);

  ZETASQL_ASSERT_OK_AND_ASSIGN(test_list, CheckedPgListDeleteNthCell(test_list, 0));
  EXPECT_FALSE(list_member_int(test_list, 1));
  EXPECT_TRUE(list_member_int(test_list, 3));
  EXPECT_EQ(test_list->length, 1);
}

TEST_F(ErrorShimTest, CheckedPgLinitialNode) {
  Value* arg1 = makeString(pstrdup("column1"));
  Value* arg2 = makeString(pstrdup("column2"));
  List* test_list = list_make2(arg1, arg2);
  ZETASQL_ASSERT_OK_AND_ASSIGN(Value * initial_arg,
                       CheckedPgLinitialNode<Value>(test_list));
  EXPECT_EQ(initial_arg, arg1);
}

TEST_F(ErrorShimTest, CheckedPgListHead) {
  List* test_list = list_make3_int(1, 2, 3);
  ListCell* lc;
  ZETASQL_ASSERT_OK_AND_ASSIGN(lc, CheckedPgListHead(test_list));
  ASSERT_NE(lc, nullptr);

  int test_int = lfirst_int(lc);
  EXPECT_EQ(test_int, 1);
}

TEST_F(ErrorShimTest, CheckedPgDefGetInt64) {
  char value_str[] = "9223372036854775807";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      DefElem * defElem,
      CheckedPgMakeDefElem(
          nullptr, internal::PostgresCastToNode(makeFloat(value_str)), 1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t value, CheckedPgDefGetInt64(defElem));
  EXPECT_EQ(value, 9223372036854775807);
}

}  // namespace
}  // namespace postgres_translator
