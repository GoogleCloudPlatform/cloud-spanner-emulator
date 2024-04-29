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

#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_parse.h"

#include <cstdint>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "zetasql/base/no_destructor.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/cord.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "tests/common/file_based_test_util.h"
#include "third_party/spanner_pg/datatypes/common/jsonb/random_json_creator.h"
#include "third_party/spanner_pg/datatypes/common/numeric_core.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "zetasql/common/utf_util.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {
namespace {

using ::absl::StrAppend;
using ::absl::StrCat;
using ::postgres_translator::test::ValidMemoryContext;
using ::postgres_translator::test::ValidMemoryContextParameterized;

// The following two values are the min value and max value the non-leading
// bytes can be in a UTF-8 multi-byte character. For more information
// regarding Valid UTF-8 bytes see https://en.wikipedia.org/wiki/UTF-8#Encoding
constexpr char kUTF8ContinuationByteMinVal = 0b10000000;
constexpr char kUTF8ContinuationByteMaxVal = 0b10111111;

const zetasql_base::NoDestructor<std::string> kMaxValuePGJSONBString(
    StrCat(std::string(kMaxPGJSONBNumericWholeDigits, '9'), ".",
           std::string(kMaxPGJSONBNumericFractionalDigits, '9')));

absl::StatusOr<std::string> ReadJsonFile(std::string path) {
  std::string jsonb;
  const auto file_path = google::spanner::emulator::test::GetTestFileDir(path);
  std::ifstream file_content_stream(file_path.c_str());
  if (!file_content_stream) {
    return absl::InvalidArgumentError("Cannot open " + path);
  }
  std::stringstream buffer;
  buffer << file_content_stream.rdbuf();
  return buffer.str();
}

struct ArrayTestCase {
  ArrayTestCase(const std::string& input_in,
                const std::vector<std::string>& array_in)
      : input(input_in), array(array_in) {}

  std::string input;
  std::vector<std::string> array;
};

class SimpleArrayTest : public testing::TestWithParam<ArrayTestCase> {};

INSTANTIATE_TEST_SUITE_P(
    ArrayTestValues, SimpleArrayTest,
    testing::Values(
        ArrayTestCase("[\"str 1\", \"str 2\", \"str 3\"]",
                      {"\"str 1\"", "\"str 2\"", "\"str 3\""}),
        ArrayTestCase("[]", {}),
        ArrayTestCase("[null, \"str\", null]", {"null", "\"str\"", "null"}),
        ArrayTestCase(
            "[[null, \"inner array string\"], \"outer array string\"]",
            {"[null, \"inner array string\"]", "\"outer array string\""})));

TEST_P(SimpleArrayTest, ArrayTest) {
  const ArrayTestCase& test_case = GetParam();
  const std::string& input = test_case.input;
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(input));
  EXPECT_EQ(jsonb, input);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<absl::Cord> jsonb_array,
                       ParseJsonbArray(input));
  ASSERT_EQ(jsonb_array.size(), test_case.array.size());
  for (int i = 0; i < jsonb_array.size(); ++i) {
    EXPECT_EQ(jsonb_array[i], test_case.array[i]);
  }
}

// Ensure that parsing nested json arrays does not result in out of memory error
TEST_P(SimpleArrayTest, NestedArrayTest) {
  // 1 ascii char = 1 byte
  // 2^22 bytes = 4 MB < 10 MB (JSONB parser supported max json size)
  // Here are some example of json array strings with depth
  // depth(1): [] | depth(2): [[]] | depth(3): [[[]]]
  // So a nested array with depth n will have 2n ascii chars aka bytes
  // Hence max depth = (2 ^ 22) / 2 = 2 ^ 21
  constexpr uint32_t max_depth = 1 << 21;
  const std::string json_input =
      StrCat(std::string(max_depth, '['), GetParam().input,
             std::string(max_depth, ']'));
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(json_input));
  EXPECT_EQ(jsonb, json_input);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<absl::Cord> jsonb_inner_array,
                       ParseJsonbArray(json_input));
  ASSERT_EQ(jsonb_inner_array.size(), 1);
  EXPECT_EQ(jsonb_inner_array[0], json_input.substr(1, json_input.size() - 2));
}

// Test that parsing a single huge array does not result in out of memory error
TEST(JSONBParseTest, LargeArrayTest) {
  constexpr uint32_t max_elements = 500000;
  std::string json_input = "[";
  json_input.reserve(3 * max_elements + 3);
  for (int i = 0; i < max_elements; ++i) {
    StrAppend(&json_input, "1, ");
  }
  StrAppend(&json_input, "1]");

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<absl::Cord> jsonb_inner_array,
                       ParseJsonbArray(json_input));
  ASSERT_EQ(jsonb_inner_array.size(), max_elements + 1);
  EXPECT_EQ(jsonb_inner_array[0], "1");
}

class SimpleStringTest : public testing::TestWithParam<std::string> {};

INSTANTIATE_TEST_SUITE_P(
    StringTestValues, SimpleStringTest,
    testing::Values("\"short string.\"", "\"\"",
                    "\"This is a relatively longer JSON string.\"",
                    "\"String with unicode char: Д\"",
                    "\"String with \\\"quotes\\\"('', \\\").\""));

TEST_P(SimpleStringTest, StringTest) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(GetParam()));
  EXPECT_EQ(jsonb, GetParam());
}

struct TestCase {
  std::string input;
  std::string expected_output;
};

using NumberTest = test::ValidMemoryContextParameterized<TestCase>;

INSTANTIATE_TEST_SUITE_P(
    NumberTestValues, NumberTest,
    testing::Values(
        TestCase{.input = "-999999999.99999",
                 .expected_output = "-999999999.99999"},
        TestCase{.input = "3e10", .expected_output = "30000000000"},
        TestCase{.input = "-3e10", .expected_output = "-30000000000"},
        TestCase{.input = "123.4560000", .expected_output = "123.4560000"},
        TestCase{.input = "-0.000000000001400",
                 .expected_output = "-0.000000000001400"},
        TestCase{.input = "7.3e-12", .expected_output = "0.0000000000073"},
        TestCase{.input = "9e4931",
                 .expected_output = StrCat("9", std::string(4931, '0'))},
        TestCase{.input = "-9e4931",
                 .expected_output = StrCat("-9", std::string(4931, '0'))},
        TestCase{.input = *kMaxValuePGJSONBString,
                 .expected_output = *kMaxValuePGJSONBString},
        TestCase{.input = StrCat("-", *kMaxValuePGJSONBString),
                 .expected_output = StrCat("-", *kMaxValuePGJSONBString)}));

TEST_P(NumberTest, NumberTest) {
  TestCase test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(test_case.input));
  EXPECT_EQ(jsonb, test_case.expected_output);
}

using BooleanTest = ValidMemoryContextParameterized<std::string>;

INSTANTIATE_TEST_SUITE_P(BooleanTestValues, BooleanTest,
                         testing::Values("true", "false"));

TEST_P(BooleanTest, BooleanTest) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(GetParam()));
  EXPECT_EQ(jsonb, GetParam());
}

// Test simple objects that can roundtrip back to their original representation.
using RoundtripObjectTest = ValidMemoryContextParameterized<std::string>;

INSTANTIATE_TEST_SUITE_P(
    ObjectTestValues, RoundtripObjectTest,
    testing::Values("{}", "{\"num\": 123.456000}",
                    "{\"array_with_array_with_object\": [[1, {}], null]}"));

TEST_P(RoundtripObjectTest, ObjectTest) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(GetParam()));
  EXPECT_EQ(jsonb, GetParam());
}

// Ensure that parsing nested json object does not result in out of memory error
TEST_P(RoundtripObjectTest, NestedObjectTest) {
  // 1 ascii char = 1 byte
  // 7 * 2^20 bytes = 7 MB < 10 MB (JSONB parser supported max json size)
  // Here are some example of json object strings with depth
  // depth(1): {"a": 1}
  // depth(2): {"a": {"a": 1}}
  // depth(3): {"a": {"a": {"a": 1}}}
  // So a nested array with depth n will have 7n ascii chars aka bytes
  // Hence max depth = 7 * (2 ^ 20) / 7 = 2 ^ 20
  constexpr uint32_t max_depth = 1 << 20;
  std::string prefix;

  // `{"a": ` 6 characters at each level hence 6 times the depth
  prefix.reserve(6 * max_depth);
  for (int i = 0; i < max_depth; ++i) {
    StrAppend(&prefix, "{\"a\": ");
  }

  const std::string json =
      StrCat(prefix, GetParam(), std::string(max_depth, '}'));
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(json));
  EXPECT_EQ(jsonb, json);
}

// Test objects. Capture the semantics of ordering done first by length of the
// keys, and then lexicographically for strings of the same length. Additionally
// make sure nested objects, duplicate keys, and empty object are tested.
using ObjectStorageTest = ValidMemoryContextParameterized<TestCase>;

INSTANTIATE_TEST_SUITE_P(
    ObjectStorageTestValues, ObjectStorageTest,
    testing::Values(
        TestCase{
            .input =
                R"({"b": 17e2,"a":"I appear first", "aaa"  : "Im last!"  , "bb": "Ill be overwritten", "bb": 13.32000})",
            .expected_output =
                R"({"a": "I appear first", "b": 1700, "bb": 13.32000, "aaa": "Im last!"})"},
        TestCase{.input = R"({})", .expected_output = R"({})"},
        TestCase{.input = R"({"a":{"b": {"c": 12.1400}}})",
                 .expected_output = R"({"a": {"b": {"c": 12.1400}}})"},
        TestCase{
            .input =
                R"({"\n":"\n", "\r":"\\r", "\t":"\t", "A":"A", "\\":"\\", "\"": "\""})",
            .expected_output =
                R"({"\t": "\t", "\n": "\n", "\r": "\\r", "\"": "\"", "A": "A", "\\": "\\"})"}));

TEST_P(ObjectStorageTest, ObjectStorageTest) {
  TestCase test_case = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(test_case.input));
  EXPECT_EQ(jsonb, test_case.expected_output);
}

TEST(JSONBParseTest, NullJSON) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb("null"));
  EXPECT_EQ(jsonb, "null");
}

// Test cases that result in errors.
using ParsingErrorTest = ValidMemoryContextParameterized<std::string>;

INSTANTIATE_TEST_SUITE_P(
    ErrorTestValues, ParsingErrorTest,
    testing::Values("[1,2,[1,2, [1,2], 1,2]",  // Missing a ']'
                    "23AAzsdf",  // Nonsense that isn't a value value
                    "1e4932",    // Positive number with too many digits
                    "-1e4932",   // Negative number with too many digits
                    "\"\\320\\224, \\xD0\\x94\"",  // Invalid escape sequences
                    "}", "{", "\"\\u0000\"", "{\"\\u0000\": \"string\"}",
                    "{\"\test\u0000key\": \"\test\u0000value\"}"));

TEST_P(ParsingErrorTest, ErrorTestValue) {
  EXPECT_FALSE(ParseJsonb(GetParam()).ok());
  EXPECT_FALSE(ParseJsonbArray(GetParam()).ok());
}

// Test cases that are not arrays and should result in an error.
class ArrayParsingErrorTest : public testing::TestWithParam<std::string> {};
INSTANTIATE_TEST_SUITE_P(ErrorTestValues, ArrayParsingErrorTest,
                         testing::Values("{\"key\": 1}", "1", "abc", "{}",
                                         "null"));

TEST_P(ArrayParsingErrorTest, ErrorTestValue) {
  EXPECT_FALSE(ParseJsonbArray(GetParam()).ok());
}

using JsonbParseTest = ValidMemoryContext;

// Ensure that the parser is capable of parsing numbers up to the full 16,383
// digits after the decimal point.
TEST_F(JsonbParseTest, FullScaleTest) {
  auto input_str = "0." + std::string(16383, '9');
  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::Cord jsonb, ParseJsonb(input_str));
  EXPECT_EQ(jsonb, input_str);
}

// This is a regression test for invalid deep nested arrays / objects which
// result in a stack overflow error due to recursive destructor calls.
TEST_F(JsonbParseTest, DeepNestedArrayWithError) {
  std::string test_str = StrCat(std::string(502335, '['), "S", "[[");
  auto jsonb_or = ParseJsonb(test_str);
  EXPECT_FALSE(jsonb_or.ok());
}

class PostgresComparisonTest : public ValidMemoryContext {
 public:
  // Tests that any valid UTF-8 input jsonb string `input` follows identical
  // normalization rules between our PG.JSONB parser and Postgres' parser. This
  // property is tested by running the same input string on both our parser and
  // Postgres' parser, ensuring the resulst are identical.
  void PGComplianceTestHelper(absl::string_view input) {
    SCOPED_TRACE(input);
    // Just because a byte sequence was made within the valid UTF-8 byte range,
    // does not necessarily mean it is valid UTF-8. Confirm that it is.
    // From testing, Postgres parser accepts all byte-sequences, even if they
    // are not valid UTF-8, while nlohmann will not accept some invalid UTF-8
    // values.
    if (!zetasql::IsWellFormedUTF8(input)) {
      EXPECT_FALSE(ParseJsonb(input).ok());
      return;
    }

    absl::StatusOr<Datum> pg_normalized =
        postgres_translator::CheckedDirectFunctionCall1(
            jsonb_in, CStringGetDatum(input.data()));
    absl::StatusOr<absl::Cord> jsonb_normalized = ParseJsonb(input);
    EXPECT_EQ(pg_normalized.ok(), jsonb_normalized.ok());

    if (pg_normalized.ok() && jsonb_normalized.ok()) {
      ZETASQL_ASSERT_OK_AND_ASSIGN(Datum psql_output,
                           postgres_translator::CheckedDirectFunctionCall1(
                               jsonb_out, pg_normalized.value()));
      std::string expected = DatumGetCString(psql_output);
      EXPECT_EQ(expected, std::string(jsonb_normalized.value()));
    }
  }
};

TEST_F(PostgresComparisonTest, ValidJsonbValues) {
  std::vector<std::string> test_values{
      // duplicate keys
      "{\"a\": null, \"b\": 2, \"a\": 64.11, \"b\": []}",
      // keys out of order
      "{}"
      // inconsistent spacing
      "{\"a\":false, \"b\":[{}],\"c\":  \n192.68}",
      // numeric normalization
      "{\"a\": 2.5E+5, \"b\": 4e3, \"c\": -3.1e-5}"};

  for (const std::string& jsonb : test_values) {
    PGComplianceTestHelper(jsonb);
  }
}

TEST_F(PostgresComparisonTest, TwitterJsonValue) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::string jsonb,
                       ReadJsonFile("com_google_cloud_spanner_emulator/third_party/spanner_pg/ddl/"
                                    "types/testdata/twitter.json"));

  PGComplianceTestHelper(jsonb);
}

TEST_F(PostgresComparisonTest, InvalidJsonbValues) {
  std::vector<std::string> test_values{
      "[1,2,[1,2, [1,2], 1,2]",      // Missing a ']'
      "23AAzsdf",                    // Nonsense that isn't a value value
      "\"\\320\\224, \\xD0\\x94\"",  // Invalid escape sequences
      "\"\\u0000\"",                 // \u0000 is invalid character in Postgres
      "}",
      "{",
      "{\"\\u0002\" \"string\"}",
      "{\"\test\u0004key\": \test\u0003value\"}"};
  for (const std::string& jsonb : test_values) {
    PGComplianceTestHelper(jsonb);
  }
}

TEST_F(PostgresComparisonTest, RandomJSONTest) {
  RandomJsonCreator random_json_creator;
  SCOPED_TRACE(
      StrCat("Random JSON with seed ", random_json_creator.GetSeed(), ")"));
  for (int i = 0; i < 1000; ++i) {
    std::string random_json = random_json_creator.GetRandomJson();
    EXPECT_THAT(zetasql::IsWellFormedUTF8(random_json), true);
    PGComplianceTestHelper(random_json);
  }
}

TEST_F(PostgresComparisonTest, AllOneByteUTF8) {
  std::string test_input = "\"a\"";
  // This is the maximum value of a single-byte UTF-8 character.
  constexpr char kMaxValue = 0b01111111;
  // NOTE that it's important for val to be an unsigned char. If it's a signed
  // char, you can get an infinite loop where once val gets to 127, it wraps
  // around to being -128 which is also <= kMaxValue, causing an infinite loop.
  for (unsigned char val = 0; val <= kMaxValue; ++val) {
    test_input[1] = val;
    PGComplianceTestHelper(test_input);
  }
}

TEST_F(PostgresComparisonTest, AllTwoByteUTF8) {
  std::string test_input = "\"aa\"";
  constexpr char kTwoByteMaxLeadingByteValue = 0b11011111;

  for (char first_byte = 0b11000000; first_byte <= kTwoByteMaxLeadingByteValue;
       ++first_byte) {
    test_input[1] = first_byte;
    for (char second_byte = kUTF8ContinuationByteMinVal;
         second_byte <= kUTF8ContinuationByteMaxVal; ++second_byte) {
      test_input[2] = second_byte;
      PGComplianceTestHelper(test_input);
    }
  }
}

TEST_F(PostgresComparisonTest, AllThreeByteUTF8) {
  std::string test_input = "\"aaa\"";
  constexpr char kThreeByteMaxLeadingByteValue = 0b11101111;

  for (char first_byte = 0b11100000;
       first_byte <= kThreeByteMaxLeadingByteValue; ++first_byte) {
    test_input[1] = first_byte;
    for (char second_byte = kUTF8ContinuationByteMinVal;
         second_byte <= kUTF8ContinuationByteMaxVal; ++second_byte) {
      test_input[2] = second_byte;
      for (char third_byte = kUTF8ContinuationByteMinVal;
           third_byte <= kUTF8ContinuationByteMaxVal; ++third_byte) {
        test_input[3] = third_byte;
        PGComplianceTestHelper(test_input);
      }
    }
  }
}

TEST_F(PostgresComparisonTest, AllFourByteUTF8) {
  std::string test_input = "\"aaaa\"";
  constexpr char kFourByteMaxLeadingByteValue = 0b11110111;

  for (char first_byte = 0b11110000; first_byte <= kFourByteMaxLeadingByteValue;
       ++first_byte) {
    test_input[1] = first_byte;
    for (char second_byte = kUTF8ContinuationByteMinVal;
         second_byte <= kUTF8ContinuationByteMaxVal; ++second_byte) {
      test_input[2] = second_byte;
      for (char third_byte = kUTF8ContinuationByteMinVal;
           third_byte <= kUTF8ContinuationByteMaxVal; ++third_byte) {
        test_input[3] = third_byte;
        for (char fourth_byte = kUTF8ContinuationByteMinVal;
             fourth_byte <= kUTF8ContinuationByteMaxVal; ++fourth_byte) {
          test_input[4] = fourth_byte;
          PGComplianceTestHelper(test_input);
        }
      }
    }
  }
}

}  // namespace
}  // namespace postgres_translator::spangres::datatypes::common::jsonb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
