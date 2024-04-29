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


#include <cstdint>
#include <limits>
#include <optional>
#include <string>

#include "zetasql/public/cast.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/extended/conversion_finder.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"

namespace postgres_translator::spangres {
namespace datatypes {

using FindConversionOptions = ::zetasql::Catalog::FindConversionOptions;
using ConversionSourceExpressionKind =
    ::zetasql::Catalog::ConversionSourceExpressionKind;

static void TestConversion(
    const zetasql::Type* from, const zetasql::Type* to,
    const zetasql::Value& input,
    const std::optional<zetasql::Value>& expected_output,
    bool is_error = false, std::optional<absl::string_view> error_msg = "") {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Conversion conversion,
      FindExtendedTypeConversion(
          from, to,
          FindConversionOptions(
              /*is_explicit=*/true, ConversionSourceExpressionKind::kOther)));
  absl::StatusOr<zetasql::Value> converted_value =
      conversion.evaluator().Eval(input);
  if (!is_error) {
    ZETASQL_ASSERT_OK(converted_value);
    EXPECT_EQ(converted_value.value(), expected_output);
  } else {
    EXPECT_FALSE(converted_value.ok());
    EXPECT_EQ(converted_value.status().message(), error_msg);
  }
}

class PgOidConversionSuccessTest : public testing::TestWithParam<int64_t> {};

TEST_P(PgOidConversionSuccessTest, ConvertPgOidToInt64Success) {
  TestConversion(GetPgOidType(), zetasql::types::Int64Type(),
                 CreatePgOidValue(GetParam()).value(),
                 zetasql::Value::Int64(GetParam()));
}

TEST_P(PgOidConversionSuccessTest, ConvertInt64ToPgOidSuccess) {
  TestConversion(zetasql::types::Int64Type(), GetPgOidType(),
                 zetasql::Value::Int64(GetParam()),
                 CreatePgOidValue(GetParam()).value());
}

TEST_P(PgOidConversionSuccessTest, ConvertPgOidToStringSuccess) {
  TestConversion(GetPgOidType(), zetasql::types::StringType(),
                 CreatePgOidValue(GetParam()).value(),
                 zetasql::Value::String(std::to_string(GetParam())));
}

TEST_P(PgOidConversionSuccessTest, ConvertStringToPgOidSuccess) {
  TestConversion(zetasql::types::StringType(), GetPgOidType(),
                 zetasql::Value::String(std::to_string(GetParam())),
                 CreatePgOidValue(GetParam()).value());
}

INSTANTIATE_TEST_SUITE_P(PgOidSuccess, PgOidConversionSuccessTest,
                         testing::Values(std::numeric_limits<uint32_t>::min(),
                                         12345,
                                         std::numeric_limits<uint32_t>::max()));

class PgOidConversionErrorTest : public testing::TestWithParam<int64_t> {};

TEST_P(PgOidConversionErrorTest, ConvertInt64ToPgOidError) {
  TestConversion(zetasql::types::Int64Type(), GetPgOidType(),
                 zetasql::Value::Int64(GetParam()), std::nullopt,
                 /* is_error= */ true, "bigint out of range [0, 4294967295]");
}

TEST_P(PgOidConversionErrorTest, ConvertStringToPgOidError) {
  TestConversion(zetasql::types::StringType(), GetPgOidType(),
                 zetasql::Value::String(std::to_string(GetParam())),
                 std::nullopt, /* is_error= */ true,
                 "varchar out of range [0, 4294967295]");
}

INSTANTIATE_TEST_SUITE_P(
    PgOidInt64Error, PgOidConversionErrorTest,
    testing::Values(std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<int64_t>::max(),
                    (int64_t)std::numeric_limits<uint32_t>::min() - 1,
                    (int64_t)std::numeric_limits<uint32_t>::max() + 1));

TEST(PGOidConversionErrorTest, CoverStringToPgOidInvalidString) {
  TestConversion(zetasql::types::StringType(), GetPgOidType(),
                 zetasql::Value::String("invalid string"), std::nullopt,
                 /* is_error = */ true, "invalid varchar");
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
