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

#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"

#include <cstdint>

#include "google/spanner/v1/type.pb.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/strings/str_cat.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace {

using ::google::spanner::v1::TypeAnnotationCode;
using ::postgres_translator::spangres::datatypes::CreatePgOidValue;
using ::postgres_translator::spangres::datatypes::GetPgOidType;
using ::postgres_translator::spangres::datatypes::GetPgOidValue;
using ::postgres_translator::spangres::datatypes::SpannerExtendedType;

TEST(PgOidTypeTest, GetExtendedType) {
  const SpannerExtendedType* pg_type = GetPgOidType();
  EXPECT_TRUE(pg_type->code() == TypeAnnotationCode::PG_OID);
  EXPECT_EQ(pg_type->ShortTypeName(zetasql::PRODUCT_EXTERNAL), "PG.OID");
  EXPECT_TRUE(pg_type->Equals(GetPgOidType()));
  EXPECT_TRUE(pg_type->Equivalent(GetPgOidType()));

  EXPECT_TRUE(pg_type->SupportsEquality());
  EXPECT_TRUE(pg_type->SupportsEquality(zetasql::LanguageOptions{}));
  EXPECT_TRUE(pg_type->SupportsGrouping(zetasql::LanguageOptions{}));
  EXPECT_TRUE(pg_type->SupportsPartitioning(zetasql::LanguageOptions{}));
  EXPECT_TRUE(pg_type->SupportsOrdering());
  EXPECT_TRUE(pg_type->SupportsOrdering(zetasql::LanguageOptions{},
                                        /*type_description=*/nullptr));
}

class PgOidTypeTest : public testing::TestWithParam<uint32_t> {};

using ValuePropertiesTest = PgOidTypeTest;
using ValueErrorsTest = PgOidTypeTest;

TEST_P(ValuePropertiesTest, ValueProperties) {
  uint32_t input_oid = GetParam();
  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value pg_oid, CreatePgOidValue(input_oid));
  ZETASQL_ASSERT_OK_AND_ASSIGN(int64_t oid_val, GetPgOidValue(pg_oid));
  EXPECT_EQ(oid_val, input_oid);

  EXPECT_TRUE(pg_oid.type()->Equals(GetPgOidType()));
  EXPECT_EQ(pg_oid.DebugString(), absl::StrCat(input_oid));
  EXPECT_EQ(pg_oid.Format(), absl::StrCat("PG.OID(", input_oid, ")"));
  EXPECT_EQ(pg_oid.GetSQL(), absl::StrCat("CAST('", input_oid, "' AS PG.OID)"));
  EXPECT_EQ(pg_oid.GetSQLLiteral(),
            absl::StrCat("CAST('", input_oid, "' AS PG.OID)"));
  EXPECT_FALSE(pg_oid.is_null());
}

INSTANTIATE_TEST_SUITE_P(PgOidTypeTest, ValuePropertiesTest,
                         testing::Values(0, 0xFFFFFFFF, 123));

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
