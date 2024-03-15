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

#include "third_party/spanner_pg/catalog/spangres_type.h"

#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/datatypes/extended/pg_oid_type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator {
namespace spangres {

namespace {

namespace spangres_datatypes = postgres_translator::spangres::datatypes;

using SpangresTypeTest = ::postgres_translator::test::ValidMemoryContext;

// Returns a numeric `Const` from the given string of numeric value
// (e.g. "3.14").
Const* MakeNumericConst(absl::string_view value, bool is_null = false) {
  Datum const_value =
      DirectFunctionCall3(numeric_in, CStringGetDatum(value.data()),
                          ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
  auto const_status =
      internal::makeScalarConst(NUMERICOID, const_value, is_null);
  ABSL_CHECK_OK(const_status);
  return *const_status;
}

// Returns a JSONB `Const` from the given string.
Const* MakeJsonbConst(absl::string_view value, bool is_null = false) {
  Datum const_value = 0;

  if (!is_null) {
    Datum conval = CStringGetDatum(value.data());
    const_value = DirectFunctionCall1(jsonb_in, conval);
  }

  absl::StatusOr<Const*> retval = CheckedPgMakeConst(
      /*consttype=*/JSONBOID,
      /*consttypmod=*/-1,
      /*constcollid=*/InvalidOid,
      /*constlen=*/-1,
      /*constvalue=*/const_value,
      /*constisnull=*/is_null,
      /*constbyval=*/false);

  ABSL_CHECK_OK(retval);
  return *retval;
}

// Returns an OID `Const` from the given string.
Const* MakeOidConst(absl::string_view value, bool is_null = false) {
  Datum const_value = 0;

  if (!is_null) {
    Datum conval = CStringGetDatum(value.data());
    const_value = DirectFunctionCall1(oidin, conval);
  }

  absl::StatusOr<Const*> retval = CheckedPgMakeConst(
      /*consttype=*/OIDOID,
      /*consttypmod=*/-1,
      /*constcollid=*/InvalidOid,
      /*constlen=*/4,
      /*constvalue=*/const_value,
      /*constisnull=*/is_null,
      /*constbyval=*/true);

  ABSL_CHECK_OK(retval);
  return *retval;
}

TEST_F(SpangresTypeTest, PgNumericMapping) {
  const PostgresTypeMapping* pg_numeric_type = types::PgNumericMapping();
  const absl::string_view numeric_value = "-13.1357315957913513502000";
  const absl::StatusOr<zetasql::Value> numeric_gsql_value =
      pg_numeric_type->MakeGsqlValue(MakeNumericConst(numeric_value));
  EXPECT_TRUE(pg_numeric_type->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_EQ(pg_numeric_type->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg.numeric");
  EXPECT_EQ(pg_numeric_type->PostgresTypeOid(), NUMERICOID);
  EXPECT_TRUE(pg_numeric_type->Equals(types::PgNumericMapping()));

  zetasql::Value val;
    EXPECT_EQ(numeric_gsql_value,
              spangres_datatypes::CreatePgNumericValue(numeric_value));
    EXPECT_THAT(pg_numeric_type->MakeGsqlValue(MakeNumericConst("0", true)),
                zetasql_base::testing::IsOkAndHolds(zetasql::Value::Null(
                    spangres_datatypes::GetPgNumericType())));

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        val, spangres_datatypes::CreatePgNumericValue(numeric_value));

  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * built_const, pg_numeric_type->MakePgConst(val));
  EXPECT_NE(built_const, nullptr);

  absl::string_view const_value = DatumGetCString(
      DirectFunctionCall1(numeric_out, built_const->constvalue));
  EXPECT_FALSE(built_const->constisnull);
  EXPECT_EQ(const_value, numeric_value);

    val = zetasql::Value::Null(spangres_datatypes::GetPgNumericType());

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_numeric_type->MakePgConst(val));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(SpangresTypeTest, PgNumericArrayMapping) {
  const PostgresTypeMapping* pg_numeric_array_type =
      types::PgNumericArrayMapping();
  EXPECT_TRUE(pg_numeric_array_type->mapped_type()->IsArray());
  EXPECT_TRUE(pg_numeric_array_type->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsExtendedType());
  EXPECT_EQ(pg_numeric_array_type->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._numeric");
  EXPECT_EQ(pg_numeric_array_type->PostgresTypeOid(), NUMERICARRAYOID);
  EXPECT_TRUE(
      pg_numeric_array_type->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_numeric_array_type->Equals(types::PgNumericArrayMapping()));
}

TEST_F(SpangresTypeTest, PgJsonbMapping) {
  constexpr absl::string_view json_val = R"({"b":"str","a":1})";
  constexpr absl::string_view normalized_val = R"({"a": 1, "b": "str"})";

  const PostgresTypeMapping* pg_jsonb_type = types::PgJsonbMapping();
  EXPECT_TRUE(pg_jsonb_type->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_EQ(pg_jsonb_type->TypeName(zetasql::PRODUCT_EXTERNAL), "pg.jsonb");
  EXPECT_EQ(pg_jsonb_type->PostgresTypeOid(), JSONBOID);
  EXPECT_TRUE(pg_jsonb_type->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_jsonb_type->Equals(types::PgJsonbMapping()));

  zetasql::Value val;
    EXPECT_EQ(pg_jsonb_type->MakeGsqlValue(MakeJsonbConst(json_val, false)),
              spangres_datatypes::CreatePgJsonbValue(json_val));
    EXPECT_THAT(pg_jsonb_type->MakeGsqlValue(MakeJsonbConst("", true)),
                zetasql_base::testing::IsOkAndHolds(zetasql::Value::Null(
                    spangres_datatypes::GetPgJsonbType())));
    ZETASQL_ASSERT_OK_AND_ASSIGN(val, spangres_datatypes::CreatePgJsonbValue(json_val));

  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * built_const, pg_jsonb_type->MakePgConst(val));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  absl::string_view const_value =
      DatumGetCString(DirectFunctionCall1(jsonb_out, built_const->constvalue));
  EXPECT_EQ(const_value, normalized_val);

    val = zetasql::Value::Null(spangres_datatypes::GetPgJsonbType());
  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_jsonb_type->MakePgConst(val));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(SpangresTypeTest, PgJsonbArrayMapping) {
  const PostgresTypeMapping* pg_jsonb_array_type = types::PgJsonbArrayMapping();
  EXPECT_TRUE(pg_jsonb_array_type->mapped_type()->IsArray());
  EXPECT_TRUE(pg_jsonb_array_type->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsExtendedType());
  EXPECT_EQ(pg_jsonb_array_type->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._jsonb");
  EXPECT_EQ(pg_jsonb_array_type->PostgresTypeOid(), JSONBARRAYOID);
  EXPECT_TRUE(
      pg_jsonb_array_type->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_jsonb_array_type->Equals(types::PgJsonbArrayMapping()));
}

TEST_F(SpangresTypeTest, PgOidMapping) {
  const PostgresTypeMapping* pg_oid_type = types::PgOidMapping();
  const absl::string_view oid_string = "12345";
  const uint32_t oid_value = 12345;
  EXPECT_TRUE(pg_oid_type->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_EQ(pg_oid_type->TypeName(zetasql::PRODUCT_EXTERNAL), "pg.oid");
  EXPECT_EQ(pg_oid_type->PostgresTypeOid(), OIDOID);

  zetasql::Value val;
    EXPECT_EQ(
        pg_oid_type->MakeGsqlValue(MakeOidConst(oid_string, /*is_null=*/false)),
        spangres_datatypes::CreatePgOidValue(oid_value));
    EXPECT_THAT(pg_oid_type->MakeGsqlValue(MakeOidConst("", /*is_null=*/true)),
                zetasql_base::testing::IsOkAndHolds(zetasql::Value::Null(
                    spangres_datatypes::GetPgOidType())));

    ZETASQL_ASSERT_OK_AND_ASSIGN(val, spangres_datatypes::CreatePgOidValue(oid_value));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * built_const, pg_oid_type->MakePgConst(val));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  absl::string_view const_value =
      DatumGetCString(DirectFunctionCall1(oidout, built_const->constvalue));
  EXPECT_EQ(const_value, oid_string);

    val = zetasql::Value::Null(spangres_datatypes::GetPgOidType());

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_oid_type->MakePgConst(val));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(SpangresTypeTest, PgOidArrayMapping) {
  const PostgresTypeMapping* pg_oid_array_type = types::PgOidArrayMapping();
  EXPECT_TRUE(pg_oid_array_type->mapped_type()->IsArray());
  EXPECT_TRUE(pg_oid_array_type->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsExtendedType());
  EXPECT_EQ(pg_oid_array_type->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._oid");
  EXPECT_EQ(pg_oid_array_type->PostgresTypeOid(), OIDARRAYOID);
  EXPECT_TRUE(pg_oid_array_type->IsSupportedType(zetasql::LanguageOptions()));
}

}  // namespace
}  // namespace spangres
}  // namespace postgres_translator
