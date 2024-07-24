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

#include "third_party/spanner_pg/catalog/type.h"

#include <string>

#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace {

using TypeTest = ::postgres_translator::test::ValidMemoryContext;
using ::zetasql_base::testing::IsOkAndHolds;

// Supported Scalar Types.
TEST_F(TypeTest, PgBoolMapping) {
  const PostgresTypeMapping* pg_bool_mapping = types::PgBoolMapping();
  EXPECT_TRUE(pg_bool_mapping->mapped_type()->IsBool());
  EXPECT_EQ(pg_bool_mapping->TypeName(zetasql::PRODUCT_EXTERNAL), "pg.bool");
  EXPECT_EQ(pg_bool_mapping->PostgresTypeOid(), BOOLOID);
  EXPECT_TRUE(pg_bool_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_bool_mapping->Equals(types::PgBoolMapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const,
                       postgres_translator::internal::makeScalarConst(
                           BOOLOID, BoolGetDatum(true), /*constisnull=*/false));
  EXPECT_THAT(pg_bool_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::Bool(true)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const,
                       postgres_translator::internal::makeScalarConst(
                           BOOLOID, BoolGetDatum(0), /*constisnull=*/true));
  EXPECT_THAT(pg_bool_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullBool()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * built_const, pg_bool_mapping->MakePgConst(
                                                zetasql::Value::Bool(true)));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  EXPECT_THAT(DatumGetBool(built_const->constvalue), true);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      built_const, pg_bool_mapping->MakePgConst(zetasql::Value::NullBool()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgInt8Mapping) {
  const PostgresTypeMapping* pg_int8_mapping = types::PgInt8Mapping();
  EXPECT_TRUE(pg_int8_mapping->mapped_type()->IsInt64());
  EXPECT_EQ(pg_int8_mapping->TypeName(zetasql::PRODUCT_EXTERNAL), "pg.int8");
  EXPECT_EQ(pg_int8_mapping->PostgresTypeOid(), INT8OID);
  EXPECT_TRUE(pg_int8_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_int8_mapping->Equals(types::PgInt8Mapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * pg_const,
      postgres_translator::internal::makeScalarConst(
          INT8OID, Int8GetDatum(3141592653589793), /*constisnull=*/false));
  EXPECT_THAT(pg_int8_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::Int64(3141592653589793)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const,
                       postgres_translator::internal::makeScalarConst(
                           INT8OID, Int8GetDatum(0), /*constisnull=*/true));
  EXPECT_THAT(pg_int8_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullInt64()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * built_const,
      pg_int8_mapping->MakePgConst(zetasql::Value::Int64(3141592653589793)));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  EXPECT_THAT(DatumGetInt64(built_const->constvalue), 3141592653589793);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      built_const, pg_int8_mapping->MakePgConst(zetasql::Value::NullInt64()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgFloat8Mapping) {
  const PostgresTypeMapping* pg_float8_mapping = types::PgFloat8Mapping();
  EXPECT_TRUE(pg_float8_mapping->mapped_type()->IsDouble());
  EXPECT_EQ(pg_float8_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg.float8");
  EXPECT_EQ(pg_float8_mapping->PostgresTypeOid(), FLOAT8OID);
  EXPECT_TRUE(pg_float8_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_float8_mapping->Equals(types::PgFloat8Mapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * pg_const,
      postgres_translator::internal::makeScalarConst(
          FLOAT8OID, Float8GetDatum(3.141592653589793), /*constisnull=*/false));
  EXPECT_THAT(pg_float8_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::Double(3.141592653589793)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const,
                       postgres_translator::internal::makeScalarConst(
                           FLOAT8OID, Float8GetDatum(0), /*constisnull=*/true));
  EXPECT_THAT(pg_float8_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullDouble()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * built_const,
      pg_float8_mapping->MakePgConst(zetasql::Value::Double(1e-10)));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  EXPECT_THAT(DatumGetFloat8(built_const->constvalue), 1e-10);

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_float8_mapping->MakePgConst(
                                        zetasql::Value::NullDouble()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgVarcharMapping) {
  const PostgresTypeMapping* pg_varchar_mapping = types::PgVarcharMapping();
  EXPECT_TRUE(pg_varchar_mapping->mapped_type()->IsString());
  EXPECT_EQ(pg_varchar_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg.varchar");
  EXPECT_EQ(pg_varchar_mapping->PostgresTypeOid(), VARCHAROID);
  EXPECT_TRUE(
      pg_varchar_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_varchar_mapping->Equals(types::PgVarcharMapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const,
                       postgres_translator::internal::makeStringConst(
                           VARCHAROID, "hello world", /*constisnull=*/false));
  EXPECT_THAT(pg_varchar_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::String("hello world")));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const,
                       postgres_translator::internal::makeStringConst(
                           VARCHAROID, nullptr, /*constisnull=*/true));
  EXPECT_THAT(pg_varchar_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullString()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * built_const,
      pg_varchar_mapping->MakePgConst(zetasql::Value::String("test data")));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  ZETASQL_ASSERT_OK_AND_ASSIGN(char* output,
                       CheckedPgTextDatumGetCString(built_const->constvalue));
  EXPECT_STREQ(output, "test data");

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_varchar_mapping->MakePgConst(
                                        zetasql::Value::NullString()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgTextMapping) {
  const PostgresTypeMapping* pg_text_mapping = types::PgTextMapping();
  EXPECT_TRUE(pg_text_mapping->mapped_type()->IsString());
  EXPECT_EQ(pg_text_mapping->TypeName(zetasql::PRODUCT_EXTERNAL), "pg.text");
  EXPECT_EQ(pg_text_mapping->PostgresTypeOid(), TEXTOID);
  EXPECT_TRUE(pg_text_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_text_mapping->Equals(types::PgTextMapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const,
                       postgres_translator::internal::makeStringConst(
                           TEXTOID, "hello world", /*constisnull=*/false));
  EXPECT_THAT(pg_text_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::String("hello world")));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const, postgres_translator::internal::makeStringConst(
                                     TEXTOID, nullptr, /*constisnull=*/true));
  EXPECT_THAT(pg_text_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullString()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * built_const,
      pg_text_mapping->MakePgConst(zetasql::Value::String("test data")));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  ZETASQL_ASSERT_OK_AND_ASSIGN(char* output,
                       CheckedPgTextDatumGetCString(built_const->constvalue));
  EXPECT_STREQ(output, "test data");

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_text_mapping->MakePgConst(
                                        zetasql::Value::NullString()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgByteaMapping) {
  const PostgresTypeMapping* pg_bytea_mapping = types::PgByteaMapping();
  EXPECT_TRUE(pg_bytea_mapping->mapped_type()->IsBytes());
  EXPECT_EQ(pg_bytea_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg.bytea");
  EXPECT_EQ(pg_bytea_mapping->PostgresTypeOid(), BYTEAOID);
  EXPECT_TRUE(pg_bytea_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_bytea_mapping->Equals(types::PgByteaMapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const,
                       postgres_translator::internal::makeByteConst(
                           "\x01 bytes", /*constisnull=*/false));
  EXPECT_THAT(pg_bytea_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::Bytes("\x01 bytes")));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const, postgres_translator::internal::makeByteConst(
                                     "", /*constisnull=*/true));
  EXPECT_THAT(pg_bytea_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullBytes()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * built_const,
      pg_bytea_mapping->MakePgConst(zetasql::Value::Bytes("\x01 bytes")));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  ZETASQL_ASSERT_OK_AND_ASSIGN(char* output,
                       CheckedPgTextDatumGetCString(built_const->constvalue));
  EXPECT_STREQ(output, "\x01 bytes");

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_bytea_mapping->MakePgConst(
                                        zetasql::Value::NullBytes()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgTimestamptzMapping) {
  const PostgresTypeMapping* pg_timestamp_mapping =
      types::PgTimestamptzMapping();
  EXPECT_TRUE(pg_timestamp_mapping->mapped_type()->IsTimestamp());
  EXPECT_EQ(pg_timestamp_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg.timestamptz");
  EXPECT_EQ(pg_timestamp_mapping->PostgresTypeOid(), TIMESTAMPTZOID);
  EXPECT_TRUE(
      pg_timestamp_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_timestamp_mapping->Equals(types::PgTimestamptzMapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * pg_const,
      postgres_translator::internal::makeScalarConst(
          TIMESTAMPTZOID, TimestampTzGetDatum(1640030937311000),
          /*constisnull=*/false));
  EXPECT_THAT(pg_timestamp_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::Timestamp(absl::TimeFromTimeval(
                  {.tv_sec = 2586715737, .tv_usec = 311000}))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const, postgres_translator::internal::makeScalarConst(
                                     TIMESTAMPTZOID, TimestampTzGetDatum(0),
                                     /*constisnull=*/true));
  EXPECT_THAT(pg_timestamp_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullTimestamp()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      Const * built_const,
      pg_timestamp_mapping->MakePgConst(
          zetasql::Value::Timestamp(absl::FromUnixSeconds(1643392995))));
  EXPECT_NE(built_const, nullptr);
  EXPECT_FALSE(built_const->constisnull);
  // constvalue is a TimestampTz Datum, hence need to be converted to unix
  // seconds
  TimestampTz tztime = DatumGetTimestampTz(built_const->constvalue);
  timeval tv{0};
  tv.tv_usec = (tztime % USECS_PER_SEC);
  tv.tv_sec = ((tztime - tv.tv_usec) / USECS_PER_SEC) +
              ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

  EXPECT_THAT(absl::ToUnixSeconds(absl::TimeFromTimeval(tv)), 1643392995);

  ZETASQL_ASSERT_OK_AND_ASSIGN(built_const, pg_timestamp_mapping->MakePgConst(
                                        zetasql::Value::NullTimestamp()));
  EXPECT_NE(built_const, nullptr);
  EXPECT_TRUE(built_const->constisnull);
}

TEST_F(TypeTest, PgDateMapping) {
  const PostgresTypeMapping* pg_date_mapping = types::PgDateMapping();
  EXPECT_TRUE(pg_date_mapping->mapped_type()->IsDate());
  EXPECT_EQ(pg_date_mapping->TypeName(zetasql::PRODUCT_EXTERNAL), "pg.date");
  EXPECT_EQ(pg_date_mapping->PostgresTypeOid(), DATEOID);
  EXPECT_TRUE(pg_date_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_date_mapping->Equals(types::PgDateMapping()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const,
                       postgres_translator::internal::makeScalarConst(
                           DATEOID, DateADTGetDatum(10000),
                           /*constisnull=*/false));
  EXPECT_THAT(pg_date_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::Date(20957)));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const, postgres_translator::internal::makeScalarConst(
                                     DATEOID, DateADTGetDatum(0),
                                     /*constisnull=*/true));
  EXPECT_THAT(pg_date_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullDate()));
}

TEST_F(TypeTest, PgIntervalMapping) {
  const PostgresTypeMapping* pg_interval_mapping = types::PgIntervalMapping();
  EXPECT_TRUE(pg_interval_mapping->mapped_type()->IsInterval());
  EXPECT_EQ(pg_interval_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg.interval");
  EXPECT_EQ(pg_interval_mapping->PostgresTypeOid(), INTERVALOID);
  EXPECT_TRUE(
      pg_interval_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_interval_mapping->Equals(types::PgIntervalMapping()));

  Interval interval;

  interval.month = 10;
  interval.day = -50;
  interval.time = 5057089;

  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const,
                       CheckedPgMakeConst(
                           /*consttype=*/INTERVALOID,
                           /*consttypmod=*/-1,
                           /*constcollid=*/InvalidOid,
                           /*constlen=*/sizeof(Interval),
                           /*constvalue=*/IntervalPGetDatum(&interval),
                           /*constisnull=*/false,
                           /*constbyval=*/false));
  EXPECT_THAT(
      pg_interval_mapping->MakeGsqlValue(pg_const),
      IsOkAndHolds(zetasql::Value::Interval(
          *zetasql::IntervalValue::FromMonthsDaysMicros(10, -50, 5057089))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_const, CheckedPgMakeConst(
                                     /*consttype=*/INTERVALOID,
                                     /*consttypmod=*/-1,
                                     /*constcollid=*/InvalidOid,
                                     /*constlen=*/sizeof(Interval),
                                     /*constvalue=*/IntervalPGetDatum(nullptr),
                                     /*constisnull=*/true,
                                     /*constbyval=*/false));
  EXPECT_THAT(pg_interval_mapping->MakeGsqlValue(pg_const),
              IsOkAndHolds(zetasql::Value::NullInterval()));
}

// Supported Array Types.
TEST_F(TypeTest, PgBoolArrayMapping) {
  const PostgresTypeMapping* pg_bool_array_mapping =
      types::PgBoolArrayMapping();
  EXPECT_TRUE(pg_bool_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_bool_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsBool());
  EXPECT_EQ(pg_bool_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._bool");
  EXPECT_EQ(pg_bool_array_mapping->PostgresTypeOid(), BOOLARRAYOID);
  EXPECT_TRUE(
      pg_bool_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_bool_array_mapping->Equals(types::PgBoolArrayMapping()));
}

TEST_F(TypeTest, PgInt8ArrayMapping) {
  const PostgresTypeMapping* pg_int8_array_mapping =
      types::PgInt8ArrayMapping();
  EXPECT_TRUE(pg_int8_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_int8_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsInt64());
  EXPECT_EQ(pg_int8_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._int8");
  EXPECT_EQ(pg_int8_array_mapping->PostgresTypeOid(), INT8ARRAYOID);
  EXPECT_TRUE(
      pg_int8_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_int8_array_mapping->Equals(types::PgInt8ArrayMapping()));
}

TEST_F(TypeTest, PgFloat8ArrayMapping) {
  const PostgresTypeMapping* pg_float8_array_mapping =
      types::PgFloat8ArrayMapping();
  EXPECT_TRUE(pg_float8_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_float8_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsDouble());
  EXPECT_EQ(pg_float8_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._float8");
  EXPECT_EQ(pg_float8_array_mapping->PostgresTypeOid(), FLOAT8ARRAYOID);
  EXPECT_TRUE(
      pg_float8_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_float8_array_mapping->Equals(types::PgFloat8ArrayMapping()));
}

TEST_F(TypeTest, PgVarcharArrayMapping) {
  const PostgresTypeMapping* pg_varchar_array_mapping =
      types::PgVarcharArrayMapping();
  EXPECT_TRUE(pg_varchar_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_varchar_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsString());
  EXPECT_EQ(pg_varchar_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._varchar");
  EXPECT_EQ(pg_varchar_array_mapping->PostgresTypeOid(), VARCHARARRAYOID);
  EXPECT_TRUE(
      pg_varchar_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_varchar_array_mapping->Equals(types::PgVarcharArrayMapping()));
}

TEST_F(TypeTest, PgTextArrayMapping) {
  const PostgresTypeMapping* pg_text_array_mapping =
      types::PgTextArrayMapping();
  EXPECT_TRUE(pg_text_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_text_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsString());
  EXPECT_EQ(pg_text_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._text");
  EXPECT_EQ(pg_text_array_mapping->PostgresTypeOid(), TEXTARRAYOID);
  EXPECT_TRUE(
      pg_text_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_text_array_mapping->Equals(types::PgTextArrayMapping()));
}

TEST_F(TypeTest, PgByteaArrayMapping) {
  const PostgresTypeMapping* pg_bytea_array_mapping =
      types::PgByteaArrayMapping();
  EXPECT_TRUE(pg_bytea_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_bytea_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsBytes());
  EXPECT_EQ(pg_bytea_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._bytea");
  EXPECT_EQ(pg_bytea_array_mapping->PostgresTypeOid(), BYTEAARRAYOID);
  EXPECT_TRUE(
      pg_bytea_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_bytea_array_mapping->Equals(types::PgByteaArrayMapping()));
}

TEST_F(TypeTest, PgTimestamptzArrayMapping) {
  const PostgresTypeMapping* pg_timestamptz_array_mapping =
      types::PgTimestamptzArrayMapping();
  EXPECT_TRUE(pg_timestamptz_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_timestamptz_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsTimestamp());
  EXPECT_EQ(pg_timestamptz_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._timestamptz");
  EXPECT_EQ(pg_timestamptz_array_mapping->PostgresTypeOid(),
            TIMESTAMPTZARRAYOID);
  EXPECT_TRUE(pg_timestamptz_array_mapping->IsSupportedType(
      zetasql::LanguageOptions()));
  EXPECT_TRUE(
      pg_timestamptz_array_mapping->Equals(types::PgTimestamptzArrayMapping()));
}

TEST_F(TypeTest, PgDateArrayMapping) {
  const PostgresTypeMapping* pg_date_array_mapping =
      types::PgDateArrayMapping();
  EXPECT_TRUE(pg_date_array_mapping->mapped_type()->IsArray());
  EXPECT_TRUE(pg_date_array_mapping->mapped_type()
                  ->AsArray()
                  ->element_type()
                  ->IsDate());
  EXPECT_EQ(pg_date_array_mapping->TypeName(zetasql::PRODUCT_EXTERNAL),
            "pg._date");
  EXPECT_EQ(pg_date_array_mapping->PostgresTypeOid(), DATEARRAYOID);
  EXPECT_TRUE(
      pg_date_array_mapping->IsSupportedType(zetasql::LanguageOptions()));
  EXPECT_TRUE(pg_date_array_mapping->Equals(types::PgDateArrayMapping()));
}

// Helper function to make a PG Array Datum for creating a Const node.
// array_vals isn't const because PG doesn't understand the word, and it isn't
// an lvalue reference because the Datums *might* be moved into the output.
Datum MakePgArray(const Oid element_type, std::vector<Datum> array_vals) {
  // Lookup type properties from bootstrap.
  int16_t elmlen;
  bool elmbyval;
  char elmalign;
  get_typlenbyvalalign(element_type, &elmlen, &elmbyval, &elmalign);

  return PointerGetDatum(construct_array(array_vals.data(), array_vals.size(),
                                         element_type, elmlen, elmbyval,
                                         elmalign));
}

// Test array constant conversions (PG Const -> GSQL Value). The per-element
// logic comes from element type classes, so we'll just use INT8 and test the
// array-generic logic.
TEST_F(TypeTest, IntArrayConstantConversions) {
  std::vector<Datum> array_elements{Int8GetDatum(1), Int8GetDatum(2),
                                    Int8GetDatum(3)};
  Datum array_val = MakePgArray(INT8OID, std::move(array_elements));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Const* pg_const,
                       CheckedPgMakeConst(
                           /*consttype=*/INT8ARRAYOID,
                           /*consttypmod=*/-1,
                           /*constcollid=*/InvalidOid,
                           /*constlen=*/-1,  // Pass-by-reference
                           /*constvalue=*/array_val,
                           /*constisnull=*/false,
                           /*constbyval=*/false));
  Datum empty_array_val = MakePgArray(INT8OID, {});
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Const* empty_pg_const,
                       CheckedPgMakeConst(
                           /*consttype=*/INT8ARRAYOID,
                           /*consttypmod=*/-1,
                           /*constcollid=*/InvalidOid,
                           /*constlen=*/-1,  // Pass-by-reference
                           /*constvalue=*/PointerGetDatum(empty_array_val),
                           /*constisnull=*/false,
                           /*constbyval=*/false));
  ZETASQL_ASSERT_OK_AND_ASSIGN(const Const* null_pg_const,
                       CheckedPgMakeConst(
                           /*consttype=*/INT8ARRAYOID,
                           /*consttypmod=*/-1,
                           /*constcollid=*/InvalidOid,
                           /*constlen=*/-1,  // Pass-by-reference
                           /*constvalue=*/PointerGetDatum(nullptr),
                           /*constisnull=*/true,
                           /*constbyval=*/false));

  const PostgresTypeMapping* pg_int8array_mapping = types::PgInt8ArrayMapping();

  EXPECT_THAT(
      pg_int8array_mapping->MakeGsqlValue(pg_const),
      IsOkAndHolds(zetasql::Value::MakeArray(
                       pg_int8array_mapping->mapped_type()->AsArray(),
                       {zetasql::Value::Int64(1), zetasql::Value::Int64(2),
                        zetasql::Value::Int64(3)})
                       .value()));
  EXPECT_THAT(
      pg_int8array_mapping->MakeGsqlValue(empty_pg_const),
      IsOkAndHolds(zetasql::Value::MakeArray(
                       pg_int8array_mapping->mapped_type()->AsArray(), {})
                       .value()));
  EXPECT_THAT(pg_int8array_mapping->MakeGsqlValue(null_pg_const),
              IsOkAndHolds(
                  zetasql::Value::Null(pg_int8array_mapping->mapped_type())));
}

// Test array constant conversions (GSQL Value -> PG Const). The per-element
// logic comes from element type classes, so we'll just use INT8 and test the
// array-generic logic.
TEST_F(TypeTest, IntArrayValueConversions) {
  const PostgresTypeMapping* pg_int8array_mapping = types::PgInt8ArrayMapping();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      zetasql::Value int8_array_value,
      zetasql::Value::MakeArray(
          pg_int8array_mapping->mapped_type()->AsArray(),
          {zetasql::Value::Int64(1), zetasql::Value::Int64(2),
           zetasql::Value::NullInt64()}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Const* int8_array_const,
                       pg_int8array_mapping->MakePgConst(int8_array_value));

  // Verify array properties.
  EXPECT_EQ(int8_array_const->consttype, INT8ARRAYOID);
  EXPECT_EQ(int8_array_const->consttypmod, -1);
  EXPECT_EQ(int8_array_const->constcollid, InvalidOid);
  EXPECT_EQ(int8_array_const->constlen, -1);  // Pass-by-reference for arrays.
  EXPECT_FALSE(int8_array_const->constisnull);
  EXPECT_FALSE(int8_array_const->constbyval);
  // Verify array contents.
  ArrayType* array_type = DatumGetArrayTypeP(int8_array_const->constvalue);
  EXPECT_EQ(ARR_DIMS(array_type)[0], int8_array_value.num_elements());
  for (int i = 0; i < int8_array_value.num_elements(); ++i) {
    bool isnull;
    int index = i + 1;
    Datum element =
        array_get_element(PointerGetDatum(array_type), /*nSubscripts=*/1,
                          /*indx=*/&index, /*arraytyplen=*/-1, /*elmlen=*/8,
                          /*elmbyval=*/true, /*elmalign=*/'d', &isnull);
    EXPECT_EQ(isnull, int8_array_value.element(i).is_null())
        << "at index " << i;
    if (!int8_array_value.element(i).is_null()) {
      EXPECT_EQ(DatumGetInt32(element),
                int8_array_value.element(i).int64_value())
          << "at index " << i;
    }
  }
}

// Null array special case
TEST_F(TypeTest, IntArrayValueNullCase) {
  const PostgresTypeMapping* pg_int8array_mapping = types::PgInt8ArrayMapping();

  zetasql::Value int8_array_null =
      zetasql::Value::Null(pg_int8array_mapping->mapped_type());

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Const* null_pg_const,
                       pg_int8array_mapping->MakePgConst(int8_array_null));

  EXPECT_EQ(null_pg_const->consttype, INT8ARRAYOID);
  EXPECT_EQ(null_pg_const->consttypmod, -1);
  EXPECT_EQ(null_pg_const->constcollid, InvalidOid);
  EXPECT_EQ(null_pg_const->constlen, -1);  // Pass-by-reference for arrays.
  EXPECT_TRUE(null_pg_const->constisnull);
  EXPECT_FALSE(null_pg_const->constbyval);
}

// Empty array special case
TEST_F(TypeTest, IntArrayValueEmptyCase) {
  const PostgresTypeMapping* pg_int8array_mapping = types::PgInt8ArrayMapping();

  ZETASQL_ASSERT_OK_AND_ASSIGN(zetasql::Value int8_array_empty,
                       zetasql::Value::MakeArray(
                           pg_int8array_mapping->mapped_type()->AsArray(), {}));
  zetasql::Value int8_array_null =
      zetasql::Value::Null(pg_int8array_mapping->mapped_type());

  ZETASQL_ASSERT_OK_AND_ASSIGN(const Const* empty_pg_const,
                       pg_int8array_mapping->MakePgConst(int8_array_empty));

  EXPECT_EQ(empty_pg_const->consttype, INT8ARRAYOID);
  EXPECT_EQ(empty_pg_const->consttypmod, -1);
  EXPECT_EQ(empty_pg_const->constcollid, InvalidOid);
  EXPECT_EQ(empty_pg_const->constlen, -1);  // Pass-by-reference for arrays.
  EXPECT_FALSE(empty_pg_const->constisnull);
  EXPECT_FALSE(empty_pg_const->constbyval);
  // Array should have zero elements.
  ArrayType* array_type = DatumGetArrayTypeP(empty_pg_const->constvalue);
  EXPECT_EQ(ARR_NDIM(array_type), 0);  // Empty arrays are 0-dimensional
}

}  // namespace
}  // namespace postgres_translator
