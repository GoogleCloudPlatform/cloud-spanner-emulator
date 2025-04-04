//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#include "backend/schema/catalog/udf.h"

#include <memory>
#include <vector>

#include "zetasql/public/json_value.h"
#include "zetasql/public/numeric_value.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/named_schema.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/sequence.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/catalog/view.h"
#include "backend/schema/graph/schema_node.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// For the following tests, a custom PG DDL statement is required as translating
// expressions from GSQL to PG is not supported in tests.
using absl::StatusCode;
using database_api::DatabaseDialect::POSTGRESQL;
using zetasql::Value;
using ::testing::HasSubstr;

TEST_P(SchemaUpdaterTest, CreateUDF_Basic) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))"}));

  const Udf* udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "x+1");
  EXPECT_EQ(udf->determinism_level(), Udf::Determinism::DETERMINISTIC);
  const absl::Span<const Udf* const> udfs = schema->udfs();
  ASSERT_EQ(udfs.size(), 1);
  EXPECT_EQ(udfs[0]->Name(), "udf_1");
  EXPECT_EQ(udfs[0]->body(), "x+1");
  EXPECT_EQ(udf->signature()->DebugString(), "(INT64 x) -> INT64");

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                       UpdateSchema(schema.get(), {R"(DROP FUNCTION udf_1)"}));
  udf = schema->FindUdf("udf_1");
  ASSERT_EQ(udf, nullptr);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema, UpdateSchema(schema.get(), {R"(DROP FUNCTION IF EXISTS udf_1)"}));
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(DROP FUNCTION udf_1)"}),
              StatusIs(error::FunctionNotFound("udf_1")));

  // Ensure the emulator can identify the determinism level of the UDF.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      CreateSchema(
          {R"(CREATE FUNCTION NOW(x INT64) RETURNS TIMESTAMP SQL SECURITY
            INVOKER AS ((SELECT CURRENT_TIMESTAMP())))"}));

  udf = schema->FindUdf("NOW");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "NOW");
  EXPECT_EQ(udf->body(), "(SELECT CURRENT_TIMESTAMP())");
  EXPECT_EQ(udf->determinism_level(),
            Udf::Determinism::NOT_DETERMINISTIC_STABLE);

  // Ensure the emulator can handle UDFs with default arguments.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      CreateSchema(
          {R"(CREATE FUNCTION test_udf(x INT64 DEFAULT 1) RETURNS INT64 SQL
            SECURITY INVOKER AS (x+1))"}));
  udf = schema->FindUdf("test_udf");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "test_udf");
  EXPECT_EQ(udf->body(), "x+1");
  EXPECT_EQ(udf->signature()->DebugString(), "(optional INT64 x) -> INT64");
  EXPECT_EQ(udf->signature()->arguments()[0].GetDefault().value(),
            zetasql::Value::Int64(1));
}

TEST_P(SchemaUpdaterTest, CreateUDF_ParameterizedTypes) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(
      CreateSchema({R"(CREATE FUNCTION func(a STRING(10)) RETURNS STRING(MAX)
                         SQL SECURITY INVOKER AS (CONCAT(a, ' world')))"}),
      ::zetasql_base::testing::StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr(
              "Parameterized types are not supported in function arguments")));

  EXPECT_THAT(
      CreateSchema({R"(CREATE FUNCTION func(a STRING) RETURNS STRING(10)
                         SQL SECURITY INVOKER AS (SUBSTR(a, 1, 10)))"}),
      StatusIs(error::FunctionTypeMismatch("func", "STRING(10)", "STRING")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({R"(CREATE FUNCTION func(a STRING) RETURNS STRING
                         SQL SECURITY INVOKER AS (SUBSTR(a, 1, 10)))"}));
  const Udf* udf = schema->FindUdf("func");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "func");
  EXPECT_EQ(udf->body(), "SUBSTR(a, 1, 10)");
  EXPECT_EQ(udf->determinism_level(), Udf::Determinism::DETERMINISTIC);
  EXPECT_EQ(udf->signature()->DebugString(), "(STRING a) -> STRING");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      CreateSchema({R"(CREATE FUNCTION func(a STRING) RETURNS ARRAY<STRING>
                         SQL SECURITY INVOKER AS ([a]))"}));
  udf = schema->FindUdf("func");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "func");
  EXPECT_EQ(udf->body(), "[a]");
  EXPECT_EQ(udf->determinism_level(), Udf::Determinism::DETERMINISTIC);
  EXPECT_EQ(udf->signature()->DebugString(), "(STRING a) -> ARRAY<STRING>");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      CreateSchema({R"(CREATE FUNCTION func(a ARRAY<STRING>) RETURNS STRING
                 SQL SECURITY INVOKER AS (ARRAY_TO_STRING(a, ', ')))"}));
  udf = schema->FindUdf("func");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "func");
  EXPECT_EQ(udf->body(), "ARRAY_TO_STRING(a, ', ')");
  EXPECT_EQ(udf->determinism_level(), Udf::Determinism::DETERMINISTIC);
  EXPECT_EQ(udf->signature()->DebugString(), "(ARRAY<STRING> a) -> STRING");
}

TEST_P(SchemaUpdaterTest, CreateUDF_DuplicateName) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+2))"}),
      StatusIs(error::SchemaObjectAlreadyExists("Function", "udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_FunctionTypeMismatch) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(
      CreateSchema({
          R"(CREATE FUNCTION udf_1(x INT64) RETURNS STRING(MAX) SQL SECURITY INVOKER
            AS (x+1))"}),
      StatusIs(error::FunctionTypeMismatch("udf_1", "STRING(MAX)", "INT64")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_ReplaceBuiltInFunction) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(
      CreateSchema({R"(CREATE OR REPLACE FUNCTION abs(x INT64) RETURNS INT64 SQL
                      SECURITY INVOKER AS (x+1))"}),
      StatusIs(error::ReplacingBuiltInFunction("create or replace", "Function",
                                               "abs")));

  EXPECT_THAT(
      CreateSchema(
          {R"(CREATE VIEW abs SQL SECURITY INVOKER AS SELECT 1 AS col1)"}),
      StatusIs(error::ReplacingBuiltInFunction("create", "View", "abs")));

  // Can have the same name as a built-in function if in named schema.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE SCHEMA my_schema)",
           R"(CREATE FUNCTION my_schema.abs(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (x+1))"}));

  const NamedSchema* my_schema = schema->FindNamedSchema("my_schema");
  ASSERT_NE(my_schema, nullptr);
  const Udf* udf = my_schema->FindUdf("my_schema.abs");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "my_schema.abs");
}

TEST_P(SchemaUpdaterTest, CreateUDF_InvalidBodyAnalysis) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(
      CreateSchema({R"(CREATE FUNCTION func(x INT64) RETURNS INT64 SQL
                      SECURITY INVOKER AS (x+func_2(x)))"}),
      ::zetasql_base::testing::StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Error parsing the definition of function `func`")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION func(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))"}));
  const Udf* udf = schema->FindUdf("func");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "func");
  EXPECT_THAT(UpdateSchema(
                  schema.get(),
                  {R"(CREATE OR REPLACE FUNCTION func(x INT64) RETURNS INT64 SQL
                      SECURITY INVOKER AS (x+func_2(x)))"}),
              ::zetasql_base::testing::StatusIs(
                  StatusCode::kFailedPrecondition,
                  HasSubstr("Cannot replace FUNCTION `func` "
                            "because new definition is invalid")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_CreateOrReplace) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE OR REPLACE FUNCTION udf_1(x INT64) RETURNS INT64 SQL
            SECURITY INVOKER AS (x+1))"}));

  const Udf* udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "x+1");

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x INT64) RETURNS INT64 SQL
          SECURITY INVOKER AS (x+2))"}));

  udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "x+2");
}

TEST_P(SchemaUpdaterTest, CreateUDF_WithTableDepedency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS ((SELECT MAX(t.col1) FROM t) + x))"}));

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);

  const Udf* udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "(SELECT MAX(t.col1) FROM t) + x");
  EXPECT_EQ(udf->dependencies().size(), 2);
  EXPECT_THAT(udf->dependencies(),
              testing::UnorderedElementsAreArray(
                  (std::vector<const SchemaNode*>{t, t->FindColumn("col1")})));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(DROP TABLE t)"}),
      StatusIs(error::InvalidDropDependentFunction("TABLE", "t", "udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_WithViewDepedency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE VIEW v SQL SECURITY INVOKER AS SELECT MAX(t.col1) AS col1
            FROM t)",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS ((SELECT v.col1 FROM v) + x))"}));

  const View* v = schema->FindView("v");
  ASSERT_NE(v, nullptr);

  const Udf* udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "(SELECT v.col1 FROM v) + x");
  EXPECT_EQ(udf->dependencies().size(), 1);
  EXPECT_THAT(udf->dependencies(), testing::UnorderedElementsAreArray(
                                       (std::vector<const SchemaNode*>{v})));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(DROP VIEW v)"}),
      StatusIs(error::InvalidDropDependentFunction("VIEW", "v", "udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_WithIndexDependency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE INDEX idx ON t(col2))",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS ((SELECT MAX(t.col1) FROM t@{FORCE_INDEX=idx}) + x))"}));

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);

  const Index* idx = t->FindIndex("idx");
  ASSERT_NE(idx, nullptr);

  const Udf* udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "(SELECT MAX(t.col1) FROM t@{FORCE_INDEX=idx}) + x");
  EXPECT_EQ(udf->dependencies().size(), 3);
  EXPECT_THAT(
      udf->dependencies(),
      testing::UnorderedElementsAreArray(
          (std::vector<const SchemaNode*>{t, t->FindColumn("col1"), idx})));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(DROP INDEX idx)"}),
      StatusIs(error::InvalidDropDependentFunction("INDEX", "idx", "udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_WithSequenceDependency) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE SEQUENCE s OPTIONS(sequence_kind="bit_reversed_positive"))",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS ((get_internal_sequence_state(SEQUENCE s)) + x))"}));

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);

  const Sequence* s = schema->FindSequence("s");
  ASSERT_NE(s, nullptr);

  const Udf* udf = schema->FindUdf("udf_1");
  ASSERT_NE(udf, nullptr);
  EXPECT_EQ(udf->Name(), "udf_1");
  EXPECT_EQ(udf->body(), "(get_internal_sequence_state(SEQUENCE s)) + x");
  EXPECT_EQ(udf->dependencies().size(), 1);
  EXPECT_THAT(udf->dependencies(), testing::UnorderedElementsAreArray(
                                       (std::vector<const SchemaNode*>{s})));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(DROP SEQUENCE s)"}),
      StatusIs(error::InvalidDropDependentFunction("SEQUENCE", "s", "udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_WithVariousDefaultParamsAndLiterals) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema,
                       CreateSchema({R"(CREATE FUNCTION func_int64(
                        x INT64 DEFAULT 1
                      ) RETURNS INT64 SQL SECURITY INVOKER AS (x + 1))"}));
  const Udf* udf_int64 = schema->FindUdf("func_int64");
  ASSERT_NE(udf_int64, nullptr);
  EXPECT_EQ(udf_int64->Name(), "func_int64");
  EXPECT_EQ(udf_int64->body(), "x + 1");
  EXPECT_EQ(udf_int64->signature()->DebugString(),
            "(optional INT64 x) -> INT64");
  EXPECT_EQ(udf_int64->signature()->arguments()[0].GetDefault().value(),
            Value::Int64(1));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_numeric(
                        x NUMERIC DEFAULT NUMERIC '123.456'
                      ) RETURNS NUMERIC SQL SECURITY INVOKER AS (x * 2))"}));
  const Udf* udf_numeric = schema->FindUdf("func_numeric");
  ASSERT_NE(udf_numeric, nullptr);
  EXPECT_EQ(udf_numeric->Name(), "func_numeric");
  EXPECT_EQ(udf_numeric->body(), "x * 2");
  EXPECT_EQ(udf_numeric->signature()->DebugString(),
            "(optional NUMERIC x) -> NUMERIC");
  EXPECT_EQ(udf_numeric->signature()->arguments()[0].GetDefault().value(),
            Value::Numeric(
                zetasql::NumericValue::FromStringStrict("123.456").value()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_string(
                        x STRING DEFAULT 'default_string'
                      ) RETURNS STRING SQL SECURITY INVOKER AS (CONCAT(x, '_suffix')))"}));
  const Udf* udf_string = schema->FindUdf("func_string");
  ASSERT_NE(udf_string, nullptr);
  EXPECT_EQ(udf_string->Name(), "func_string");
  EXPECT_EQ(udf_string->body(), "CONCAT(x, '_suffix')");
  EXPECT_EQ(udf_string->signature()->DebugString(),
            "(optional STRING x) -> STRING");
  EXPECT_EQ(udf_string->signature()->arguments()[0].GetDefault().value(),
            Value::String("default_string"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      CreateSchema(
          {R"(CREATE FUNCTION func_bool(x BOOL DEFAULT TRUE) RETURNS BOOL SQL
            SECURITY INVOKER AS (NOT x))"}));
  const Udf* udf_bool = schema->FindUdf("func_bool");
  ASSERT_NE(udf_bool, nullptr);
  EXPECT_EQ(udf_bool->Name(), "func_bool");
  EXPECT_EQ(udf_bool->body(), "NOT x");
  EXPECT_EQ(udf_bool->signature()->DebugString(), "(optional BOOL x) -> BOOL");
  EXPECT_EQ(udf_bool->signature()->arguments()[0].GetDefault().value(),
            Value::Bool(true));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_timestamp(
                        x TIMESTAMP DEFAULT TIMESTAMP '2021-01-01 00:00:00+00'
                      ) RETURNS TIMESTAMP SQL SECURITY INVOKER AS (x))"}));
  const Udf* udf_timestamp = schema->FindUdf("func_timestamp");
  ASSERT_NE(udf_timestamp, nullptr);
  EXPECT_EQ(udf_timestamp->Name(), "func_timestamp");
  EXPECT_EQ(udf_timestamp->body(), "x");
  EXPECT_EQ(udf_timestamp->signature()->DebugString(),
            "(optional TIMESTAMP x) -> TIMESTAMP");
  EXPECT_EQ(udf_timestamp->signature()->arguments()[0].GetDefault().value(),
            Value::Timestamp(absl::FromUnixSeconds(1609459200)));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_struct(
                        x STRUCT<field1 INT64> DEFAULT STRUCT<field1 INT64>(1)
                      ) RETURNS INT64 SQL SECURITY INVOKER AS
                      (x.field1))"}));
  const Udf* udf_struct = schema->FindUdf("func_struct");
  ASSERT_NE(udf_struct, nullptr);
  EXPECT_EQ(udf_struct->Name(), "func_struct");
  EXPECT_EQ(udf_struct->body(), "x.field1");
  EXPECT_EQ(udf_struct->signature()->DebugString(),
            "(optional STRUCT<field1 INT64> x) -> INT64");

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_json(
                        x JSON DEFAULT JSON '{"key": "value"}'
                      ) RETURNS JSON SQL SECURITY INVOKER AS (x))"}));
  const Udf* udf_json = schema->FindUdf("func_json");
  ASSERT_NE(udf_json, nullptr);
  EXPECT_EQ(udf_json->Name(), "func_json");
  EXPECT_EQ(udf_json->body(), "x");
  EXPECT_EQ(udf_json->signature()->DebugString(), "(optional JSON x) -> JSON");
  EXPECT_EQ(
      udf_json->signature()->arguments()[0].GetDefault().value(),
      Value::Json(zetasql::JSONValue::ParseJSONString("{\"key\": \"value\"}")
                      .value()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_null(
                        x INT64 DEFAULT NULL
                      ) RETURNS INT64 SQL SECURITY INVOKER AS (x))"}));
  const Udf* udf_null = schema->FindUdf("func_null");
  ASSERT_NE(udf_null, nullptr);
  EXPECT_EQ(udf_null->Name(), "func_null");
  EXPECT_EQ(udf_null->body(), "x");
  EXPECT_EQ(udf_null->signature()->DebugString(),
            "(optional INT64 x) -> INT64");
  EXPECT_TRUE(
      udf_null->signature()->arguments()[0].GetDefault().value().is_null());

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, CreateSchema({R"(CREATE FUNCTION func_array_int(
                        x ARRAY<INT64> DEFAULT [1, 2, 3]
                      ) RETURNS INT64 SQL SECURITY INVOKER AS (ARRAY_LENGTH(x)))"}));
  const Udf* udf_array_int = schema->FindUdf("func_array_int");
  ASSERT_NE(udf_array_int, nullptr);
  EXPECT_EQ(udf_array_int->Name(), "func_array_int");
  EXPECT_EQ(udf_array_int->body(), "ARRAY_LENGTH(x)");
  EXPECT_EQ(udf_array_int->signature()->DebugString(),
            "(optional ARRAY<INT64> x) -> INT64");
  EXPECT_EQ(udf_array_int->signature()->arguments()[0].GetDefault().value(),
            Value::Array(zetasql::types::Int64ArrayType(),
                         {Value::Int64(1), Value::Int64(2), Value::Int64(3)}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema,
                       CreateSchema({R"(CREATE FUNCTION func_array_string(
                        x ARRAY<STRING> DEFAULT ['x', 'y', 'xy']
                      ) RETURNS INT64 SQL SECURITY INVOKER AS (ARRAY_LENGTH(x)))"}));
  const Udf* udf_array_string = schema->FindUdf("func_array_string");
  ASSERT_NE(udf_array_string, nullptr);
  EXPECT_EQ(udf_array_string->Name(), "func_array_string");
  EXPECT_EQ(udf_array_string->body(), "ARRAY_LENGTH(x)");
  EXPECT_EQ(udf_array_string->signature()->DebugString(),
            "(optional ARRAY<STRING> x) -> INT64");
  EXPECT_EQ(udf_array_string->signature()->arguments()[0].GetDefault().value(),
            Value::Array(
                zetasql::types::StringArrayType(),
                {Value::String("x"), Value::String("y"), Value::String("xy")}));

  EXPECT_THAT(
      CreateSchema({R"(CREATE FUNCTION func_array_array_int(
                        x ARRAY<ARRAY<INT64>> DEFAULT [[1, 2], [3, 4]]
                      ) RETURNS INT64 SQL SECURITY INVOKER AS (ARRAY_LENGTH(x)))"}),
      zetasql_base::testing::StatusIs(
          StatusCode::kInvalidArgument,
          HasSubstr("Cannot construct array with element type ARRAY<INT64> "
                    "because nested arrays are not supported")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_ViewsAndFunctionsWithSameName) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(
      CreateSchema(
          {R"(CREATE VIEW foo SQL SECURITY INVOKER AS SELECT 1 AS col1)",
           R"(CREATE FUNCTION foo(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))"}),
      StatusIs(error::SchemaObjectAlreadyExists("View", "foo")));

  EXPECT_THAT(
      CreateSchema({
          R"(CREATE FUNCTION foo(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
          R"(CREATE VIEW foo SQL SECURITY INVOKER AS SELECT 1 AS col1)",
      }),
      StatusIs(error::SchemaObjectAlreadyExists("Function", "foo")));

  EXPECT_THAT(
      CreateSchema(
          {R"(CREATE VIEW foo SQL SECURITY INVOKER AS SELECT 1 AS col1)",
           R"(CREATE OR REPLACE FUNCTION foo(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (x+1))"}),
      StatusIs(error::SchemaObjectAlreadyExists("View", "foo")));

  EXPECT_THAT(
      CreateSchema({
          R"(CREATE FUNCTION foo(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
          R"(CREATE OR REPLACE VIEW foo SQL SECURITY INVOKER AS SELECT 1 AS col1)",
      }),
      StatusIs(error::SchemaObjectAlreadyExists("Function", "foo")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_UsingInUdf) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE FUNCTION udf_2(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (2 + udf_1(x)))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->dependencies().size(), 0);

  const Udf* udf_2 = schema->FindUdf("udf_2");
  ASSERT_NE(udf_2, nullptr);
  EXPECT_EQ(udf_2->dependencies().size(), 1);
  EXPECT_THAT(udf_2->dependencies(),
              testing::UnorderedElementsAreArray(
                  (std::vector<const SchemaNode*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(DROP FUNCTION udf_1)"}),
      StatusIs(error::InvalidDropDependentFunction("UDF", "udf_1", "udf_2")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_UsingInTable) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER AS (x+1))",
           R"(CREATE TABLE t1 (col1 INT64 AS (udf_1(10)), col2 INT64) PRIMARY KEY(col2))",
           R"(CREATE TABLE t2 (col1 INT64, col2 INT64 DEFAULT (udf_1(10))) PRIMARY KEY(col1))",
           R"(CREATE FUNCTION udf_2(x INT64) RETURNS INT64 SQL SECURITY INVOKER AS (x + 1))",
           R"(CREATE TABLE t3 (col1 INT64, col2 INT64, CONSTRAINT c1 CHECK(udf_2(col2) > 0)) PRIMARY KEY(col1))"}));  // Crash OK

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->dependencies().size(), 0);

  const Table* t1 = schema->FindTable("t1");
  ASSERT_NE(t1, nullptr);
  EXPECT_THAT(
      t1->FindColumn("col1")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_1})));

  const Table* t2 = schema->FindTable("t2");
  ASSERT_NE(t2, nullptr);
  EXPECT_THAT(
      t2->FindColumn("col2")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_1})));

  const Udf* udf_2 = schema->FindUdf("udf_2");
  ASSERT_NE(udf_2, nullptr);
  EXPECT_EQ(udf_2->dependencies().size(), 0);

  const Table* t3 = schema->FindTable("t3");
  ASSERT_NE(t3, nullptr);
  EXPECT_THAT(
      t3->FindCheckConstraint("c1")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_2})));

  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {R"(ALTER TABLE t1 ALTER COLUMN col1 INT64 AS (1))"}),
      StatusIs(error::CannotAlterGeneratedColumnExpression("t1", "col1")));

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      schema,
      UpdateSchema(schema.get(),
                   {R"(ALTER TABLE t2 ALTER COLUMN col2 SET DEFAULT (1))"}));

  t2 = schema->FindTable("t2");
  ASSERT_NE(t2, nullptr);
  EXPECT_EQ(t2->FindColumn("col2")->udf_dependencies().size(), 0);

  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(DROP FUNCTION udf_1)"}),
      StatusIs(error::InvalidDropDependentColumn("UDF", "udf_1", "t1.col1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_UsingInIndexes) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  // Indexes will not directly depend on UDFs; though, they may depend on UDFs
  // when index expressions are supported.
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
              AS (CASE WHEN x > 10 THEN NULL ELSE x + 1 END))",
           R"(CREATE TABLE t1 (col1 INT64, col2 INT64 AS (udf_1(col1)))
              PRIMARY KEY (col1))",
           R"(CREATE INDEX Idx1 ON t1(col2) WHERE col1 IS NOT NULL)"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const Table* t1 = schema->FindTable("t1");
  ASSERT_NE(t1, nullptr);
  const Column* col1 = t1->FindColumn("col1");
  ASSERT_NE(col1, nullptr);
  const Column* col2 = t1->FindColumn("col2");
  ASSERT_NE(col2, nullptr);

  EXPECT_THAT(col2->udf_dependencies(), testing::UnorderedElementsAreArray(
                                            (std::vector<const Udf*>{udf_1})));

  const Index* idx1 = schema->FindIndex("Idx1");
  ASSERT_NE(idx1, nullptr);

  ASSERT_EQ(idx1->key_columns().size(), 1);
  const KeyColumn* key_col = idx1->key_columns()[0];
  ASSERT_NE(key_col, nullptr);
  EXPECT_THAT(key_col->column(), SourceColumnIs(col2));

  // Stored expressions in indexes are not supported; however, dependency
  // checking should occur if this is supported in the future.
  EXPECT_THAT(
      UpdateSchema(schema.get(),
                   {R"(CREATE INDEX Idx2 ON t1 (col2) STORING (col2+1))"}),
      ::zetasql_base::testing::StatusIs(StatusCode::kInvalidArgument,
                                  HasSubstr("Expecting ')' but found '+'")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_UsingInViews) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
          R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
          R"(CREATE VIEW v SQL SECURITY INVOKER AS SELECT t.col1, udf_1(t.col2)
            AS col2 FROM t)"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_THAT(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_THAT(t->Name(), "t");

  const View* v = schema->FindView("v");
  ASSERT_NE(v, nullptr);
  EXPECT_THAT(v->Name(), "v");
  EXPECT_THAT(
      v->dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const SchemaNode*>{
          t, t->FindColumn("col2"), t->FindColumn("col1"), udf_1})));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(DROP FUNCTION udf_1)"}),
              StatusIs(error::InvalidDropDependentViews("UDF", "udf_1", "v")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_TransitiveFunctionDeterminism) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS TIMESTAMP SQL SECURITY INVOKER
            AS ((SELECT CURRENT_TIMESTAMP())))",
           R"(CREATE FUNCTION udf_2(x INT64) RETURNS STRING SQL SECURITY INVOKER
            AS (CONCAT('Timestamp: ', CAST(udf_1(x) as STRING))))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  const Udf* udf_2 = schema->FindUdf("udf_2");
  EXPECT_EQ(udf_1->determinism_level(),
            Udf::Determinism::NOT_DETERMINISTIC_STABLE);
  EXPECT_EQ(udf_2->determinism_level(),
            Udf::Determinism::NOT_DETERMINISTIC_STABLE);

  // Volatile functions should be transitive, but are not supported in the
  // emulator.
  EXPECT_THAT(
      CreateSchema(
          {R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (EXTRACT(YEAR from CURRENT_TIMESTAMP())))",
           R"(CREATE FUNCTION udf_2(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS ((SELECT APPROX_DOT_PRODUCT([100, 10], [200, 6]) AS results) + udf_1(x)))"}),
      StatusIs(error::FunctionTypeMismatch("udf_2", "INT64", "FLOAT64")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_CyclicDependencyOnViewFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY
          INVOKER
            AS (x+1))",
          R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
          R"(CREATE VIEW v SQL SECURITY INVOKER AS SELECT t.col1,
          udf_1(t.col2)
            AS col2 FROM t)"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_THAT(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_THAT(t->Name(), "t");

  const View* v = schema->FindView("v");
  ASSERT_NE(v, nullptr);
  EXPECT_THAT(v->Name(), "v");
  EXPECT_THAT(
      v->dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const SchemaNode*>{
          t, t->FindColumn("col2"), t->FindColumn("col1"), udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x INT64) RETURNS INT64 SQL
          SECURITY INVOKER AS ((SELECT v.col2 FROM v) + x))"}),
      StatusIs(error::ViewReplaceRecursive("udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_CyclicDependencyOnUdfFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY
          INVOKER AS (x+1))",
          R"(CREATE FUNCTION udf_2(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (udf_1(x) + 2))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_THAT(udf_1->Name(), "udf_1");

  const Udf* udf_2 = schema->FindUdf("udf_2");
  ASSERT_NE(udf_2, nullptr);
  EXPECT_THAT(udf_2->Name(), "udf_2");
  EXPECT_THAT(udf_2->dependencies(),
              testing::UnorderedElementsAreArray(
                  (std::vector<const SchemaNode*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x INT64) RETURNS INT64 SQL
          SECURITY INVOKER AS (udf_2(x) + 1))"}),
      StatusIs(error::ViewReplaceRecursive("udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_CyclicDependencyOnColumnExpressionFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE TABLE t (col1 INT64, col2 INT64, col3 INT64 AS
            (udf_1(col2))) PRIMARY KEY (col1))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_THAT(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_THAT(t->Name(), "t");
  EXPECT_THAT(
      t->FindColumn("col3")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x INT64) RETURNS INT64 SQL
            SECURITY INVOKER AS ((SELECT MAX(t.col3) FROM t) + x))"}),
      StatusIs(error::ViewReplaceRecursive("udf_1")));
}

TEST_P(SchemaUpdaterTest, CreateUDF_InvalidNewSignatureOnUdfFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY
          INVOKER AS (x+1))",
          R"(CREATE FUNCTION udf_2(x INT64) RETURNS INT64 SQL SECURITY
            INVOKER AS (udf_1(x) + 2))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const Udf* udf_2 = schema->FindUdf("udf_2");
  ASSERT_NE(udf_2, nullptr);
  EXPECT_EQ(udf_2->Name(), "udf_2");
  EXPECT_THAT(udf_2->dependencies(),
              testing::UnorderedElementsAreArray(
                  (std::vector<const SchemaNode*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x BOOL) RETURNS STRING(MAX) SQL
            SECURITY INVOKER AS (CASE WHEN x THEN 'true' ELSE 'false' END || 'c'))"}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, CreateUDF_InvalidNewSignatureOnViewFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE VIEW v SQL SECURITY INVOKER AS SELECT t.col1,
            (udf_1(t.col2) + 2) AS col2 FROM t)"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->Name(), "t");

  const View* v = schema->FindView("v");
  ASSERT_NE(v, nullptr);
  EXPECT_EQ(v->Name(), "v");
  EXPECT_THAT(
      v->dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const SchemaNode*>{
          t, t->FindColumn("col2"), t->FindColumn("col1"), udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x BOOL) RETURNS STRING(MAX)
          SQL SECURITY INVOKER AS (CASE WHEN x THEN 'true' ELSE 'false' END
          || 'c'))"}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, CreateUDF_InvalidNewSignatureOnCheckConstraintFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE TABLE t (col1 INT64, col2 INT64, CONSTRAINT c1
            CHECK(udf_1(col2) > 0)) PRIMARY KEY(col1))"}));  // Crash OK

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->Name(), "t");
  EXPECT_THAT(
      t->FindCheckConstraint("c1")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x BOOL) RETURNS STRING(MAX)
          SQL SECURITY INVOKER AS (CASE WHEN x THEN 'true' ELSE 'false' END
          || 'c'))"}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest,
       CreateUDF_InvalidNewSignatureOnColumnExpressionFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE TABLE t (col1 INT64, col2 INT64, col3 INT64 AS
            (udf_1(col2))) PRIMARY KEY (col1))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->Name(), "t");
  EXPECT_THAT(
      t->FindColumn("col3")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x BOOL) RETURNS STRING(MAX)
          SQL SECURITY INVOKER AS (CASE WHEN x THEN 'true' ELSE 'false' END
          || 'c'))"}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));

  // Changing generated column expression is not supported, so no need to handle
  // this cyclic case.
}

TEST_P(SchemaUpdaterTest, CreateUDF_InvalidNewSignatureOnDefaultValueFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema(
          {R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS (x+1))",
           R"(CREATE TABLE t (col1 INT64, col2 INT64 DEFAULT (udf_1(10)))
            PRIMARY KEY(col1))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  const Table* t = schema->FindTable("t");
  ASSERT_NE(t, nullptr);
  EXPECT_EQ(t->Name(), "t");
  EXPECT_THAT(
      t->FindColumn("col2")->udf_dependencies(),
      testing::UnorderedElementsAreArray((std::vector<const Udf*>{udf_1})));

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x BOOL) RETURNS STRING(MAX)
          SQL SECURITY INVOKER AS (CASE WHEN x THEN 'true' ELSE 'false' END
          || 'c'))"}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, CreateUDF_InvalidNewSignatureOnSequenceFails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const Schema> schema,
      CreateSchema({
          R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64
            SQL SECURITY INVOKER AS (x+1))",
          R"(CREATE TABLE t (col1 INT64, col2 INT64 DEFAULT (udf_1(10)))
            PRIMARY KEY(col1))"}));

  const Udf* udf_1 = schema->FindUdf("udf_1");
  ASSERT_NE(udf_1, nullptr);
  EXPECT_EQ(udf_1->Name(), "udf_1");

  EXPECT_THAT(
      UpdateSchema(
          schema.get(),
          {R"(CREATE OR REPLACE FUNCTION udf_1(x BOOL) RETURNS STRING(MAX)
          SQL SECURITY INVOKER AS (CASE WHEN x THEN 'true' ELSE 'false' END
          || 'c'))"}),
      zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, CreateUDF_WithForUpdateInvalid) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();

  EXPECT_THAT(
      CreateSchema(
          {R"(CREATE TABLE t (col1 INT64, col2 INT64) PRIMARY KEY (col1))",
           R"(CREATE FUNCTION udf_1(x INT64) RETURNS INT64 SQL SECURITY INVOKER
            AS ((SELECT MAX(t.col1) FROM t FOR UPDATE) + x))"}),
      ::zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          testing::HasSubstr("Unexpected lock mode in function body query")));
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
