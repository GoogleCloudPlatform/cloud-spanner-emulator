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

#include "third_party/spanner_pg/test_catalog/emulator_catalog.h"

#include <memory>
#include <string>
#include <utility>

#include "zetasql/public/catalog.h"
#include "zetasql/public/type.h"
#include "backend/query/catalog.h"
#include "backend/query/function_catalog.h"
#include "backend/schema/builders/change_stream_builder.h"
#include "backend/schema/builders/column_builder.h"
#include "backend/schema/builders/table_builder.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/graph/schema_graph.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/catalog/type.h"

using google::spanner::emulator::backend::Catalog;
using google::spanner::emulator::backend::ChangeStream;
using google::spanner::emulator::backend::Column;
using google::spanner::emulator::backend::FunctionCatalog;
using google::spanner::emulator::backend::KeyColumn;
using google::spanner::emulator::backend::OwningSchema;
using google::spanner::emulator::backend::SchemaGraph;
using google::spanner::emulator::backend::Table;

namespace postgres_translator::spangres::test {

namespace {

Table::Builder table_builder(const std::string& name) {
  Table::Builder b;
  b.set_name(name).set_id(name);
  return b;
}

Column::Builder column_builder(const std::string& name, const Table* table,
                               const zetasql::Type* type) {
  Column::Builder c;
  c.set_name(name).set_id(name).set_type(type).set_table(table);
  return c;
}

ChangeStream::Builder change_stream_builder(const std::string& name) {
  ChangeStream::Builder c;
  c.set_name(name).set_id(name);
  return c;
}

// Gets the array type given the element type in the array. Since the types in
// type_factory are static, they can live longer than the lifetime of the
// type_factory.
absl::StatusOr<const zetasql::Type*> get_array_type(
    zetasql::TypeFactory& type_factory,
    const zetasql::Type* array_element_type) {
  const zetasql::Type* array_type;
  ZETASQL_RETURN_IF_ERROR(type_factory.MakeArrayType(array_element_type, &array_type));
  return array_type;
}

void create_key_value_table(const std::string table_name,
                            const std::string key_column_name,
                            const zetasql::Type * key_column_type,
                            const std::string value_column_name,
                            const zetasql::Type * value_column_type,
                            std::optional<int64_t> value_column_max_length,
                            SchemaGraph* graph) {
  // The DDL template looks like:
  // R"(
  //     CREATE TABLE [table_name] (
  //       [key_column_name] [key_column_type],
  //       [value_column_name] [value_column_type],
  //     ) PRIMARY KEY([key_column_name])
  //   )",
  Table::Builder tb = table_builder(table_name);
  std::unique_ptr<const Column> key_column =
      column_builder(key_column_name, tb.get(), key_column_type).build();
  std::unique_ptr<const KeyColumn> key_column_constraint =
      KeyColumn::Builder().set_column(key_column.get()).build();
  std::unique_ptr<const Column> value_column =
      column_builder(value_column_name, tb.get(), value_column_type)
          .set_declared_max_length(value_column_max_length)
          .build();
  std::unique_ptr<const Table> table =
      tb.add_column(key_column.get())
          .add_column(value_column.get())
          .add_key_column(key_column_constraint.get())
          .build();
  graph->Add(std::move(key_column));
  graph->Add(std::move(value_column));
  graph->Add(std::move(key_column_constraint));
  graph->Add(std::move(table));
}

void create_primitive_types_table(zetasql::TypeFactory& type_factory,
                                  SchemaGraph* graph) {
  // R"(
  //     CREATE TABLE AllSpangresTypes (
  //       int64_value INT64,
  //       bool_value BOOL,
  //       double_value DOUBLE,
  //       string_value STRING(20),
  //       bytes_value BYTES,
  //       timestamp_value TIMESTAMP,
  //       date_value DATE,
  //       numeric_value PG.NUMERIC,
  //       jsonb_value PG.JSONB,
  //       float_value DOUBLE,
  //     ) PRIMARY KEY(int64_value)
  //   )",
  Table::Builder tb = table_builder("AllSpangresTypes");
  std::unique_ptr<const Column> int64_value =
      column_builder("int64_value", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Column> bool_value =
      column_builder("bool_value", tb.get(), type_factory.get_bool()).build();
  std::unique_ptr<const Column> double_value =
      column_builder("double_value", tb.get(), type_factory.get_double())
          .build();
  std::unique_ptr<const Column> string_value =
      column_builder("string_value", tb.get(), type_factory.get_string())
          .set_declared_max_length(20)
          .build();
  std::unique_ptr<const Column> bytes_value =
      column_builder("bytes_value", tb.get(), type_factory.get_bytes())
          .build();
  std::unique_ptr<const Column> timestamp_value =
      column_builder("timestamp_value", tb.get(), type_factory.get_timestamp())
          .build();
  std::unique_ptr<const Column> date_value =
      column_builder("date_value", tb.get(), type_factory.get_date()).build();

  // PG.NUMERIC
  std::unique_ptr<const Column> numeric_value =
      column_builder("numeric_value", tb.get(),
                     postgres_translator::spangres::types::PgNumericMapping()
                         ->mapped_type())
          .build();

  // PG.JSONB
  std::unique_ptr<const Column> jsonb_value =
      column_builder(
          "jsonb_value", tb.get(),
          postgres_translator::spangres::types::PgJsonbMapping()->mapped_type())
          .build();

  // TODO: b/299250195 - change to float when it is available in the emulator.
  std::unique_ptr<const Column> float_value =
      column_builder("float_value", tb.get(), type_factory.get_double()).build();

  std::unique_ptr<const KeyColumn> int64_value_primary =
      KeyColumn::Builder().set_column(int64_value.get()).build();
  std::unique_ptr<const Table> table =
      tb.add_column(int64_value.get())
          .add_column(bool_value.get())
          .add_column(double_value.get())
          .add_column(string_value.get())
          .add_column(bytes_value.get())
          .add_column(timestamp_value.get())
          .add_column(date_value.get())
          .add_column(numeric_value.get())
          .add_column(jsonb_value.get())
          .add_column(float_value.get())
          .add_key_column(int64_value_primary.get())
          .build();
  graph->Add(std::move(int64_value));
  graph->Add(std::move(bool_value));
  graph->Add(std::move(double_value));
  graph->Add(std::move(string_value));
  graph->Add(std::move(bytes_value));
  graph->Add(std::move(timestamp_value));
  graph->Add(std::move(date_value));
  graph->Add(std::move(numeric_value));
  graph->Add(std::move(jsonb_value));
  graph->Add(std::move(float_value));
  graph->Add(std::move(int64_value_primary));
  graph->Add(std::move(table));
}

void create_array_types_table(zetasql::TypeFactory& type_factory,
                                  SchemaGraph* graph) {
  // R"(
  //     CREATE TABLE ArrayTypes (
  //       key INT64,
  //       bool_array ARRAY<BOOL>,
  //       int_array ARRAY<INT64>,
  //       double_array ARRAY<DOUBLE>,
  //       string_array ARRAY<STRING(20)>,
  //       bytes_array ARRAY<BYTES>,
  //       timestamp_array ARRAY<TIMESTAMP>,
  //       date_array ARRAY<DATE>,
  //       numeric_array ARRAY<PG.NUMERIC>,
  //       jsonb_array ARRAY<PG.JSONB>,
  //       float_array ARRAY<DOUBLE>,
  //     ) PRIMARY KEY(key)
  //   )",
  Table::Builder tb = table_builder("ArrayTypes");
  std::unique_ptr<const Column> key_column =
      column_builder("key", tb.get(), type_factory.get_int64()).build();
  const zetasql::Type* bool_array_type =
      get_array_type(type_factory, type_factory.get_bool()).value();
  std::unique_ptr<const Column> bool_array =
      column_builder("bool_array", tb.get(), bool_array_type).build();
  const zetasql::Type* int_array_type =
      get_array_type(type_factory, type_factory.get_int64()).value();
  std::unique_ptr<const Column> int_array =
      column_builder("int_array", tb.get(), int_array_type).build();
  const zetasql::Type* double_array_type =
      get_array_type(type_factory, type_factory.get_double()).value();
  std::unique_ptr<const Column> double_array =
      column_builder("double_array", tb.get(), double_array_type).build();
  const zetasql::Type* string_array_type =
      get_array_type(type_factory, type_factory.get_string()).value();
  std::unique_ptr<const Column> string_array =
      column_builder("string_array", tb.get(), string_array_type)
          .set_declared_max_length(20)
          .build();
  const zetasql::Type* bytes_array_type =
      get_array_type(type_factory, type_factory.get_bytes()).value();
  std::unique_ptr<const Column> bytes_array =
      column_builder("bytes_array", tb.get(), bytes_array_type)
          .build();
  const zetasql::Type* timestamp_array_type =
      get_array_type(type_factory, type_factory.get_bytes()).value();
  std::unique_ptr<const Column> timestamp_array =
      column_builder("timestamp_array", tb.get(), timestamp_array_type).build();
  const zetasql::Type* date_array_type =
      get_array_type(type_factory, type_factory.get_bytes()).value();
  std::unique_ptr<const Column> date_array =
      column_builder("date_array", tb.get(), date_array_type).build();

  // PG.NUMERIC ARRAY
  std::unique_ptr<const Column> numeric_array =
      column_builder(
          "numeric_array", tb.get(),
          postgres_translator::spangres::types::PgNumericArrayMapping()
              ->mapped_type())
          .build();

  // PG.JSONB ARRAY
  std::unique_ptr<const Column> jsonb_array =
      column_builder("jsonb_array", tb.get(),
                     postgres_translator::spangres::types::PgJsonbArrayMapping()
                         ->mapped_type())
          .build();

  // TODO: b/299250195 - change to float when it is available in the emulator.
  const zetasql::Type* float_array_type =
      get_array_type(type_factory, type_factory.get_double()).value();
  std::unique_ptr<const Column> float_array =
      column_builder("float_array", tb.get(), float_array_type).build();

  std::unique_ptr<const KeyColumn> primary_key_constraint =
      KeyColumn::Builder().set_column(key_column.get()).build();
  std::unique_ptr<const Table> table =
      tb.add_column(key_column.get())
          .add_column(bool_array.get())
          .add_column(int_array.get())
          .add_column(double_array.get())
          .add_column(string_array.get())
          .add_column(bytes_array.get())
          .add_column(timestamp_array.get())
          .add_column(date_array.get())
          .add_column(numeric_array.get())
          .add_column(jsonb_array.get())
          .add_column(float_array.get())
          .add_key_column(primary_key_constraint.get())
          .build();
  graph->Add(std::move(key_column));
  graph->Add(std::move(bool_array));
  graph->Add(std::move(int_array));
  graph->Add(std::move(double_array));
  graph->Add(std::move(string_array));
  graph->Add(std::move(bytes_array));
  graph->Add(std::move(timestamp_array));
  graph->Add(std::move(date_array));
  graph->Add(std::move(numeric_array));
  graph->Add(std::move(jsonb_array));
  graph->Add(std::move(float_array));
  graph->Add(std::move(primary_key_constraint));
  graph->Add(std::move(table));
}

void create_withpseudo_table(zetasql::TypeFactory& type_factory,
                             SchemaGraph* graph) {
  // R"(
  //     CREATE TABLE withpseudo (
  //       key INT64,
  //       hidden1 INT64 HIDDEN,
  //       hidden2 INT64 HIDDEN,
  //       value INT64
  //   ) PRIMARY KEY(key);
  //  )"
  Table::Builder tb = table_builder("withpseudo");
  std::unique_ptr<const Column> key_column =
      column_builder("key", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const KeyColumn> primary_key_constraint =
      KeyColumn::Builder().set_column(key_column.get()).build();
  std::unique_ptr<const Column> hidden1_column =
      column_builder("hidden1", tb.get(), type_factory.get_int64())
          .set_hidden(true)
          .build();
  std::unique_ptr<const Column> hidden2_column =
      column_builder("hidden2", tb.get(), type_factory.get_int64())
          .set_hidden(true)
          .build();
  std::unique_ptr<const Column> value_column =
      column_builder("value", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Table> table =
      tb.add_column(key_column.get())
          .add_column(hidden1_column.get())
          .add_column(hidden2_column.get())
          .add_column(value_column.get())
          .add_key_column(primary_key_constraint.get())
          .build();
  graph->Add(std::move(key_column));
  graph->Add(std::move(primary_key_constraint));
  graph->Add(std::move(hidden1_column));
  graph->Add(std::move(hidden2_column));
  graph->Add(std::move(value_column));
  graph->Add(std::move(table));
}

void create_generated_columns_table(zetasql::TypeFactory& type_factory,
                                    SchemaGraph* graph) {
  // R"(
  //     CREATE TABLE generated_columns(
  //       a INT64,
  //       b INT64 AS (a * 2) STORED,
  //     ) PRIMARY KEY(a)
  //   )",
  Table::Builder tb = table_builder("generated_columns");
  std::unique_ptr<const Column> key_column =
      column_builder("a", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const KeyColumn> primary_key_constraint =
      KeyColumn::Builder().set_column(key_column.get()).build();
  std::unique_ptr<const Column> value_column =
      column_builder("b", tb.get(), type_factory.get_int64())
          .set_expression("(a * 2)")
          .set_original_expression("(a * 2)")
          .add_dependent_column_name("a")
          .build();
  std::unique_ptr<const Table> table =
      tb.add_column(key_column.get())
          .add_column(value_column.get())
          .add_key_column(primary_key_constraint.get())
          .build();
  const_cast<Column*>(value_column.get())->PopulateDependentColumns();

  graph->Add(std::move(key_column));
  graph->Add(std::move(primary_key_constraint));
  graph->Add(std::move(value_column));
  graph->Add(std::move(table));
}

void create_many_columns_table(const std::string table_name,
                               zetasql::TypeFactory& type_factory,
                               SchemaGraph* graph) {
  // R"(
  //    CREATE TABLE [table_name](
  //      one INT64,
  //      two INT64,
  //      three INT64,
  //      four INT64,
  //      five INT64,
  //  ) PRIMARY KEY(one);
  // )",
  Table::Builder tb = table_builder(table_name);
  std::unique_ptr<const Column> one_column =
      column_builder("one", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Column> two_column =
      column_builder("two", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Column> three_column =
      column_builder("three", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Column> four_column =
      column_builder("four", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Column> five_column =
      column_builder("five", tb.get(), type_factory.get_int64()).build();
  std::unique_ptr<const Table> table = tb.add_column(one_column.get())
                                           .add_column(two_column.get())
                                           .add_column(three_column.get())
                                           .add_column(four_column.get())
                                           .add_column(five_column.get())
                                           .build();
  graph->Add(std::move(one_column));
  graph->Add(std::move(two_column));
  graph->Add(std::move(three_column));
  graph->Add(std::move(four_column));
  graph->Add(std::move(five_column));
  graph->Add(std::move(table));
}

void create_change_stream(zetasql::TypeFactory& type_factory,
                          SchemaGraph* graph) {
  // R"(
  //     CREATE CHANGE STREAM keyvalue_change_stream FOR keyvalue(value)
  //   )",
  ChangeStream::Builder cb = change_stream_builder("keyvalue_change_stream");
  std::unique_ptr<const ChangeStream> change_stream =
      cb.set_tvf_name("read_json_keyvalue_change_stream")
          .add_tracked_tables_columns("keyvalue", {"value"})
          .build();
  graph->Add(std::move(change_stream));
}

std::unique_ptr<const OwningSchema> CreateSchema(
    zetasql::TypeFactory& type_factory) {
  auto graph = std::make_unique<SchemaGraph>();

  // Basic key, value int64_t,string table.
  // This table is intentionally defined without mixed-case identifiers
  // so it can be accessed from PostgreSQL without quoting the
  // identifier.
  // R"(
  //     CREATE TABLE keyvalue (
  //       key INT64,
  //       value STRING(20),
  //     ) PRIMARY KEY(key)
  //   )",
  create_key_value_table("keyvalue", "key", type_factory.get_int64(), "value",
                         type_factory.get_string(),
                         /*value_column_max_length=*/20, graph.get());

  // Basic key, value int64_t,string table.
  // This table is intentionally defined without mixed-case identifiers
  // so it can be accessed from PostgreSQL without quoting the
  // identifier.
  // R"(
  //     CREATE TABLE keyvaluecopy (
  //       key INT64,
  //       value STRING(20),
  //     ) PRIMARY KEY(key)
  //   )",
  create_key_value_table("keyvaluecopy", "key", type_factory.get_int64(),
                         "value", type_factory.get_string(),
                         /*value_column_max_length=*/20, graph.get());

  // Basic key, value int64_t,string table.
  // This table is intentionally defined without mixed-case identifiers
  // so it can be accessed from PostgreSQL without quoting the
  // identifier.
  // R"(
  //     CREATE TABLE keyvaluecopy2 (
  //       key INT64,
  //       value STRING(20),
  //     ) PRIMARY KEY(key)
  //   )",
  create_key_value_table("keyvaluecopy2", "key", type_factory.get_int64(),
                         "value", type_factory.get_string(),
                         /*value_column_max_length=*/20, graph.get());

  // Tables below are intentionally defined in mixed-case to provide
  // coverage for quoted identifiers in PostgreSQL.

  // The next tables are duplicates of googlesql sample_catalog tables
  // against which some of our tests were written.
  // R"(
  //     CREATE TABLE KeyValue2 (
  //       key INT64,
  //       value2 STRING(20),
  //     ) PRIMARY KEY(key)
  //   )",
  create_key_value_table("KeyValue2", "key", type_factory.get_int64(),
                         "value2", type_factory.get_string(),
                         /*value_column_max_length=*/20, graph.get());

  // R"(
  //     CREATE TABLE Value (
  //       value INT64,
  //       value_1 INT64,
  //     ) PRIMARY KEY(value)
  //   )",
  create_key_value_table("Value", "value", type_factory.get_int64(), "value_1",
                         type_factory.get_int64(),
                         /*value_column_max_length=*/std::nullopt, graph.get());

  // Table containing all Cloud Spanner types initially planned for
  // Spangres.
  create_primitive_types_table(type_factory, graph.get());

  // Table containing array types.
  create_array_types_table(type_factory, graph.get());

  // Table with hidden/pseudo columns that precede real columns.
  create_withpseudo_table(type_factory, graph.get());

  // Table containing a generated column.
  create_generated_columns_table(type_factory, graph.get());

  // Table and a copy containins many columns.
  create_many_columns_table("many_columns", type_factory, graph.get());
  create_many_columns_table("many_columns_copy", type_factory, graph.get());

  // Simple change stream over keyvalue for TVF testing.
  create_change_stream(type_factory, graph.get());

  return std::make_unique<const OwningSchema>(
      std::move(graph),
      google::spanner::emulator::backend::database_api::DatabaseDialect::
          POSTGRESQL);
}

zetasql::LanguageOptions MakeGoogleSqlLanguageOptions() {
  zetasql::LanguageOptions options;

  options.set_name_resolution_mode(zetasql::NAME_RESOLUTION_DEFAULT);
  options.set_product_mode(zetasql::PRODUCT_EXTERNAL);
  options.SetEnabledLanguageFeatures({
      zetasql::FEATURE_EXTENDED_TYPES, zetasql::FEATURE_NAMED_ARGUMENTS,
      zetasql::FEATURE_NUMERIC_TYPE, zetasql::FEATURE_TABLESAMPLE,
      zetasql::FEATURE_TIMESTAMP_NANOS,
      zetasql::FEATURE_V_1_1_HAVING_IN_AGGREGATE,
      zetasql::FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE,
      zetasql::FEATURE_V_1_1_ORDER_BY_COLLATE,
      zetasql::FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE,
      zetasql::FEATURE_V_1_2_SAFE_FUNCTION_CALL, zetasql::FEATURE_JSON_TYPE,
      zetasql::FEATURE_JSON_ARRAY_FUNCTIONS,
      zetasql::FEATURE_JSON_STRICT_NUMBER_PARSING,
      zetasql::FEATURE_V_1_4_WITH_EXPRESSION,
  });
  options.EnableLanguageFeature(zetasql::FEATURE_V_1_3_DML_RETURNING);
  options.SetSupportedStatementKinds({
      zetasql::RESOLVED_QUERY_STMT,
      zetasql::RESOLVED_INSERT_STMT,
      zetasql::RESOLVED_UPDATE_STMT,
      zetasql::RESOLVED_DELETE_STMT,
  });

  return options;
}

}  // namespace

std::unique_ptr<zetasql::EnumerableCatalog> GetEmulatorCatalog() {
  // Need to statically allocate the unique_ptrs since the callers of this
  // function keep a static unique_ptr of the emulator catalog returned by this
  // function.
  static std::unique_ptr<const OwningSchema> schema =
      CreateSchema(*GetTypeFactory());
  static auto function_catalog =
      std::make_unique<FunctionCatalog>(GetTypeFactory(),
                                        /*catalog_name = */ "spanner");
  return std::make_unique<Catalog>(schema.get(), function_catalog.get(),
                                   GetTypeFactory());
}

zetasql::AnalyzerOptions GetPGEmulatorTestAnalyzerOptions() {
  zetasql::AnalyzerOptions options;
  absl::TimeZone time_zone;
  ABSL_CHECK(absl::LoadTimeZone("America/Los_Angeles", &time_zone));
  options.set_default_time_zone(time_zone);
  options.set_error_message_mode(
      zetasql::AnalyzerOptions::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);
  options.set_prune_unused_columns(true);
  options.set_language(MakeGoogleSqlLanguageOptions());
  options.CreateDefaultArenasIfNotSet();
  return options;
}

}  // namespace postgres_translator::spangres::test
