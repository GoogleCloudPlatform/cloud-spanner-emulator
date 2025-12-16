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

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/ddl/operations.pb.h"
#include "third_party/spanner_pg/ddl/ddl_test_helper.h"
#include "third_party/spanner_pg/ddl/ddl_translator.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"
#include "third_party/spanner_pg/ddl/translation_utils.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "google/protobuf/text_format.h"

namespace postgres_translator::spangres {
namespace {

using ::google::spanner::emulator::backend::ddl::DDLStatementList;
using ::testing::ElementsAre;
using ::testing::SizeIs;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

class DdlTest : public testing::Test {
 protected:
  DdlTestHelper base_helper_;
};

// Compare identifier quoting done by direct deparser against original PG.
void CheckIdentifierQuoting(const std::string& identifier) {
  std::string quoted = internal::QuoteIdentifier(identifier);
  ABSL_LOG(INFO) << "Quoted identifier <" << identifier << ">=<" << quoted << ">";
  EXPECT_EQ(quoted, quote_identifier(identifier.c_str()))
      << "Identifier quoting is invalid for identifier '" << identifier << "'";
}

// Compare string literal quoting done by direct deparser against original PG.
void CheckStringLiteralQuoting(const std::string& str) {
  StringInfo buf = makeStringInfo();
  simple_quote_literal(buf, str.c_str());
  std::string quoted = internal::QuoteStringLiteral(str);
  ABSL_LOG(INFO) << "Quoted literal <" << str << ">=<" << quoted << ">";
  EXPECT_EQ(quoted, buf->data)
      << "String literal quoting is invalid for literal '" << str << "'";
}

TEST_F(DdlTest, NoStatement) {
  std::string input = " ";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ZETASQL_ASSERT_OK(parsed_statements.global_status());
  ASSERT_EQ(parsed_statements.output().size(), 1);

  EXPECT_THAT(
      base_helper_.Translator()->Translate(parsed_statements),
      StatusIs(absl::StatusCode::kInvalidArgument, "No statement found."));
}

TEST_F(DdlTest, DisabledNullsOrderingInvalidInput) {
  const std::string input =
      "CREATE INDEX nulls_test_idx ON Nulls ("
      "f1 DESC NULLS FIRST,"
      "f2 ASC NULLS LAST"
      ")";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_nulls_ordering = false});

  EXPECT_THAT(
      statements,
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          "<DESC NULLS FIRST> is not supported in <CREATE INDEX> statement."));
}

TEST_F(DdlTest, DisableDateType) {
  const std::string input =
      "CREATE Table users(id BIGINT PRIMARY KEY, unsupp DATE);";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                          {.enable_date_type = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                                        "Type <date> is not supported."));
}

TEST_F(DdlTest, DisablePgJsonbType) {
  const std::string input =
      "CREATE Table users(id BIGINT PRIMARY KEY, unsupp JSONB);";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                          {.enable_jsonb_type = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                                        "Type <jsonb> is not supported."));
}

TEST_F(DdlTest, DisableIntervalType) {
  const std::string input =
      "CREATE Table users(id BIGINT PRIMARY KEY, unsupp INTERVAL);";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_interval_type = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Column of type <interval> is not supported."));
}

TEST_F(DdlTest, DisableUUIDType) {
  const std::string input = "CREATE Table users(id UUID PRIMARY KEY);";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_uuid_type = false});

  EXPECT_THAT(statements, StatusIs(absl::StatusCode::kFailedPrecondition,
                                   "Type <uuid> is not supported."));
}

TEST_F(DdlTest, DisableAnalyze) {
  const std::string input = "ANALYZE;";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_analyze = false});

  EXPECT_THAT(statements, StatusIs(
                              absl::StatusCode::kFailedPrecondition,
                              "<ANALYZE> statement is not supported."));
}

TEST_F(DdlTest, DisableCreateView) {
  const std::string input = "CREATE VIEW test AS SELECT 1";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_create_view = false});

  EXPECT_THAT(statements, StatusIs(
                              absl::StatusCode::kFailedPrecondition,
                              "<CREATE VIEW> statement is not supported."));
}

TEST_F(DdlTest, UnsupportedDropTypeDomain) {
  const std::string input = "DROP DOMAIN admin";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_create_view = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       testing::HasSubstr("<DROP DOMAIN> is not supported.")));
}

// For unsupported by Spangres statements SchemaPrinter should error out
TEST_F(DdlTest, UnsupportedCreateFunctionStatement) {
  google::spanner::emulator::backend::ddl::DDLStatement input;
  auto create_function = input.mutable_create_function();
  create_function->set_function_name("Test");
  create_function->set_function_kind(google::spanner::emulator::backend::ddl::Function::VIEW);
  create_function->set_sql_security(
      google::spanner::emulator::backend::ddl::Function::UNSPECIFIED_SQL_SECURITY);
  EXPECT_THAT(
      base_helper_.SchemaPrinter()->PrintDDLStatement(input),
      StatusIs(absl::StatusCode::kInternal,
               testing::HasSubstr(
                   "Only security_invoker={true,false} are supported.")));
}

// Certain SDL options should be ignored by the schema printer. CREATE DATABASE
// should be printed only when no group settings present.
TEST_F(DdlTest, PrintCreateDatabase) {
  google::spanner::emulator::backend::ddl::DDLStatementList input;
  google::spanner::emulator::backend::ddl::CreateDatabase* create_stmt =
      input.add_statement()->mutable_create_database();

  create_stmt->set_db_name("test_db");
  absl::StatusOr<std::vector<std::string>> result =
      base_helper_.SchemaPrinter()->PrintDDLStatements(input);
  ZETASQL_ASSERT_OK(result);
  ASSERT_EQ(result->size(), 1);
  EXPECT_EQ(result->at(0), "CREATE DATABASE test_db");
}

TEST_F(DdlTest, PrintingDefaultOrderedPrimaryKeys) {
  google::spanner::emulator::backend::ddl::DDLStatementList input;
  google::spanner::emulator::backend::ddl::CreateTable* create_stmt =
      input.add_statement()->mutable_create_table();
  google::spanner::emulator::backend::ddl::ColumnDefinition* column = create_stmt->add_column();
  column->set_column_name("id");
  column->set_type(google::spanner::emulator::backend::ddl::ColumnDefinition::INT64);

  google::spanner::emulator::backend::ddl::KeyPartClause* key_part = create_stmt->add_primary_key();
  key_part->set_key_name("id");
  key_part->set_order(google::spanner::emulator::backend::ddl::KeyPartClause::ASC_NULLS_LAST);

  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> printed,
                       base_helper_.SchemaPrinter()->PrintDDLStatements(input));
  ASSERT_EQ(printed.size(), 1);
  EXPECT_EQ(printed[0],
            "CREATE TABLE \"\" (\n  id bigint,\n  PRIMARY KEY(id)\n)");
}

// The schema printing is allowing a primary key to have KeyPartClause::DESC.
TEST_F(DdlTest, DISABLED_ErrorPrintingOrderedPrimaryKeys) {
  // clang-format on
  google::spanner::emulator::backend::ddl::DDLStatementList input;
  google::spanner::emulator::backend::ddl::CreateTable* create_stmt =
      input.add_statement()->mutable_create_table();
  google::spanner::emulator::backend::ddl::ColumnDefinition* column = create_stmt->add_column();
  column->set_column_name("id");
  column->set_type(google::spanner::emulator::backend::ddl::ColumnDefinition::INT64);

  google::spanner::emulator::backend::ddl::KeyPartClause* key_part = create_stmt->add_primary_key();
  key_part->set_key_name("id");
  key_part->set_order(google::spanner::emulator::backend::ddl::KeyPartClause::DESC);

  EXPECT_THAT(
      base_helper_.SchemaPrinter()->PrintDDLStatements(input),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               "Non-default ordering is not supported for primary keys."));
}

TEST(TranslationUtilsTest, QuoteIdentifier) {
  // Memory arena is required for PG quote_identifier to allocate against.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<interfaces::PGArena> arena,
                       MemoryContextPGArena::Init(nullptr));

  CheckIdentifierQuoting("");
  CheckIdentifierQuoting("users");
  CheckIdentifierQuoting("Users");
  CheckIdentifierQuoting("uSers");
  CheckIdentifierQuoting("u\"ser\"s");
  CheckIdentifierQuoting("u\"\"\"ser\"\"s");
  CheckIdentifierQuoting("0users");
  // RESERVED_KEYWORD
  CheckIdentifierQuoting("all");
  // COL_NAME_KEYWORD
  CheckIdentifierQuoting("bigint");
  // TYPE_FUNC_NAME_KEYWORD
  CheckIdentifierQuoting("collation");
  // UNRESERVED_KEYWORD
  CheckIdentifierQuoting("action");
}

TEST(TranslationUtilsTest, QuoteStringLiteral) {
  // Memory arena is required for PG simple_quote_literal to allocate against.
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<interfaces::PGArena> arena,
                       MemoryContextPGArena::Init(nullptr));

  CheckStringLiteralQuoting("");
  CheckStringLiteralQuoting("simple string");
  CheckStringLiteralQuoting("string\nwith\nnewlines");
  CheckStringLiteralQuoting("string\\with\\backslashes");
  CheckStringLiteralQuoting("string'that'needs'escaping");
  CheckStringLiteralQuoting("string\"with\"double\"quotes");
}

// Setting the TTL time parsing is not supported in the emulator.
TEST_F(DdlTest, TTLDayBoundary) {
  const std::string input =
      "CREATE TABLE ttl_table (id bigint PRIMARY KEY) TTL INTERVAL '5 days' "
      "ON id";

  // First make sure things parse OK, which they should
  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ZETASQL_EXPECT_OK(parsed_statements.global_status());
  EXPECT_EQ(parsed_statements.output().size(), 1);

  // Translate the parse tree to an SDL proto, which should work.
  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_create_view = false});
  EXPECT_TRUE(statements.ok());
}

void ArrayJsonbBlockOptionTest(const DdlTestHelper& test_helper,
                               const std::string& input) {
  // First make sure things parse OK, which they should.
  interfaces::ParserBatchOutput parsed_statements =
      test_helper.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ZETASQL_ASSERT_OK(parsed_statements.global_status());
  EXPECT_EQ(parsed_statements.output().size(), 1);

  // When enable_array_jsonb_type is disabled, the translation should fail.
  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      test_helper.Translator()->Translate(
          parsed_statements,
          {.enable_jsonb_type = true, .enable_array_jsonb_type = false});
  EXPECT_THAT(
      statements.status(),
      StatusIs(absl::StatusCode::kFailedPrecondition,
               testing::HasSubstr("Type Array of <jsonb> is not supported")));

  // When enable_array_jsonb_type is enabled, the translation should success.
  statements = test_helper.Translator()->Translate(
      parsed_statements,
      {.enable_jsonb_type = true, .enable_array_jsonb_type = true});
  ZETASQL_EXPECT_OK(statements);
}

TEST_F(DdlTest, ArrayJsonbBlock) {
  // <CREATE> and <INSERT> are two kind of statements which require checking
  // the flag to block the accessing of the jsonb[] type if it is not enabled.
  ArrayJsonbBlockOptionTest(
      base_helper_,
      "CREATE TABLE jsonb_table (id bigint PRIMARY KEY, jsonb_v jsonb[])");
  ArrayJsonbBlockOptionTest(
      base_helper_, "Alter TABLE jsonb_teat Add column jsonb_v jsonb[]");
}

TEST_F(DdlTest, DisableCreateChangeStream) {
  const std::string input = "CREATE CHANGE STREAM change_stream FOR table1";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_change_streams = false});

  EXPECT_THAT(statements,
              StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  "<CREATE CHANGE STREAM> statement is not supported."));
}

TEST_F(DdlTest, DisableChangeStreamModTypeFilter) {
  const std::string input =
      "CREATE CHANGE STREAM change_stream FOR table1 WITH ( exclude_insert = "
      "true, exclude_update = false, exclude_delete = null)";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(
          parsed_statements,
          {.enable_change_streams = true,
           .enable_change_streams_mod_type_filter_options = false});
  EXPECT_THAT(statements, StatusIs(
                              absl::StatusCode::kFailedPrecondition,
                              "Options exclude_insert, exclude_update, and "
                              "exclude_delete are not supported yet."));
}

TEST_F(DdlTest, DisableChangeStreamTtlDeletesFilter) {
  const std::string input =
      "CREATE CHANGE STREAM change_stream FOR table1 WITH ( "
      "exclude_ttl_deletes = true)";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(
          parsed_statements,
          {.enable_change_streams = true,
           .enable_change_streams_ttl_deletes_filter_option = false});
  EXPECT_THAT(statements,
              StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  "Option exclude_ttl_deletes is not supported yet."));
}

TEST_F(DdlTest, DisableChangeStreamAllowTxnExclusion) {
  const std::string input =
      "CREATE CHANGE STREAM change_stream_txn_exclusion9 FOR table1 WITH ( "
      "allow_txn_exclusion = true)";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(
          parsed_statements,
          {.enable_change_streams = true,
           .enable_change_streams_allow_txn_exclusion_option = false});
  EXPECT_THAT(statements,
              StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  "Option allow_txn_exclusion is not supported yet."));
}

TEST_F(DdlTest, DisableChangeStreamIfNotExists) {
  const std::string input =
      "CREATE CHANGE STREAM IF NOT EXISTS change_stream FOR table1";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(
          parsed_statements,
          {.enable_change_streams = true,
           .enable_change_streams_if_not_exists = false});
  EXPECT_THAT(statements, StatusIs(absl::StatusCode::kFailedPrecondition,
                                   "<IF NOT EXISTS> clause is not supported in "
                                   "<CREATE CHANGE STREAM> statement."));
}

TEST_F(DdlTest, DisableDropChangeStreamIfExists) {
  const std::string input =
      "DROP CHANGE STREAM IF EXISTS change_stream";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(
          parsed_statements, {.enable_if_not_exists = true,
                              .enable_change_streams = true,
                              .enable_change_streams_if_not_exists = false});
  EXPECT_THAT(statements, StatusIs(absl::StatusCode::kFailedPrecondition,
                                   "<IF EXISTS> clause is not supported by "
                                   "<DROP CHANGE STREAM> statement."));
}

TEST_F(DdlTest, DisableChangeStreamPartitionModeOption) {
  const std::string input =
      "CREATE CHANGE STREAM ChangeStream FOR table1 WITH (partition_mode = "
      "'MUTABLE_KEY_RANGE')";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(
          parsed_statements,
          {.enable_change_streams = true,
           .enable_change_streams_partition_mode_option = false});
  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Option partition_mode is not supported yet."));
}

TEST_F(DdlTest, CreateDatabaseForEmulator) {
  const std::string input = "CREATE DATABASE test_db";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList>
      statements =
          base_helper_.Translator()->TranslateForEmulator(parsed_statements);

  EXPECT_TRUE(statements.ok());
  EXPECT_THAT(statements->statement().size(), 1);
  EXPECT_THAT(statements->statement().at(0).create_database().db_name(),
              "test_db");
}

TEST_F(DdlTest, DisableAlterChangeStream) {
  const std::string input = "ALTER CHANGE STREAM change_stream SET FOR table1";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_change_streams = false});

  EXPECT_THAT(statements,
              StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  "<ALTER CHANGE STREAM> statement is not supported."));
}

TEST_F(DdlTest, DisableDropChangeStream) {
  const std::string input = "DROP CHANGE STREAM change_stream";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_change_streams = false});

  EXPECT_THAT(statements,
              StatusIs(
                  absl::StatusCode::kFailedPrecondition,
                  "<DROP CHANGE STREAM> statement is not supported."));
}

TEST_F(DdlTest, DisableCreateSearchIndex) {
  const std::string input =
      "CREATE SEARCH INDEX index_name ON table_name (token_columns)";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_search_index = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "<CREATE SEARCH INDEX> statement is not supported."));
}

TEST_F(DdlTest, DisableDropSearchIndex) {
  const std::string input = "DROP SEARCH INDEX index_name";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_search_index = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "<DROP SEARCH INDEX> statement is not supported."));
}

// TODO: expose when queue is implemented.

TEST_F(DdlTest, DisableCreateLocalityGroup) {
  const std::string input =
      "CREATE LOCALITY GROUP lg";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "<CREATE LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, DisableDropLocalityGroup) {
  const std::string input = "DROP LOCALITY GROUP lg";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "<DROP LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, DisableAlterLocalityGroup) {
  const std::string input = "ALTER LOCALITY GROUP lg STORAGE 'hdd'";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(statements,
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "<ALTER LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, DisableAlterTableSetLocalityGroup) {
  const std::string input = "ALTER TABLE t SET LOCALITY GROUP lg";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(
      statements,
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          "<ALTER TABLE ... SET LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, DisableAlterColumnSetLocalityGroup) {
  const std::string input =
      "ALTER TABLE t ALTER COLUMN c SET LOCALITY GROUP lg";

  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(
      statements,
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          "<ALTER TABLE ... SET LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, DisableTableLevelLocalityGroupOnCreateTable) {
  const std::string input =
      R"sql(
        CREATE TABLE Users (
          id bigint PRIMARY KEY,
          abc varchar NOT NULL
        ) LOCALITY GROUP lg_2
      )sql";
  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(
      statements,
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          "<CREATE TABLE ... SET LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, DisableColumnLevelLocalityGroupOnCreateTable) {
  const std::string input =
      R"sql(
        CREATE TABLE Users (
          id bigint PRIMARY KEY,
          abc varchar NOT NULL LOCALITY GROUP lg
        )
      )sql";
  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements =
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_locality_groups = false});

  EXPECT_THAT(
      statements,
      StatusIs(
          absl::StatusCode::kFailedPrecondition,
          "<CREATE TABLE ... SET LOCALITY GROUP> statement is not supported."));
}

TEST_F(DdlTest, PrintDDLStatementForEmulator) {
  google::protobuf::TextFormat::Parser parser;
  google::spanner::emulator::backend::ddl::DDLStatement ddl;
  ASSERT_TRUE(parser.ParseFromString(
      R"pb(
        create_table {
          table_name: "users"
          column { column_name: "user_id" type: INT64 not_null: true }
          column { column_name: "name" type: STRING }
          primary_key { key_name: "user_id" }
        }
      )pb",
      &ddl));

  absl::StatusOr<std::vector<std::string>> result =
      base_helper_.SchemaPrinter()->PrintDDLStatementForEmulator(ddl);
  ASSERT_THAT(result, zetasql_base::testing::IsOkAndHolds(
                          testing::ElementsAre("CREATE TABLE users (\n"
                                               "  user_id bigint NOT NULL,\n"
                                               "  name character varying,\n"
                                               "  PRIMARY KEY(user_id)\n"
                                               ")")));
}

TEST_F(DdlTest, PrintTypeForEmulator) {
  google::protobuf::TextFormat::Parser parser;
  google::spanner::emulator::backend::ddl::ColumnDefinition column;
  ASSERT_TRUE(parser.ParseFromString(
      R"pb(
        column_name: "user_id" type: INT64 not_null: true
      )pb",
      &column));

  absl::StatusOr<std::string> result =
      base_helper_.SchemaPrinter()->PrintTypeForEmulator(column);
  ASSERT_THAT(result, zetasql_base::testing::IsOkAndHolds("bigint"));
}

TEST_F(DdlTest, PrintRowDeletionPolicyForEmulator) {
  google::protobuf::TextFormat::Parser parser;
  google::spanner::emulator::backend::ddl::RowDeletionPolicy policy;
  ASSERT_TRUE(parser.ParseFromString(
      R"pb(
        column_name: "user_id"
        older_than { count: 7 unit: DAYS }
      )pb",
      &policy));

  absl::StatusOr<std::string> result =
      base_helper_.SchemaPrinter()->PrintRowDeletionPolicyForEmulator(policy);
  ASSERT_THAT(result,
              zetasql_base::testing::IsOkAndHolds("INTERVAL '7 DAYS' ON user_id"));
}

TEST_F(DdlTest, DisableDefaultTimeZone) {
  const std::string input =
      "ALTER DATABASE TestDatabase SET spanner.default_time_zone = 'UTC'";
  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  EXPECT_THAT(base_helper_.Translator()->Translate(
                  parsed_statements, {.enable_default_time_zone = false}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       "Database option <spanner.default_time_zone> "
                       "is not supported."));
}

TEST_F(DdlTest, EnableDefaultTimeZone) {
  const std::string input =
      "ALTER DATABASE TestDatabase SET spanner.default_time_zone = 'UTC'";
  interfaces::ParserBatchOutput parsed_statements =
      base_helper_.Parser()->ParseBatch(
          interfaces::ParserParamsBuilder(input).Build());
  ABSL_CHECK_OK(parsed_statements.global_status());
  ABSL_CHECK_EQ(parsed_statements.output().size(), 1);

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> statements,
      base_helper_.Translator()->Translate(parsed_statements,
                                           {.enable_default_time_zone = true}));
  ASSERT_THAT(statements->statement(), SizeIs(1));
  ASSERT_THAT(statements->statement(0).alter_database().set_options().options(),
              SizeIs(1));
  EXPECT_EQ(statements->statement(0)
                .alter_database()
                .set_options()
                .options(0)
                .option_name(),
            "spanner.internal.cloud_default_time_zone");
  EXPECT_EQ(statements->statement(0)
                .alter_database()
                .set_options()
                .options(0)
                .string_value(),
            "UTC");
}

}  // namespace
}  // namespace postgres_translator::spangres
