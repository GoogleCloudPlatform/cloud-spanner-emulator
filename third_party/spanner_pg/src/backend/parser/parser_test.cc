// Copyright 2025 Google LLC

#include "third_party/spanner_pg/src/spangres/parser.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/ascii.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/postgres_includes/all.h"  // IWYU pragma: keep
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "third_party/spanner_pg/src/include/nodes/parsenodes.h"

namespace postgres_translator {
namespace {

using ParserTest =
    ::postgres_translator::test::ValidMemoryContext;
using ::testing::MatchesRegex;

TEST_F(ParserTest, StatementHintGrammarParsesOneHint) {
  const char* test_string = "/*@ statement_hint = 1 */ select 1";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  ASSERT_NE(stmt->statementHints, nullptr);
  ASSERT_EQ(list_length(stmt->statementHints), 1);
  DefElem* hint = list_nth_node(DefElem, stmt->statementHints, 0);
  EXPECT_STREQ(hint->defname, "statement_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

TEST_F(ParserTest, StatementHintGrammarParsesNamespacedHint) {
  const char* test_string = "/*@ hint_namespace.statement_hint = 1 */ select 1";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  ASSERT_NE(stmt->statementHints, nullptr);
  ASSERT_EQ(list_length(stmt->statementHints), 1);
  DefElem* hint = list_nth_node(DefElem, stmt->statementHints, 0);
  EXPECT_STREQ(hint->defnamespace, "hint_namespace");
  EXPECT_STREQ(hint->defname, "statement_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

TEST_F(ParserTest, StatementHintGrammarParsesTwoHints) {
  const char* test_string =
      "/*@ statement_hint1 = 1, statement_hint2 = 2 */ select 1";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  ASSERT_NE(stmt->statementHints, nullptr);
  ASSERT_EQ(list_length(stmt->statementHints), 2);
  DefElem* hint1 = list_nth_node(DefElem, stmt->statementHints, 0);
  EXPECT_STREQ(hint1->defname, "statement_hint1");
  ASSERT_TRUE(IsA(hint1->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint1->arg)->val), 1);
  DefElem* hint2 = list_nth_node(DefElem, stmt->statementHints, 1);
  EXPECT_STREQ(hint2->defname, "statement_hint2");
  ASSERT_TRUE(IsA(hint2->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint2->arg)->val), 2);
}

// Test all PostgreSQL relation syntaxes to ensure hints are picked up by them.
// We don't expect users to use the inheritence syntaxes, but as long as we
// aren't blocking them, we should support hints in them as well.
TEST_F(ParserTest, TableHintGrammarParsesHint) {
  // These are all equivalent for the parts we care about.
  std::vector<std::string> test_strings{
      "select 1 from keyvalue /*@ table_hint = 1 */",
      "select 1 from keyvalue * /*@ table_hint = 1 */",
      "select 1 from ONLY keyvalue /*@ table_hint = 1 */",
      "select 1 from ONLY (keyvalue) /*@ table_hint = 1 */"};

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

    SelectStmt* select = castNode(SelectStmt, stmt->stmt);
    RangeVar* range_var = list_nth_node(RangeVar, select->fromClause, 0);
    ASSERT_NE(range_var->tableHints, nullptr);
    ASSERT_EQ(list_length(range_var->tableHints), 1);
    DefElem* hint = list_nth_node(DefElem, range_var->tableHints, 0);
    EXPECT_STREQ(hint->defname, "table_hint");
    ASSERT_TRUE(IsA(hint->arg, A_Const));
    EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
  }
}

// Test all PostgreSQL function syntaxes to ensure hints are picked up by them.
TEST_F(ParserTest, FunctionGrammarParsesHint) {
  // These are all equivalent for the parts we care about.
  std::vector<std::string> test_strings{
      "SELECT count() /*@ function_hint = 1 */ AS pi",
      "SELECT count(key) /*@ function_hint = 1 */ FROM keyvalue",
      "SELECT count(*) /*@ function_hint = 1 */ FROM keyvalue",
      "SELECT count() /*@ function_hint = 1 */ WITHIN GROUP (ORDER BY key) "
      "FROM keyvalue",
      "SELECT count(key) /*@ function_hint = 1 */ FILTER (WHERE (value = "
      "'abc'))"
      " FROM keyvalue"};

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
    SelectStmt* select = castNode(SelectStmt, stmt->stmt);
    ResTarget* target = list_nth_node(ResTarget, select->targetList, 0);
    FuncCall* func = castNode(FuncCall, target->val);
    ASSERT_NE(func->functionHints, nullptr);
    ASSERT_EQ(list_length(func->functionHints), 1);
    DefElem* hint = list_nth_node(DefElem, func->functionHints, 0);
    EXPECT_STREQ(hint->defname, "function_hint");
    ASSERT_TRUE(IsA(hint->arg, A_Const));
    EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
  }
}

// Test to make sure the unreserved keywords we add to postgres are indeed
// unreserved.
TEST_F(ParserTest, UnreservedKeywordsAreUnreserved) {
  std::vector<std::string> test_strings{
    "select 1 as ttl from keyvalue",
    "select 1 as sequences from keyvalue",
    "select 1 as bit_reversed_positive from keyvalue",
    "select 1 as counter from keyvalue"
  };

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
  }
}

// Test all PostgreSQL JOIN syntaxes to ensure hints are picked up by them.
TEST_F(ParserTest, TableJoinGrammarParsesHint) {
  // These are all equivalent for the parts we care about.
  std::vector<std::string> test_strings{
      "select 1 from t1 join /*@ join_hint = 1 */ keyvalue t2 on true",
      "select 1 from t1 join /*@ join_hint = 1 */ keyvalue t2 using (cols)",
      "select 1 from t1 cross join /*@ join_hint = 1 */ t2",
      "select 1 from t1 full outer join /*@ join_hint = 1 */ t2 on true",
      "select 1 from t1 inner join /*@ join_hint = 1 */ t2 on true"};

  for (const std::string& test_case : test_strings) {
    SpangresTokenLocations locations;

    List* parse_tree =
        raw_parser_spangres(test_case.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

    SelectStmt* select = castNode(SelectStmt, stmt->stmt);
    JoinExpr* join_expr = list_nth_node(JoinExpr, select->fromClause, 0);
    ASSERT_NE(join_expr->joinHints, nullptr);
    ASSERT_EQ(list_length(join_expr->joinHints), 1);
    DefElem* hint = list_nth_node(DefElem, join_expr->joinHints, 0);
    EXPECT_STREQ(hint->defname, "join_hint");
    ASSERT_TRUE(IsA(hint->arg, A_Const));
    EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
  }
}

// Test PostgreSQL IN List syntaxe to ensure hints are picked up.
TEST_F(ParserTest, InListGrammarParsesHint) {
  std::string test_string =
      "select 1 from t1 where a IN /*@ join_hint = 1 */ (select 1)";

  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  SelectStmt* select = castNode(SelectStmt, stmt->stmt);
  ASSERT_NE(select, nullptr);
  ASSERT_NE(select->whereClause, nullptr);
  SubLink* sublink = castNode(SubLink, select->whereClause);
  ASSERT_NE(sublink->joinHints, nullptr);
  ASSERT_EQ(list_length(sublink->joinHints), 1);
  DefElem* hint = list_nth_node(DefElem, sublink->joinHints, 0);
  EXPECT_STREQ(hint->defname, "join_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

// Test PostgreSQL NOT IN List syntax to ensure hints are picked up.
TEST_F(ParserTest, NotInListGrammarParsesHint) {
  std::string test_string =
      "select 1 from t1 where a NOT IN /*@ join_hint = 1 */ (select 1)";

  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);

  SelectStmt* select = castNode(SelectStmt, stmt->stmt);
  ASSERT_NE(select, nullptr);
  ASSERT_NE(select->whereClause, nullptr);
  BoolExpr* boolexpr = castNode(BoolExpr, select->whereClause);
  ASSERT_EQ(list_length(boolexpr->args), 1);
  SubLink* sublink = linitial_node(SubLink, boolexpr->args);
  ASSERT_NE(sublink->joinHints, nullptr);
  ASSERT_EQ(list_length(sublink->joinHints), 1);
  DefElem* hint = list_nth_node(DefElem, sublink->joinHints, 0);
  EXPECT_STREQ(hint->defname, "join_hint");
  ASSERT_TRUE(IsA(hint->arg, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, hint->arg)->val), 1);
}

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;
TEST_F(ParserTest, InValuesListHintErrors) {
  // These should error because IN with Values isn't a hintable Join.
  std::vector<std::string> test_strings{
      "select 1 from t1 where a IN /*@ join_hint = 1 */ (1, 2)",
      "select 1 from t1 where a NOT IN /*@ join_hint = 1 */ (1, 2)"};

  for (const std::string& test_case : test_strings) {
    EXPECT_THAT(
        CheckedPgRawParser(test_case.c_str()),
        StatusIs(
            absl::StatusCode::kInvalidArgument,
            HasSubstr(
                "HINTs cannot be specified on IN clause with value list")));
  }
}

void VerifyCreateFunctionStmtText(absl::string_view expected,
                                  std::string& input, int rawstmt_index) {
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(input.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, rawstmt_index);
  CreateFunctionStmt* view = castNode(CreateFunctionStmt, stmt->stmt);
  EXPECT_EQ(expected, absl::StripAsciiWhitespace(view->routine_body_string));
}

TEST_F(ParserTest, SingleCreateFunctionStmt) {
  const std::string routine_body = "RETURN (SELECT 1)";
  const std::string base_string = "CREATE FUNCTION test() RETURNS INTEGER";

  std::string test_string = base_string + " " + routine_body;
  VerifyCreateFunctionStmtText(routine_body, test_string, 0);

  test_string = base_string + " LANGUAGE sql " + routine_body;
  VerifyCreateFunctionStmtText(routine_body, test_string, 0);

  const std::string block_routine_body = "BEGIN ATOMIC SELECT 1; SELECT 2;";
  test_string = base_string + " " + block_routine_body + " END";
  VerifyCreateFunctionStmtText(block_routine_body, test_string, 0);
}

TEST_F(ParserTest, CreateFunctionInMultipleStmt) {
  const std::string simple_query = "SELECT 1 AS c1, 2 AS c2;";
  const std::string routine_body = "RETURN (SELECT 1)";
  const std::string base_string =
      "CREATE FUNCTION test(x INTEGER DEFAULT 1) RETURNS INTEGER";

  std::string test_string =
      simple_query + " " + base_string + " " + routine_body + ";";
  VerifyCreateFunctionStmtText(routine_body, test_string, 1);

  test_string += " " + simple_query;
  VerifyCreateFunctionStmtText(routine_body, test_string, 1);
}

void VerifyViewStmtText(absl::string_view expected, std::string& input,
                        int rawstmt_index) {
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(input.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, rawstmt_index);
  ViewStmt* view = castNode(ViewStmt, stmt->stmt);
  ASSERT_EQ(expected, absl::StripAsciiWhitespace(view->query_string));
}

TEST_F(ParserTest, SingleViewStmt) {
  const std::string view_query_body = "select 1 as c1, 2 as c2";
  std::string test_string = "CREATE VIEW test AS " + view_query_body;

  VerifyViewStmtText(view_query_body, test_string, 0);

  test_string = test_string + " WITH LOCAL CHECK OPTION";
  VerifyViewStmtText(view_query_body, test_string, 0);

  std::string comments = "/*comments*/";
  test_string = "CREATE VIEW test AS " + view_query_body + comments +
                " WITH CHECK OPTION";

  // Comments will be included from the extracted text
  VerifyViewStmtText(view_query_body + comments, test_string, 0);
}

TEST_F(ParserTest, ViewInMultipleStmt) {
  std::string select = "select 1 as c1, 2 as c2;";
  const std::string view_query_body = "select 1 as c1, 2 as c2";
  std::string test_string = select + "CREATE VIEW test AS " + view_query_body;

  VerifyViewStmtText(view_query_body, test_string, 1);

  test_string = test_string + " WITH LOCAL CHECK OPTION";
  VerifyViewStmtText(view_query_body, test_string, 1);

  std::string comments = "/*comments*/";
  test_string = select + "CREATE VIEW test AS " + view_query_body + comments +
                " WITH CHECK OPTION;" + select;
  // Comments will be included from the extracted text
  VerifyViewStmtText(view_query_body + comments, test_string, 1);
}

void VerifyColumnConstraintExprString(
    CreateStmt* create, int nth, absl::string_view costraint_expression_text) {
  ColumnDef* column = list_nth_node(ColumnDef, create->tableElts, nth);
  Constraint* constraint = list_nth_node(Constraint, column->constraints, 0);
  EXPECT_EQ(costraint_expression_text, constraint->constraint_expr_string);
}

TEST_F(ParserTest, ExprStringCheckConstraint) {
  std::string create_table =
      "create table users (age bigint check(age > 0) not null); create table "
      "users2 (id bigint, age bigint, check(age > id));";
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(create_table.c_str(), RAW_PARSE_DEFAULT, &locations);
  ASSERT_EQ(list_length(parse_tree), 2);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 0,
                                   "age > 0");
  CreateStmt* create =
      castNode(CreateStmt, list_nth_node(RawStmt, parse_tree, 1)->stmt);
  EXPECT_EQ("age > id",
            absl::string_view{list_nth_node(Constraint, create->tableElts, 2)
                                  ->constraint_expr_string});
}

TEST_F(ParserTest, ExprStringDefault) {
  std::string create_table =
      "create table users (id bigint primary key, age bigint default 9);create "
      "table users2 (id bigint primary key, age bigint default (1+1*9)         "
      "  not null);"
      "alter table users2 alter column age set default 30;";
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(create_table.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1, "9");

  stmt = list_nth_node(RawStmt, parse_tree, 1);
  // The extra space will be included in the extracted string
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1,
                                   "(1+1*9)           ");

  stmt = list_nth_node(RawStmt, parse_tree, 2);
  AlterTableStmt* alter = castNode(AlterTableStmt, stmt->stmt);
  AlterTableCmd* cmd = list_nth_node(AlterTableCmd, alter->cmds, 0);
  EXPECT_STREQ(cmd->raw_expr_string, "30");
}

TEST_F(ParserTest, ExprStringGeneratedColumn) {
  std::string create_table =
      "create table users (id bigint primary key, age bigint generated always "
      "as (id+1) stored);create table users2 (id bigint primary key, age "
      "bigint generated always as ('100'::bigint) stored);";
  SpangresTokenLocations locations;
  List* parse_tree =
      raw_parser_spangres(create_table.c_str(), RAW_PARSE_DEFAULT, &locations);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1, "id+1");

  stmt = list_nth_node(RawStmt, parse_tree, 1);
  VerifyColumnConstraintExprString(castNode(CreateStmt, stmt->stmt), 1,
                                   "'100'::bigint");
}

TEST_F(ParserTest, OnUpdateBasicCreateTable) {
  const char* test_string = "CREATE TABLE T (col INT ON UPDATE 5)";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  // Verify this table has 1 column, with 1 ON UPDATE constraint node whose
  // expression is "5"
  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  ASSERT_TRUE(IsA(stmt->stmt, CreateStmt));
  CreateStmt* create = castNode(CreateStmt, stmt->stmt);
  ASSERT_EQ(list_length(create->tableElts), 1);
  ColumnDef* column = list_nth_node(ColumnDef, create->tableElts, 0);
  ASSERT_EQ(list_length(column->constraints), 1);
  Constraint* constraint = list_nth_node(Constraint, column->constraints, 0);
  EXPECT_EQ(constraint->contype, CONSTR_ON_UPDATE);
  ASSERT_TRUE(IsA(constraint->raw_expr, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, constraint->raw_expr)->val), 5);
  EXPECT_STREQ(constraint->constraint_expr_string, "5");
}

// Test that ON UPDATE can appear in any order with respect to DEFAULT AND NOT
// NULL in CREATE TABLE.
TEST_F(ParserTest, OnUpdateOrderingsCreateTable) {
  std::vector<std::string> test_strings{
      "CREATE TABLE T (col INT ON UPDATE 5 DEFAULT 1 NOT NULL)",
      "CREATE TABLE T (col INT ON UPDATE 5 NOT NULL DEFAULT 1)",
      "CREATE TABLE T (col INT DEFAULT 1 ON UPDATE 5 NOT NULL)",
      "CREATE TABLE T (col INT DEFAULT 1 NOT NULL ON UPDATE 5)",
      "CREATE TABLE T (col INT NOT NULL ON UPDATE 5 DEFAULT 1)",
      "CREATE TABLE T (col INT NOT NULL DEFAULT 1 ON UPDATE 5)",
  };

  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    SpangresTokenLocations locations;
    List* parse_tree =
        raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
    ASSERT_TRUE(IsA(stmt->stmt, CreateStmt));
    CreateStmt* create = castNode(CreateStmt, stmt->stmt);
    ASSERT_EQ(list_length(create->tableElts), 1);
    ColumnDef* column = list_nth_node(ColumnDef, create->tableElts, 0);
    ASSERT_EQ(list_length(column->constraints), 3);

    bool found_on_update = false;
    for (Constraint* constraint :
         StructList<Constraint*>(column->constraints)) {
      if (constraint->contype == CONSTR_ON_UPDATE) {
        ASSERT_TRUE(IsA(constraint->raw_expr, A_Const));
        EXPECT_EQ(intVal(&castNode(A_Const, constraint->raw_expr)->val), 5);
        // The raw expression string is captured between tokens, so it might
        // include trailing whitespace.
        EXPECT_THAT(constraint->constraint_expr_string, MatchesRegex("5\\s?"));
        found_on_update = true;
      }
    }
    EXPECT_TRUE(found_on_update);
  }
}

// Like above but with "NULL" column expressions.
TEST_F(ParserTest, OnUpdateOrderingsCreateTableNullValues) {
  std::vector<std::string> test_strings{
      "CREATE TABLE T (col INT ON UPDATE NULL DEFAULT NULL NOT NULL)",
      "CREATE TABLE T (col INT ON UPDATE NULL NOT NULL DEFAULT NULL)",
      "CREATE TABLE T (col INT DEFAULT NULL ON UPDATE NULL NOT NULL)",
      "CREATE TABLE T (col INT DEFAULT NULL NOT NULL ON UPDATE NULL)",
      "CREATE TABLE T (col INT NOT NULL ON UPDATE NULL DEFAULT NULL)",
      "CREATE TABLE T (col INT NOT NULL DEFAULT NULL ON UPDATE NULL)",
  };

  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    SpangresTokenLocations locations;
    List* parse_tree =
        raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
    ASSERT_TRUE(IsA(stmt->stmt, CreateStmt));
    CreateStmt* create = castNode(CreateStmt, stmt->stmt);
    ASSERT_EQ(list_length(create->tableElts), 1);
    ColumnDef* column = list_nth_node(ColumnDef, create->tableElts, 0);
    ASSERT_EQ(list_length(column->constraints), 3);

    bool found_on_update = false;
    for (Constraint* constraint :
         StructList<Constraint*>(column->constraints)) {
      if (constraint->contype == CONSTR_ON_UPDATE) {
        ASSERT_TRUE(IsA(constraint->raw_expr, A_Const));
        EXPECT_TRUE(castNode(A_Const, constraint->raw_expr)->isnull);
        // The raw expression string is captured between tokens, so it might
        // include trailing whitespace.
        EXPECT_THAT(constraint->constraint_expr_string,
                    MatchesRegex("NULL\\s?"));
        found_on_update = true;
      }
    }
    EXPECT_TRUE(found_on_update);
  }
}

TEST_F(ParserTest, AlterColumnSetOnUpdate) {
  const char* test_string = "ALTER TABLE T ALTER COLUMN col SET ON UPDATE 10";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  ASSERT_TRUE(IsA(stmt->stmt, AlterTableStmt));
  AlterTableStmt* alter = castNode(AlterTableStmt, stmt->stmt);
  ASSERT_EQ(list_length(alter->cmds), 1);
  AlterTableCmd* cmd = list_nth_node(AlterTableCmd, alter->cmds, 0);
  EXPECT_EQ(cmd->subtype, AT_ColumnOnUpdate);
  ASSERT_TRUE(IsA(cmd->def, A_Const));
  EXPECT_EQ(intVal(&castNode(A_Const, cmd->def)->val), 10);
  EXPECT_STREQ(cmd->raw_expr_string, "10");
}

TEST_F(ParserTest, AlterColumnDropOnUpdate) {
  const char* test_string = "ALTER TABLE T ALTER COLUMN col DROP ON UPDATE";
  SpangresTokenLocations locations;

  List* parse_tree =
      raw_parser_spangres(test_string, RAW_PARSE_DEFAULT, &locations);

  ASSERT_EQ(list_length(parse_tree), 1);
  RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
  ASSERT_TRUE(IsA(stmt->stmt, AlterTableStmt));
  AlterTableStmt* alter = castNode(AlterTableStmt, stmt->stmt);
  ASSERT_EQ(list_length(alter->cmds), 1);
  AlterTableCmd* cmd = list_nth_node(AlterTableCmd, alter->cmds, 0);
  EXPECT_EQ(cmd->subtype, AT_ColumnOnUpdate);
  EXPECT_EQ(cmd->def, nullptr);  // NULL definition is DROP ON UPDATE.
}

TEST_F(ParserTest, AddColumnOnUpdateOrderings) {
  std::vector<std::string> test_strings{
      "ALTER TABLE T ADD COLUMN col INT ON UPDATE 5 DEFAULT 1 NOT NULL",
      "ALTER TABLE T ADD COLUMN col INT ON UPDATE 5 NOT NULL DEFAULT 1",
      "ALTER TABLE T ADD COLUMN col INT DEFAULT 1 ON UPDATE 5 NOT NULL",
      "ALTER TABLE T ADD COLUMN col INT DEFAULT 1 NOT NULL ON UPDATE 5",
      "ALTER TABLE T ADD COLUMN col INT NOT NULL ON UPDATE 5 DEFAULT 1",
      "ALTER TABLE T ADD COLUMN col INT NOT NULL DEFAULT 1 ON UPDATE 5",
  };

  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    SpangresTokenLocations locations;
    List* parse_tree =
        raw_parser_spangres(test_string.c_str(), RAW_PARSE_DEFAULT, &locations);

    ASSERT_EQ(list_length(parse_tree), 1);
    RawStmt* stmt = list_nth_node(RawStmt, parse_tree, 0);
    ASSERT_TRUE(IsA(stmt->stmt, AlterTableStmt));
    AlterTableStmt* alter = castNode(AlterTableStmt, stmt->stmt);
    ASSERT_EQ(list_length(alter->cmds), 1);
    AlterTableCmd* cmd = list_nth_node(AlterTableCmd, alter->cmds, 0);
    EXPECT_EQ(cmd->subtype, AT_AddColumn);
    ColumnDef* column = castNode(ColumnDef, cmd->def);
    ASSERT_EQ(list_length(column->constraints), 3);

    bool found_on_update = false;
    for (Constraint* constraint :
         StructList<Constraint*>(column->constraints)) {
      if (constraint->contype == CONSTR_ON_UPDATE) {
        ASSERT_TRUE(IsA(constraint->raw_expr, A_Const));
        EXPECT_EQ(intVal(&castNode(A_Const, constraint->raw_expr)->val), 5);
        // The raw expression string is captured between tokens, so it might
        // include trailing whitespace.
        EXPECT_THAT(constraint->constraint_expr_string, MatchesRegex("5\\s?"));
        found_on_update = true;
      }
    }
    EXPECT_TRUE(found_on_update);
  }
}

// Tests for ON UPDATE two-token lookahead error handling. Since much of the
// lookahead state keeping is about preserving the internal parser state that is
// used to report the specific token that had the error, we'll test errors in a
// variety of places during the 2-token "ON UPDATE/DELETE" handling.

// These error out in the lexer since "$" is not a valid token.
TEST_F(ParserTest, OnUpdateLookaheadLexicalError) {
  std::vector<std::string> test_strings{
      "CREATE TABLE t (id int ON UPDATE $)",
      "ALTER TABLE t ADD COLUMN upd SET ON UPDATE $",
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES s(id) ON "
      "DELETE $",
  };
  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    EXPECT_THAT(CheckedPgRawParser(test_string.c_str()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("syntax error at or near \"$\"")));
  }
}

// In these cases, since the expression does look like a valid FOREIGN KEY
// action expression, we've replaced the FOREIGN KEY ... ON UPDATE token with a
// lookahead ON_LA token. As a result, the syntax error for bad FOREIGN KEY
// expression now points to the ON token instead of the action token. Likewise,
// the syntax for a bad ON UPDATE expression points to ON if the expression
// looks like a foreign key action.
TEST_F(ParserTest, ForeignKeyOnUpdateLookaheadLexicalError) {
  std::vector<std::string> test_strings{
      "CREATE TABLE t (id int, CONSTRAINT fk FOREIGN KEY (id) REFERENCES s(id) "
      "ON UPDATE $)",
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (id) REFERENCES s(id) ON "
      "UPDATE $",
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES t2(a) ON "
      "UPDATE 5",
      "CREATE TABLE t (id int ON UPDATE CASCADE)"};
  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    EXPECT_THAT(CheckedPgRawParser(test_string.c_str()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("syntax error at or near \"ON\"")));
  }
}

// In these cases, the lexer picks the wrong version of the "ON" token, so the
// parser errors out trying to select a rule for the ON UPDATE <foo> expression.
TEST_F(ParserTest, OnUpdateLookaheadGrammaticalError) {
  std::vector<std::string> test_strings{
      "CREATE TABLE t (id int ON UPDATE RESTRICT)",
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES t2(a) ON "
      "UPDATE FROM",
      "CREATE TABLE t (id int ON",
  };
  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    EXPECT_THAT(CheckedPgRawParser(test_string.c_str()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("syntax error at or near \"ON\"")));
  }
}

// Make sure the error points to the end of the input and not a token.
TEST_F(ParserTest, OnUpdateLookaheadEofError) {
  std::vector<std::string> test_strings{
      "CREATE TABLE t (id int ON UPDATE",
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES t2(a) ON",
      "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES t2(a) ON "
      "DELETE",
  };
  for (const std::string& test_string : test_strings) {
    SCOPED_TRACE(test_string);
    EXPECT_THAT(CheckedPgRawParser(test_string.c_str()),
                StatusIs(absl::StatusCode::kInvalidArgument,
                         HasSubstr("syntax error at end of input")));
  }
}

}  // namespace
}  // namespace postgres_translator
