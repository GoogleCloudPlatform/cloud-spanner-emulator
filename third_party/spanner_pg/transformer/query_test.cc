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
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "absl/log/absl_log.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_column.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "zetasql/resolved_ast/validator.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/str_split.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/interface/catalog_wrappers.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer.h"
#include "third_party/spanner_pg/transformer/transformer_test.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/unittest_utils.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"
#include "re2/re2.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::test {
namespace {

using ::zetasql::AnalyzerOutput;
using ::zetasql::ResolvedColumn;
using ::zetasql::ResolvedComputedColumn;
using ::zetasql::ResolvedOutputColumn;
using ::zetasql::ResolvedProjectScan;
using ::zetasql::ResolvedQueryStmt;
using ::zetasql::ResolvedStatement;
using ::zetasql::ResolvedTableScan;

using ::postgres_translator::internal::PostgresCastNodeTemplate;
using ::postgres_translator::internal::PostgresCastToExpr;
using ::postgres_translator::internal::PostgresCastToNode;
using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

using QueryTransformerTest = ::postgres_translator::TransformerTest;

const TableName kTableName({"public", "keyvalue"});

void StripTargetEntry(TargetEntry* entry);
void StripPgExpr(Expr* expr);
void StripPgDefElem(DefElem* elem);
void StripPgJoinExpr(JoinExpr* join_expr);
void StripPgQueryDifferences(Query* pg_query);

const ResolvedQueryStmt* CastToGsqlQueryStmt(const ResolvedStatement* stmt) {
  if (stmt == nullptr) return nullptr;
  return stmt->GetAs<ResolvedQueryStmt>();
}

const ResolvedProjectScan* CastToGsqlProjectScan(
    const ResolvedStatement* stmt) {
  if (stmt == nullptr) {
    return nullptr;
  }
  const ResolvedQueryStmt* query_stmt = CastToGsqlQueryStmt(stmt);
  return query_stmt->query()->GetAs<ResolvedProjectScan>();
}

// Builds a mutable copy of the input ZetaSQL table scan.
// ZetaSQL only exposes read-only references to children scans of a resolved
// AST, it doesn't let call sites own them. Meanwhile, in order to build a
// ZetaSQL scan that contains a child scan, the parent scan needs to own the
// child scan. Call sites can use this function to build a ResolvedTableScan,
// which can later be used to build other ResolvedScan objects.
absl::StatusOr<std::unique_ptr<ResolvedTableScan>> BuildMutableGsqlTableScan(
    const ResolvedTableScan* gsql_table_scan) {
  if (gsql_table_scan == nullptr) {
    return absl::InternalError(
        "Cannot build a mutable table scan. The source table scan is invalid.");
  }
  std::unique_ptr<ResolvedTableScan> new_table_scan =
      zetasql::MakeResolvedTableScan(
          gsql_table_scan->column_list(), gsql_table_scan->table(),
          /*for_system_time_expr=*/nullptr, gsql_table_scan->alias());
  new_table_scan->set_column_index_list(gsql_table_scan->column_index_list());
  return new_table_scan;
}

// Strips parse_location=??-?? strings from SQL debug strings.
std::string StripParseLocations(const std::string& original_debug_string) {
  const std::string kParseLocationRegex = "parse_location=\\d+-\\d+(?:, )?";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  std::vector<std::string> stripped_lines;
  for (auto& line : lines) {
    RE2::GlobalReplace(&line, kParseLocationRegex, "");
    if (!absl::EndsWith(line, "+-")) {
      stripped_lines.push_back(line);
    }
  }
  return absl::StrJoin(stripped_lines, "\n");
}

void StripTargetEntry(TargetEntry* entry) {
  StripPgExpr(entry->expr);
  entry->resorigtbl = InvalidOid;
  entry->resorigcol = InvalidOid;
}

// The has_explicit_type boolean field on a ZetaSQL literal is not used during
// query execution and is not possible for the Spangres transformer to match
// exactly. This method strips the has_explicit_type field from a resolved AST
// debug string so that we can compare debug strings between Spangres and
// ZetaSQL.
std::string StripHasExplicitType(
    const std::string& original_debug_string) {
  const std::string kHasExplicitType = ", has_explicit_type=TRUE";
  std::vector<std::string> lines = absl::StrSplit(original_debug_string, '\n');
  std::vector<std::string> stripped_lines;
  for (auto& line : lines) {
    RE2::GlobalReplace(&line, kHasExplicitType, "");
    stripped_lines.push_back(std::string(line));
  }
  return absl::StrJoin(stripped_lines, "\n");
}

// Remove actual values of certain fields in a PostgreSQL Query object produced
// by the PostgreSQL parser and analyzer. It is the expected input argument of
// this function, pg_query.
//
// This function replaces actual values of these fields in pg_query with
// default values, so that string comparison between serialized PostgreSQL
// query trees can succeed. E.g.:
// StripPgQueryDifferences(pg_query);
// EXPECT_EQ(nodeToString(new_transformed_query), nodeToString(pg_query));
void StripPgExpr(Expr* expr) {
  // To simplify writing recursive calls, just return if expr is null.
  if (expr == nullptr) {
    return;
  }
  switch (expr->type) {
    case T_Var: {
      Var* var = PostgresCastNode(Var, expr);
      var->location = -1;
      var->varcollid = 0;
      if (var->vartype == VARCHAROID) {
        var->vartype = TEXTOID;
      }
      break;
    }
    case T_Const: {
      Const* const_literal = PostgresCastNode(Const, expr);
      const_literal->location = -1;
      if (const_literal->consttype == VARCHAROID) {
        const_literal->consttype = TEXTOID;
      }
      break;
    }
    case T_FuncExpr: {
      FuncExpr* func_expr = PostgresCastNode(FuncExpr, expr);
      func_expr->funccollid = InvalidOid;
      func_expr->inputcollid = InvalidOid;
      func_expr->location = -1;
      for (Expr* arg : StructList<Expr*>(func_expr->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_SubLink: {
      SubLink* sublink = PostgresCastNode(SubLink, expr);
      StripPgExpr(PostgresCastToExpr(sublink->testexpr));
      StripPgQueryDifferences(PostgresCastNode(Query, sublink->subselect));
      sublink->location = -1;
      break;
    }
    case T_OpExpr: {
      OpExpr* op_expr = PostgresCastNode(OpExpr, expr);
      op_expr->inputcollid = InvalidOid;
      op_expr->location = -1;
      for (Expr* arg : StructList<Expr*>(op_expr->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_Aggref: {
      Aggref* agg_ref = PostgresCastNode(Aggref, expr);
      agg_ref->aggcollid = InvalidOid;
      agg_ref->inputcollid = InvalidOid;
      agg_ref->location = -1;
      for (TargetEntry* arg : StructList<TargetEntry*>(agg_ref->args)) {
        StripTargetEntry(arg);
      }
      break;
    }
    case T_SetToDefault: {
      SetToDefault* set_to_default = PostgresCastNode(SetToDefault, expr);
      set_to_default->collation = InvalidOid;
      set_to_default->location = -1;
      break;
    }
    case T_BoolExpr: {
      BoolExpr* bool_expr = PostgresCastNode(BoolExpr, expr);
      bool_expr->location = -1;
      for (Expr* arg : StructList<Expr*>(bool_expr->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_Param: {
      Param* param = PostgresCastNode(Param, expr);
      param->paramtypmod = -1;
      param->paramcollid = InvalidOid;
      param->location = -1;
      break;
    }
    case T_CaseExpr: {
      CaseExpr* case_expr = PostgresCastNode(CaseExpr, expr);
      case_expr->location = -1;
      case_expr->casecollid = InvalidOid;
      StripPgExpr(case_expr->defresult);
      if (case_expr->arg != nullptr) {
        StripPgExpr(case_expr->arg);
      }
      for (Expr* arg : StructList<Expr*>(case_expr->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_CaseWhen: {
      CaseWhen* when_expr = PostgresCastNode(CaseWhen, expr);
      when_expr->location = -1;
      StripPgExpr(when_expr->expr);
      StripPgExpr(when_expr->result);
      break;
    }
    case T_CaseTestExpr: {
      CaseTestExpr* test_expr = PostgresCastNode(CaseTestExpr, expr);
      test_expr->collation = 100;
      test_expr->typeMod = -1;
      break;
    }
    case T_NullIfExpr: {
      NullIfExpr* nullif_expr = PostgresCastNode(NullIfExpr, expr);
      nullif_expr->location = -1;
      nullif_expr->inputcollid = InvalidOid;
      nullif_expr->opcollid = InvalidOid;
      for (Expr* arg : StructList<Expr*>(nullif_expr->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_ScalarArrayOpExpr: {
      ScalarArrayOpExpr* scalar_array =
          PostgresCastNode(ScalarArrayOpExpr, expr);
      scalar_array->location = -1;
      scalar_array->inputcollid = InvalidOid;
      for (Expr* arg : StructList<Expr*>(scalar_array->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_ArrayExpr: {
      ArrayExpr* array_expr = PostgresCastNode(ArrayExpr, expr);
      array_expr->location = -1;
      array_expr->array_collid = InvalidOid;
      array_expr->multidims = false;
      for (Expr* arg : StructList<Expr*>(array_expr->elements)) {
        StripPgExpr(arg);
      }
      break;
    }
    case T_SQLValueFunction: {
      SQLValueFunction* function = PostgresCastNode(SQLValueFunction, expr);
      function->typmod = -1;
      function->location = -1;
      break;
    }
    case T_SubscriptingRef: {
      SubscriptingRef* subscripting_ref =
          PostgresCastNode(SubscriptingRef, expr);
      for (Expr* index_expr :
           StructList<Expr*>(subscripting_ref->refupperindexpr)) {
        StripPgExpr(index_expr);
      }
      for (Expr* index_expr :
           StructList<Expr*>(subscripting_ref->reflowerindexpr)) {
        StripPgExpr(index_expr);
      }
      StripPgExpr(subscripting_ref->refexpr);
      StripPgExpr(subscripting_ref->refassgnexpr);
      break;
    }
    case T_DistinctExpr: {
      DistinctExpr* distinct_expr = PostgresCastNode(DistinctExpr, expr);
      distinct_expr->location = -1;
      distinct_expr->inputcollid = InvalidOid;
      distinct_expr->opcollid = InvalidOid;
      for (Expr* arg : StructList<Expr*>(distinct_expr->args)) {
        StripPgExpr(arg);
      }
      break;
    }
    default:
      ABSL_LOG(ERROR) << "Unexpected Expr type: "
                            << NodeTagToString(expr->type);
  }
}

void StripPgDefElem(DefElem* elem) {
  elem->location = -1;
  StripPgExpr(PostgresCastToExpr(elem->arg));
}

void StripPgHintList(List* hint_list) {
  for (DefElem* hint : StructList<DefElem*>(hint_list)) {
    StripPgDefElem(hint);
  }
}

void StripPgJoinExpr(JoinExpr* join_expr) {
  Node* join_predicate = join_expr->quals;
  if (join_predicate != nullptr) {
    StripPgExpr(PostgresCastToExpr(join_predicate));
  }
  if (join_expr->larg->type == T_JoinExpr) {
    StripPgJoinExpr(PostgresCastNode(JoinExpr, join_expr->larg));
  }
  if (join_expr->rarg->type == T_JoinExpr) {
    StripPgJoinExpr(PostgresCastNode(JoinExpr, join_expr->rarg));
  }
  StripPgHintList(join_expr->joinHints);
}

void StripPgQueryDifferences(Query* pg_query) {
  if (pg_query == nullptr) {
    return;
  }
  for (RangeTblEntry* rte : StructList<RangeTblEntry*>(pg_query->rtable)) {
    // Recursively strip subqueries.
    switch (rte->rtekind) {
      case RTE_SUBQUERY: {
        ABSL_CHECK_NE(rte->subquery, nullptr);
        StripPgQueryDifferences(rte->subquery);
        break;
      }
      case RTE_JOIN: {
        for (Expr* expr : StructList<Expr*>(rte->joinaliasvars)) {
          StripPgExpr(expr);
        }
        break;
      }
      case RTE_VALUES: {
        for (List* expr_list : StructList<List*>(rte->values_lists)) {
          for (Expr* expr : StructList<Expr*>(expr_list)) {
            StripPgExpr(expr);
          }
        }
        break;
      }
      case RTE_RELATION: {
        StripPgHintList(rte->tableHints);
        break;
      }
      case RTE_CTE: {
        rte->ctelevelsup = 0;
        break;
      }
      case RTE_FUNCTION: {
        // Strip the alias because there's no way to differentiate user-supplied
        // from system default aliases with functions (since ZetaSQL doesn't
        // really support FROM functions).
        rte->alias = nullptr;
        for (RangeTblFunction* function :
             StructList<RangeTblFunction*>(rte->functions)) {
          StripPgExpr(internal::PostgresCastToExpr(function->funcexpr));
        }
        break;
      }
      default: {
        // Do nothing
      }
    }
    rte->coltypes = nullptr;
    rte->coltypmods = nullptr;
    rte->colcollations = nullptr;
  }
  // Ignore stmt_location:
  pg_query->stmt_location = -1;
  // Ignore differences in TargetEntry and its expressions:
  for (TargetEntry* entry : StructList<TargetEntry*>(pg_query->targetList)) {
    StripTargetEntry(entry);
  }
  // Ignore differences in TargetEntry and its expressions:
  for (TargetEntry* entry : StructList<TargetEntry*>(pg_query->returningList)) {
    StripTargetEntry(entry);
  }

  // If applicable, replace where clause location with the default value, -1.
  Node* where_clause = pg_query->jointree->quals;
  if (where_clause != nullptr) {
    StripPgExpr(PostgresCastToExpr(where_clause));
  }

  for (Node* node : StructList<Node*>(pg_query->jointree->fromlist)) {
    if (node->type == T_JoinExpr) {
      StripPgJoinExpr(PostgresCastNode(JoinExpr, node));
    }
  }

  for (CommonTableExpr* cte : StructList<CommonTableExpr*>(pg_query->cteList)) {
    StripPgQueryDifferences(PostgresCastNode(Query, cte->ctequery));
    cte->location = -1;
    cte->cterefcount = 0;
    cte->ctecolnames = nullptr;
    cte->ctecoltypes = nullptr;
    cte->ctecoltypmods = nullptr;
    cte->ctecolcollations = nullptr;
  }

  if (pg_query->onConflict != nullptr &&
      (pg_query->onConflict->action == ONCONFLICT_UPDATE ||
       pg_query->onConflict->action == ONCONFLICT_NOTHING)) {
    for (InferenceElem* elem :
         StructList<InferenceElem*>(pg_query->onConflict->arbiterElems)) {
      StripPgExpr(PostgresCastToExpr(elem->expr));
    }
    if (pg_query->onConflict->onConflictWhere != nullptr) {
      StripPgExpr(PostgresCastToExpr(pg_query->onConflict->onConflictWhere));
    }
    for (TargetEntry* entry :
         StructList<TargetEntry*>(pg_query->onConflict->onConflictSet)) {
      StripTargetEntry(entry);
    }
  }

  StripPgHintList(pg_query->statementHints);
}

// Test case for QueryTransformationTest
//
// Defaults to kTransformRoundTrip phases and same query text for ZetaSQL
// and PostgreSQL. Use mutators to override those defaults.
class TransformTestCase {
 public:
  TransformTestCase(const std::string& name, const std::string& pg_query)
      : planning_phases_(PlanningPhase::kTransformRoundTrip),
        name_(name),
        pg_query_(pg_query),
        gsql_query_(pg_query) {}
  TransformTestCase& SetPhases(int phases) {
    planning_phases_ = phases;
    return *this;
  }
  TransformTestCase& SetGsqlQuery(const std::string& gsql_query) {
    gsql_query_ = gsql_query;
    return *this;
  }
  TransformTestCase& SetParameterTypes(
      const absl::flat_hash_map<std::string, const zetasql::Type*>&
          parameter_types) {
    parameter_types_ = parameter_types;
    return *this;
  }
  TransformTestCase& SetLanguageFeature(zetasql::LanguageFeature feature,
                                        bool value) {
    language_features_.push_back(std::make_pair(feature, value));
    return *this;
  }
  TransformTestCase& SetFlagOverride(absl::Flag<bool>* flag, bool value) {
    flag_overrides_.push_back(std::make_pair(flag, value));
    return *this;
  }
  const std::string& name() const { return name_; }
  int planning_phases() const { return planning_phases_; }
  const std::string& pg_query() const { return pg_query_; }
  const std::string& gsql_query() const { return gsql_query_; }
  const absl::flat_hash_map<std::string, const zetasql::Type*>&
  parameter_types() const {
    return parameter_types_;
  }
  const std::vector<std::pair<zetasql::LanguageFeature, bool>>&
  language_features() const {
    return language_features_;
  }
  const std::vector<std::pair<absl::Flag<bool>*, bool>>& flag_overrides()
      const {
    return flag_overrides_;
  }

  friend std::ostream& operator<<(std::ostream& os,
                                  const TransformTestCase& test_case) {
    if (test_case.gsql_query_ == test_case.pg_query_) {
      os << "PostgreSQL and ZetaSQL test query:\n"
         << test_case.pg_query_ << "\n";
    } else {
      os << "PostgreSQL test query:\n" << test_case.pg_query_ << "\n";
      os << "ZetaSQL test query:\n" << test_case.gsql_query_ << "\n";
    }
    return os << PrintPhases(test_case.planning_phases_);
  }

 private:
  int planning_phases_;
  std::string name_;
  std::string pg_query_;
  std::string gsql_query_;
  absl::flat_hash_map<std::string, const zetasql::Type*> parameter_types_;
  std::vector<std::pair<zetasql::LanguageFeature, bool>> language_features_;
  std::vector<std::pair<absl::Flag<bool>*, bool>> flag_overrides_;
};

class QueryTransformationTest
    : public ::testing::WithParamInterface<TransformTestCase>,
      public ::postgres_translator::test::ValidMemoryContext {
 protected:
  zetasql::AnalyzerOptions analyzer_options_ =
      GetSpangresTestAnalyzerOptions();
};

TEST_P(QueryTransformationTest, TestTransform) {
  TransformTestCase test_case = GetParam();

  for (const auto& [flag, value] : test_case.flag_overrides()) {
    absl::SetFlag(flag, value);
  }

  for (const auto& [feature, enabled] : test_case.language_features()) {
    if (enabled) {
      analyzer_options_.mutable_language()->EnableLanguageFeature(feature);
    } else {
      analyzer_options_.mutable_language()->DisableLanguageFeature(feature);
    }
  }
  std::unique_ptr<CatalogAdapterHolder> adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);

  // Build the analyzer options.
  zetasql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  analyzer_options.set_record_parse_locations(false);
  for (const auto& [parameter_name, parameter_type] :
       test_case.parameter_types()) {
    ZETASQL_ASSERT_OK(
        analyzer_options.AddQueryParameter(parameter_name, parameter_type));
  }

  // Build Postgres and ZetaSQL objects from the sql string:
  Query* pg_analyzer_output;
  ZETASQL_ASSERT_OK_AND_ASSIGN(pg_analyzer_output,
                       BuildPgQuery(test_case.pg_query(), analyzer_options));
  zetasql::TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> gsql_output;
  ZETASQL_ASSERT_OK(ParseAndAnalyzeFromZetaSQLForTest(
      test_case.gsql_query(), &type_factory, analyzer_options, &gsql_output));

  auto transformer = std::make_unique<ForwardTransformer>(
      adapter_holder->ReleaseCatalogAdapter());

  const ResolvedStatement* gsql_statement = gsql_output->resolved_statement();
  std::unique_ptr<const ResolvedStatement> transformed_statement;

  if (test_case.planning_phases() < PlanningPhase::kTransform) {
    FAIL() << "Invalid planning phase ("
           << PrintPhases(test_case.planning_phases()) << "). "
           << "Only transformation phases are supported";
  }
  if (test_case.planning_phases() & PlanningPhase::kTransform) {
    // Strip the top level location so that it won't be included in the
    // resolved AST.
    pg_analyzer_output->stmt_location = -1;

    // PostgreSQL -> ZetaSQL
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        transformed_statement,
        transformer->BuildGsqlResolvedStatement(*pg_analyzer_output));

    // Validate the resolved AST.
    zetasql::Validator validator(analyzer_options.language());
    ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(transformed_statement.get()));

    // Check that the ASTs match.
    ASSERT_EQ(StripParseLocations(transformed_statement->DebugString()),
              StripHasExplicitType(
                  StripParseLocations(gsql_statement->DebugString())));
  }
}

INSTANTIATE_TEST_SUITE_P(
    AllTransforms, QueryTransformationTest,
    testing::Values(
        TransformTestCase(
            "DeleteFromTableWithKey",
            "DELETE from keyvalue AS keyvalue where key = 12312312"),
        TransformTestCase(
            "DeleteFromTableWithValue",
            "DELETE from keyvalue AS keyvalue where value = 'abc'"),
        TransformTestCase("SelectFromATableRoundTrip",
                          "select key as k, value as v from keyvalue"),
        TransformTestCase("SelectTableAlias", "select key from keyvalue kv"),
        // This query contains both table columns and literals in its selected
        // list. The selected items are not in any particular order.
        TransformTestCase(
            "SelectMixedItemsFromATableRoundTrip",
            "select value, key, 12345678912 as col3, 98765432121 as col4, key, "
            "value from keyvalue"),
        TransformTestCase("SelectSameColumnTwiceRoundTrip",
                          "select value, value from keyvalue"),
        TransformTestCase("SelectWithWhereClauseRoundTrip",
                          "select key from keyvalue where true"),
        TransformTestCase("SelectFromTwoTablesWithWhereRoundTrip",
                          "select kv.key, kv2.key from keyvalue kv, "
                          "\"KeyValue2\" kv2 where true")
            .SetGsqlQuery("select kv.key, kv2.key from keyvalue kv, KeyValue2 "
                          "kv2 where true"),
        TransformTestCase(
            "SelectFromTwoTablesRoundTrip",
            "select kv.key, kv2.key from keyvalue kv, \"KeyValue2\" kv2")
            .SetGsqlQuery(
                "select kv.key, kv2.key from keyvalue kv, KeyValue2 kv2"),
        // ---- BEGIN SELECT with Joins Tests -----
        TransformTestCase(
            "TwoTablesJoin",
            "select kv.value from keyvalue kv "
            "left join \"KeyValue2\" kv2 on (kv.value = kv2.value2)")
            .SetGsqlQuery("select kv.value from keyvalue kv "
                          "left join KeyValue2 kv2 on (kv.value = kv2.value2)"),
        TransformTestCase(
            "ThreeTablesJoinLeftRight",
            "select kv.key, kv2.key, val.value from "
            "keyvalue kv left join \"KeyValue2\" kv2 on (kv.key = kv2.key) "
            "right join \"Value\" val on (kv.key = val.value)")
            .SetGsqlQuery(
                "select kv.key, kv2.key, val.value from "
                "keyvalue kv left join KeyValue2 kv2 on (kv.key = kv2.key)"
                "right join Value val on (kv.key = val.value)"),
        TransformTestCase(
            "FourTablesTwoJoins",
            "select kv.key, kv2.value2, kv3.value, kv4.key from keyvalue kv "
            "join \"KeyValue2\" kv2 on kv.value = kv2.value2, keyvalue "
            "kv3 join keyvalue kv4 on kv3.key = kv4.key")
            .SetGsqlQuery(
                "select kv.key, kv2.value2, kv3.value, kv4.key from keyvalue "
                "kv join KeyValue2 kv2 on kv.value = kv2.value2, "
                "(keyvalue kv3 join keyvalue kv4 on kv3.key = kv4.key)"),
        TransformTestCase(
            "LeftJoinCommaRightJoin",
            "select kv.key, kv2.value2, kv3.value, kv4.key from keyvalue kv "
            "left join \"KeyValue2\" kv2 on kv.value = kv2.value2, (keyvalue "
            "kv3 right join keyvalue kv4 on kv3.key = kv4.key)")
            .SetGsqlQuery("select kv.key, kv2.value2, kv3.value, kv4.key from "
                          "keyvalue kv left join KeyValue2 kv2 on kv.value = "
                          "kv2.value2, "
                          "(keyvalue kv3 right join keyvalue kv4 on kv3.key = "
                          "kv4.key)"),
        TransformTestCase("NestedJoinsOnTheRight",
                          "select kv.value, kv2.key, kv3.value from keyvalue "
                          "kv left join (\"KeyValue2\" kv2 cross join keyvalue "
                          "kv3) on kv.value = kv2.value2")
            .SetGsqlQuery("select kv.value, kv2.key, kv3.value from keyvalue "
                          "kv left join (KeyValue2 kv2 cross join keyvalue "
                          "kv3) on kv.value = kv2.value2"),
        TransformTestCase("JoinsWithConsecutiveOnClauses",
                          "select kv.key, kv2.value, kv3.value from "
                          "keyvalue kv join "
                          "keyvalue kv2 join keyvalue kv3 "
                          "on kv2.value=kv3.value "
                          "on kv.key=kv2.key")
            .SetGsqlQuery("select kv.key, kv2.value, kv3.value from "
                          "keyvalue kv join "
                          "(keyvalue kv2 join keyvalue kv3 "
                          "on kv2.value=kv3.value) "
                          "on kv.key=kv2.key"),
        // ---- END SELECT with Joins Tests -----

        TransformTestCase("SimpleOrderBy",
                          "select key from keyvalue order by key")
            .SetGsqlQuery("select key from keyvalue order by key nulls last"),
        TransformTestCase(
            "OrderByWithComputedColumn",
            "select 'abc' as abc, key from keyvalue order by 2 desc, 1")
            .SetGsqlQuery(
                "select 'abc' as abc, key from keyvalue order by 2 desc "
                "nulls first, 1 nulls last"),
        TransformTestCase("OrderByWithNonSelectedItem",
                          "select value from keyvalue order by key")
            .SetGsqlQuery("select value from keyvalue order by key nulls last"),
        TransformTestCase("OrderByInsideSubquery",
                          "select \"$subquery1\".key from "
                          "(select key, value from keyvalue kv "
                          "    order by 2,1) "
                          "\"$subquery1\" order by 1")
            .SetGsqlQuery("select key from "
                          "(select key, value from keyvalue kv "
                          "    order by 2 nulls last, 1 nulls last) "
                          "order by 1 nulls last"),
        TransformTestCase("SimpleInsertValuesRoundTrip",
                          "insert into keyvalue(key, value) values "
                          "(1234567890123, 'value')"),
        TransformTestCase("SimpleGroupBy",
                          "select key from keyvalue group by key"),
        TransformTestCase("GroupByWithMultipleItemsAndAliases",
                          "select key as k,value as v from keyvalue "
                          "group by v, k"),
        TransformTestCase(
            "GroupByWithColumnsInGroupByListOnly",
            "select 'abc' as col1,key+1 as col2 from keyvalue group by key"),
        TransformTestCase("GroupByWithComputedColumns",
                          "select key,'abc' as abc from keyvalue "
                          "group by abc, 1"),
        TransformTestCase("GroupByWithComputedColumnsInSelectListOnly",
                          "select key,'abc' as abc from keyvalue group by 1"),
        TransformTestCase("GroupByInsideSubquery",
                          "select k from "
                          "(select key+1 as k, value from keyvalue kv "
                          "group by 2,key) sub group by 1 "),
        TransformTestCase("OrderByAndGroupByWithNonSelectedColumn",
                          "select key from keyvalue group by key,value "
                          "order by key nulls last,value nulls last"),

        TransformTestCase("InsertValuesUnorderedColumnsRoundTrip",
                          "insert into \"Value\"(value_1, value) values "
                          "(9876543210987, 1234567890123)")
            .SetGsqlQuery("insert into Value(value_1, value) values "
                          "(9876543210987, 1234567890123)"),
        TransformTestCase(
            "MultiValueInsertRoundTrip",
            "insert into keyvalue(key, value) values(1234567890123, 'one'), "
            "(2345678901234, 'two')"),
        TransformTestCase(
            "MultiValueUnorderedInsertRoundTrip",
            "insert into \"AllSpangresTypes\"(string_value, bool_value, "
            "int64_value) "
            "values ('hello world', default, 1234567890123), (default, true, "
            "9876543210987)")
            .SetGsqlQuery("insert into AllSpangresTypes(string_value, "
                          "bool_value, int64_value) "
                          "values ('hello world', default, 1234567890123), "
                          "(default, true, "
                          "9876543210987)"),
        TransformTestCase(
            "InsertSelect",
            "insert into keyvalue(key, value) select key, value from keyvalue"),
        TransformTestCase(
            "InsertSelectMultiSubquery",
            "insert into keyvalue(key) select x from (select 123 as x) as tmp"),

        TransformTestCase("InsertReturning",
                          "insert into keyvalue(key, value) "
                          "values(1234567890123, 'one') returning key")
            .SetGsqlQuery("insert into keyvalue(key, value) "
                          "values(1234567890123, 'one') then return key"),
        TransformTestCase("SimpleUpdateWithoutWhereClauseSucceeds",
                          "update keyvalue set key=12345678912")
            .SetGsqlQuery("update keyvalue set key=12345678912 where true")
            // TODO: Add a WHERE clause and make this roundtrip.
            .SetPhases(PlanningPhase::kTransform),
        TransformTestCase(
            "SimpleUpdateWithWhereClauseSucceeds",
            "update keyvalue set key=12345678912 where value='v'"),
        TransformTestCase("SimpleUpdateWithUnorderedColumnsSucceeds",
                          "update keyvalue set value='new_v', key=12345678912 "
                          "where value='v'")
            .SetPhases(PlanningPhase::kTransformRoundTrip),
        TransformTestCase("SimpleUpdateUsingOneColumn",
                          "update keyvalue set value='v'where true"),
        TransformTestCase("UpdateReturning",
                          "update keyvalue set value='v'where key=12345678912 "
                          "returning key + 1 as newkey, value as newvalue")
            .SetGsqlQuery("update keyvalue set value='v'where key=12345678912 "
                          "then return key + 1 as newkey, value as newvalue"),
        TransformTestCase("SimpleDelete", "delete from keyvalue where true"),
        TransformTestCase("DeleteReturning",
                          "delete from keyvalue where true returning *")
            .SetGsqlQuery("delete from keyvalue where true then return *"),
        TransformTestCase(
            "DeleteReturningWithTableAlias",
            "delete from keyvalue as table0 where true returning table0.key")
            .SetGsqlQuery("delete from keyvalue as table0 where true then "
                          "return table0.key"),
        TransformTestCase(
            "SelectFloorDoubleSucceeds",
            "select floor(double_value) as floor from \"AllSpangresTypes\"")
            .SetGsqlQuery(
                "select floor(double_value) as floor from AllSpangresTypes"),
        TransformTestCase("SimpleSelectSubqueryRoundTrip", R"(
          select
            subquery.value, subquery.key, subquery.f1
          from (
            select
              key, value, 123456789012 as f1
            from
              keyvalue
          ) subquery
        )"),
        TransformTestCase("PgToGsqlSelectExistsSelect1",
                          "select exists(select 1000000000000) as exists_col")
            .SetGsqlQuery("select exists(select 1000000000000 as `?column?`) "
                          "as exists_col")
            // TODO: Make it kTransformRoundTrip instead.
            .SetPhases(PlanningPhase::kTransform),
        TransformTestCase("AggregateFunction",
                          "select count(key) as count from keyvalue where key "
                          "> 1234567890123"),
        TransformTestCase(
            "AggregateDistinctFunction",
            "select count(distinct key) as count from keyvalue where key > "
            "1234567890123"),
        TransformTestCase(
            "BoolExpr",
            "select * from keyvalue where key > 5 and not(key > 10) and "
            "value='hello' and (key = 6 or value='world')"),
        TransformTestCase("ParameterizedQuery",
                          "select key, $2 as col2 from keyvalue where key=$1")
            .SetGsqlQuery("select key, @p2 as col2 from keyvalue where key=@p1")
            .SetParameterTypes({{"p1", zetasql::types::Int64Type()},
                                {"p2", zetasql::types::BoolType()}}),
        TransformTestCase("CastParameterInSelectList",
                          "select $1::bigint as col1")
            .SetGsqlQuery(
                "select cast(@p1 as int64) as col1"),  // DO_NOT_TRANSFORM
        // The following queries are NOT equal because PostgreSQL will coerce
        // any expression in the top level SELECT list with type unknown to type
        // text. This is done to prevent CREATE TABLE AS SELECT statements
        // from creating tables with columns of type unknown.
        // Note that the type coercion happens during the parse phase and is not
        // added as a cast. It also means that the parameter is NOT treated as
        // untyped by PostgreSQL, but as having type text.
        // TransformTestCase("ParameterWithUnknownTypeInSelectList",
        //                   "select $1 as col1")
        //     .SetGsqlQuery("select @p1 as col1"),
        TransformTestCase("NestedFunctionCall",
                          "select (key + 2) * 4 as product from keyvalue"),
        // TODO Analyzing 'pg.sum' as GSQL fails because function
        // is not registered with appropriate namespace.
        // TransformTestCase("NestedAggregateFunctionCall",
        //                   "select sum(key + 2) as sum from keyvalue")
        //     .SetGsqlQuery("select pg.sum(key + 2) as sum from keyvalue"),
        TransformTestCase("FilterColumnNotSelected",
                          "select key from keyvalue where value = 'abc'"),
        TransformTestCase("CountStar",
                          "select count(*) as count from keyvalue"),
        TransformTestCase(
            "SimpleNullif",
            "SELECT nullif(value, 'abc') as nullif from keyvalue"),
        TransformTestCase("CaseWhenWithValue",
                          "select case key * 2 when 4 then 8 when 6 then "
                          "24 else 100 end as result from keyvalue"),
        TransformTestCase("CaseWhenNoValue",
                          "select case when key = 4 then 8 when key > 6 then "
                          "24 else 100 end as result from keyvalue"),
        TransformTestCase("StatementHint",
                          "/*@ statement_hint = 1 */ select 1 as one")
            .SetGsqlQuery("@{ statement_hint = 1 } select 1 as one"),
        TransformTestCase(
            "JoinHint",
            "select 1 as one from keyvalue join /*@ join_hint = 1 "
            "*/ keyvalue kv2 on true")
            .SetGsqlQuery("select 1 as one from keyvalue join @{ join_hint = 1 "
                          "} keyvalue kv2 on true"),
        TransformTestCase("TableHint",
                          "select key from keyvalue /*@ table_hint = 1 */ kv")
            .SetGsqlQuery("select key from keyvalue @{ table_hint = 1 } kv"),
        TransformTestCase("InStatement", "select 1 in (1,2,3) as one"),
        TransformTestCase("WithAlias",
                          "with t1 as (select key kk, value vv from keyvalue), "
                          "t2 as (select kk xx, vv from t1) "
                          "select xx, vv, '--' as col, * from t2"),
        TransformTestCase("WithUnreferenced",
                          "with t as (select key from keyvalue), "
                          "t2 as (select 123 as col from keyvalue) "
                          "select value from keyvalue"),
        TransformTestCase("SQLValueFunction",
                          "select current_timestamp as current_timestamp")
            .SetGsqlQuery("select current_timestamp() as current_timestamp"),
        TransformTestCase(
            "ArrayConstructorExpression",
            "select array[1, 2, null, 3] as col1, array[null]::bigint[] as "
            "col2, array[]::bigint[] as col3")
            .SetGsqlQuery("select array[1, 2, null, 3] as col1, array[null] as "
                          "col2, array[] as col3")
            .SetPhases(PlanningPhase::kTransform),
        // TODO: Add additional cases like empty and NULL arrays.
        TransformTestCase("ArrayLiteralExpression",
                          "select '{null, 1, 2, 3}'::bigint[] as col1")
            .SetGsqlQuery("select array[null, 1, 2, 3] as col1"),
        TransformTestCase(
            "ArrayAtIndexConstExpression",
            "select ('{null, 1, 2, 3}'::bigint[])[2] as array_col")
            .SetGsqlQuery(
                "select (array[null, 1, 2, 3])[SAFE_ORDINAL(2)] as array_col"),
        TransformTestCase(
            "ArrayAtIndexVarExpression",
            "select timestamp_array[2] as array_col from \"ArrayTypes\"")
            .SetGsqlQuery("select timestamp_array[SAFE_ORDINAL(2)] as "
                          "array_col from ArrayTypes"),
        TransformTestCase(
            "ArrayConstructorComplexElements",
            "select array[1+1, key, null] array_col from keyvalue"),
        TransformTestCase("SelectForUpdateNoFrom", "SELECT 1 as one FOR UPDATE")
            .SetGsqlQuery("SELECT 1 as one"),
        TransformTestCase("SelectForUpdateWithFrom",
                          "SELECT key, value FROM keyvalue FOR UPDATE"),
        TransformTestCase("SelectForUpdateWithJoin",
                          "SELECT kv1.key, kv2.key "
                          "FROM keyvalue kv1, keyvalue kv2 "
                          "FOR UPDATE")
            // We can't compare the original parse tree with the deparsed parse
            // tree as the original parse tree has two RowMarkClauses (one for
            // each table in the join) but we don't produce a deparsed parse
            // tree with two RowMarkClauses here because that leads to a
            // deparsed query with duplicate FOR UPDATE clauses.
            .SetPhases(PlanningPhase::kGsqlDeparse),
        TransformTestCase(
            "SelectForUpdateWithSubquery",
            "SELECT kv1.key, kv2.key "
            "FROM (SELECT key FROM keyvalue) as kv1, keyvalue kv2 "
            "FOR UPDATE")
            // We can't compare the original parse tree with the deparsed parse
            // tree for the following reasons:
            // 1. The deparsed parse tree has `hasForUpdate true` and
            //    `pushedDown false` because we lose information about the fact
            //     that the FOR UPDATE was pushed down from the outer query to
            //     the subquery.
            // 2. We don't add the second RowMarkClause as explained above.
            .SetPhases(PlanningPhase::kGsqlDeparse)
        ),
    [](const testing::TestParamInfo<TransformTestCase>& info) {
      return info.param.name();
    });

TEST_F(QueryTransformerTest, PgQueryToGsqlResolvedStatementUnimplementedTest) {
  // Build an empty Postgres Query object, we only need to verify its
  // commandType field.
  Query* pg_query = makeNode(Query);

  pg_query->commandType = CMD_UTILITY;
  pg_query->utilityStmt = PostgresCastToNode(makeNode(CreatedbStmt));
  EXPECT_THAT(forward_transformer_->BuildGsqlResolvedStatement(*pg_query),
              StatusIs(absl::StatusCode::kUnimplemented));

  pg_query->commandType = CMD_UTILITY;
  pg_query->utilityStmt = PostgresCastToNode(makeNode(ExplainStmt));
  EXPECT_THAT(forward_transformer_->BuildGsqlResolvedStatement(*pg_query),
              StatusIs(absl::StatusCode::kUnimplemented));

  pg_query->commandType = CMD_UNKNOWN;
  EXPECT_THAT(forward_transformer_->BuildGsqlResolvedStatement(*pg_query),
              StatusIs(absl::StatusCode::kInternal));

  pg_query->commandType = CMD_NOTHING;
  EXPECT_THAT(forward_transformer_->BuildGsqlResolvedStatement(*pg_query),
              StatusIs(absl::StatusCode::kInternal));
}

TEST_F(QueryTransformerTest, PgQueryToGsqlResolvedStatementNoLocation) {
  const std::string sql = "select 12345678912";

  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  // Build a PostgreSQL Query object from the sql string
  // clang-format off
  ZETASQL_ASSERT_OK_AND_ASSIGN(Query* pg_query, BuildPgQuery(sql));
  // clang-format on

  // When the flag is set to false, expect the parse location of the statement
  // to be NULL when stmt_location is set but stmt_len is not
  absl::SetFlag(&FLAGS_spangres_include_invalid_statement_parse_locations,
                false);

  pg_query->stmt_location = 10;
  pg_query->stmt_len = 0;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const ResolvedStatement> new_statement,
      forward_transformer_->BuildGsqlResolvedStatement(*pg_query));
  EXPECT_EQ(new_statement->GetParseLocationRangeOrNULL(), nullptr);

  pg_query->stmt_len = 20;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      new_statement,
      forward_transformer_->BuildGsqlResolvedStatement(*pg_query));
  const zetasql::ParseLocationRange* location_range =
      new_statement->GetParseLocationRangeOrNULL();
  ASSERT_NE(location_range, nullptr);
  EXPECT_EQ(location_range->start().GetByteOffset(), pg_query->stmt_location);
  EXPECT_EQ(location_range->end().GetByteOffset(),
            pg_query->stmt_location + pg_query->stmt_len);

  absl::SetFlag(&FLAGS_spangres_include_invalid_statement_parse_locations,
                true);
}

TEST_F(QueryTransformerTest, PgQueryToGsqlResolvedStatementWithLocation) {
  const std::string sql = "select 12345678912";

  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  // Build a PostgreSQL Query object from the sql string
  // clang-format off
  ZETASQL_ASSERT_OK_AND_ASSIGN(Query* pg_query, BuildPgQuery(sql));
  // clang-format on

  // With stmt_len == 0, expect the range's end to be 0 as well, while
  // the range's start is the same as stmt_location.
  pg_query->stmt_location = 10;
  pg_query->stmt_len = 0;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const ResolvedStatement> new_statement,
      forward_transformer_->BuildGsqlResolvedStatement(*pg_query));
  const zetasql::ParseLocationRange* location_range =
      new_statement->GetParseLocationRangeOrNULL();
  EXPECT_EQ(location_range->start().GetByteOffset(), pg_query->stmt_location);
  EXPECT_EQ(location_range->end().GetByteOffset(), 0);

  // Now set a positive stmt_len, expect the range's end to change
  // accordingly.
  pg_query->stmt_len = 20;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      new_statement,
      forward_transformer_->BuildGsqlResolvedStatement(*pg_query));
  location_range = new_statement->GetParseLocationRangeOrNULL();
  EXPECT_EQ(location_range->start().GetByteOffset(), pg_query->stmt_location);
  EXPECT_EQ(location_range->end().GetByteOffset(),
            pg_query->stmt_location + pg_query->stmt_len);
}

TEST_F(QueryTransformerTest, BuildResolvedFilterScanNegativeTest) {
  const std::string sql = "select key from keyvalue where true";
  // Build ZetaSQL objects from the sql string:
  zetasql::TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> gsql_output;
  ZETASQL_ASSERT_OK(
      ParseAndAnalyzeFromZetaSQLForTest(sql, &type_factory, &gsql_output));

  // Retrieve ZetaSQL AST objects
  const ResolvedProjectScan* gsql_project_scan =
      CastToGsqlProjectScan(gsql_output->resolved_statement());
  const zetasql::ResolvedFilterScan* gsql_filter_scan =
      gsql_project_scan->input_scan()->GetAs<zetasql::ResolvedFilterScan>();
  const zetasql::ResolvedTableScan* gsql_table_scan =
      gsql_filter_scan->input_scan()->GetAs<zetasql::ResolvedTableScan>();

  // Build a mutable copy of the table scan:
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::ResolvedTableScan> new_table_scan,
      BuildMutableGsqlTableScan(gsql_table_scan));

  // Negative test case: invalid node type
  RangeTblEntry* rte = makeNode(RangeTblEntry);
  VarIndexScope empty_from_scan_scope;
  Node* rte_node = PostgresCastToNode(rte);
  EXPECT_THAT(
      forward_transformer_->BuildGsqlResolvedFilterScan(
          *rte_node, &empty_from_scan_scope, std::move(new_table_scan)),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("RangeTblEntry is unsupported in WHERE clauses")));
}

TEST_F(QueryTransformerTest, BuildResolvedFilterScanTest) {
  const std::string sql = "select key from keyvalue where true";

  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  // Build Postgres and ZetaSQL objects from the sql string:
  // clang-format off
  ZETASQL_ASSERT_OK_AND_ASSIGN(Query* pg_query, BuildPgQuery(sql));
  // clang-format on
  zetasql::TypeFactory type_factory;
  std::unique_ptr<const AnalyzerOutput> gsql_output;
  ZETASQL_ASSERT_OK(
      ParseAndAnalyzeFromZetaSQLForTest(sql, &type_factory, &gsql_output));

  // Retrieve ZetaSQL AST objects
  const ResolvedProjectScan* gsql_project_scan =
      CastToGsqlProjectScan(gsql_output->resolved_statement());
  const zetasql::ResolvedFilterScan* gsql_filter_scan =
      gsql_project_scan->input_scan()->GetAs<zetasql::ResolvedFilterScan>();
  const zetasql::ResolvedTableScan* gsql_table_scan =
      gsql_filter_scan->input_scan()->GetAs<zetasql::ResolvedTableScan>();

  // Build a mutable copy of the table scan:
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<zetasql::ResolvedTableScan> new_table_scan,
      BuildMutableGsqlTableScan(gsql_table_scan));

  VarIndexScope empty_from_scan_scope;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::ResolvedFilterScan> new_filter_scan,
      forward_transformer_->BuildGsqlResolvedFilterScan(
          *(pg_query->jointree->quals), &empty_from_scan_scope,
          std::move(new_table_scan)));

  EXPECT_EQ(new_filter_scan->DebugString(),
            StripParseLocations(gsql_filter_scan->DebugString()));
}

// Build a list of Value nodes containing palloc'd strings from the provided
// list and attach to the provided Alias.
void BuildAliasColumnList(const std::vector<std::string>& column_aliases,
                          Alias& alias) {
  for (const auto& column_name : column_aliases) {
    alias.colnames =
        lappend(alias.colnames, makeString(pstrdup(column_name.c_str())));
  }
}

const zetasql::Table* FindTable(CatalogAdapter* adapter,
                                  TableName table_name) {
  const zetasql::Table* table = nullptr;
  ABSL_CHECK_OK(
      adapter->GetEngineUserCatalog()->FindTable(table_name.AsSpan(), &table));
  ABSL_CHECK_NE(table, nullptr);
  return table;
}

// Transform a zetasql::Table into a PostgreSQL RangeTblEntry and verify
// basic fields values are set.
TEST_F(QueryTransformerTest, BasicTableTransformTest) {
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Test that we can build an RTE from a simple table.
  RangeTblEntry* key_value_rte;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      key_value_rte,
      Transformer::BuildPgRangeTblEntry(
          *catalog_adapter, *FindTable(catalog_adapter, kTableName),
          /*alias=*/nullptr, /*inFromCl=*/true, ACL_SELECT));

  // Validate basic RTE contents.
  EXPECT_STREQ(key_value_rte->eref->aliasname, "keyvalue");
  EXPECT_EQ(key_value_rte->rtekind, RTE_RELATION);
  EXPECT_EQ(key_value_rte->relkind, RELKIND_RELATION);
}

// Transform a zetasql::Table into a PostgreSQL RangeTblEntry and verify
// relid (oid) values are set properly.
TEST_F(QueryTransformerTest, TableOidAssignmentTest) {
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());

  // Build an RTE from a simple table.
  RangeTblEntry* key_value_rte;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      key_value_rte,
      Transformer::BuildPgRangeTblEntry(
          *catalog_adapter, *FindTable(catalog_adapter, kTableName),
          /*alias=*/nullptr, /*inFromCl=*/true, ACL_SELECT));

  // Validate basic RTE relid being generated is valid.
  EXPECT_LT(key_value_rte->relid, CatalogAdapter::kOidCounterStart);
  EXPECT_GT(key_value_rte->relid, CatalogAdapter::kOidCounterEnd);

  // Validate that catalog_adapter stores correct information:
  // Note that the table name has been converted to lowercase
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(key_value_rte->relid),
              IsOkAndHolds(kTableName));

  EXPECT_THAT(catalog_adapter->GetOrGenerateOidFromTableName(kTableName),
              IsOkAndHolds(key_value_rte->relid));

  // Ask for table name for another oid; expect failure.
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(key_value_rte->relid - 1),
              StatusIs(absl::StatusCode::kInternal));

  // Try transforming again; expect the same oid.
  EXPECT_THAT(Transformer::BuildPgRangeTblEntry(
                  *catalog_adapter, *FindTable(catalog_adapter, kTableName),
                  /*alias=*/nullptr,
                  /*inFromCl=*/true, ACL_SELECT),
              IsOkAndHolds(
                  testing::Field(&RangeTblEntry::relid, key_value_rte->relid)));
}

// Transform 2 zetasql::Table objects into 2 PostgreSQL RangeTblEntry
// objects and verify relid (oid) values are set properly.
TEST_F(QueryTransformerTest, TwoTableOidAssignmentTest) {
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  // Build an RTE from a simple table.
  const TableName kKeyValue2TableName({"public", "KeyValue2"});

  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
  RangeTblEntry* key_value_rte;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      key_value_rte,
      Transformer::BuildPgRangeTblEntry(
          *catalog_adapter, *FindTable(catalog_adapter, kTableName),
          /*alias=*/nullptr, /*inFromCl=*/true, ACL_SELECT));

  // Build a second RTE from another table
  RangeTblEntry* key_value2_rte;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      key_value2_rte,
      Transformer::BuildPgRangeTblEntry(
          *catalog_adapter, *FindTable(catalog_adapter, kKeyValue2TableName),
          /*alias=*/nullptr, /*inFromCl=*/true, ACL_SELECT));

  // Validate that catalog_adapter stores correct information:
  EXPECT_THAT(catalog_adapter->GetTableNameFromOid(key_value2_rte->relid),
              IsOkAndHolds(kKeyValue2TableName));

  EXPECT_THAT(
      catalog_adapter->GetOrGenerateOidFromTableName(kKeyValue2TableName),
      IsOkAndHolds(key_value2_rte->relid));

  // Make sure the assigned relid is valid and different from the first table
  EXPECT_LT(key_value2_rte->relid, CatalogAdapter::kOidCounterStart);
  EXPECT_GT(key_value2_rte->relid, CatalogAdapter::kOidCounterEnd);
  EXPECT_NE(key_value2_rte->relid, key_value_rte->relid);
}

// Transform a zetasql::Table into a PostgreSQL RangeTblEntry and verify
// column properties.
TEST_F(QueryTransformerTest, TableColumnTransformTest) {
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  // Test that we can build an RTE from a simple table.
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
  RangeTblEntry* key_value_rte;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      key_value_rte,
      Transformer::BuildPgRangeTblEntry(
          *catalog_adapter, *FindTable(catalog_adapter, kTableName),
          /*alias=*/nullptr, /*inFromCl=*/true, ACL_SELECT));

  // Validate per-column contents.
  const zetasql::Table* key_value_table;
  ZETASQL_ASSERT_OK(catalog_adapter->GetEngineUserCatalog()->FindTable(
      kTableName.AsSpan(), &key_value_table));
  EXPECT_EQ(list_length(key_value_rte->eref->colnames),
            key_value_table->NumColumns());
  Oid col_typeoid;
  int32_t col_typmod;
  Oid col_collationid;
  // For each column, check type information, name, and not is_dropped.
  // Note that PostgreSQL attributes are 1-indexed, so we convert.
  for (int i = 0; i < key_value_table->NumColumns(); ++i) {
    GetAttributeTypeC(key_value_rte->relid, /*1-indexed*/ i + 1, &col_typeoid,
                      &col_typmod, &col_collationid);

    ZETASQL_ASSERT_OK_AND_ASSIGN(
        Oid oid_from_gsql,
        Transformer::BuildPgTypeOid(*catalog_adapter,
                                    key_value_table->GetColumn(i)->GetType()));
    EXPECT_EQ(col_typeoid, oid_from_gsql);
    ZETASQL_ASSERT_OK_AND_ASSIGN(const FormData_pg_type* pg_type,
                         PgBootstrapCatalog::Default()->GetType(oid_from_gsql));
    EXPECT_EQ(col_typmod, pg_type->typtypmod);
    EXPECT_EQ(col_collationid, pg_type->typcollation);

    EXPECT_EQ(get_rte_attribute_name(key_value_rte, /*1-indexed*/ i + 1),
              key_value_table->GetColumn(i)->Name());

    EXPECT_FALSE(get_rte_attribute_is_dropped(key_value_rte,
                                              /*1-indexed*/ i + 1));
  }
}

// Test that column names and aliases are properly set in a transformed
// RangeTblEntry.
TEST_F(QueryTransformerTest, TableAliasTransformTest) {
  std::unique_ptr<CatalogAdapterHolder> catalog_adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  // Test that we correctly rename columns when aliases are supplied.
  TableName all_types_table_name({"AllSpangresTypes"});
  const zetasql::Table* all_types_table;
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSERT_OK_AND_ASSIGN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSERT_OK(catalog_adapter->GetEngineUserCatalog()->FindTable(
      all_types_table_name.AsSpan(), &all_types_table));
  // Build the alias names vector programatically to be robust to test catalog
  // schema changes.
  std::vector<std::string> alias_names;
  for (int i = 0; i < all_types_table->NumColumns(); ++i) {
    alias_names.push_back(absl::StrCat("Column", i));
  }
  Alias* all_types_alias = makeAlias("AllSpangresTypesAlias", nullptr);
  BuildAliasColumnList(alias_names, *all_types_alias);
  ASSERT_EQ(list_length(all_types_alias->colnames), alias_names.size());

  RangeTblEntry* all_types_rte;
  ZETASQL_ASSERT_OK_AND_ASSIGN(all_types_rte,
                       Transformer::BuildPgRangeTblEntry(
                           *catalog_adapter, *all_types_table, all_types_alias,
                           /*inFromCl=*/true, ACL_SELECT));

  // Validate names only.
  EXPECT_STREQ(all_types_rte->eref->aliasname, "AllSpangresTypesAlias");
  EXPECT_EQ(list_length(all_types_rte->alias->colnames),
            all_types_table->NumColumns());
  for (int i = 0; i < all_types_table->NumColumns(); ++i) {
    EXPECT_STREQ(get_rte_attribute_name(all_types_rte, /*1-indexed*/ i + 1),
                 alias_names[i].c_str());
  }
}

TEST_F(QueryTransformerTest, UnsupportedUpdateWithFromClause) {
  const std::string sql =
      "update keyvalue set value='abc' from keyvalue kv2 where "
      "keyvalue.key=kv2.key";
  EXPECT_THAT(
      ForwardTransformQuery(sql, analyzer_options_),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("UPDATE...FROM statements are not supported")));
}

// Helper function to extract the single TableScan from a ZetaSQL
// AnalyzerOutput. Currently this only supports statements with a single
// TableScan.
absl::StatusOr<const zetasql::ResolvedTableScan*> GetTableScan(
    const zetasql::AnalyzerOutput* analyzer_output) {
  std::vector<const zetasql::ResolvedNode*> nodes;
  analyzer_output->resolved_statement()->GetDescendantsSatisfying(
      &zetasql::ResolvedNode::Is<zetasql::ResolvedTableScan>, &nodes);
  ZETASQL_RET_CHECK_EQ(nodes.size(), 1)
      << "This helper only supports statements with a single table scan";
  return nodes[0]->GetAs<zetasql::ResolvedTableScan>();
}

TEST(TransformerTest, PruneUnusedPrunesOneColumn) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement("select key, value from keyvalue",
                                        options));

  // We expect that "key" and "value" were not pruned and only "_exists" was
  // pruned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"key", "value"};
  const zetasql::Table* catalog_table;
  ZETASQL_ASSERT_OK(GetSpangresTestSpannerUserCatalog()->FindTable({"keyvalue"},
                                                           &catalog_table));
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
    EXPECT_EQ(
        catalog_table->GetColumn(table_scan->column_index_list(i))->Name(),
        column_names[i]);
  }
}

TEST(TransformerTest, PruneUnusedPrunesMultipleColumns) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement("select value from keyvalue", options));

  // We expect that "key" and "exists_" were pruned from the table scan. That
  // should leave a single "value" column.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  EXPECT_EQ(table_scan->column_list_size(), 1);
  EXPECT_EQ(table_scan->column_list(0).table_name(), "keyvalue");
  EXPECT_EQ(table_scan->column_list(0).name(), "value");
  EXPECT_EQ(table_scan->column_index_list(0), 1);
}

TEST(TransformerTest, PruneUnusedPrunesInsert) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement(
          "insert into keyvalue values (1, 'abc')", options));

  // We are implicitly inserting into "key" and "value" columns so we expect
  // that "key" and "value" were not pruned and only "_exists" was pruned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"key", "value"};
  const zetasql::Table* catalog_table;
  ZETASQL_ASSERT_OK(GetSpangresTestSpannerUserCatalog()->FindTable({"keyvalue"},
                                                           &catalog_table));
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
    EXPECT_EQ(
        catalog_table->GetColumn(table_scan->column_index_list(i))->Name(),
        column_names[i]);
  }
}

TEST(TransformerTest, InsertOnConflictDoNothing) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  options.mutable_language()->DisableLanguageFeature(
      zetasql::FEATURE_INSERT_ON_CONFLICT_CLAUSE);
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement(
          "INSERT INTO keyvalue VALUES (1, 'abc') ON CONFLICT(key) DO NOTHING",
          options));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"key", "value"};
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
  }

  const zetasql::ResolvedInsertStmt* stmt =
      gsql_output->resolved_statement()->GetAs<zetasql::ResolvedInsertStmt>();
  EXPECT_EQ(stmt->insert_mode(), zetasql::ResolvedInsertStmt::OR_IGNORE);
  EXPECT_EQ(stmt->row_list_size(), 1);
}

TEST(TransformerTest, InsertOnConflictDoUpdate) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  options.mutable_language()->DisableLanguageFeature(
      zetasql::FEATURE_INSERT_ON_CONFLICT_CLAUSE);
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement(
          "INSERT INTO keyvalue VALUES (1, 'abc') ON CONFLICT(key) "
          "DO UPDATE SET key = excluded.key, value = excluded.value",
          options));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"key", "value"};
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
  }

  const zetasql::ResolvedInsertStmt* stmt =
      gsql_output->resolved_statement()->GetAs<zetasql::ResolvedInsertStmt>();
  EXPECT_EQ(stmt->insert_mode(), zetasql::ResolvedInsertStmt::OR_UPDATE);
  EXPECT_EQ(stmt->row_list_size(), 1);
}

TEST(TransformerTest, PruneUnusedPrunesDelete) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement(
          "delete from keyvalue where value = 'f in the chat'", options));

  // We are deleting by specifying the "value" column so we expect
  // that "value" was not pruned and "key" and "_exists" were pruned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"value"};
  const zetasql::Table* catalog_table;
  ZETASQL_ASSERT_OK(GetSpangresTestSpannerUserCatalog()->FindTable({"keyvalue"},
                                                           &catalog_table));
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
    EXPECT_EQ(
        catalog_table->GetColumn(table_scan->column_index_list(i))->Name(),
        column_names[i]);
  }
}

TEST(TransformerTest, PruneUnusedPrunesUpdate) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement(
          "update keyvalue set key=1234567890123, value='its a new day';",
          options));

  // We are updating "key" and "value" columns so we expect that "key" and
  // "value" were not pruned and only "_exists" was pruned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"key", "value"};
  const zetasql::Table* catalog_table;
  ZETASQL_ASSERT_OK(GetSpangresTestSpannerUserCatalog()->FindTable({"keyvalue"},
                                                           &catalog_table));
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
    EXPECT_EQ(
        catalog_table->GetColumn(table_scan->column_index_list(i))->Name(),
        column_names[i]);
  }
}

TEST(TransformerTest, PruneUnusedPrunesUpdateValueOnly) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement("UPDATE keyvalue SET value = '10'",
                                        options));

  // We are updating the "value" column so we expect that "key" and "_exists"
  // were pruned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"value"};
  const zetasql::Table* catalog_table;
  ZETASQL_ASSERT_OK(GetSpangresTestSpannerUserCatalog()->FindTable({"keyvalue"},
                                                           &catalog_table));
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
    EXPECT_EQ(
        catalog_table->GetColumn(table_scan->column_index_list(i))->Name(),
        column_names[i]);
  }
}

TEST(TransformerTest, PruneUnusedPrunesUpdateKeyAndValue) {
  zetasql::AnalyzerOptions options = GetSpangresTestAnalyzerOptions();
  ASSERT_TRUE(options.prune_unused_columns());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const zetasql::AnalyzerOutput> gsql_output,
      ParseAnalyzeAndTransformStatement(
          "UPDATE keyvalue SET value = 'abc' WHERE key=1", options));

  // We are updating the "value" column and referencing "key" in the WHERE
  // clause so we expect that "key" and "value" are retained and only
  // "_exists" is pruned.
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::ResolvedTableScan* table_scan,
                       GetTableScan(gsql_output.get()));
  const std::vector<std::string> column_names{"key", "value"};
  const zetasql::Table* catalog_table;
  ZETASQL_ASSERT_OK(GetSpangresTestSpannerUserCatalog()->FindTable({"keyvalue"},
                                                           &catalog_table));
  EXPECT_EQ(table_scan->column_list_size(), column_names.size());
  for (int i = 0; i < table_scan->column_list_size(); ++i) {
    EXPECT_EQ(table_scan->column_list(i).name(), column_names[i]);
    EXPECT_EQ(table_scan->column_list(i).table_name(), "keyvalue");
    EXPECT_EQ(
        catalog_table->GetColumn(table_scan->column_index_list(i))->Name(),
        column_names[i]);
  }
}

TEST_F(QueryTransformerTest, ExpressionRecursionLimitTest) {
  int recursion_limit =
      absl::GetFlag(FLAGS_spangres_expression_recursion_limit);
  absl::SetFlag(&FLAGS_spangres_expression_recursion_limit, 3);

  const std::string sql = "select 1+1+2+3+4";
  EXPECT_THAT(ForwardTransformQuery(sql, analyzer_options_),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("SQL expression limit exceeded")));
  absl::SetFlag(&FLAGS_spangres_expression_recursion_limit, recursion_limit);
}

TEST_F(QueryTransformerTest, MultipleExpressionsTest) {
  int recursion_limit =
      absl::GetFlag(FLAGS_spangres_expression_recursion_limit);
  absl::SetFlag(&FLAGS_spangres_expression_recursion_limit, 3);

  const std::string sql = "select 1+1,1+2, 1+3, 1+4";
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<const ResolvedStatement> transformed_statement,
      ForwardTransformQuery(sql, analyzer_options_));

  // Validate the resolved AST.
  zetasql::Validator validator(analyzer_options_.language());
  ZETASQL_ASSERT_OK(validator.ValidateResolvedStatement(transformed_statement.get()));

  absl::SetFlag(&FLAGS_spangres_expression_recursion_limit, recursion_limit);
}

// Verify default RTE settings is inh == true.
TEST_F(QueryTransformerTest, RTEDefaultsToInh) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const RangeTblEntry* rte,
      internal::makePartialRangeTblEntry(/*inFromCl=*/true, ACL_SELECT));

  EXPECT_TRUE(rte->inh);
}

TEST_F(QueryTransformerTest, SetRecursionDepth) {
  absl::SetFlag(&FLAGS_spangres_set_operation_recursion_limit, 2);

  const std::string sql =
      "select 1 intersect select 2 union select 3 except select 4";

  EXPECT_THAT(ForwardTransformQuery(sql, analyzer_options_),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(
                           "SQL set operation recursion limit exceeded")));
}

TEST_F(QueryTransformerTest, UnsupportedQueryFields) {
  // Run the parser and analyzer to get a generic Query*.
  std::unique_ptr<CatalogAdapterHolder> adapter_holder =
      GetSpangresTestCatalogAdapterHolder(analyzer_options_);
  Query* query;
  ZETASQL_ASSERT_OK_AND_ASSIGN(query, BuildPgQuery("select 1", analyzer_options_));

  // Run the forward transformer.
  auto transformer = std::make_unique<ForwardTransformer>(
      adapter_holder->ReleaseCatalogAdapter());

  // ON CONFLICT without an action.
  query->onConflict = makeNode(OnConflictExpr);
  EXPECT_EQ(query->onConflict->action, ONCONFLICT_NONE);
  EXPECT_THAT(
      transformer->BuildGsqlResolvedStatement(*query),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("ON CONFLICT clauses are not supported")));
  query->onConflict = nullptr;

  // GROUPING SET clauses.
  query->groupingSets = list_make1(makeNode(GroupingSet));
  EXPECT_THAT(
      transformer->BuildGsqlResolvedStatement(*query),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("GROUPING SET clauses are not supported")));
  query->groupingSets = nullptr;

  // WINDOW clauses.
  query->windowClause = list_make1(makeNode(WindowClause));
  EXPECT_THAT(transformer->BuildGsqlResolvedStatement(*query),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("WINDOW clauses are not supported")));
  query->windowClause = nullptr;

  // DISTINCT ON clauses.
  query->distinctClause = list_make1(makeNode(SortGroupClause));
  query->hasDistinctOn = true;
  EXPECT_THAT(
      transformer->BuildGsqlResolvedStatement(*query),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("DISTINCT ON clauses are not supported")));
  query->distinctClause = nullptr;
  query->hasDistinctOn = false;

  // ROW MARK clause with unsupported lock strength.
  auto row_mark_clause = makeNode(RowMarkClause);
  row_mark_clause->strength = LockClauseStrength::LCS_FORKEYSHARE;
  query->rowMarks = list_make1(row_mark_clause);
  EXPECT_THAT(transformer->BuildGsqlResolvedStatement(*query),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("Statements with locking clauses other than "
                                 "FOR UPDATE are not supported")));
  query->rowMarks = nullptr;

  // ROW MARK clause with unsupported lock wait policy.
  row_mark_clause->strength = LockClauseStrength::LCS_FORUPDATE;
  row_mark_clause->waitPolicy = LockWaitPolicy::LockWaitSkip;
  query->rowMarks = list_make1(row_mark_clause);
  EXPECT_THAT(
      transformer->BuildGsqlResolvedStatement(*query),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("Statements with locking clauses with lock wait "
                         "policies are not supported")));
  query->rowMarks = nullptr;

  // Constraint clauses.
  query->constraintDeps = list_make1_oid(InvalidOid);
  EXPECT_THAT(
      transformer->BuildGsqlResolvedStatement(*query),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("constraint clauses are not supported")));
  query->constraintDeps = nullptr;

  // WITH ABSL_CHECK options.
  query->withCheckOptions = list_make1(makeNode(WithCheckOption));
  EXPECT_THAT(
      transformer->BuildGsqlResolvedStatement(*query),
      StatusIs(absl::StatusCode::kUnimplemented,
               HasSubstr("WITH CHECK options are not supported")));
  query->withCheckOptions = nullptr;
}
}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
