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

#include "third_party/spanner_pg/interface/spangres_translator.h"

#include <limits>
#include <memory>

#include "zetasql/base/logging.h"
#include "zetasql/parser/parser.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_visitor.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/cleanup/cleanup.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/interface/sql_builder.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/shims/memory_reservation_holder.h"
#include "third_party/spanner_pg/shims/parser_output_serialization.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"

ABSL_FLAG(int64_t, spangres_sql_length_limit, 100'000,
          "The maximum length of a supported SQL text.");
ABSL_FLAG(int64_t, spangres_stub_memory_reservation_size, 64'000'000,
          "The reservable amount of memory for the test stub memory "
          "reservation system, 0 for no limit.");

namespace postgres_translator {
namespace spangres {

SpangresTranslator::SpangresTranslator() {}

interfaces::ParserSingleOutput SpangresTranslator::ParseQuery(
    interfaces::ParserInterface* parser, absl::string_view sql,
    std::unique_ptr<interfaces::MemoryReservationManager> res_manager,
    const std::function<void(const interfaces::ParserBatchOutput::Statistics&)>&
        parser_statistics_callback) {
  std::string sql_str(sql);
  interfaces::ParserSingleOutput output =
      parser->Parse(sql_str, std::move(res_manager));
  if (parser_statistics_callback) {
    parser_statistics_callback(output.statistics());
  }
  return output;
}

template <typename ParamsT>
std::unique_ptr<interfaces::MemoryReservationManager>
SpangresTranslator::GetMemoryReservationManager(ParamsT& params) {
  std::unique_ptr<interfaces::MemoryReservationManager> manager(
      params.TransferMemoryReservationManager());
  return manager == nullptr
             ? std::make_unique<StubMemoryReservationManager>(
                   absl::GetFlag(FLAGS_spangres_stub_memory_reservation_size))
             : std::move(manager);
}

absl::StatusOr<interfaces::ParserOutput>
SpangresTranslator::GetParserQueryOutput(
    interfaces::TranslateParsedQueryParams& params,
    std::unique_ptr<interfaces::PGArena>& arena) {
  interfaces::ParserOutput* deserialized_parser_output =
      params.mutable_parser_output();
  if (deserialized_parser_output != nullptr) {
    return std::move(*deserialized_parser_output);
  }
  const std::string* serialized_parse_tree = params.serialized_parse_tree();
  if (serialized_parse_tree != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        arena, MemoryContextPGArena::Init(GetMemoryReservationManager(params)));
    List* parse_tree;
    ZETASQL_ASSIGN_OR_RETURN(parse_tree, DeserializeParseQuery(*serialized_parse_tree));
    return interfaces::ParserOutput(parse_tree, /*token_locations=*/{});
  }
  return absl::InternalError(
      "TranslateParsedQueryParams did not contain serialized or deserialized "
      "parser output");
}

absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
SpangresTranslator::TranslateQuery(interfaces::TranslateQueryParams params) {
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserSingleOutput parser_single_output,
                   GetQueryParserOutput(params));
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserOutput parser_output,
                   std::move(*parser_single_output.mutable_output()));
  return TranslateParsedQuery(interfaces::TranslateParsedQueryParams(
      std::move(parser_output), params.TransferCommonParams()));
}

// TODO: Refactor code to remove the two function parameters.
absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
SpangresTranslator::TranslateParsedTree(
    interfaces::TranslateParsedQueryParams& params,
    std::function<decltype(GetParserQueryOutput)> parser_output_getter,
    std::function<absl::Status(Query* query)> query_deparser_callback =
        nullptr) {
  interfaces::TranslationProgress* progress =
      params.translation_progress_output();
  if (progress != nullptr) {
    *progress = interfaces::TranslationProgress::NONE;
  }

  std::unique_ptr<interfaces::PGArena> arena;
  absl::StatusOr<interfaces::ParserOutput> parser_output(
      parser_output_getter(params, arena));
  ZETASQL_RETURN_IF_ERROR(parser_output.status());
  if (list_length(parser_output->parse_tree()) != 1) {
    return absl::InvalidArgumentError(
        "SQL queries are limited to single statements");
  }

  if (progress != nullptr) {
    *progress = interfaces::TranslationProgress::PARSER;
  }

  // Set the default time zone. This time zone will be used to parse TIMESTAMPTZ
  // literals.
  ZETASQL_ASSIGN_OR_RETURN(
      auto time_zone_scope,
      TimeZoneScope::Create(
          params.googlesql_analyzer_options().default_time_zone()));

  // Set the EngineSystemCatalog singleton.
  ZETASQL_ASSIGN_OR_RETURN(bool initialized_catalog,
                   SpangresSystemCatalog::TryInitializeEngineSystemCatalog(
                       params.TransferEngineBuiltinFunctionCatalog(),
                       params.googlesql_analyzer_options().language()));

  if (initialized_catalog) {
    ABSL_LOG(INFO) << "EngineSystemCatalog initialized";
  }

  // Wrap catalog with EngineUserCatalog wrapper. EngineUserCatalog wrapper
  // provides schema_name mapping and other PostgreSQL
  // features.
  auto engine_user_catalog =
      std::make_unique<SpangresUserCatalog>(params.engine_provided_catalog());
  ZETASQL_ASSIGN_OR_RETURN(auto catalog_adapter_holder,
                   CatalogAdapterHolder::Create(
                       std::move(engine_user_catalog),
                       EngineSystemCatalog::GetEngineSystemCatalog(),
                       params.googlesql_analyzer_options(),
                       parser_output->token_locations()));

  RawStmt* raw_stmt = linitial_node(RawStmt, parser_output->parse_tree());

  // Transform the prepared statement parameter types from ZetaSQL types to
  // PostgreSQL type oids.
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  zetasql::QueryParametersMap gsql_param_types =
      params.googlesql_analyzer_options().query_parameters();

  Oid* pg_param_types;
  int num_params = 0;
  ZETASQL_ASSIGN_OR_RETURN(pg_param_types,
                   Transformer::BuildPgParameterTypeList(
                       *catalog_adapter, gsql_param_types, &num_params));

  // We need null-termination on the SQL expression, which absl::string_view
  // doesn't guarantee.
  std::string original_sql(params.sql_expression());
  ZETASQL_ASSIGN_OR_RETURN(
      Query * pg_query,
      CheckedPgParseAnalyzeVarparams(raw_stmt, original_sql.c_str(),
                                     &pg_param_types, &num_params));
  ZETASQL_RET_CHECK_NE(pg_query, nullptr) << "analyzed query not found";

  if (progress != nullptr) {
    *progress = interfaces::TranslationProgress::ANALYZER;
  }
  if (params.pg_query_callback()) {
    ZETASQL_RETURN_IF_ERROR(params.pg_query_callback()(pg_query));
  }

  if (query_deparser_callback) {
    ZETASQL_RETURN_IF_ERROR(query_deparser_callback(pg_query));
  }

  auto transformer = std::make_unique<postgres_translator::ForwardTransformer>(
      catalog_adapter_holder->ReleaseCatalogAdapter());
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedStatement> stmt,
                   transformer->BuildGsqlResolvedStatement(*pg_query));

  gsql_param_types = transformer->query_parameter_types();

  if (params.validate_ast()) {
    zetasql::Validator validator(
        params.googlesql_analyzer_options().language());
    ZETASQL_RETURN_IF_ERROR(validator.ValidateResolvedStatement(stmt.get()));
  }

  int max_column_id = transformer->catalog_adapter().max_column_id();

  if (progress != nullptr) {
    *progress = interfaces::TranslationProgress::COMPLETE;
  }

  // TODO: Fill in the remaining fields of AnalyzerOutput with
  // actual values.
  auto analyzer_output = std::make_unique<zetasql::AnalyzerOutput>(
      params.googlesql_analyzer_options().id_string_pool(),
      params.googlesql_analyzer_options().arena(), std::move(stmt),
      zetasql::AnalyzerOutputProperties(),
      /*parser_output=*/nullptr,
      /*deprecation_warnings=*/std::vector<absl::Status>(), gsql_param_types,
      /*undeclared_position_parameters=*/std::vector<const zetasql::Type*>(),
      max_column_id);
  return analyzer_output;
}

absl::Status SpangresTranslator::RewriteTranslatedTree(
    zetasql::AnalyzerOutput* analyzer_output,
    interfaces::TranslateParsedQueryParams& params) {
  auto analyzer_options = params.googlesql_analyzer_options();
  // Enable relevant rewriters only.
  analyzer_options.set_enabled_rewrites(
      {zetasql::REWRITE_BUILTIN_FUNCTION_INLINER});
  return zetasql::RewriteResolvedAst(
      analyzer_options, params.sql_expression(),
      params.engine_provided_catalog(), GetTypeFactory(), *analyzer_output);
}

absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
SpangresTranslator::TranslateParsedQuery(
    interfaces::TranslateParsedQueryParams params) {
  return TranslateParsedTree(params, GetParserQueryOutput);
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateTableLevelExpression(
    interfaces::TranslateQueryParams params, absl::string_view table_name) {
  // Wrap the expression in SELECT <expression> FROM <table_name>.
  ZETASQL_ASSIGN_OR_RETURN(std::string wrapped_expression,
                   WrapExpressionInSelect(params.sql_expression(), table_name));

  // Get the parser to construct a new instance of TranslateQueryParams.
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserInterface * parser, params.parser());
  interfaces::TranslateQueryParams wrapped_params(
      wrapped_expression, parser, params.engine_provided_catalog(),
      params.TransferEngineBuiltinFunctionCatalog());
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserSingleOutput parser_single_output,
                   GetQueryParserOutput(wrapped_params));

  // Get the parser output to construct the instance of
  // TranslateParsedQueryParams.
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserOutput parser_output,
                   std::move(*parser_single_output.mutable_output()));

  return TranslateParsedTableLevelExpression(
      interfaces::TranslateParsedQueryParamsBuilder(
          std::move(parser_output), {}, params.engine_provided_catalog(),
          wrapped_params.TransferEngineBuiltinFunctionCatalog())
          .SetAnalyzerOptions(params.googlesql_analyzer_options())
          .SetTypeFactory(params.type_factory())
          .Build(),
      table_name);
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateParsedTableLevelExpression(
    interfaces::TranslateParsedQueryParams params,
    absl::string_view table_name) {
  static auto expression_deparser = [](std::string* deparsed_expression,
                                       Query* query) -> absl::Status {
    ZETASQL_RET_CHECK_NE(query->targetList, nullptr)
        << "Target list should be empty in query";

    // The Pg analyzer may generate two target entries from one expression, for
    // example: `row_number() OVER (ORDER BY a)`. This window function is not
    // supported by the generated column, because it is not for single row.
    //
    // TODO: Ideally, we should use Pg's analyzer to analyze the
    // expression under DDL context, which is controlled by this enum
    // `ParseExprKind`.
    if (list_length(query->targetList) > 1) {
      // TODO: Improve the error messages to include the
      // problematic expression.
      return absl::InvalidArgumentError("Unsupported expression in statement.");
    }
    ZETASQL_RET_CHECK(list_length(query->targetList) == 1)
        << "Target list's size should be one";

    void* obj = list_head(query->targetList)->ptr_value;
    Expr* expr = internal::PostgresCastNode(TargetEntry, obj)->expr;
    ZETASQL_ASSIGN_OR_RETURN(
        absl::string_view deparsed,
        CheckedPgDeparseExprInQuery(internal::PostgresCastToNode(expr), query));
    // The deparsed expression may have extra parenthesis, this is consistent
    // with how pg_dump printing the schema.
    deparsed_expression->assign(deparsed);
    return absl::OkStatus();
  };

  std::string deparsed_expression;
  std::unique_ptr<zetasql::AnalyzerOutput> analyzer_output;

  // Choose how to get the parsed expression based on the value passed in
  // in the `params`. If serialized_parse_tree() is not null, then use the
  // deserializer to construct parsed tree, otherwise, get the parsed tree
  // from `params` directly.
  if (params.serialized_parse_tree() != nullptr) {
    ZETASQL_ASSIGN_OR_RETURN(
        analyzer_output,
        TranslateParsedTree(params,
                            bind(GetParserExpressionOutput, table_name,
                                 std::placeholders::_1, std::placeholders::_2),
                            bind(expression_deparser, &deparsed_expression,
                                 std::placeholders::_1)));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(
        analyzer_output,
        TranslateParsedTree(params, GetParserQueryOutput,
                            bind(expression_deparser, &deparsed_expression,
                                 std::placeholders::_1)));
  }

  const zetasql::ResolvedStatement* stmt =
      analyzer_output->resolved_statement();
  // Default SQLBuilder will print the ColumnRef by using a random generated
  // column name
  class : public SQLBuilder {
    absl::Status VisitResolvedColumnRef(
        const zetasql::ResolvedColumnRef* node) {
      PushQueryFragment(node,
                        // Identifier requires quoteing if necessary.
                        zetasql::ToIdentifierLiteral(node->column().name()));
      return absl::OkStatus();
    }
  } builder;

  const zetasql::ResolvedQueryStmt* query =
      stmt->GetAs<zetasql::ResolvedQueryStmt>();
  // Process and extract the ResolvedExpr from statement.
  const zetasql::ResolvedProjectScan* ps =
      query->query()->GetAs<zetasql::ResolvedProjectScan>();

  // It is possible a parser valid node which supposed to be an expression
  // is actually not a valid expression. Pg relies on analysis phase to catch
  // such invalid input. If the result, after translated by analyzer under
  // SELECT context , not a expression, it must be not a valid expression also
  // in ddl's context like: <CREATE TABLE>.
  // TODO: Fix the error message in google sql analyzer.
  if (ps->expr_list().size() == 1) {
    const zetasql::ResolvedExpr* expr = ps->expr_list(0)->expr();
    ZETASQL_RET_CHECK_NE(expr, nullptr) << "Expr in ResolvedExpr should not be null";

    ZETASQL_RET_CHECK_OK(builder.Process(*expr));

    return interfaces::ExpressionTranslateResult{deparsed_expression,
                                                 builder.sql()};
  }

  if (ps->expr_list_size() == 0 && ps->column_list_size() == 1) {
    // If the expression is just a column reference, there will be no expression
    // list in the resolved query statement.
    return interfaces::ExpressionTranslateResult{
        deparsed_expression,
        zetasql::ToIdentifierLiteral(ps->column_list(0).name())};
  }

  return absl::InvalidArgumentError("Expression in the statement is not valid");
}

absl::StatusOr<interfaces::ParserOutput>
SpangresTranslator::GetParserExpressionOutput(
    absl::string_view table_name,
    interfaces::TranslateParsedQueryParams& params,
    std::unique_ptr<interfaces::PGArena>& arena) {
  ZETASQL_RET_CHECK(!table_name.empty()) << "Table name can not be null or empty";

  ZETASQL_RET_CHECK_NE(params.serialized_parse_tree(), nullptr)
      << "Expression can not be null";
  ZETASQL_ASSIGN_OR_RETURN(
      arena, MemoryContextPGArena::Init(GetMemoryReservationManager(params)));
  ZETASQL_ASSIGN_OR_RETURN(Node * expression,
                   DeserializeParseExpression(*params.serialized_parse_tree()));

  ZETASQL_ASSIGN_OR_RETURN(List * parse_tree,
                   WrapExpressionInSelect(expression, table_name));
  return interfaces::ParserOutput(parse_tree, /*token_locations=*/{});
}

absl::StatusOr<std::string> SpangresTranslator::WrapExpressionInSelect(
    absl::string_view expression, std::string_view table_name) {
  // Always quote the table name: $1, otherwise it cause issues, for example:
  // table_name lost the case sensitivity which can cause "table not found".
  static constexpr char select_template[] = R"(SELECT ($0) from "$1")";
  ZETASQL_RET_CHECK(!expression.empty()) << "Expression can not be null or empty";
  ZETASQL_RET_CHECK(!table_name.empty()) << "Table name can not be null or empty";
  return absl::Substitute(select_template, expression, table_name);
}

absl::StatusOr<List*> SpangresTranslator::WrapExpressionInSelect(
    Node* expression, std::string_view table_name) {
  ZETASQL_RET_CHECK_NE(expression, nullptr) << "Expression can not be null";
  ZETASQL_RET_CHECK(!table_name.empty()) << "Table name can not be null or empty";

  // List of statements
  ZETASQL_ASSIGN_OR_RETURN(RawStmt * raw_stmt, CheckedPgMakeNode(RawStmt));
  ZETASQL_ASSIGN_OR_RETURN(List * wrapped_tree,
                   CheckedPgLappend(/*list=*/nullptr, raw_stmt));

  // SELECT
  ZETASQL_ASSIGN_OR_RETURN(SelectStmt * select, CheckedPgMakeNode(SelectStmt));
  raw_stmt->stmt = internal::PostgresCastToNode(select);

  // Set expression as SELECT's target
  ZETASQL_ASSIGN_OR_RETURN(ResTarget * rt, CheckedPgMakeNode(ResTarget));
  rt->val = expression;
  ZETASQL_ASSIGN_OR_RETURN(List * target_list, CheckedPgLappend(/*list=*/nullptr, rt));
  select->targetList = target_list;

  // Set table name in SELECT's FROM clause
  ZETASQL_ASSIGN_OR_RETURN(RangeVar * relation, CheckedPgMakeNode(RangeVar));
  ZETASQL_ASSIGN_OR_RETURN(relation->relname, CheckedPgPstrdup(table_name.data()));
  ZETASQL_ASSIGN_OR_RETURN(List * from_clause, CheckedPgLappend(nullptr, relation));
  select->fromClause = from_clause;

  return wrapped_tree;
}

absl::StatusOr<interfaces::ParserSingleOutput>
SpangresTranslator::GetQueryParserOutput(
    interfaces::TranslateQueryParams& params) {
  ZETASQL_VLOG(4) << params.sql_expression();
  if (params.sql_expression().size() >
      absl::GetFlag(FLAGS_spangres_sql_length_limit)) {
    return absl::InvalidArgumentError(absl::Substitute(
        "The SQL text is longer than the limit ($0): length = $1",
        absl::GetFlag(FLAGS_spangres_sql_length_limit),
        params.sql_expression().size()));
  }

  interfaces::TranslationProgress* progress =
      params.translation_progress_output();
  if (progress != nullptr) {
    *progress = interfaces::TranslationProgress::NONE;
  }

  ZETASQL_RET_CHECK_NE(params.engine_provided_catalog(), nullptr)
      << "ZetaSQL provided catalog cannot be null";

  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserInterface * parser, params.parser());
  ZETASQL_RET_CHECK_NE(parser, nullptr);
  interfaces::ParserSingleOutput parser_single_output(ParseQuery(
      parser, params.sql_expression(), GetMemoryReservationManager(params),
      params.parser_statistics_callback()));

  ZETASQL_RETURN_IF_ERROR(parser_single_output.output().status());
  if (params.pg_raw_stmt_callback()) {
    ZETASQL_RETURN_IF_ERROR(params.pg_raw_stmt_callback()(
        linitial_node(RawStmt, parser_single_output.output()->parse_tree())));
  }
  return parser_single_output;
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateQueryInView(
    interfaces::TranslateQueryParams params) {
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserSingleOutput parser_single_output,
                   GetQueryParserOutput(params));
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserOutput parser_output,
                   std::move(*parser_single_output.mutable_output()));
  return TranslateParsedQueryInView(interfaces::TranslateParsedQueryParams(
      std::move(parser_output), params.TransferCommonParams()));
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateParsedQueryInView(
    interfaces::TranslateParsedQueryParams params) {
  static auto query_deparser = [](std::string* deparsed_query,
                                  Query* query) -> absl::Status {
    ZETASQL_RET_CHECK_NE(query, nullptr) << "Query should not be null";
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view deparsed, CheckedPgDeparseQuery(query));
    deparsed_query->assign(deparsed);
    return absl::OkStatus();
  };
  std::string deparsed_query;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::AnalyzerOutput> analyzer_output,
      TranslateParsedTree(
          params, GetParserQueryOutput,
          bind(query_deparser, &deparsed_query, std::placeholders::_1)));

  SQLBuilder builder;
  ZETASQL_RETURN_IF_ERROR(
      builder.Process(*analyzer_output.get()->resolved_statement()));

  return interfaces::ExpressionTranslateResult{deparsed_query, builder.sql()};
}

}  // namespace spangres
}  // namespace postgres_translator
