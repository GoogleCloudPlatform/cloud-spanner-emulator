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

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/analyzer_output_properties.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/public/strings.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "zetasql/resolved_ast/resolved_ast_deep_copy_visitor.h"
#include "zetasql/resolved_ast/resolved_node.h"
#include "zetasql/resolved_ast/validator.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/interface/sql_builder.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/shims/parser_output_serialization.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_FLAG(int64_t, spangres_sql_length_limit, 100'000,
          "The maximum length of a supported SQL text.");
ABSL_FLAG(int64_t, spangres_stub_memory_reservation_size, 64'000'000,
          "The reservable amount of memory for the test stub memory "
          "reservation system, 0 for no limit.");

namespace postgres_translator {
namespace spangres {
using ::postgres_translator::internal::GetUdfParameterName;

namespace {
const char* FormatNode(const void* obj) {
  return pretty_format_node_dump(nodeToString(obj));
}
}  // namespace

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
    return interfaces::ParserOutput(
        parse_tree,
        {.token_locations = {},
         .serialized_parse_tree_size = serialized_parse_tree->size()});
  }
  return absl::InternalError(
      "TranslateParsedQueryParams did not contain serialized or deserialized "
      "parser output");
}

absl::StatusOr<std::unique_ptr<zetasql::ResolvedCreateFunctionStmt>>
WrapInResolvedCreateFunctionStmt(
    const zetasql::ResolvedStatement* stmt,
    const std::vector<std::string>& input_params,
    const std::map<std::string, const zetasql::Type*>& param_types_map) {
  const zetasql::ResolvedExpr* expr = nullptr;
  std::vector<const zetasql::ResolvedNode*> child_nodes;
  stmt->GetChildNodes(&child_nodes);
  for (const zetasql::ResolvedNode* child_node : child_nodes) {
    // Get the ResolvedScan
    std::vector<const zetasql::ResolvedNode*> scan_child_nodes;
    if (child_node->IsScan()) {
      const zetasql::ResolvedScan* scan =
          child_node->GetAs<zetasql::ResolvedScan>();
      scan->GetChildNodes(&scan_child_nodes);
      if (!scan_child_nodes.empty()) {
        const zetasql::ResolvedComputedColumn* computed_column =
            scan_child_nodes[0]->GetAs<zetasql::ResolvedComputedColumn>();
        expr = computed_column->expr();
      }
    }
  }
  ZETASQL_RET_CHECK_NE(expr, nullptr);
  // Create a deep copy visitor
  zetasql::ResolvedASTDeepCopyVisitor deep_copy_visitor;
  // Accept the visitor on the expression
  ZETASQL_RETURN_IF_ERROR(expr->Accept(&deep_copy_visitor));
  // Consume the copied expression
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::ResolvedExpr> copied_expr,
      deep_copy_visitor.ConsumeRootNode<zetasql::ResolvedExpr>());

  // Get the return type from the output column list of stmt
  const zetasql::ResolvedQueryStmt* query_stmt =
      stmt->GetAs<zetasql::ResolvedQueryStmt>();
  ZETASQL_RET_CHECK_NE(query_stmt, nullptr);
  const zetasql::Type* return_type =
      query_stmt->output_column_list()[0]->column().type();

  zetasql::FunctionArgumentTypeList arg_list;
  for (const std::string& param_name : input_params) {
    auto it = param_types_map.find(param_name);
    if (it == param_types_map.end()) {
      return absl::InternalError(
          absl::StrCat("Could not find type for parameter: ", param_name));
    }
    arg_list.push_back(it->second);
  }
  auto signature =
      std::make_unique<zetasql::FunctionSignature>(return_type, arg_list,
                                                     /*context_id=*/0);
  // Create the ResolvedCreateFunctionStmt to validate the expression.
  std::unique_ptr<zetasql::ResolvedCreateFunctionStmt> create_function_stmt =
      zetasql::MakeResolvedCreateFunctionStmt(
          /*name_path=*/{"foo"},
          zetasql::ResolvedCreateStatement::CREATE_DEFAULT_SCOPE,
          zetasql::ResolvedCreateStatement::CREATE_DEFAULT,
          /*has_explicit_return_type=*/true, return_type, input_params,
          *signature,
          /*is_aggregate=*/false,
          /*language=*/"SQL",
          /*code=*/"",
          /*aggregate_expression_list=*/{},
          /*function_expression=*/std::move(copied_expr),
          /*option_list=*/{},
          zetasql::ResolvedCreateStatement::SQL_SECURITY_UNSPECIFIED,
          zetasql::ResolvedCreateStatement::DETERMINISM_UNSPECIFIED,
          /*is_remote=*/false,
          /*connection=*/nullptr);
  return create_function_stmt;
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

// Update the query if it's a simple scenario that isn't supported by the
// current implementation but can be rewritten to a supported form.
// Specifically this handles the case where there is a single target SRF in a
// SELECT statement without any FROM or JOIN clause.
// E.g. `SELECT generate_series(1, 10);`
void RewriteSimpleSrfInSelect(Query* pg_query) {
  pg_query->hasTargetSRFs = false;
  TargetEntry* target_entry = internal::PostgresCastNode(
      TargetEntry, list_head(pg_query->targetList)->ptr_value);
  FuncExpr* func_expr =
      internal::PostgresCastNode(FuncExpr, target_entry->expr);
  RangeTblEntry* rte = makeNode(RangeTblEntry);
  // Set up the RTE for the SRF
  pg_query->rtable = list_make1(rte);
  pg_query->hasTargetSRFs = false;
  // Set up the alias for the RTE
  rte->eref = makeNode(Alias);
  // To simplify, we'll always use `$array` as the alias.
  rte->eref->aliasname = makeString(pstrdup("$array"))->sval;
  rte->eref->colnames =
      lappend(rte->eref->colnames, makeString(pstrdup(target_entry->resname)));
  rte->alias = rte->eref;
  rte->rtekind = RTE_FUNCTION;
  rte->inFromCl = true;  // Defined in the FROM clause
  // The FuncExpr is now the only entry in the rtable
  RangeTblFunction* rt_func = makeNode(RangeTblFunction);
  rt_func->funcexpr = reinterpret_cast<Node*>(func_expr);
  rt_func->funccolcount = 1;  // 1 output column
  rte->functions = list_make1(rt_func);
  // With the SRF in the FROM clause, the FromExpr needs to be updated to
  // reference the RTE.
  FromExpr* from_expr = pg_query->jointree;
  RangeTblRef* rt_ref = makeNode(RangeTblRef);
  rt_ref->rtindex = 1;
  from_expr->fromlist = list_make1(rt_ref);
  // Update the target entry to be a var referencing the RTE
  Var* var = makeNode(Var);
  var->varno = 1;     // 1st and only relation in the RangeTblEntry
  var->varattno = 1;  // 1st and only column in the relation
  var->vartype = func_expr->funcresulttype;
  var->vartypmod = -1;   // No type-specific modifier needed
  var->varnosyn = 1;     // Syntactic version of varno
  var->varattnosyn = 1;  // Syntactic version of varattno
  var->location = func_expr->location;
  target_entry->expr = reinterpret_cast<Expr*>(var);
}

// TODO: Refactor code to remove the two function parameters.
absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
SpangresTranslator::TranslateParsedTree(
    interfaces::TranslateParsedQueryParams& params,
    std::function<decltype(GetParserQueryOutput)> parser_output_getter,
    std::function<absl::Status(Query* query)> query_deparser_callback =
        nullptr,
    bool enable_rewrite = true) {
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
  std::vector<std::string> input_arguments;
  if (IsA(raw_stmt->stmt, CreateFunctionStmt)) {
    CreateFunctionStmt* create_function_stmt =
        internal::PostgresCastNode(CreateFunctionStmt, raw_stmt->stmt);
    for (ListCell* lc = list_head(create_function_stmt->parameters);
         lc != nullptr; lc = lnext(create_function_stmt->parameters, lc)) {
      int i = list_cell_number(create_function_stmt->parameters, lc) + 1;
      FunctionParameter* func_param =
          internal::PostgresCastNode(FunctionParameter, lfirst(lc));
      // Note that the parameter name is 1-based, not 0-based.
      std::string default_param_name = GetUdfParameterName(i);
      std::string param_name =
          func_param->name != nullptr ? func_param->name : default_param_name;
      input_arguments.push_back(param_name);
    }
  }

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

  // Perform a rewrite if there is a single target SRF and no FROM or JOIN
  // clause.
  if (pg_query->hasTargetSRFs && list_length(pg_query->targetList) == 1 &&
      pg_query->rtable == nullptr &&
      list_length(pg_query->jointree->fromlist) == 0) {
    RewriteSimpleSrfInSelect(pg_query);
  }

  bool is_create_function_stmt = IsA(raw_stmt->stmt, CreateFunctionStmt);
  auto transformer = std::make_unique<postgres_translator::ForwardTransformer>(
      catalog_adapter_holder->ReleaseCatalogAdapter(), input_arguments,
      is_create_function_stmt);
  ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedStatement> stmt,
                   transformer->BuildGsqlResolvedStatement(*pg_query));

  gsql_param_types = transformer->query_parameter_types();

    zetasql::Validator validator(
        params.googlesql_analyzer_options().language());

    if (is_create_function_stmt) {
      ZETASQL_ASSIGN_OR_RETURN(std::unique_ptr<zetasql::ResolvedCreateFunctionStmt>
                           create_function_stmt,
                       WrapInResolvedCreateFunctionStmt(
                           stmt.get(), input_arguments,
                           transformer->create_func_arg_types()));

      ZETASQL_RETURN_IF_ERROR(
          validator.ValidateResolvedStatement(create_function_stmt.get()));
    } else {
      ZETASQL_RETURN_IF_ERROR(validator.ValidateResolvedStatement(stmt.get()));
    }

  int max_column_id = transformer->catalog_adapter().max_column_id();

  if (progress != nullptr) {
    *progress = interfaces::TranslationProgress::COMPLETE;
  }

  auto analyzer_output = std::make_unique<zetasql::AnalyzerOutput>(
      params.googlesql_analyzer_options().id_string_pool(),
      params.googlesql_analyzer_options().arena(), std::move(stmt),
      zetasql::AnalyzerOutputProperties(),
      /*parser_output=*/nullptr,
      /*deprecation_warnings=*/std::vector<absl::Status>(), gsql_param_types,
      /*undeclared_position_parameters=*/std::vector<const zetasql::Type*>(),
      max_column_id);
  // TODO: Gate this check behind a schema flag.
  if (enable_rewrite) {
    // NOTE: Unnecessary to rewrite the expression for the extracted expression
    // in the create function statement.
    if (!is_create_function_stmt) {
      ZETASQL_RETURN_IF_ERROR(RewriteTranslatedTree(analyzer_output.get(), params));
    }
  }
  return analyzer_output;
}

absl::Status SpangresTranslator::RewriteTranslatedTree(
    zetasql::AnalyzerOutput* analyzer_output,
    interfaces::TranslateParsedQueryParams& params) {
  auto analyzer_options = params.googlesql_analyzer_options();
  analyzer_options.set_enabled_rewrites({
      zetasql::REWRITE_BUILTIN_FUNCTION_INLINER,
  });
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
SpangresTranslator::TranslateExpression(
    interfaces::TranslateQueryParams params,
    std::optional<absl::string_view> table_name) {
  std::string wrapped_expression;
  // Wrap the expression in SELECT <expression> FROM <table_name>.
  ZETASQL_ASSIGN_OR_RETURN(wrapped_expression,
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

  return TranslateParsedExpression(
      interfaces::TranslateParsedQueryParamsBuilder(
          std::move(parser_output), {}, params.engine_provided_catalog(),
          wrapped_params.TransferEngineBuiltinFunctionCatalog())
          .SetAnalyzerOptions(params.googlesql_analyzer_options())
          .SetTypeFactory(params.type_factory())
          .Build(),
      table_name);
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateParsedExpression(
    interfaces::TranslateParsedQueryParams params,
    std::optional<absl::string_view> table_name) {
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
  std::function<decltype(GetParserQueryOutput)> parser_output_getter;
  if (params.serialized_parse_tree() != nullptr) {
    parser_output_getter =
        std::bind(GetParserExpressionOutput, table_name, std::placeholders::_1,
                  std::placeholders::_2);
  } else {
    parser_output_getter = std::bind(
        GetParserQueryOutput, std::placeholders::_1, std::placeholders::_2);
  }
  ZETASQL_ASSIGN_OR_RETURN(
      analyzer_output,
      TranslateParsedTree(params, parser_output_getter,
                          std::bind(expression_deparser, &deparsed_expression,
                                    std::placeholders::_1),
                          // See this function's comment to understand why we
                          // disable the rewriter.
                          /*enable_rewrite=*/false));

  const zetasql::ResolvedStatement* stmt =
      analyzer_output->resolved_statement();
  // Default SQLBuilder will print the ColumnRef by using a random generated
  // column name
  class ColumnRefSQLBuilder: public SQLBuilder {
   public:
    ColumnRefSQLBuilder(SQLBuilder::SQLBuilderOptions options)
        : SQLBuilder(options) {}

    absl::Status VisitResolvedColumnRef(
        const zetasql::ResolvedColumnRef* node) {
      PushQueryFragment(node,
                        // Identifier requires quoteing if necessary.
                        zetasql::ToIdentifierLiteral(node->column().name()));
      return absl::OkStatus();
    }
  };
  ColumnRefSQLBuilder builder{SQLBuilder::SQLBuilderOptions()};

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
    std::optional<absl::string_view> table_name,
    interfaces::TranslateParsedQueryParams& params,
    std::unique_ptr<interfaces::PGArena>& arena) {
  ZETASQL_RET_CHECK_NE(params.serialized_parse_tree(), nullptr)
      << "Expression can not be null";
  ZETASQL_ASSIGN_OR_RETURN(
      arena, MemoryContextPGArena::Init(GetMemoryReservationManager(params)));
  ZETASQL_ASSIGN_OR_RETURN(Node * expression,
                   DeserializeParseExpression(*params.serialized_parse_tree()));

  ZETASQL_ASSIGN_OR_RETURN(List * parse_tree,
                   WrapExpressionInSelect(expression, table_name));
  return interfaces::ParserOutput(
      parse_tree,
      {.token_locations = {},
       .serialized_parse_tree_size = params.serialized_parse_tree()->size()});
}

absl::StatusOr<std::string> SpangresTranslator::WrapExpressionInSelect(
    absl::string_view expression, std::optional<std::string_view> table_name) {
  // Always quote the table name: $1, otherwise it cause issues, for example:
  // table_name lost the case sensitivity which can cause "table not found".
  static constexpr char select_template_with_table[] =
      R"(SELECT ($0) from "$1")";
  static constexpr char select_template_without_table[] = R"(SELECT ($0))";
  ZETASQL_RET_CHECK(!expression.empty()) << "Expression can not be null or empty";
  return table_name.has_value()
             ? absl::Substitute(select_template_with_table, expression,
                                table_name.value())
             : absl::Substitute(select_template_without_table, expression);
}

absl::StatusOr<List*> SpangresTranslator::WrapExpressionInSelect(
    Node* expression, std::optional<absl::string_view> table_name) {
  ZETASQL_RET_CHECK_NE(expression, nullptr) << "Expression can not be null";

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

  if (table_name.has_value()) {
    // Set table name in SELECT's FROM clause
    ZETASQL_ASSIGN_OR_RETURN(RangeVar * relation, CheckedPgMakeNode(RangeVar));
    ZETASQL_ASSIGN_OR_RETURN(relation->relname, CheckedPgPstrdup(table_name->data()));
    ZETASQL_ASSIGN_OR_RETURN(List * from_clause, CheckedPgLappend(nullptr, relation));
    RawStmt* raw_stmt =
        internal::PostgresCastNode(RawStmt, linitial(wrapped_tree));
    SelectStmt* select = internal::PostgresCastNode(SelectStmt, raw_stmt->stmt);
    select->fromClause = from_clause;
  }

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

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateFunctionBody(
    interfaces::TranslateQueryParams params) {
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserSingleOutput parser_single_output,
                   GetQueryParserOutput(params));
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserOutput parser_output,
                   std::move(*parser_single_output.mutable_output()));
  return TranslateParsedFunctionBody(interfaces::TranslateParsedQueryParams(
      std::move(parser_output), params.TransferCommonParams()));
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
SpangresTranslator::TranslateParsedFunctionBody(
    interfaces::TranslateParsedQueryParams params) {
  // Deparse the query to standardize the formatting of the string
  static auto query_deparser = [](std::string* deparsed_query,
                                  Query* query) -> absl::Status {
    ZETASQL_RET_CHECK_NE(query, nullptr) << "Query should not be null";
    ZETASQL_RET_CHECK_NE(list_head(query->targetList), nullptr)
        << "Query target list should not be null";

    void* obj = list_head(query->targetList)->ptr_value;
    Expr* expr = internal::PostgresCastNode(TargetEntry, obj)->expr;
    ZETASQL_ASSIGN_OR_RETURN(
        absl::string_view deparsed,
        CheckedPgDeparseExprInQuery(internal::PostgresCastToNode(expr), query));

    deparsed_query->assign(deparsed);
    return absl::OkStatus();
  };

  std::string deparsed_query;
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<zetasql::AnalyzerOutput> analyzer_output,
      TranslateParsedTree(
          params, GetParserQueryOutput,
          bind(query_deparser, &deparsed_query, std::placeholders::_1), true));

  // pull the expression out of the select statement
  const zetasql::ResolvedQueryStmt* query =
      analyzer_output->resolved_statement()
          ->GetAs<zetasql::ResolvedQueryStmt>();
  // Process and extract the ResolvedExpr from statement.
  const zetasql::ResolvedProjectScan* project_scan =
      query->query()->GetAs<zetasql::ResolvedProjectScan>();

  // Note that here for function creation we explicitly execute the statement
  // to take advantage of the input validations found there.
  // TODO: Fix the error message in google sql analyzer.
  ZETASQL_RET_CHECK_EQ(project_scan->expr_list().size(), 1)
      << "Expected 1 expression in scan from function body";
  const zetasql::ResolvedExpr* expr = project_scan->expr_list(0)->expr();
  ZETASQL_RET_CHECK_NE(expr, nullptr) << "Expr in ResolvedExpr should not be null";

  SQLBuilder builder;
  ZETASQL_RETURN_IF_ERROR(builder.Process(*expr));
  ZETASQL_ASSIGN_OR_RETURN(std::string sql, builder.GetSql());

  return interfaces::ExpressionTranslateResult{deparsed_query, sql};
}

}  // namespace spangres
}  // namespace postgres_translator
