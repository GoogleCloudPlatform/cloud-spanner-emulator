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

#include "third_party/spanner_pg/util/unittest_utils.h"

#include "googlesql/public/catalog.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"
#include "third_party/spanner_pg/interface/test/postgres_transformer.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer.h"
#include "third_party/spanner_pg/src/spangres/memory_cc.h"

namespace postgres_translator::spangres::test {

std::string PrintPhases(int phases) {
  // Special case the named groups of flags.
  if (phases == kAllPhases) {
    return "Phases: [AllPhases]";
  } else if (phases == kTransformRoundTrip) {
    return "Phases: [TransformRoundTrip]";
  }

  std::vector<std::string> phase_list;
  if (phases & kParse) {
    phase_list.push_back("Parse");
  }
  if (phases & kAnalyze) {
    phase_list.push_back("Analyze");
  }
  if (phases & kTransform) {
    phase_list.push_back("Transform");
  }
  if (phases & kReverseTransform) {
    phase_list.push_back("ReverseTransform");
  }
  if (phases & kDeparse) {
    phase_list.push_back("Deparse");
  }
  return absl::StrCat("Phases: [", absl::StrJoin(phase_list, ", "), "]");
}

absl::StatusOr<List*> ParseFromPostgres(const std::string& sql) {
  GOOGLESQL_ASSIGN_OR_RETURN(interfaces::ParserOutput parser_output,
                   CheckedPgRawParserFullOutput(sql.c_str()));
  List* parse_tree = parser_output.parse_tree();
  if (list_length(parse_tree) != 1) {
    return absl::InternalError("Only one query is permitted.");
  }
  return parse_tree;
}

absl::StatusOr<Query*> AnalyzeFromPostgresForTest(
    const std::string& sql, List* parse_tree,
    const googlesql::AnalyzerOptions& analyzer_options) {
  if (parse_tree == nullptr) {
    return absl::InternalError(
        "There is no valid PostgreSQL parse tree to analyze.");
  }

  if (postgres_translator::thread_memory_reservation == nullptr) {
    return absl::InternalError(
        "There is no valid thread-local thread_memory_reservation.");
  }

  // Transform the prepared statement parameter types from GoogleSQL types to
  // PostgreSQL type oids.
  CatalogAdapter* catalog_adapter;
  GOOGLESQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  const std::map<std::string, const googlesql::Type*> gsql_param_types =
      analyzer_options.query_parameters();

  Oid* pg_param_types;
  int num_params = 0;
  GOOGLESQL_ASSIGN_OR_RETURN(pg_param_types,
                   Transformer::BuildPgParameterTypeList(
                       *catalog_adapter, gsql_param_types, &num_params));

  RawStmt* raw_stmt = linitial_node(RawStmt, parse_tree);
  GOOGLESQL_ASSIGN_OR_RETURN(Query * query_tree, CheckedPgParseAnalyzeVarparams(
      raw_stmt, sql.c_str(), &pg_param_types, &num_params));
  return query_tree;
}

absl::StatusOr<std::unique_ptr<const googlesql::AnalyzerOutput>>
ParseAnalyzeAndTransformStatement(const std::string& sql,
                                  const googlesql::AnalyzerOptions& options) {
  std::unique_ptr<EngineBuiltinFunctionCatalog> function_catalog =
      GetSpangresTestBuiltinFunctionCatalog(options.language());
  std::vector<std::string> analyze_query_trees;

  return ParseAndAnalyzeSQLString(sql, GetSpangresTestSpannerUserCatalog(),
                                  std::move(function_catalog), options,
                                  &analyze_query_trees,
                                  /*query_id=*/1);
}

absl::Status ParseAndAnalyzeFromGoogleSQLForTest(
    const std::string& sql, googlesql::TypeFactory* type_factory,
    std::unique_ptr<const googlesql::AnalyzerOutput>* gsql_output) {
  googlesql::AnalyzerOptions analyzer_options =
      GetSpangresTestAnalyzerOptions();
  return ParseAndAnalyzeFromGoogleSQLForTest(sql, type_factory,
                                             analyzer_options, gsql_output);
}

absl::Status ParseAndAnalyzeFromGoogleSQLForTest(
    const std::string& sql, googlesql::TypeFactory* type_factory,
    const googlesql::AnalyzerOptions& analyzer_options,
    std::unique_ptr<const googlesql::AnalyzerOutput>* gsql_output) {
  if (type_factory == nullptr) {
    return absl::InternalError(
        "A valid googlesql::TypeFactory object is needed to analyze a "
        "GoogleSQL string.");
  }

  if (gsql_output == nullptr) {
    return absl::InternalError(
        "A valid googlesql::AnalyzerOutput object is needed to analyze a "
        "GoogleSQL string.");
  }

  googlesql::EnumerableCatalog* engine_provided_catalog =
      GetSpangresTestSpannerUserCatalog();

  return googlesql::AnalyzeStatement(sql, analyzer_options,
                                     engine_provided_catalog, type_factory,
                                     gsql_output);
}

absl::StatusOr<const googlesql::Type*> ParseTypeName(const std::string& type) {
  if (type == "int64") {
    return googlesql::types::Int64Type();
  } else if (type == "double") {
    return googlesql::types::DoubleType();
  } else if (type == "float") {
    return googlesql::types::FloatType();
  } else if (type == "string") {
    return googlesql::types::StringType();
  } else if (type == "bool") {
    return googlesql::types::BoolType();
  } else if (type == "bytes") {
    return googlesql::types::BytesType();
  } else if (type == "timestamp") {
    return googlesql::types::TimestampType();
  } else if (type == "date") {
    return googlesql::types::DateType();
  } else if (type == "pg_jsonb") {
    return postgres_translator::spangres::types::PgJsonbMapping()
        ->mapped_type();
  } else if (type == "pg_numeric") {
    return postgres_translator::spangres::types::PgNumericMapping()
        ->mapped_type();
  } else if (type == "pg_oid") {
    return postgres_translator::spangres::types::PgOidMapping()->mapped_type();
  } else if (type == "int64_array") {
    return googlesql::types::Int64ArrayType();
  } else if (type == "double_array") {
    return googlesql::types::DoubleArrayType();
  } else if (type == "float_array") {
    return googlesql::types::FloatArrayType();
  } else if (type == "string_array") {
    return googlesql::types::StringType();
  } else if (type == "bool_array") {
    return googlesql::types::BoolType();
  } else if (type == "bytes_array") {
    return googlesql::types::BytesType();
  } else if (type == "timestamp_array") {
    return googlesql::types::TimestampArrayType();
  } else if (type == "date_array") {
    return googlesql::types::DateArrayType();
  } else if (type == "pg_numeric_array") {
    return postgres_translator::spangres::types::PgNumericArrayMapping()
        ->mapped_type();
  } else if (type == "pg_jsonb_array") {
    return postgres_translator::spangres::types::PgJsonbArrayMapping()
        ->mapped_type();
  } else if (type == "pg_oid_array") {
    return postgres_translator::spangres::types::PgOidArrayMapping()
        ->mapped_type();
  } else {
    return absl::InvalidArgumentError(
        absl::StrCat("Bad option types input: ", type));
  }
}

absl::Status ParseParameters(
    const std::string& parameters,
    std::map<std::string, const googlesql::Type*>& result) {
  std::vector<std::string> parameter_list =
      absl::StrSplit(parameters, ',', absl::SkipEmpty());
  for (const std::string& parameter : parameter_list) {
    std::vector<std::string> name_type =
        absl::StrSplit(parameter, '=', absl::SkipEmpty());
    GOOGLESQL_RET_CHECK_EQ(name_type.size(), 2);
    GOOGLESQL_ASSIGN_OR_RETURN(const googlesql::Type* type, ParseTypeName(name_type[1]));
    result.insert({name_type[0], type});
  }
  return absl::OkStatus();
}

absl::StatusOr<Query*> BuildPgQuery(
    const std::string& sql,
    const googlesql::AnalyzerOptions& analyzer_options) {
  GOOGLESQL_ASSIGN_OR_RETURN(List* pg_parse_tree,
                   spangres::test::ParseFromPostgres(sql));

  if (pg_parse_tree == nullptr) {
    return absl::InternalError(
        absl::StrCat("Cannot parse PostgreSQL string: ", sql));
  }

  return spangres::test::AnalyzeFromPostgresForTest(sql, pg_parse_tree,
                                                    analyzer_options);
}

absl::StatusOr<Query*> BuildPgQuery(const std::string& sql) {
  return BuildPgQuery(sql, spangres::test::GetSpangresTestAnalyzerOptions());
}

absl::StatusOr<std::unique_ptr<googlesql::ResolvedStatement>>
ForwardTransformQuery(const std::string& sql,
                      const googlesql::AnalyzerOptions& analyzer_options) {
  // Run the parser and analyzer
  std::unique_ptr<CatalogAdapterHolder> adapter_holder =
      spangres::test::GetSpangresTestCatalogAdapterHolder(analyzer_options);
  GOOGLESQL_ASSIGN_OR_RETURN(const Query* pg_output, BuildPgQuery(sql, analyzer_options));

  // Run the forward transformer
  auto transformer = std::make_unique<ForwardTransformer>(
      adapter_holder->ReleaseCatalogAdapter());
  return transformer->BuildGsqlResolvedStatement(*pg_output);
}

}  // namespace postgres_translator::spangres::test
