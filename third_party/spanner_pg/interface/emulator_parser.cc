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

#include "third_party/spanner_pg/interface/emulator_parser.h"

#include "zetasql/public/function_signature.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/interface/emulator_builtin_function_catalog.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/spangres_translator.h"
#include "third_party/spanner_pg/interface/spangres_translator_factory.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace spangres {

using ::postgres_translator::interfaces::ParserOutput;
using ::postgres_translator::interfaces::SpangresTranslatorFactory;
using ::postgres_translator::interfaces::TranslateParsedQueryParamsBuilder;

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
ParseAndAnalyzePostgreSQL(const std::string& sql,
                          zetasql::EnumerableCatalog* catalog,
                          const zetasql::AnalyzerOptions& analyzer_options,
                          zetasql::TypeFactory* type_factory) {
  using postgres_translator::interfaces::ParserOutput;
  using postgres_translator::interfaces::SpangresTranslatorFactory;
  using postgres_translator::interfaces::TranslateParsedQueryParamsBuilder;

  if (sql.size() > absl::GetFlag(FLAGS_spangres_sql_length_limit)) {
    return absl::InvalidArgumentError(absl::Substitute(
        "Query failed: Query string length of $0 exceeds maximum allowed "
        "length of $1.",
        sql.size(), absl::GetFlag(FLAGS_spangres_sql_length_limit)));
  }

  ZETASQL_ASSIGN_OR_RETURN(ParserOutput parser_output,
                   CheckedPgRawParserFullOutput(sql.c_str()));

  return SpangresTranslatorFactory::Create()->TranslateParsedQuery(
      TranslateParsedQueryParamsBuilder(
          std::move(parser_output), sql, catalog,
          absl::make_unique<EmulatorBuiltinFunctionCatalog>(type_factory))
          .SetAnalyzerOptions(analyzer_options)
          .SetTypeFactory(type_factory)
          .Build());
}

absl::StatusOr<interfaces::ExpressionTranslateResult>
TranslateTableLevelExpression(
    absl::string_view expression, absl::string_view table_name,
    zetasql::EnumerableCatalog& catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::TypeFactory* type_factory) {
  // Wrap the expression in SELECT <expression> FROM <table_name>.
  ZETASQL_ASSIGN_OR_RETURN(
      std::string wrapped_expression,
      SpangresTranslator::WrapExpressionInSelect(expression, table_name));

  ZETASQL_ASSIGN_OR_RETURN(ParserOutput parser_output,
                   CheckedPgRawParserFullOutput(wrapped_expression.c_str()));

  return SpangresTranslatorFactory::Create()
      ->TranslateParsedTableLevelExpression(
          TranslateParsedQueryParamsBuilder(
              std::move(parser_output), {}, &catalog,
              absl::make_unique<EmulatorBuiltinFunctionCatalog>(type_factory))
              .SetAnalyzerOptions(analyzer_options)
              .SetTypeFactory(type_factory)
              .Build(),
          table_name);
}

absl::StatusOr<interfaces::ExpressionTranslateResult> TranslateQueryInView(
    absl::string_view query, zetasql::EnumerableCatalog& catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::TypeFactory* type_factory) {
  ZETASQL_ASSIGN_OR_RETURN(
      ParserOutput parser_output,
      CheckedPgRawParserFullOutput(std::string(query).c_str()));

  return SpangresTranslatorFactory::Create()->TranslateParsedQueryInView(
      TranslateParsedQueryParamsBuilder(
          std::move(parser_output), query, &catalog,
          std::make_unique<EmulatorBuiltinFunctionCatalog>(type_factory))
          .SetAnalyzerOptions(analyzer_options)
          .SetTypeFactory(type_factory)
          .Build());
}

}  // namespace spangres
}  // namespace postgres_translator
