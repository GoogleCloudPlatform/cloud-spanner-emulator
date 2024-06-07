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

#ifndef INTERFACE_SPANGRES_TRANSLATOR_H_
#define INTERFACE_SPANGRES_TRANSLATOR_H_

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/flags/declare.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/shims/error_shim.h"

ABSL_DECLARE_FLAG(int64_t, spangres_sql_length_limit);

namespace postgres_translator {
namespace spangres {

// Invokes Spangres to parse Postgres-dialect SQL and transform to a ZetaSQL
// resolved AST.
class SpangresTranslator : public interfaces::SpangresTranslatorInterface {
 public:
  SpangresTranslator();
  ~SpangresTranslator() override = default;

  // Not copyable or movable
  SpangresTranslator(const SpangresTranslator&) = delete;
  SpangresTranslator& operator=(const SpangresTranslator&) = delete;
  SpangresTranslator(SpangresTranslator&&) = delete;
  SpangresTranslator& operator=(SpangresTranslator&&) = delete;

  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>> TranslateQuery(
      interfaces::TranslateQueryParams params) override;

  // Invokes ZetaSQL AST rewriter workflow on top of translator output.
  absl::Status RewriteTranslatedTree(
    zetasql::AnalyzerOutput* analyzer_output,
    interfaces::TranslateParsedQueryParams& params);

  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
  TranslateParsedQuery(interfaces::TranslateParsedQueryParams params) override;

  // Translates SQL expressions that operate within the scope of particular
  // table, which means expressions that can only reference columns of a
  // particular table. This table should exist in the catalog provided with
  // TranslateParsedQueryParams and the name of this table is given in
  // `table_name`. The main use case for this function is to be called during
  // the DDL translation of table expressions such as generated columns,
  // check constraints, and default value expressions.
  //
  // This translation does not rewrite functions implemented with ZetaSQL's
  // SQL-inlined framework. The reason is because the analyzer later runs a
  // check on the translated AST to verify that column expressions
  // (i.e., generated columns, default values, and check constraints) do not
  // contain subqueries or scans. That test will fail if such functions are
  // inlined earlier because they get inlined as subqueries. The analyzer
  // does the inlining after completing its validation.
  //
  // TODO: table_name requirement revisit.
  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedTableLevelExpression(
      interfaces::TranslateParsedQueryParams params,
      std::string_view table_name) override;

  // TranslateTableLevelExpression aims to replace old API:
  // TranslateParsedTableLevelExpression, this new API accepts
  // TranslateQueryParams as parameter. It translates expression in text from
  // PostgreSQL dialect to ZetaSQL dialect. It is also just used for DDL
  // translation.
  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateTableLevelExpression(interfaces::TranslateQueryParams params,
                                absl::string_view table_name) override;

  // Translates SQL body of the create view statement. It is used by DDL
  // which requires the result of both deparsed ZetaSQL and PostgreSQL.
  // TODO: renaming ExpressionTranslateResult to make it more
  // general to share between APIs for query and expression.
  absl::StatusOr<interfaces::ExpressionTranslateResult> TranslateQueryInView(
      interfaces::TranslateQueryParams params) override;

  // Translates SQL body of the create view statement. It is used by DDL
  // which requires the result of both deparsed ZetaSQL and PostgreSQL.
  // This is used by the emulator.
  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedQueryInView(
      interfaces::TranslateParsedQueryParams params) override;

  // Wraps node of expression in a SELECT $expression FROM $table_name
  // statement.
  static absl::StatusOr<List*> WrapExpressionInSelect(
      Node* expression, absl::string_view table_name);

  // Wraps string of expression in a SELECT $expression FROM $table_name
  // statement.
  static absl::StatusOr<std::string> WrapExpressionInSelect(
      absl::string_view expression, absl::string_view table_name);

 private:
  static interfaces::ParserSingleOutput ParseQuery(
      interfaces::ParserInterface* parser, absl::string_view sql,
      std::unique_ptr<interfaces::MemoryReservationManager> res_manager,
      const std::function<
          void(const interfaces::ParserBatchOutput::Statistics&)>&
          parser_statistics_callback);

  // If params contains a MemoryReservationManager, return it.  Otherwise,
  // return an instance of StubMemoryReservationManager.
  template <typename ParamsT>
  static std::unique_ptr<interfaces::MemoryReservationManager>
  GetMemoryReservationManager(ParamsT& params);

  // Parse a single sql expression and return the ParserSingleOutput as result.
  // ParserSingleOutput is the memory arena holder.
  static absl::StatusOr<interfaces::ParserSingleOutput> GetQueryParserOutput(
      interfaces::TranslateQueryParams& params);

  // Either returns the deserialized parser output or deserializes the
  // serialized parse tree and returns that. If deserialization was necessary, a
  // new MemoryContextPGArena will be created using the MemoryReservationManager
  // from params and assigned to arena.
  static absl::StatusOr<interfaces::ParserOutput> GetParserQueryOutput(
      interfaces::TranslateParsedQueryParams& params,
      std::unique_ptr<interfaces::PGArena>& arena);

  // Deserializes the serialized expression tree, wraps it using SELECT
  // statement cause our deparser and analyzer only support analyzed/resolved
  // AST. table_name is used for the statement wrapping purpose. A new
  // MemoryContextPGArena will be created using the MemoryReservationManager
  // from params and assigned to arena.
  static absl::StatusOr<interfaces::ParserOutput> GetParserExpressionOutput(
      absl::string_view table_name,
      interfaces::TranslateParsedQueryParams& params,
      std::unique_ptr<interfaces::PGArena>& arena);

  // Internal translator API supports two customizations through function:
  // 1: parser_output_getter: get parse tree from serialized string. For
  // serialized expression, the getter needs to wrap it.
  // 2: query_deparser_callback: a callback to process query, it is used by
  // TranslateParsedExpression to deparse and get the analyzed expression.
  //
  // `enable_rewrite` is used to control whether to apply ZetaSQL rewriter to
  // SQL-inlined functions in the translated AST. The rewriter should not be
  // applied to generated column, default value, and check constraint
  // expressions. For details, see b/341765529.
  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
  TranslateParsedTree(
      interfaces::TranslateParsedQueryParams& params,
      std::function<decltype(GetParserQueryOutput)> parser_output_getter,
      std::function<absl::Status(Query* query)> query_deparser_callback,
      bool enable_rewrite);

  static absl::StatusOr<int> FindMaxColumnID(
      const zetasql::ResolvedStatement& stmt);
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // INTERFACE_SPANGRES_TRANSLATOR_H_
