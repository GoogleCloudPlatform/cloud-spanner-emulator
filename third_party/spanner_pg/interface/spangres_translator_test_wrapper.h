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

#ifndef INTERFACE_SPANGRES_TRANSLATOR_TEST_WRAPPER_H_
#define INTERFACE_SPANGRES_TRANSLATOR_TEST_WRAPPER_H_

#include <memory>

#include "zetasql/base/logging.h"
#include "zetasql/public/catalog.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/interface/spangres_translator_factory.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"

namespace postgres_translator {
namespace spangres {

class SpangresTranslatorTestWrapper {
 public:
  // Use this when the test wrapper should own the parser.
  explicit SpangresTranslatorTestWrapper(
      std::unique_ptr<interfaces::ParserInterface> parser)
      : parser_(std::move(parser)),
        parser_raw_(parser_.get()),
        translator_(interfaces::SpangresTranslatorFactory::Create()) {
    ABSL_CHECK_NE(Parser(), nullptr);
  }

  // Use this when the test wrapper should not own the parser. The
  // caller is responsible for keeping the parser alive until any translate
  // query calls are complete.
  explicit SpangresTranslatorTestWrapper(interfaces::ParserInterface* parser)
      : parser_raw_(parser),
        translator_(interfaces::SpangresTranslatorFactory::Create()) {
    ABSL_CHECK_NE(Parser(), nullptr);
  }

  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>> TranslateQuery(
      absl::string_view sql,
      std::function<void(const interfaces::ParserBatchOutput::Statistics&)>
          parser_statistics_callback) {
    return TranslateQuery(
        sql, std::function<absl::Status(const Query*)>(),
        /*options=*/test::GetSpangresTestAnalyzerOptions(),
        /*engine_provided_catalog=*/test::GetSpangresTestSpannerUserCatalog(),
        std::move(parser_statistics_callback));
  }

  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>> TranslateQuery(
      absl::string_view sql, const zetasql::AnalyzerOptions& options =
                                 test::GetSpangresTestAnalyzerOptions()) {
    return TranslateQuery(sql, std::function<absl::Status(const Query*)>(),
                          options);
  }

  // TODO: Accept a function catalog as an input.
  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>> TranslateQuery(
      absl::string_view sql,
      std::function<absl::Status(const Query*)> pg_query_callback,
      const zetasql::AnalyzerOptions& options =
          test::GetSpangresTestAnalyzerOptions(),
      zetasql::EnumerableCatalog* engine_provided_catalog =
          test::GetSpangresTestSpannerUserCatalog(),
      std::function<void(const interfaces::ParserBatchOutput::Statistics&)>
          parser_statistics_callback = std::function<
              void(const interfaces::ParserBatchOutput::Statistics&)>()) {
    std::unique_ptr<EngineBuiltinFunctionCatalog> function_catalog =
        test::GetSpangresTestBuiltinFunctionCatalog(options.language());
    ZETASQL_RET_CHECK_NE(Parser(), nullptr);
    return translator_->TranslateQuery(
        interfaces::TranslateQueryParamsBuilder(
            sql, Parser(), engine_provided_catalog, std::move(function_catalog))
            .SetAnalyzerOptions(options)
            .SetMemoryReservationManager(
                std::make_unique<StubMemoryReservationManager>())
            .SetPGQueryCallback(std::move(pg_query_callback))
            .SetParserStatisticsCallback(std::move(parser_statistics_callback))
            .Build());
  }

  absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
  TranslateParsedQuery(const std::string& serialized_parse_tree,
                       absl::string_view sql,
                       const zetasql::AnalyzerOptions& options =
                           test::GetSpangresTestAnalyzerOptions(),
                       zetasql::EnumerableCatalog* engine_provided_catalog =
                           test::GetSpangresTestSpannerUserCatalog()) {
    return translator_->TranslateParsedQuery(
        interfaces::TranslateParsedQueryParamsBuilder(
            serialized_parse_tree, sql, engine_provided_catalog,
            /*engine_builtin_function_catalog=*/
            test::GetSpangresTestBuiltinFunctionCatalog(options.language()))
            .SetAnalyzerOptions(options)
            .Build());
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedExpression(
      const std::string& serialized_expr, absl::string_view sql,
      std::string table_name,
      const zetasql::AnalyzerOptions& options =
          test::GetSpangresTestAnalyzerOptions(),
      zetasql::EnumerableCatalog* engine_provided_catalog =
          test::GetSpangresTestSpannerUserCatalog()) {
    return translator_->TranslateParsedTableLevelExpression(
        interfaces::TranslateParsedQueryParamsBuilder(
            serialized_expr, sql, engine_provided_catalog,
            /*engine_builtin_function_catalog=*/
            test::GetSpangresTestBuiltinFunctionCatalog(options.language()))
            .SetAnalyzerOptions(options)
            .Build(),
        table_name);
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult> TranslateExpression(
      const std::string& string_expr, absl::string_view sql,
      std::string table_name,
      const zetasql::AnalyzerOptions& options =
          test::GetSpangresTestAnalyzerOptions(),
      zetasql::EnumerableCatalog* engine_provided_catalog =
          test::GetSpangresTestSpannerUserCatalog()) {
    return translator_->TranslateTableLevelExpression(
        interfaces::TranslateQueryParamsBuilder(
            string_expr, Parser(), engine_provided_catalog,
            /*engine_builtin_function_catalog=*/
            test::GetSpangresTestBuiltinFunctionCatalog(options.language()))
            .SetAnalyzerOptions(options)
            .Build(),
        table_name);
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult> TranslateQueryInView(
      absl::string_view sql,
      const zetasql::AnalyzerOptions& options =
          test::GetSpangresTestAnalyzerOptions(),
      zetasql::EnumerableCatalog* engine_provided_catalog =
          test::GetSpangresTestSpannerUserCatalog(),
      std::function<void(const interfaces::ParserBatchOutput::Statistics&)>
          parser_statistics_callback = std::function<
              void(const interfaces::ParserBatchOutput::Statistics&)>()) {
    std::unique_ptr<EngineBuiltinFunctionCatalog> function_catalog =
        test::GetSpangresTestBuiltinFunctionCatalog(options.language());
    ZETASQL_RET_CHECK_NE(Parser(), nullptr);
    return translator_->TranslateQueryInView(
        interfaces::TranslateQueryParamsBuilder(
            sql, Parser(), engine_provided_catalog, std::move(function_catalog))
            .SetAnalyzerOptions(options)
            .SetMemoryReservationManager(
                std::make_unique<StubMemoryReservationManager>())
            .SetParserStatisticsCallback(std::move(parser_statistics_callback))
            .Build());
  }

  absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedQueryInView(
      const std::string& serialized_parse_tree, absl::string_view sql,
      const zetasql::AnalyzerOptions& options =
          test::GetSpangresTestAnalyzerOptions(),
      zetasql::EnumerableCatalog* engine_provided_catalog =
          test::GetSpangresTestSpannerUserCatalog()) {
    return translator_->TranslateParsedQueryInView(
        interfaces::TranslateParsedQueryParamsBuilder(
            serialized_parse_tree, sql, engine_provided_catalog,
            /*engine_builtin_function_catalog=*/
            test::GetSpangresTestBuiltinFunctionCatalog(options.language()))
            .SetAnalyzerOptions(options)
            .SetMemoryReservationManager(
                std::make_unique<StubMemoryReservationManager>())
            .Build());
  }

  interfaces::ParserInterface* Parser() { return parser_raw_; }

 private:
  std::unique_ptr<interfaces::ParserInterface> parser_;
  interfaces::ParserInterface* parser_raw_;
  std::unique_ptr<interfaces::SpangresTranslatorInterface> translator_;
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // INTERFACE_SPANGRES_TRANSLATOR_TEST_WRAPPER_H_
