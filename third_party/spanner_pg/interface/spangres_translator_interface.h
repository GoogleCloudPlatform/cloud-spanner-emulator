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

#ifndef INTERFACE_SPANGRES_TRANSLATOR_INTERFACE_H_
#define INTERFACE_SPANGRES_TRANSLATOR_INTERFACE_H_

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "absl/types/variant.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/interface/parser_interface.h"
#include "third_party/spanner_pg/interface/parser_output.h"

// Defined in PostgreSQL
extern "C" {
struct Query;
struct RawStmt;
}

namespace postgres_translator {
namespace interfaces {

// Represents the degree to which a given query is successfully processed by the
// PostgreSQL compatibility layer.
enum class TranslationProgress {
  // The query did not make it through the parser.
  NONE,

  // The PostgreSQL parser processed the query, but the PostgreSQL analyzer did
  // not.
  PARSER,

  // The PostgreSQL parser and analyzer processed the query, but it was not
  // translated into ZetaSQL AST.
  ANALYZER,

  // Translation was successful.
  COMPLETE,
};

// `ExpressionTranslateResult` stores the result of expression/query translation
// from PostgreSQL dialect to ZetaSQL dialect.
// `original_postgresql_expression` stores original PG expression/query provided
// for translation, while `translated_googlesql_expression` stores the
// translation result. This function currently used in
// TranslateParsedTableLevelExpression and TranslateQueryInView.
struct ExpressionTranslateResult {
    std::string original_postgresql_expression;
    std::string translated_googlesql_expression;
};

class TranslateQueryParamsBuilder;
class TranslateParsedQueryParamsBuilder;

// Translator params that are common to the two cases of: (1) SQL being passed
// in for parsing, analysis, and translation; or (2) parser output being passed
// in for analysis and translation.
class TranslatorCommonParams {
 public:
  // Create a params instance with the required fields: engine provided catalog
  // and engine builtin function catalog, both of which must be non-null. The
  // caller is responsible for keeping the engine provided catalog alive until
  // the call to the translator is complete.
  TranslatorCommonParams(absl::string_view sql_expression,
                         zetasql::EnumerableCatalog* engine_provided_catalog,
                         std::unique_ptr<EngineBuiltinFunctionCatalog>
                             engine_builtin_function_catalog)
      : sql_expression_(sql_expression),
        engine_provided_catalog_(engine_provided_catalog),
        engine_builtin_function_catalog_(
            std::move(engine_builtin_function_catalog)) {}

  // The PostgreSQL query. Required.
  absl::string_view sql_expression() const { return sql_expression_; }

  // Optional analyzer options. If not provided, a default constructed instance
  // will be returned here.
  const zetasql::AnalyzerOptions& googlesql_analyzer_options() const;

  // The ZetaSQL provided catalog to use when parsing and analyzing the query.
  // Required.
  zetasql::EnumerableCatalog* engine_provided_catalog() const {
    return engine_provided_catalog_;
  }

  // Gives ownership of this object's EngineBuiltinFunctionCatalog to the
  // caller. Required.
  std::unique_ptr<EngineBuiltinFunctionCatalog>
  TransferEngineBuiltinFunctionCatalog() {
    return std::move(engine_builtin_function_catalog_);
  }

  // The TypeFactory for interning complex types. Optional.
  zetasql::TypeFactory* type_factory() const { return type_factory_; }

  // If set to a callable target, the PostgreSQL analyzed Query object will be
  // passed to it before transformation to ZetaSQL AST. If a failed status is
  // returned, translation will stop and that failed status will be propagated
  // up. Intended as a test hook. Optional.
  const std::function<absl::Status(const Query*)>& pg_query_callback() const {
    return pg_query_callback_;
  }

  // If non-null, will be set to some TranslationProgress value to indicate how
  // far in the translation process the query got. It will be set to COMPLETE
  // for any successful call. Optional.
  TranslationProgress* translation_progress_output() const {
    return translation_progress_output_;
  }

  // Gives ownership of this object's MemoryReservationManager to the caller.
  // Optional. If provided, this is used by Spangres to request permission from
  // the caller to allocate memory.
  std::unique_ptr<MemoryReservationManager> TransferMemoryReservationManager() {
    return std::move(memory_reservation_manager_);
  }

 private:
  absl::string_view sql_expression_;
  const zetasql::AnalyzerOptions* analyzer_options_ = nullptr;
  zetasql::EnumerableCatalog* engine_provided_catalog_;
  std::unique_ptr<EngineBuiltinFunctionCatalog>
      engine_builtin_function_catalog_;
  zetasql::TypeFactory* type_factory_ = nullptr;
  std::function<absl::Status(const Query*)> pg_query_callback_;
  TranslationProgress* translation_progress_output_ = nullptr;
  std::unique_ptr<MemoryReservationManager> memory_reservation_manager_ =
      nullptr;

  friend class TranslateQueryParamsBuilder;
  friend class TranslateParsedQueryParamsBuilder;
};

// Parameters for TranslateParsedQuery(...) below. Passed by value.
class TranslateParsedQueryParams {
 public:
  // The `original_sql` argument is used for error message generation.  In the
  // case of an analyzer failure, the error message will contain an excerpt of
  // the original SQL with a pointer to the source of the error within that
  // excerpt, determined by token location offsets found in the parse tree.
  TranslateParsedQueryParams(
      ParserOutput parser_output, absl::string_view original_sql,
      zetasql::EnumerableCatalog* engine_provided_catalog,
      std::unique_ptr<EngineBuiltinFunctionCatalog>
          engine_builtin_function_catalog)
      : TranslateParsedQueryParams(
            std::move(parser_output),
            TranslatorCommonParams(
                original_sql, engine_provided_catalog,
                std::move(engine_builtin_function_catalog))) {}

  TranslateParsedQueryParams(ParserOutput parser_output,
                             TranslatorCommonParams common_params);

  // original_sql is used to generate better error messages.  See above.
  TranslateParsedQueryParams(
      const std::string& serialized_parse_tree, absl::string_view original_sql,
      zetasql::EnumerableCatalog* engine_provided_catalog,
      std::unique_ptr<EngineBuiltinFunctionCatalog>
          engine_builtin_function_catalog)
      : TranslateParsedQueryParams(
            serialized_parse_tree,
            TranslatorCommonParams(
                original_sql, engine_provided_catalog,
                std::move(engine_builtin_function_catalog))) {}

  TranslateParsedQueryParams(const std::string& serialized_parse_tree,
                             TranslatorCommonParams common_params);

  // The parser output. Returns nullptr if this object was constructed from a
  // serialized parse tree instead.
  ParserOutput* mutable_parser_output();

  // The serialized parse tree. Returns nullptr if this object was constructed
  // from a ParserOutput instead.
  const std::string* serialized_parse_tree() const;

  // The original PostgreSQL query. Required.
  absl::string_view sql_expression() const {
    return common_params_.sql_expression();
  }

  // Optional analyzer options. If not provided, a default constructed instance
  // will be returned here.
  const zetasql::AnalyzerOptions& googlesql_analyzer_options() const {
    return common_params_.googlesql_analyzer_options();
  }

  // The ZetaSQL provided catalog to use when parsing and analyzing the query.
  // Required.
  zetasql::EnumerableCatalog* engine_provided_catalog() const {
    return common_params_.engine_provided_catalog();
  }

  // Gives ownership of this object's EngineBuiltinFunctionCatalog to the
  // caller.
  // Required.
  std::unique_ptr<EngineBuiltinFunctionCatalog>
  TransferEngineBuiltinFunctionCatalog() {
    return common_params_.TransferEngineBuiltinFunctionCatalog();
  }

  // The TypeFactory for interning complex types. Optional.
  zetasql::TypeFactory* type_factory() const {
    return common_params_.type_factory();
  }

  // If set to a callable target, the PostgreSQL analyzed Query object will be
  // passed to it before transformation to ZetaSQL AST. If a failed status is
  // returned, translation will stop and that failed status will be propagated
  // up. Intended as a test hook. Optional.
  const std::function<absl::Status(const Query*)>& pg_query_callback() const {
    return common_params_.pg_query_callback();
  }

  // If non-null, will be set to some TranslationProgress value to indicate how
  // far in the translation process the query got. It will be set to COMPLETE
  // for any successful call. Optional.
  TranslationProgress* translation_progress_output() const {
    return common_params_.translation_progress_output();
  }

  // Gives ownership of this object's MemoryReservationManager to the caller.
  // Optional. If provided, this is used by Spangres to request permission from
  // the caller to allocate memory. Only used if this object was constructed
  // with a serialized parse tree.
  std::unique_ptr<MemoryReservationManager> TransferMemoryReservationManager() {
    return common_params_.TransferMemoryReservationManager();
  }

  // The number of input arguments of the function. Only used if this object
  // was constructed for a `CREATE FUNCTION` statement.
  int num_input_arguments;
  // The names of the input arguments of the function. Only used if this object
  // was constructed for a `CREATE FUNCTION` statement.
  char** input_argument_names;
  // The name of the function. Only used if this object was constructed for a
  // `CREATE FUNCTION` statement.
  List* function_name;

 private:
  TranslatorCommonParams common_params_;
  absl::variant<ParserOutput, const std::string*> parser_output_;

  friend class TranslateParsedQueryParamsBuilder;
};

class TranslateParsedQueryParamsBuilder {
 public:
  // Initialize the builder with deserialized parser output, the original query
  // string, provided catalog, and builtin function catalog. The catalogs and
  // parser may not be null. The caller is responsible for keeping the query
  // string, provided catalog, and parser
  // alive. The TranslateParsedQueryParams will take ownership of the builtin
  // function catalog. The original query is used to generate better error
  // messages; see TranslateParsedQueryParams constructor.
  TranslateParsedQueryParamsBuilder(
      ParserOutput parser_output, absl::string_view original_sql,
      zetasql::EnumerableCatalog* engine_provided_catalog,
      std::unique_ptr<EngineBuiltinFunctionCatalog>
          engine_builtin_function_catalog)
      : params_(std::move(parser_output), original_sql, engine_provided_catalog,
                std::move(engine_builtin_function_catalog)) {}

  // Initialize the builder with a serialized parse tree, the original query
  // string, provided catalog, and builtin function catalog. The catalogs and
  // parser may not be null. The caller is responsible for keeping the
  // serialized parse tree, query string, provided catalog, and parser
  // alive. The TranslateParsedQueryParams will take ownership of the builtin
  // function catalog. The original query is used to generate better error
  // messages; see TranslateParsedQueryParams constructor.
  TranslateParsedQueryParamsBuilder(
      const std::string& serialized_parse_tree, absl::string_view original_sql,
      zetasql::EnumerableCatalog* engine_provided_catalog,
      std::unique_ptr<EngineBuiltinFunctionCatalog>
          engine_builtin_function_catalog)
      : params_(serialized_parse_tree, original_sql, engine_provided_catalog,
                std::move(engine_builtin_function_catalog)) {}

  // Not copyable or movable
  TranslateParsedQueryParamsBuilder(const TranslateParsedQueryParamsBuilder&) =
      delete;
  TranslateParsedQueryParamsBuilder& operator=(
      const TranslateParsedQueryParamsBuilder&) = delete;
  TranslateParsedQueryParamsBuilder(TranslateParsedQueryParamsBuilder&&) =
      delete;
  TranslateParsedQueryParamsBuilder& operator=(
      TranslateParsedQueryParamsBuilder&&) = delete;

  // Adds an optional MemoryReservationManager. Spangres will use this to
  // request permission to allocate memory and destruct it to inform the caller
  // that all memory has been freed.
  TranslateParsedQueryParamsBuilder& SetMemoryReservationManager(
      std::unique_ptr<MemoryReservationManager> memory_reservation_manager) {
    params_.common_params_.memory_reservation_manager_ =
        std::move(memory_reservation_manager);
    return *this;
  }

  // Adds an optional zetasql::AnalyzerOptions. Caller is responsible for
  // keeping the options object alive until the generated TranslateQueryParams
  // object is destroyed.
  TranslateParsedQueryParamsBuilder& SetAnalyzerOptions(
      const zetasql::AnalyzerOptions& analyzer_options) {
    params_.common_params_.analyzer_options_ = &analyzer_options;
    return *this;
  }

  // The TypeFactory for interning complex types. Caller-owned and optional.
  TranslateParsedQueryParamsBuilder& SetTypeFactory(
      zetasql::TypeFactory* type_factory) {
    params_.common_params_.type_factory_ = type_factory;
    return *this;
  }

  // If set to a callable target, the PostgreSQL analyzed Query object will be
  // passed to it before transformation to ZetaSQL AST. If a failed status is
  // returned, translation will stop and that failed status will be propagated
  // up. Intended as a test hook. Optional.
  TranslateParsedQueryParamsBuilder& SetPGQueryCallback(
      std::function<absl::Status(const Query*)> callback) {
    params_.common_params_.pg_query_callback_ = std::move(callback);
    return *this;
  }

  // If non-null, will be set to some TranslationProgress value to indicate how
  // far in the translation process the query got. It will be set to COMPLETE
  // for any successful call. Optional.
  TranslateParsedQueryParamsBuilder& SetTranslationProgressOutput(
      TranslationProgress* output) {
    params_.common_params_.translation_progress_output_ = output;
    return *this;
  }

  TranslateParsedQueryParams Build();

 private:
  TranslateParsedQueryParams params_;
};

// Parameters for TranslateQuery(...) below. Passed by value.
class TranslateQueryParams {
 public:
  // Create a params instance with only the required parameters: PostgreSQL
  // expression, parser, provided catalog, and builtin function catalog. The
  // catalog and parser pointers cannot be null. The caller is responsible for
  // keeping the provided catalog and parser
  // alive as long as this params
  // object is alive. The consumer of TranslateQueryParams takes ownership of
  // the builtin function catalog.
  TranslateQueryParams(absl::string_view sql_expression,
                       ParserInterface* parser,
                       zetasql::EnumerableCatalog* engine_provided_catalog,
                       std::unique_ptr<EngineBuiltinFunctionCatalog>
                           engine_builtin_function_catalog);

  // Movable but not copyable
  TranslateQueryParams(const TranslateQueryParams&) = delete;
  TranslateQueryParams& operator=(const TranslateQueryParams&) = delete;
  TranslateQueryParams(TranslateQueryParams&&) = default;
  TranslateQueryParams& operator=(TranslateQueryParams&&) = default;

  // The PostgreSQL query. Required.
  absl::string_view sql_expression() const {
    return common_params_.sql_expression();
  }

  // Optional analyzer options. If not provided, a default constructed instance
  // will be returned here.
  const zetasql::AnalyzerOptions& googlesql_analyzer_options() const {
    return common_params_.googlesql_analyzer_options();
  }

  // The ZetaSQL provided catalog to use when parsing and analyzing the query.
  // Required.
  zetasql::EnumerableCatalog* engine_provided_catalog() const {
    return common_params_.engine_provided_catalog();
  }

  // Gives ownership of this object's EngineBuiltinFunctionCatalog to the
  // caller.
  // Required.
  std::unique_ptr<EngineBuiltinFunctionCatalog>
  TransferEngineBuiltinFunctionCatalog() {
    return common_params_.TransferEngineBuiltinFunctionCatalog();
  }

  // Interface to the PostgreSQL parser.
  // Required.
  absl::StatusOr<ParserInterface*> parser() const;

  // The TypeFactory for interning complex types. Optional.
  zetasql::TypeFactory* type_factory() const {
    return common_params_.type_factory();
  }

  // If provided, this will be called with performance statistics from the
  // parser call, even if that call resulted in a failed status.
  // Optional.
  const std::function<void(const ParserBatchOutput::Statistics&)>&
  parser_statistics_callback() const {
    return parser_statistics_callback_;
  }

  // If set to a callable target, the PostgreSQL parsed RawStmt object will be
  // passed to it before analysis and conversion to a Query. If a failed status
  // is returned, translation will stop and that failed status will be
  // propagated up. Intended as a test hook. Optional.
  const std::function<absl::Status(const RawStmt*)>& pg_raw_stmt_callback()
      const {
    return pg_raw_stmt_callback_;
  }

  // If set to a callable target, the PostgreSQL analyzed Query object will be
  // passed to it before transformation to ZetaSQL AST. If a failed status is
  // returned, translation will stop and that failed status will be propagated
  // up. Intended as a test hook. Optional.
  const std::function<absl::Status(const Query*)>& pg_query_callback() const {
    return common_params_.pg_query_callback();
  }

  // If non-null, will be set to some TranslationProgress value to indicate how
  // far in the translation process the query got. It will be set to COMPLETE
  // for any successful call. Optional.
  TranslationProgress* translation_progress_output() const {
    return common_params_.translation_progress_output();
  }

  // Gives ownership of this object's MemoryReservationManager to the caller.
  // Optional. If provided, this is used by Spangres to request permission from
  // the caller to allocate memory.
  std::unique_ptr<MemoryReservationManager> TransferMemoryReservationManager() {
    return common_params_.TransferMemoryReservationManager();
  }

  TranslatorCommonParams TransferCommonParams() {
    return std::move(common_params_);
  }

 private:
  TranslatorCommonParams common_params_;
  absl::StatusOr<ParserInterface*> parser_;
  std::function<void(const ParserBatchOutput::Statistics&)>
      parser_statistics_callback_;
  std::function<absl::Status(const RawStmt*)> pg_raw_stmt_callback_;

  friend class TranslateQueryParamsBuilder;
};

class TranslateQueryParamsBuilder {
 public:
  // Initialize the builder with a query string, parser, provided catalog, and
  // builtin function catalog. A copy of the query string will be made and kept
  // in the generated TranslateQueryParams. The catalogs and parser pointer may
  // not be null. The caller is responsible for keeping the provided catalog and
  // parser
  // alive. The TranslateQueryParams will take ownership of the
  // builtin function catalog.
  TranslateQueryParamsBuilder(
      absl::string_view sql, ParserInterface* parser,
      zetasql::EnumerableCatalog* engine_provided_catalog,
      std::unique_ptr<EngineBuiltinFunctionCatalog>
          engine_builtin_function_catalog)
      : params_(sql, parser, engine_provided_catalog,
                std::move(engine_builtin_function_catalog)) {}

  // Not copyable or movable
  TranslateQueryParamsBuilder(const TranslateQueryParamsBuilder&) = delete;
  TranslateQueryParamsBuilder& operator=(const TranslateQueryParamsBuilder&) =
      delete;
  TranslateQueryParamsBuilder(TranslateQueryParamsBuilder&&) = delete;
  TranslateQueryParamsBuilder& operator=(TranslateQueryParamsBuilder&&) =
      delete;

  // Adds an optional MemoryReservationManager. Spangres will use this to
  // request permission to allocate memory and destruct it to inform the caller
  // that all memory has been freed.
  TranslateQueryParamsBuilder& SetMemoryReservationManager(
      std::unique_ptr<MemoryReservationManager> memory_reservation_manager) {
    params_.common_params_.memory_reservation_manager_ =
        std::move(memory_reservation_manager);
    return *this;
  }

  // Adds an optional zetasql::AnalyzerOptions. Caller is responsible for
  // keeping the options object alive until the generated TranslateQueryParams
  // object is destroyed.
  TranslateQueryParamsBuilder& SetAnalyzerOptions(
      const zetasql::AnalyzerOptions& analyzer_options) {
    params_.common_params_.analyzer_options_ = &analyzer_options;
    return *this;
  }

  // The TypeFactory for interning complex types. Caller-owned and optional.
  TranslateQueryParamsBuilder& SetTypeFactory(
      zetasql::TypeFactory* type_factory) {
    params_.common_params_.type_factory_ = type_factory;
    return *this;
  }

  // If provided, this will be called with performance statistics from the
  // parser call, even if that call resulted in a failed status.
  // Optional.
  TranslateQueryParamsBuilder& SetParserStatisticsCallback(
      std::function<void(const ParserBatchOutput::Statistics&)> callback) {
    params_.parser_statistics_callback_ = std::move(callback);
    return *this;
  }

  // If set to a callable target, the PostgreSQL parsed RawStmt object will be
  // passed to it before analysis and conversion to a Query. If a failed status
  // is returned, translation will stop and that failed status will be
  // propagated up. Intended as a test hook. Optional.
  TranslateQueryParamsBuilder& SetPGRawStmtCallback(
      std::function<absl::Status(const RawStmt*)> callback) {
    params_.pg_raw_stmt_callback_ = std::move(callback);
    return *this;
  }

  // If set to a callable target, the PostgreSQL analyzed Query object will be
  // passed to it before transformation to ZetaSQL AST. If a failed status is
  // returned, translation will stop and that failed status will be propagated
  // up. Intended as a test hook. Optional.
  TranslateQueryParamsBuilder& SetPGQueryCallback(
      std::function<absl::Status(const Query*)> callback) {
    params_.common_params_.pg_query_callback_ = std::move(callback);
    return *this;
  }

  // If non-null, will be set to some TranslationProgress value to indicate how
  // far in the translation process the query got. It will be set to COMPLETE
  // for any successful call. Optional.
  TranslateQueryParamsBuilder& SetTranslationProgressOutput(
      TranslationProgress* output) {
    params_.common_params_.translation_progress_output_ = output;
    return *this;
  }

  TranslateQueryParams Build();

 private:
  TranslateQueryParams params_;
};

// Invokes Spangres to parse Postgres-dialect SQL and transform to a ZetaSQL
// resolved AST.
// TODO: Move this to the Spanner dev branch once it's more stable.
class SpangresTranslatorInterface {
 public:
  virtual ~SpangresTranslatorInterface() = default;

  // Not copyable or movable
  SpangresTranslatorInterface(const SpangresTranslatorInterface&) = delete;
  SpangresTranslatorInterface& operator=(const SpangresTranslatorInterface&) =
      delete;
  SpangresTranslatorInterface(SpangresTranslatorInterface&&) = delete;
  SpangresTranslatorInterface& operator=(SpangresTranslatorInterface&&) =
      delete;

  virtual absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
  TranslateQuery(TranslateQueryParams params) = 0;

  virtual absl::StatusOr<std::unique_ptr<zetasql::AnalyzerOutput>>
  TranslateParsedQuery(TranslateParsedQueryParams params) = 0;

  virtual absl::StatusOr<ExpressionTranslateResult> TranslateParsedExpression(
      TranslateParsedQueryParams params) = 0;

  virtual absl::StatusOr<ExpressionTranslateResult> TranslateExpression(
      TranslateQueryParams params) = 0;

  virtual absl::StatusOr<ExpressionTranslateResult>
  TranslateParsedTableLevelExpression(TranslateParsedQueryParams params,
                                      absl::string_view table_name) = 0;

  virtual absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateTableLevelExpression(interfaces::TranslateQueryParams params,
                                absl::string_view table_name) = 0;

  virtual absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateQueryInView(interfaces::TranslateQueryParams params) = 0;

  virtual absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedQueryInView(TranslateParsedQueryParams params) = 0;

  virtual absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateFunctionBody(interfaces::TranslateQueryParams params) = 0;

  virtual absl::StatusOr<interfaces::ExpressionTranslateResult>
  TranslateParsedFunctionBody(TranslateParsedQueryParams params) = 0;

 protected:
  SpangresTranslatorInterface() = default;
};

}  // namespace interfaces
}  // namespace postgres_translator

#endif  // INTERFACE_SPANGRES_TRANSLATOR_INTERFACE_H_
