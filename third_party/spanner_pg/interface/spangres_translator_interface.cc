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

#include "third_party/spanner_pg/interface/spangres_translator_interface.h"

#include <memory>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/types/type_factory.h"
#include "absl/utility/utility.h"
#include "third_party/spanner_pg/interface/parser_interface.h"

namespace postgres_translator {
namespace interfaces {

namespace {

std::unique_ptr<zetasql::AnalyzerOptions> CreateDefaultAnalyzerOptions() {
  auto default_options = std::make_unique<zetasql::AnalyzerOptions>();
  default_options->CreateDefaultArenasIfNotSet();
  return default_options;
}

const zetasql::AnalyzerOptions& DefaultAnalyzerOptions() {
  static const zetasql::AnalyzerOptions* default_options =
      CreateDefaultAnalyzerOptions().release();
  return *default_options;
}

}  // namespace

const zetasql::AnalyzerOptions&
TranslatorCommonParams::googlesql_analyzer_options() const {
  return analyzer_options_ == nullptr ? DefaultAnalyzerOptions()
                                      : *analyzer_options_;
}

TranslateParsedQueryParams::TranslateParsedQueryParams(
    ParserOutput parser_output, TranslatorCommonParams common_params)
    : common_params_(std::move(common_params)),
      parser_output_(absl::in_place_type_t<ParserOutput>(),
                     std::move(parser_output)) {}

TranslateParsedQueryParams::TranslateParsedQueryParams(
    const std::string& serialized_parse_tree,
    TranslatorCommonParams common_params)
    : common_params_(std::move(common_params)),
      parser_output_(absl::in_place_type_t<const std::string*>(),
                     &serialized_parse_tree) {}

ParserOutput* TranslateParsedQueryParams::mutable_parser_output() {
  return absl::get_if<ParserOutput>(&parser_output_);
}

const std::string* TranslateParsedQueryParams::serialized_parse_tree() const {
  const std::string* const* serialized_parse_tree_ptr_ptr =
      absl::get_if<const std::string*>(&parser_output_);
  return serialized_parse_tree_ptr_ptr == nullptr
             ? nullptr
             : *serialized_parse_tree_ptr_ptr;
}

TranslateQueryParams::TranslateQueryParams(
    absl::string_view sql_expression, ParserInterface* parser,
    zetasql::EnumerableCatalog* engine_user_catalog,
    std::unique_ptr<EngineBuiltinFunctionCatalog>
        engine_builtin_function_catalog)
    : common_params_(sql_expression, engine_user_catalog,
                     std::move(engine_builtin_function_catalog)),
      parser_(parser) {}

absl::StatusOr<ParserInterface*> TranslateQueryParams::parser() const {
  return parser_;
}

TranslateQueryParams TranslateQueryParamsBuilder::Build() {
  return std::move(params_);
}

TranslateParsedQueryParams TranslateParsedQueryParamsBuilder::Build() {
  return std::move(params_);
}

}  // namespace interfaces
}  // namespace postgres_translator
