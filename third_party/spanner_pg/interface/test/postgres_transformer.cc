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

#include "third_party/spanner_pg/interface/test/postgres_transformer.h"

#include <stdint.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/base/logging.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/analyzer_output.h"
#include "zetasql/public/catalog.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"
// clang-format off
#include "third_party/spanner_pg/interface/parser_without_serialization.h"
// clang-format on
#include "third_party/spanner_pg/interface/spangres_translator_factory.h"
#include "third_party/spanner_pg/interface/spangres_translator_interface.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/memory_reservation_holder.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

namespace {

absl::string_view MessageForProgressLevel(
    interfaces::TranslationProgress progress) {
  switch (progress) {
    case interfaces::TranslationProgress::NONE:
      return "failed to parse the query.";
    case interfaces::TranslationProgress::PARSER:
      return "failed to analyze the query.";
    case interfaces::TranslationProgress::ANALYZER:
      return "failed to transform the analyzed query.";
    case interfaces::TranslationProgress::COMPLETE:
      return "";
  }
}

}  // namespace

absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
ParseAndAnalyzeSQLString(
    const std::string& sql, zetasql::EnumerableCatalog* catalog,
    std::unique_ptr<EngineBuiltinFunctionCatalog> function_catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    std::vector<std::string>* analyze_query_trees,
    const uint64_t query_id
) {
  auto translator = interfaces::SpangresTranslatorFactory::Create();
  interfaces::TranslationProgress progress =
      interfaces::TranslationProgress::NONE;
  auto status_or_result = translator->TranslateQuery(
      interfaces::TranslateQueryParamsBuilder(
          sql, std::make_unique<spangres::ParserWithoutSerialization>().get(),
          catalog, std::move(function_catalog))
          .SetTranslationProgressOutput(&progress)
          .SetAnalyzerOptions(analyzer_options)
          .SetPGQueryCallback([analyze_query_trees,
                               query_id
  ](const Query* query) -> absl::Status {
            // Test serializer and deserializer for a Query Tree.
            ZETASQL_ASSIGN_OR_RETURN(char* serialized_query,
                             CheckedPgNodeToString(query),
                             _ << "failed to serialize the analyzed query.");

            ZETASQL_RETURN_IF_ERROR(CheckedPgStringToNode(serialized_query).status())
                << "failed to deserialize the analyzed query.";

            ZETASQL_ASSIGN_OR_RETURN(
                serialized_query,
                CheckedPgPrettyFormatNodeDump(serialized_query),
                _ << "failed to pretty format the serialized query.");

            analyze_query_trees->push_back(std::string(serialized_query));

            return absl::OkStatus();
          })
          .Build());
  ZETASQL_RETURN_IF_ERROR(status_or_result.status())
      << MessageForProgressLevel(progress);

  return status_or_result;
}
}  // namespace postgres_translator
