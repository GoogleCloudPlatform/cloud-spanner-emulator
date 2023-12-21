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

#include "third_party/spanner_pg/test_catalog/test_catalog.h"

#include "backend/query/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/simple_catalog.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_user_catalog.h"
#include "third_party/spanner_pg/interface/emulator_builtin_function_catalog.h"
// clang-format off
#include "third_party/spanner_pg/catalog/type.h"
// clang-format on
#include "third_party/spanner_pg/test_catalog/emulator_catalog.h"
#include "third_party/spanner_pg/test_catalog/spanner_test_catalog.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres::test {

namespace {

void MutateLanguageOptionsForSpangres(zetasql::LanguageOptions* options) {
  // Override the name resolution mode -- it only applies to ZetaSQL queries
  // in the golden tests/unit tests, not to PostgreSQL queries and most of the
  // existing Spangres tests have non-strict mode ZetaSQL queries.
  options->set_name_resolution_mode(zetasql::NAME_RESOLUTION_DEFAULT);

  options->EnableLanguageFeature(zetasql::FEATURE_V_1_3_DML_RETURNING);

  // Proto and enum are not supported in spangres.
  options->DisableLanguageFeature(zetasql::FEATURE_PROTO_BASE);

  options->EnableLanguageFeature(
      zetasql::FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY);
}

}  // namespace

zetasql::AnalyzerOptions GetSpangresTestAnalyzerOptions() {
  zetasql::AnalyzerOptions options =
      google::spanner::emulator::backend::MakeGoogleSqlAnalyzerOptions();
  // We set this option to true as some tests expect it. But the transformer can
  // run without this option as well.
  options.set_prune_unused_columns(true);
  MutateLanguageOptionsForSpangres(options.mutable_language());

  options.CreateDefaultArenasIfNotSet();
  // By default, parse locations are recorded to provide better error messages
  // in the algebrizer. A number of spangres tests were authored before we
  // started recording parse locations, and a number of those tests rely on
  // the DebugStrings of ResolvedASTs matching. To keep these tests passing,
  // we disable the recording of parse locations here.
  options.set_parse_location_record_type(zetasql::PARSE_LOCATION_RECORD_NONE);
  return options;
}

std::unique_ptr<EngineBuiltinFunctionCatalog>
GetSpangresTestBuiltinFunctionCatalog(
    const zetasql::LanguageOptions& language_options) {
  return absl::make_unique<EmulatorBuiltinFunctionCatalog>(
      absl::make_unique<google::spanner::emulator::backend::FunctionCatalog>(
      GetTypeFactory()));
}

EngineSystemCatalog* GetSpangresTestSystemCatalog(
    const zetasql::LanguageOptions& language_options) {
  // Initialize the EngineSystemCatalog singleton as needed, but throw away
  // the result since it's ok if it was already initialized.
  std::unique_ptr<EngineBuiltinFunctionCatalog> builtin_function_catalog;
    builtin_function_catalog = absl::make_unique<
        EmulatorBuiltinFunctionCatalog>(
        absl::make_unique<google::spanner::emulator::backend::FunctionCatalog>(
            GetTypeFactory(), /* catalog_name =*/"spanner"));
  absl::StatusOr<bool> initialized_catalog_or =
      SpangresSystemCatalog::TryInitializeEngineSystemCatalog(
          std::move(builtin_function_catalog),
          language_options);

  // Return an error immediately if there is a problem initializing the
  // catalog. Note that production code should use ASSIGN_OR_RETURN
  // instead of ABSL_CHECK.
  ABSL_CHECK_OK(initialized_catalog_or.status());
  return EngineSystemCatalog::GetEngineSystemCatalog();
}

std::unique_ptr<CatalogAdapter> GetSpangresTestCatalogAdapter(
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::EnumerableCatalog* rqg_user_catalog,
    absl::flat_hash_map<int, int> token_locations) {
  zetasql::EnumerableCatalog* engine_provided_catalog;
  if (rqg_user_catalog != nullptr) {
    engine_provided_catalog = rqg_user_catalog;
  } else {
    engine_provided_catalog = GetSpangresTestSpannerUserCatalog();
  }
  EngineSystemCatalog* engine_system_catalog = GetSpangresTestSystemCatalog();
  // Wrap catalog with EngineUserCatalog wrapper. EngineUserCatalog wrapper
  // provides schema_name mapping and other PostgreSQL features.
  auto engine_user_catalog =
      std::make_unique<SpangresUserCatalog>(engine_provided_catalog);

  auto catalog_adapter_or = CatalogAdapter::Create(
      std::move(engine_user_catalog), engine_system_catalog, analyzer_options,
      token_locations);
  // Return an error immediately if there is a problem initializing the
  // CatalogAdapter. Note that production code should use ASSIGN_OR_RETURN
  // instead of ABSL_CHECK.
  ABSL_CHECK(catalog_adapter_or.ok());
  return std::move(*catalog_adapter_or);
}

std::unique_ptr<CatalogAdapterHolder> GetSpangresTestCatalogAdapterHolder(
    const zetasql::AnalyzerOptions& analyzer_options,
    zetasql::EnumerableCatalog* rqg_user_catalog,
    absl::flat_hash_map<int, int> token_locations) {
  auto catalog_adapter_holder_or =
      CatalogAdapterHolder::Create(GetSpangresTestCatalogAdapter(
          analyzer_options, rqg_user_catalog, token_locations));
  // Return an error immediately if there is a problem initializing the
  // CatalogAdapterHolder. Note that production code should use ASSIGN_OR_RETURN
  // instead of ABSL_CHECK.
  ABSL_CHECK(catalog_adapter_holder_or.ok());
  return std::move(*catalog_adapter_holder_or);
}

}  // namespace postgres_translator::spangres::test
