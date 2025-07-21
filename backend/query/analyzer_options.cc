//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/query/analyzer_options.h"

#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/builtin_function_options.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "zetasql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/time/time.h"
#include "common/constants.h"
#include "common/feature_flags.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using admin::database::v1::DatabaseDialect;

zetasql::AnalyzerOptions MakeGoogleSqlAnalyzerOptions(
    const std::string time_zone) {
  zetasql::AnalyzerOptions options;
  absl::TimeZone time_zone_obj;
  absl::LoadTimeZone(time_zone, &time_zone_obj);
  options.set_default_time_zone(time_zone_obj);
  options.set_error_message_mode(
      zetasql::AnalyzerOptions::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

  options.set_language(MakeGoogleSqlLanguageOptions());

  options.set_allow_undeclared_parameters(true);

  // Spanner does not support positional parameters, so tell ZetaSQL to always
  // use named parameter bindings.
  options.set_parameter_mode(zetasql::PARAMETER_NAMED);

  return options;
}

zetasql::LanguageOptions MakeGoogleSqlLanguageOptions() {
  zetasql::LanguageOptions options;

  options.set_name_resolution_mode(zetasql::NAME_RESOLUTION_DEFAULT);
  options.set_product_mode(zetasql::PRODUCT_EXTERNAL);
  options.SetEnabledLanguageFeatures({
      zetasql::FEATURE_EXTENDED_TYPES,
      zetasql::FEATURE_FUNCTION_ARGUMENTS_WITH_DEFAULTS,
      zetasql::FEATURE_NAMED_ARGUMENTS,
      zetasql::FEATURE_NUMERIC_TYPE,
      zetasql::FEATURE_TABLESAMPLE,
      zetasql::FEATURE_TIMESTAMP_NANOS,
      zetasql::FEATURE_V_1_1_HAVING_IN_AGGREGATE,
      zetasql::FEATURE_V_1_1_NULL_HANDLING_MODIFIER_IN_AGGREGATE,
      zetasql::FEATURE_V_1_1_ORDER_BY_IN_AGGREGATE,
      zetasql::FEATURE_V_1_1_LIMIT_IN_AGGREGATE,
      zetasql::FEATURE_V_1_1_ORDER_BY_COLLATE,
      zetasql::FEATURE_V_1_1_SELECT_STAR_EXCEPT_REPLACE,
      zetasql::FEATURE_V_1_2_SAFE_FUNCTION_CALL,
      zetasql::FEATURE_JSON_TYPE,
      zetasql::FEATURE_JSON_ARRAY_FUNCTIONS,
      zetasql::FEATURE_JSON_CONSTRUCTOR_FUNCTIONS,
      zetasql::FEATURE_JSON_MUTATOR_FUNCTIONS,
      zetasql::FEATURE_JSON_STRICT_NUMBER_PARSING,
      zetasql::FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS,
      zetasql::FEATURE_JSON_LAX_VALUE_EXTRACTION_FUNCTIONS,
      zetasql::FEATURE_V_1_3_DML_RETURNING,
      zetasql::FEATURE_V_1_4_WITH_EXPRESSION,
      zetasql::FEATURE_TABLE_VALUED_FUNCTIONS,
      zetasql::FEATURE_TOKENIZED_SEARCH,
      zetasql::FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS,
      zetasql::FEATURE_V_1_4_SEQUENCE_ARG,
      zetasql::FEATURE_V_1_4_ENABLE_FLOAT_DISTANCE_FUNCTIONS,
      zetasql::FEATURE_V_1_4_DOT_PRODUCT,
      zetasql::FEATURE_INTERVAL_TYPE,
      zetasql::FEATURE_V_1_4_SQL_GRAPH,
      zetasql::FEATURE_V_1_4_SQL_GRAPH_ADVANCED_QUERY,
      zetasql::FEATURE_V_1_4_SQL_GRAPH_BOUNDED_PATH_QUANTIFICATION,
      zetasql::FEATURE_V_1_4_SQL_GRAPH_PATH_TYPE,
      zetasql::FEATURE_V_1_4_SQL_GRAPH_PATH_MODE,
      zetasql::FEATURE_V_1_4_FOR_UPDATE,
  });
  if (EmulatorFeatureFlags::instance().flags().enable_protos) {
    options.EnableLanguageFeature(zetasql::FEATURE_PROTO_BASE);
    options.EnableLanguageFeature(
        zetasql::FEATURE_V_1_3_BRACED_PROTO_CONSTRUCTORS);
    options.EnableLanguageFeature(zetasql::FEATURE_V_1_3_REPLACE_FIELDS);
  }

  options.SetSupportedStatementKinds({
      zetasql::RESOLVED_QUERY_STMT,
      zetasql::RESOLVED_INSERT_STMT,
      zetasql::RESOLVED_UPDATE_STMT,
      zetasql::RESOLVED_DELETE_STMT,
      zetasql::RESOLVED_CALL_STMT,
  });

  return options;
}

static void DisableOption(zetasql::LanguageFeature feature,
                          zetasql::LanguageOptions* options) {
  auto features = options->GetEnabledLanguageFeatures();
  features.erase(feature);
  options->SetEnabledLanguageFeatures(features);
}

zetasql::LanguageOptions MakeGoogleSqlLanguageOptionsForCompliance() {
  auto options = MakeGoogleSqlLanguageOptions();
  DisableOption(zetasql::FEATURE_ANALYTIC_FUNCTIONS, &options);
  return options;
}

zetasql::AnalyzerOptions MakeGoogleSqlAnalyzerOptionsForViewsAndFunctions(
    std::string time_zone, DatabaseDialect dialect) {
  auto language_opts = MakeGoogleSqlLanguageOptions();
  if (dialect == DatabaseDialect::POSTGRESQL) {
    // PG needs ASC NULLS LAST and DESC NULLS FIRST for default values.
    language_opts.EnableLanguageFeature(
        zetasql::FEATURE_V_1_3_NULLS_FIRST_LAST_IN_ORDER_BY);
  }
  // Only CREATE VIEW and CREATE FUNCTION are supported in DDL.
  language_opts.SetSupportedStatementKinds({
      zetasql::RESOLVED_CREATE_VIEW_STMT,
      zetasql::RESOLVED_CREATE_FUNCTION_STMT,
  });
  // VIEW defintions must be specified in strict name resolution mode.
  language_opts.set_name_resolution_mode(zetasql::NAME_RESOLUTION_STRICT);

  auto analyzer_options = MakeGoogleSqlAnalyzerOptions(time_zone);
  analyzer_options.set_prune_unused_columns(true);
  analyzer_options.set_language(language_opts);
  return analyzer_options;
}

zetasql::BuiltinFunctionOptions MakeGoogleSqlBuiltinFunctionOptions() {
  zetasql::BuiltinFunctionOptions options(MakeGoogleSqlLanguageOptions());
  // Modify the GSQL function options to exclude function signatures that
  // aren't yet supported in spanner.
  const std::vector<zetasql::FunctionSignatureId> exclude_function_ids = {
      // Exclude sparse distance signatures. There are no sparse fn signatures
      // for DOT_PRODUCT at the moment; put them here if they are ever added by
      // ZetaSQL.
      zetasql::FN_COSINE_DISTANCE_SPARSE_INT64,
      zetasql::FN_COSINE_DISTANCE_SPARSE_STRING,
      zetasql::FN_EUCLIDEAN_DISTANCE_SPARSE_INT64,
      zetasql::FN_EUCLIDEAN_DISTANCE_SPARSE_STRING,
      // Exclude additional string functions not yet supported in Spanner since
      // we have enabled FEATURE_V_1_3_ADDITIONAL_STRING_FUNCTIONS for SOUNDEX.
      zetasql::FN_INSTR_STRING,
      zetasql::FN_INSTR_BYTES,
      zetasql::FN_TRANSLATE_STRING,
      zetasql::FN_TRANSLATE_BYTES,
      zetasql::FN_INITCAP_STRING,
  };

  for (const auto& exclude_function_id : exclude_function_ids) {
    options.exclude_function_ids.insert(exclude_function_id);
  }
  return options;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
