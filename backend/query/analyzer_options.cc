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
#include "googlesql/public/analyzer_options.h"
#include "googlesql/public/builtin_function_options.h"
#include "googlesql/public/language_options.h"
#include "googlesql/public/options.pb.h"
#include "googlesql/resolved_ast/resolved_node_kind.pb.h"
#include "absl/time/time.h"
#include "common/constants.h"
#include "common/feature_flags.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using admin::database::v1::DatabaseDialect;

googlesql::AnalyzerOptions MakeGoogleSqlAnalyzerOptions(
    const std::string time_zone) {
  googlesql::AnalyzerOptions options;
  absl::TimeZone time_zone_obj;
  absl::LoadTimeZone(time_zone, &time_zone_obj);
  options.set_default_time_zone(time_zone_obj);
  options.set_error_message_mode(
      googlesql::AnalyzerOptions::ERROR_MESSAGE_MULTI_LINE_WITH_CARET);

  options.set_language(MakeGoogleSqlLanguageOptions());

  options.set_allow_undeclared_parameters(true);

  // Spanner does not support positional parameters, so tell GoogleSQL to always
  // use named parameter bindings.
  options.set_parameter_mode(googlesql::PARAMETER_NAMED);

  return options;
}

googlesql::LanguageOptions MakeGoogleSqlLanguageOptions() {
  googlesql::LanguageOptions options;

  options.set_name_resolution_mode(googlesql::NAME_RESOLUTION_DEFAULT);
  options.set_product_mode(googlesql::PRODUCT_EXTERNAL);
  options.SetEnabledLanguageFeatures({
      googlesql::FEATURE_EXTENDED_TYPES,
      googlesql::FEATURE_FUNCTION_ARGUMENTS_WITH_DEFAULTS,
      googlesql::FEATURE_NAMED_ARGUMENTS,
      googlesql::FEATURE_NUMERIC_TYPE,
      googlesql::FEATURE_TABLESAMPLE,
      googlesql::FEATURE_TIMESTAMP_NANOS,
      googlesql::FEATURE_HAVING_IN_AGGREGATE,
      googlesql::FEATURE_NULL_HANDLING_MODIFIER_IN_AGGREGATE,
      googlesql::FEATURE_ORDER_BY_IN_AGGREGATE,
      googlesql::FEATURE_LIMIT_IN_AGGREGATE,
      googlesql::FEATURE_ORDER_BY_COLLATE,
      googlesql::FEATURE_SELECT_STAR_EXCEPT_REPLACE,
      googlesql::FEATURE_SAFE_FUNCTION_CALL,
      googlesql::FEATURE_JSON_KEYS_FUNCTION,
      googlesql::FEATURE_JSON_TYPE,
      googlesql::FEATURE_JSON_ARRAY_FUNCTIONS,
      googlesql::FEATURE_JSON_CONSTRUCTOR_FUNCTIONS,
      googlesql::FEATURE_JSON_CONTAINS_FUNCTION,
      googlesql::FEATURE_JSON_MUTATOR_FUNCTIONS,
      googlesql::FEATURE_JSON_STRICT_NUMBER_PARSING,
      googlesql::FEATURE_JSON_VALUE_EXTRACTION_FUNCTIONS,
      googlesql::FEATURE_JSON_LAX_VALUE_EXTRACTION_FUNCTIONS,
      googlesql::FEATURE_DML_RETURNING,
      googlesql::FEATURE_WITH_EXPRESSION,
      googlesql::FEATURE_TABLE_VALUED_FUNCTIONS,
      googlesql::FEATURE_TOKENIZED_SEARCH,
      googlesql::FEATURE_ADDITIONAL_STRING_FUNCTIONS,
      googlesql::FEATURE_SEQUENCE_ARG,
      googlesql::FEATURE_JSON_ARRAY_VALUE_EXTRACTION_FUNCTIONS,
      googlesql::FEATURE_JSON_MORE_VALUE_EXTRACTION_FUNCTIONS,
      googlesql::FEATURE_ENABLE_FLOAT_DISTANCE_FUNCTIONS,
      googlesql::FEATURE_DOT_PRODUCT,
      googlesql::FEATURE_INTERVAL_TYPE,
      googlesql::FEATURE_SQL_GRAPH,
      googlesql::FEATURE_SQL_GRAPH_ADVANCED_QUERY,
      googlesql::FEATURE_SQL_GRAPH_BOUNDED_PATH_QUANTIFICATION,
      googlesql::FEATURE_SQL_GRAPH_PATH_TYPE,
      googlesql::FEATURE_SQL_GRAPH_PATH_MODE,
      googlesql::FEATURE_SQL_GRAPH_DYNAMIC_LABEL_PROPERTIES_IN_DDL,
      googlesql::FEATURE_SQL_GRAPH_DYNAMIC_LABEL_EXTENSION_IN_DDL,
      googlesql::FEATURE_SQL_GRAPH_DYNAMIC_ELEMENT_TYPE,
      googlesql::FEATURE_SQL_GRAPH_RETURN_EXTENSIONS,
      googlesql::FEATURE_SQL_GRAPH_CALL,
      googlesql::FEATURE_UUID_TYPE,
      googlesql::FEATURE_FOR_UPDATE,
      googlesql::FEATURE_INLINE_LAMBDA_ARGUMENT,
  });
  if (EmulatorFeatureFlags::instance().flags().enable_protos) {
    options.EnableLanguageFeature(googlesql::FEATURE_PROTO_BASE);
    options.EnableLanguageFeature(googlesql::FEATURE_BRACED_PROTO_CONSTRUCTORS);
    options.EnableLanguageFeature(googlesql::FEATURE_REPLACE_FIELDS);
  }

  options.SetSupportedStatementKinds({
      googlesql::RESOLVED_QUERY_STMT,
      googlesql::RESOLVED_INSERT_STMT,
      googlesql::RESOLVED_UPDATE_STMT,
      googlesql::RESOLVED_DELETE_STMT,
      googlesql::RESOLVED_CALL_STMT,
      googlesql::RESOLVED_EXPORT_DATA_STMT,
  });

  return options;
}

static void DisableOption(googlesql::LanguageFeature feature,
                          googlesql::LanguageOptions* options) {
  auto features = options->GetEnabledLanguageFeatures();
  features.erase(feature);
  options->SetEnabledLanguageFeatures(features);
}

googlesql::LanguageOptions MakeGoogleSqlLanguageOptionsForCompliance() {
  auto options = MakeGoogleSqlLanguageOptions();
  DisableOption(googlesql::FEATURE_ANALYTIC_FUNCTIONS, &options);
  return options;
}

googlesql::AnalyzerOptions MakeGoogleSqlAnalyzerOptionsForViewsAndFunctions(
    std::string time_zone, DatabaseDialect dialect) {
  auto language_opts = MakeGoogleSqlLanguageOptions();
  if (dialect == DatabaseDialect::POSTGRESQL) {
    // PG needs ASC NULLS LAST and DESC NULLS FIRST for default values.
    language_opts.EnableLanguageFeature(
        googlesql::FEATURE_NULLS_FIRST_LAST_IN_ORDER_BY);
  }
  // Only CREATE VIEW and CREATE FUNCTION are supported in DDL.
  language_opts.SetSupportedStatementKinds({
      googlesql::RESOLVED_CREATE_VIEW_STMT,
      googlesql::RESOLVED_CREATE_FUNCTION_STMT,
  });
  // VIEW defintions must be specified in strict name resolution mode.
  language_opts.set_name_resolution_mode(googlesql::NAME_RESOLUTION_STRICT);

  auto analyzer_options = MakeGoogleSqlAnalyzerOptions(time_zone);
  analyzer_options.set_prune_unused_columns(true);
  analyzer_options.set_language(language_opts);
  return analyzer_options;
}

googlesql::BuiltinFunctionOptions MakeGoogleSqlBuiltinFunctionOptions() {
  googlesql::BuiltinFunctionOptions options(MakeGoogleSqlLanguageOptions());
  // Modify the GSQL function options to exclude function signatures that
  // aren't yet supported in spanner.
  const std::vector<googlesql::FunctionSignatureId> exclude_function_ids = {
      // Exclude sparse distance signatures. There are no sparse fn signatures
      // for DOT_PRODUCT at the moment; put them here if they are ever added by
      // GoogleSQL.
      googlesql::FN_COSINE_DISTANCE_SPARSE_INT64,
      googlesql::FN_COSINE_DISTANCE_SPARSE_STRING,
      googlesql::FN_EUCLIDEAN_DISTANCE_SPARSE_INT64,
      googlesql::FN_EUCLIDEAN_DISTANCE_SPARSE_STRING,
      // Exclude additional string functions not yet supported in Spanner since
      // we have enabled FEATURE_ADDITIONAL_STRING_FUNCTIONS for SOUNDEX.
      googlesql::FN_INSTR_STRING,
      googlesql::FN_INSTR_BYTES,
      googlesql::FN_TRANSLATE_STRING,
      googlesql::FN_TRANSLATE_BYTES,
      googlesql::FN_INITCAP_STRING,
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
