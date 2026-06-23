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

#include "backend/query/search/search_function_catalog.h"

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/function.h"
#include "googlesql/public/function.pb.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/type.h"
#include "googlesql/public/types/type_factory.h"
#include "googlesql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/bool_tokenizer.h"
#include "backend/query/search/exact_match_tokenizer.h"
#include "backend/query/search/json_tokenizer.h"
#include "backend/query/search/jsonb_tokenizer.h"
#include "backend/query/search/ngrams_tokenizer.h"
#include "backend/query/search/numeric_tokenizer.h"
#include "backend/query/search/plain_full_text_tokenizer.h"
#include "backend/query/search/score_evaluator.h"
#include "backend/query/search/score_ngrams_evaluator.h"
#include "backend/query/search/search_evaluator.h"
#include "backend/query/search/search_ngrams_evaluator.h"
#include "backend/query/search/search_substring_evaluator.h"
#include "backend/query/search/snippet_evaluator.h"
#include "backend/query/search/substring_tokenizer.h"
#include "backend/query/search/tokenlist_concat.h"
#include "googlesql/base/status_macros.h"
#include "third_party/spanner_pg/datatypes/extended/pg_jsonb_type.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {

using postgres_translator::spangres::datatypes::CreatePgJsonbValue;
using postgres_translator::spangres::datatypes::GetPgJsonbType;

// Function name to tokenize text. The function is used to define generated
// TOKENLIST column used for full text search.
constexpr char kTokenizeFullTextFunctionName[] = "tokenize_fulltext";

// Function name to create exact match token. The function creates a TOKENLIST
// that contains just one TextToken that stores the tokenization function name.
constexpr char kTokenFunctionName[] = "token";

// Function name for substring tokenization. The function creates token list
// of substrings from the given input.
constexpr char kTokenizeSubstringFunctionName[] = "tokenize_substring";

// Function name for tokenizing numbers. The function creates a TOKENLIST that
// contains one TextToken that stores the tokenization function name.
constexpr char kTokenizeNumberFunctionName[] = "tokenize_number";

// Function name for tokenizing bool values. The function creates a TOKENLIST
// that contains one TextToken storing the tokenization function name.
constexpr char kTokenizeBoolFunctionName[] = "tokenize_bool";

// Function name for concatenating array of TOKENLIST.
constexpr char kTokenlistConcatFunctionName[] = "tokenlist_concat";

// Function name for doing full text search.
constexpr char kSearchFunctionName[] = "search";

// Function name for doing substring search.
constexpr char kSearchSubstringFunctionName[] = "search_substring";

// Function name for doing snippet.
constexpr char kSnippetFunctionName[] = "snippet";

// Function name for doing score.
constexpr char kScoreFunctionName[] = "score";

// Function name for ngrams tokenization. The function creates token list
// of ngrams from the given input.
constexpr char kTokenizeNgramsFunctionName[] = "tokenize_ngrams";

// Function name for doing ngrams search.
constexpr char kSearchNgramsFunctionName[] = "search_ngrams";

// Function name for doing score ngrams.
constexpr char kScoreNgramsFunctionName[] = "score_ngrams";

// Function name for tokenizing json.
constexpr char kTokenizeJsonFunctionName[] = "tokenize_json";

// Function name for tokenizing jsonb.
constexpr char kTokenizeJsonbFunctionName[] = "tokenize_jsonb";

googlesql::FunctionArgumentTypeOptions GetArgumentTypeOptions(
    absl::string_view arg_name,
    googlesql::FunctionEnums::NamedArgumentKind named_argument,
    bool is_required, bool must_be_constant) {
  googlesql::FunctionArgumentTypeOptions result;
  result.set_argument_name(arg_name, named_argument);
  result.set_cardinality(is_required
                             ? googlesql::FunctionArgumentType::REQUIRED
                             : googlesql::FunctionArgumentType::OPTIONAL);
  result.set_must_be_constant(must_be_constant);
  return result;
}

googlesql::FunctionArgumentTypeOptions GetPositionalRequiredArgumentTypeOptions(
    absl::string_view arg_name, bool must_be_constant = true) {
  return GetArgumentTypeOptions(arg_name, googlesql::kPositionalOnly,
                                /*is_required=*/true, must_be_constant);
}

googlesql::FunctionArgumentTypeOptions GetRequiredArgumentTypeOptions(
    absl::string_view arg_name, bool must_be_constant = true) {
  return GetArgumentTypeOptions(arg_name, googlesql::kPositionalOrNamed,
                                /*is_required=*/true, must_be_constant);
}

googlesql::FunctionArgumentTypeOptions GetNamedOptionalArgTypeOptions(
    absl::string_view arg_name, bool must_be_constant = true) {
  return GetArgumentTypeOptions(arg_name, googlesql::kNamedOnly,
                                /*is_required=*/false, must_be_constant);
}

absl::StatusOr<googlesql::Value> EvalToken(
    absl::Span<const googlesql::Value> args) {
  return ExactMatchTokenizer::Tokenize(args);
}

std::unique_ptr<googlesql::Function> TokenFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalToken));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* bytes_type = type_factory->get_bytes();
  const googlesql::ArrayType* string_array_type = nullptr;
  const googlesql::ArrayType* bytes_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok() ||
      !type_factory->MakeArrayType(bytes_type, &bytes_array_type).ok()) {
    // Don't expect either of them to fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<STRING> or ARRAY<BYTE> types.";
  }

  // Signature:
  //  TOKEN(string|byte|array[string|byte] value)
  return std::make_unique<googlesql::Function>(
      kTokenFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              tokenlist_type,
              {{string_type,
                GetPositionalRequiredArgumentTypeOptions("value", false)}},
              nullptr},
          googlesql::FunctionSignature{
              tokenlist_type,
              {{bytes_type,
                GetPositionalRequiredArgumentTypeOptions("value", false)}},
              nullptr},
          googlesql::FunctionSignature{
              tokenlist_type,
              {{string_array_type,
                GetPositionalRequiredArgumentTypeOptions("value", false)}},
              nullptr},
          googlesql::FunctionSignature{
              tokenlist_type,
              {{bytes_array_type,
                GetPositionalRequiredArgumentTypeOptions("value", false)}},
              nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTokenizeNumber(
    absl::Span<const googlesql::Value> args) {
  return NumericTokenizer::Tokenize(args);
}

std::unique_ptr<googlesql::Function> TokenizeNumberFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name,
    database_api::DatabaseDialect dialect) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalTokenizeNumber));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* int64_type = type_factory->get_int64();
  const googlesql::Type* uint64_type = type_factory->get_uint64();
  const googlesql::Type* double_type = type_factory->get_double();

  // Signature:
  //  TOKENIZE_NUMBER(int64_t|uint64|double|array[int64|uint64_t|double] value,
  //                  string comparison_type = "all",
  //                  string algorithm = "auto",
  //                  int64_t|uint64|double min = min(type),
  //                  int64_t|uint64|double max = max(type),
  //                  int64_t|uint64|double granularity = 1,
  //                  int64_t tree_base = 2,
  //                  int64_t precision = 15)
  std::vector<const googlesql::Type*> numeric_types{int64_type, uint64_type,
                                                    double_type};

  std::vector<googlesql::FunctionSignature> signatures;
  std::vector<std::string> precision_names = {"ieee_precision"};
  if (dialect == database_api::DatabaseDialect::GOOGLE_STANDARD_SQL) {
    precision_names.push_back("precision");
  }
  for (auto& precision_name : precision_names) {
    for (auto type : numeric_types) {
      const googlesql::FunctionArgumentTypeList tokenize_number_args = {
          {string_type, GetNamedOptionalArgTypeOptions("comparison_type")},
          {string_type, GetNamedOptionalArgTypeOptions("algorithm")},
          {type, GetNamedOptionalArgTypeOptions("min")},
          {type, GetNamedOptionalArgTypeOptions("max")},
          {type, GetNamedOptionalArgTypeOptions("granularity")},
          {int64_type, GetNamedOptionalArgTypeOptions("tree_base")},
          {int64_type, GetNamedOptionalArgTypeOptions(precision_name)}};

      googlesql::FunctionArgumentTypeList num_arg_type_list = {
          {type, GetRequiredArgumentTypeOptions("value", false)}};
      num_arg_type_list.insert(num_arg_type_list.end(),
                               tokenize_number_args.begin(),
                               tokenize_number_args.end());
      signatures.push_back(googlesql::FunctionSignature{
          tokenlist_type, num_arg_type_list, nullptr});

      const googlesql::ArrayType* array_type;
      if (type_factory->MakeArrayType(type, &array_type).ok()) {
        googlesql::FunctionArgumentTypeList array_arg_type_list = {
            {array_type, GetRequiredArgumentTypeOptions("value", false)}};
        array_arg_type_list.insert(array_arg_type_list.end(),
                                   tokenize_number_args.begin(),
                                   tokenize_number_args.end());
        googlesql::FunctionSignature signature{tokenlist_type,
                                               array_arg_type_list, nullptr};
        signatures.push_back(googlesql::FunctionSignature{
            tokenlist_type, array_arg_type_list, nullptr});
      }
    }
  }

  return std::make_unique<googlesql::Function>(
      kTokenizeNumberFunctionName, catalog_name, googlesql::Function::SCALAR,
      signatures, function_options);
}

absl::StatusOr<googlesql::Value> EvalTokenizeBool(
    absl::Span<const googlesql::Value> args) {
  return BoolTokenizer::Tokenize(args);
}

std::unique_ptr<googlesql::Function> TokenizeBoolFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalTokenizeBool));
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* bool_type = type_factory->get_bool();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();

  // Signature: TOKENIZE_BOOL(bool value)
  return std::make_unique<googlesql::Function>(
      kTokenizeBoolFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          tokenlist_type,
          {{bool_type, GetRequiredArgumentTypeOptions("value", false)}},
          nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTokenizeFullText(
    absl::Span<const googlesql::Value> args) {
  return PlainFullTextTokenizer::Tokenize(args);
}

std::unique_ptr<googlesql::Function> TokenizeFullTextFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalTokenizeFullText));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::ArrayType* string_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<STRING> or ARRAY<BYTE> types.";
  }

  // Signature: TOKENIZE_FULLTEXT(string|array[string] value,
  //                              string language_tag = NULL,
  //                              string content_type = "text/plain",
  //                              string token_category = NULL)
  const googlesql::FunctionArgumentTypeList tokenize_fulltext_args = {
      {string_type, GetNamedOptionalArgTypeOptions("language_tag", false)},
      {string_type, GetNamedOptionalArgTypeOptions("content_type")},
      {string_type, GetNamedOptionalArgTypeOptions("token_category")},
  };
  googlesql::FunctionArgumentTypeList string_arg_type_list = {
      {string_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_arg_type_list.insert(string_arg_type_list.end(),
                              tokenize_fulltext_args.begin(),
                              tokenize_fulltext_args.end());
  googlesql::FunctionArgumentTypeList string_array_arg_type_list = {
      {string_array_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_array_arg_type_list.insert(string_array_arg_type_list.end(),
                                    tokenize_fulltext_args.begin(),
                                    tokenize_fulltext_args.end());

  return std::make_unique<googlesql::Function>(
      kTokenizeFullTextFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{tokenlist_type, string_arg_type_list,
                                       nullptr},
          googlesql::FunctionSignature{tokenlist_type,
                                       string_array_arg_type_list, nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTokenizeSubstring(
    absl::Span<const googlesql::Value> args) {
  return SubstringTokenizer::Tokenize(args);
}

std::unique_ptr<googlesql::Function> TokenizeSubstringFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalTokenizeSubstring));
  function_options.set_supports_safe_error_mode(false);
  function_options.set_arguments_are_coercible(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* int_type = type_factory->get_int64();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* bool_type = type_factory->get_bool();
  const googlesql::ArrayType* string_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<STRING> type.";
  }

  // Signature: TOKENIZE_SUBSTRING(string|array[string] value,
  //                               int64_t ngram_size_max = 4,
  //                               int64_t ngram_size_min = 1,
  //                               [bool support_relative_search = false,]
  //                               string content_type = "text/plain",
  //                               [array[string] relative_search_types = NULL,]
  //                               bool remove_diacritics = false,
  //                               bool short_tokens_only_for_anchors = false,
  //                               string language_tag = NULL)
  // Notice that the the support_relative_search and relative_search_types are
  // mutually exclusive. Only one can be specified in the function.
  // `support_relative_search` is the same as specifying `relative_search_types`
  // as ["all"]. It is recommended to use `relative_search_types` over
  // `support_relative_search` as the latter may be subjected to deprecation in
  // the future.
  const googlesql::FunctionArgumentTypeList tokenize_substring_args = {
      {int_type, GetNamedOptionalArgTypeOptions("ngram_size_max")},
      {int_type, GetNamedOptionalArgTypeOptions("ngram_size_min")},
      {bool_type, GetNamedOptionalArgTypeOptions("support_relative_search")},
      {string_type, GetNamedOptionalArgTypeOptions("content_type")},
      {string_array_type,
       GetNamedOptionalArgTypeOptions("relative_search_types")},
      {bool_type, GetNamedOptionalArgTypeOptions("remove_diacritics")},
      {bool_type,
       GetNamedOptionalArgTypeOptions("short_tokens_only_for_anchors")},
      {string_type, GetNamedOptionalArgTypeOptions("language_tag", false)}};
  googlesql::FunctionArgumentTypeList string_arg_type_list = {
      {string_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_arg_type_list.insert(string_arg_type_list.end(),
                              tokenize_substring_args.begin(),
                              tokenize_substring_args.end());
  googlesql::FunctionArgumentTypeList string_array_arg_type_list = {
      {string_array_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_array_arg_type_list.insert(string_array_arg_type_list.end(),
                                    tokenize_substring_args.begin(),
                                    tokenize_substring_args.end());
  return std::make_unique<googlesql::Function>(
      kTokenizeSubstringFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{tokenlist_type, string_arg_type_list,
                                       nullptr},
          googlesql::FunctionSignature{tokenlist_type,
                                       string_array_arg_type_list, nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTokenlistConcat(
    absl::Span<const googlesql::Value> args) {
  return TokenlistConcat::Concat(args);
}

std::unique_ptr<googlesql::Function> TokenlistConcatFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalTokenlistConcat));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::ArrayType* tokenlist_array_type = nullptr;
  if (!type_factory->MakeArrayType(tokenlist_type, &tokenlist_array_type)
           .ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<TOKENLIST> type.";
  }

  // Signature: TOKENLIST_CONCAT(ARRAY<TOKENLIST>)
  return std::make_unique<googlesql::Function>(
      kTokenlistConcatFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          tokenlist_type,
          {{tokenlist_array_type,
            GetRequiredArgumentTypeOptions("tokens", false)}},
          nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalSearch(
    absl::Span<const googlesql::Value> args) {
  return SearchEvaluator::Evaluate(args);
}

std::unique_ptr<googlesql::Function> SearchFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalSearch));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* bool_type = type_factory->get_bool();

  // Signature: SEARCH(tokenlist value,
  //                   string query,
  //                   bool enhance_query = false,
  //                   string language_tag = NULL,
  //                   string dialect = NULL)
  return std::make_unique<googlesql::Function>(
      kSearchFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              bool_type,
              {{tokenlist_type,
                GetRequiredArgumentTypeOptions("tokens", false)},
               {string_type, GetRequiredArgumentTypeOptions("query")},
               {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
               {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
               {string_type, GetNamedOptionalArgTypeOptions("dialect")}},
              nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalSearchSubstring(
    absl::Span<const googlesql::Value> args) {
  return SearchSubstringEvaluator::Evaluate(args);
}

std::unique_ptr<googlesql::Function> SearchSubstringFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalSearchSubstring));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* bool_type = type_factory->get_bool();

  // Signature: SEARCH_SUBSTRING(tokenlist value,
  //                             string query,
  //                             string relative_search_type = NULL,
  //                             string language_tag = NULL)
  return std::make_unique<googlesql::Function>(
      kSearchSubstringFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              bool_type,
              {{tokenlist_type,
                GetRequiredArgumentTypeOptions("tokens", false)},
               {string_type, GetRequiredArgumentTypeOptions("query")},
               {string_type,
                GetNamedOptionalArgTypeOptions("relative_search_type")},
               {string_type, GetNamedOptionalArgTypeOptions("language_tag")}},
              nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalScore(
    absl::Span<const googlesql::Value> args) {
  return ScoreEvaluator::Evaluate(args);
}

std::unique_ptr<googlesql::Function> ScoreFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalScore));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* double_type = type_factory->get_double();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* bytes_type = type_factory->get_bytes();
  const googlesql::Type* bool_type = type_factory->get_bool();
  const googlesql::Type* json_type = type_factory->get_json();

  // Signature: SCORE(tokenlist value,
  //                  string query,
  //                  bool enhance_query = false,
  //                  string language_tag = NULL,
  //                  string dialect = NULL,
  //                  json options = NULL)
  return std::make_unique<googlesql::Function>(
      kScoreFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              double_type,
              {
                  {tokenlist_type,
                   GetRequiredArgumentTypeOptions("tokens", false)},
                  {string_type, GetRequiredArgumentTypeOptions("query")},
                  {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
                  {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
                  {string_type, GetNamedOptionalArgTypeOptions("dialect")},
                  {json_type, GetNamedOptionalArgTypeOptions("options", false)},
              },
              nullptr},
          googlesql::FunctionSignature{
              double_type,
              {
                  {tokenlist_type,
                   GetRequiredArgumentTypeOptions("tokens", false)},
                  {bytes_type, GetRequiredArgumentTypeOptions("query")},
                  {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
                  {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
                  {string_type, GetNamedOptionalArgTypeOptions("dialect")},
                  {json_type, GetNamedOptionalArgTypeOptions("options", false)},
              },
              nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalSnippet(
    absl::Span<const googlesql::Value> args) {
  auto snippet_value = SnippetEvaluator::Evaluate(args);
  if (!snippet_value.ok()) {
    return snippet_value.status();
  }
  std::optional<std::string> snippet = snippet_value.value();
  if (!snippet.has_value()) {
    return googlesql::Value::NullJson();
  }
  GOOGLESQL_ASSIGN_OR_RETURN(auto json_value,
                   googlesql::JSONValue::ParseJSONString(snippet.value()));
  return googlesql::Value::Json(std::move(json_value));
}

absl::StatusOr<googlesql::Value> EvalSnippetPG(
    absl::Span<const googlesql::Value> args) {
  auto snippet_value = SnippetEvaluator::Evaluate(args);
  if (!snippet_value.ok()) {
    return snippet_value.status();
  }
  std::optional<std::string> snippet = snippet_value.value();
  if (!snippet.has_value()) {
    return googlesql::Value::Null(GetPgJsonbType());
  }
  return CreatePgJsonbValue(snippet.value());
}

std::unique_ptr<googlesql::Function> SnippetFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name,
    database_api::DatabaseDialect dialect) {
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* bool_type = type_factory->get_bool();
  const googlesql::Type* int64_type = type_factory->get_int64();
  const googlesql::Type* json_type = type_factory->get_json();
  auto pg_jsonb = postgres_translator::spangres::datatypes::GetPgJsonbType();

  // Signature: SNIPPET(string value,
  //                    string query,
  //                    bool enhance_query = false,
  //                    string language_tag = NULL,
  //                    int64_t max_snippet_width = 160,
  //                    int64_t max_snippets = 3,
  //                    string content_type = "text/html")

  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    function_options.set_evaluator(googlesql::FunctionEvaluator(EvalSnippetPG));
    return std::make_unique<googlesql::Function>(
        kSnippetFunctionName, catalog_name, googlesql::Function::SCALAR,
        std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
            pg_jsonb,
            {{string_type, GetRequiredArgumentTypeOptions("value", false)},
             {string_type, GetRequiredArgumentTypeOptions("query")},
             {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
             {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
             {int64_type,
              GetNamedOptionalArgTypeOptions("max_snippet_width", false)},
             {int64_type,
              GetNamedOptionalArgTypeOptions("max_snippets", false)},
             {string_type, GetNamedOptionalArgTypeOptions("content_type")}},
            nullptr}},
        function_options);
  }

  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalSnippet));
  return std::make_unique<googlesql::Function>(
      kSnippetFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          json_type,
          {{string_type, GetRequiredArgumentTypeOptions("value", false)},
           {string_type, GetRequiredArgumentTypeOptions("query")},
           {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
           {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
           {int64_type,
            GetNamedOptionalArgTypeOptions("max_snippet_width", false)},
           {int64_type, GetNamedOptionalArgTypeOptions("max_snippets", false)},
           {string_type, GetNamedOptionalArgTypeOptions("content_type")}},
          nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalTokenizeNgrams(
    absl::Span<const googlesql::Value> args) {
  return NgramsTokenizer::Tokenize(args);
}

std::unique_ptr<googlesql::Function> TokenizeNgramsFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalTokenizeNgrams));
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* int_type = type_factory->get_int64();
  const googlesql::Type* bool_type = type_factory->get_bool();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::ArrayType* string_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(ERROR) << "Fail to make ARRAY<STRING> type.";
  }

  // Signature: TOKENIZE_NGRAMS(string|array[string] value,
  //                               int64_t ngram_size_max = 4,
  //                               int64_t ngram_size_min = 1,
  //                               bool remove_diacritics = false)
  const googlesql::FunctionArgumentTypeList tokenize_ngrams_args = {
      {int_type, GetNamedOptionalArgTypeOptions("ngram_size_max")},
      {int_type, GetNamedOptionalArgTypeOptions("ngram_size_min")},
      {bool_type, GetNamedOptionalArgTypeOptions("remove_diacritics")}};
  googlesql::FunctionArgumentTypeList string_arg_type_list = {
      {string_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_arg_type_list.insert(string_arg_type_list.end(),
                              tokenize_ngrams_args.begin(),
                              tokenize_ngrams_args.end());
  googlesql::FunctionArgumentTypeList string_array_arg_type_list = {
      {string_array_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_array_arg_type_list.insert(string_array_arg_type_list.end(),
                                    tokenize_ngrams_args.begin(),
                                    tokenize_ngrams_args.end());
  return std::make_unique<googlesql::Function>(
      kTokenizeNgramsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{tokenlist_type, string_arg_type_list,
                                       nullptr},
          googlesql::FunctionSignature{tokenlist_type,
                                       string_array_arg_type_list, nullptr}},
      function_options);
}

absl::StatusOr<googlesql::Value> EvalSearchNgrams(
    absl::Span<const googlesql::Value> args) {
  return SearchNgramsEvaluator::Evaluate(args);
}

std::unique_ptr<googlesql::Function> SearchNgramsFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(EvalSearchNgrams));
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* string_type = type_factory->get_string();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* int_type = type_factory->get_int64();
  const googlesql::Type* double_type = type_factory->get_double();
  const googlesql::Type* bool_type = type_factory->get_bool();

  // Signature: SEARCH_NGRAMS(tokenlist value,
  //                          string query,
  //                          int64_t min_ngrams = 2,
  //                          double min_ngrams_percent = 0,
  //                          string language_tag = NULL)
  return std::make_unique<googlesql::Function>(
      kSearchNgramsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              bool_type,
              {{tokenlist_type,
                GetRequiredArgumentTypeOptions("tokens", false)},
               {string_type, GetRequiredArgumentTypeOptions("ngrams_query")},
               {int_type, GetNamedOptionalArgTypeOptions("min_ngrams")},
               {double_type,
                GetNamedOptionalArgTypeOptions("min_ngrams_percent")},
               {string_type, GetNamedOptionalArgTypeOptions("language_tag")}},
              nullptr},
      },
      function_options);
}

absl::StatusOr<googlesql::Value> EvalScoreNgrams(
    absl::Span<const googlesql::Value> args) {
  return ScoreNgramsEvaluator::Evaluate(args);
}

std::unique_ptr<googlesql::Function> ScoreNgramsFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name) {
  googlesql::FunctionOptions function_options;
  function_options.set_evaluator(googlesql::FunctionEvaluator(EvalScoreNgrams));
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* double_type = type_factory->get_double();
  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  const googlesql::Type* string_type = type_factory->get_string();

  // Signature: SCORE_NGRAMS(tokenlist value,
  //                         string ngrams_query,
  //                         string algorithm = "trigrams",
  //                         string language_tag = NULL)
  return std::make_unique<googlesql::Function>(
      kScoreNgramsFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{
          googlesql::FunctionSignature{
              double_type,
              {
                  {tokenlist_type,
                   GetRequiredArgumentTypeOptions("tokens", false)},
                  {string_type, GetRequiredArgumentTypeOptions("ngrams_query")},
                  {string_type, GetNamedOptionalArgTypeOptions("algorithm")},
                  {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
              },
              nullptr},
      },
      function_options);
}

std::unique_ptr<googlesql::Function> TokenizeJsonFunction(
    googlesql::TypeFactory* type_factory, const std::string& catalog_name,
    database_api::DatabaseDialect dialect) {
  googlesql::FunctionOptions function_options;
  function_options.set_arguments_are_coercible(false);
  function_options.set_supports_safe_error_mode(false);

  const googlesql::Type* tokenlist_type = type_factory->get_tokenlist();
  if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
    // Signature: TOKENIZE_JSONB(jsonb value)
    function_options.set_evaluator(
        googlesql::FunctionEvaluator(JsonbTokenizer::Tokenize));
    auto pg_jsonb = postgres_translator::spangres::datatypes::GetPgJsonbType();
    return std::make_unique<googlesql::Function>(
        kTokenizeJsonbFunctionName, catalog_name, googlesql::Function::SCALAR,
        std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
            tokenlist_type,
            {{pg_jsonb, GetRequiredArgumentTypeOptions("value", false)}},
            nullptr}},
        function_options);
  }

  // GoogleSQL dialect signature: TOKENIZE_JSON(json value).
  function_options.set_evaluator(
      googlesql::FunctionEvaluator(JsonTokenizer::Tokenize));
  const googlesql::Type* json_type = type_factory->get_json();
  return std::make_unique<googlesql::Function>(
      kTokenizeJsonFunctionName, catalog_name, googlesql::Function::SCALAR,
      std::vector<googlesql::FunctionSignature>{googlesql::FunctionSignature{
          tokenlist_type,
          {{json_type, GetRequiredArgumentTypeOptions("value", false)}},
          nullptr}},
      function_options);
}
}  // namespace

absl::flat_hash_map<std::string, std::unique_ptr<googlesql::Function>>
GetSearchFunctions(googlesql::TypeFactory* type_factory,
                   const std::string& catalog_name,
                   database_api::DatabaseDialect dialect) {
  absl::flat_hash_map<std::string, std::unique_ptr<googlesql::Function>>
      function_map;
  auto token_func = TokenFunction(type_factory, catalog_name);
  function_map[token_func->Name()] = std::move(token_func);

  auto tokenize_number_func =
      TokenizeNumberFunction(type_factory, catalog_name, dialect);
  function_map[tokenize_number_func->Name()] = std::move(tokenize_number_func);

  auto tokenize_bool_func = TokenizeBoolFunction(type_factory, catalog_name);
  function_map[tokenize_bool_func->Name()] = std::move(tokenize_bool_func);

  auto tokenize_func = TokenizeFullTextFunction(type_factory, catalog_name);
  function_map[tokenize_func->Name()] = std::move(tokenize_func);

  auto tokenize_substr_func =
      TokenizeSubstringFunction(type_factory, catalog_name);
  function_map[tokenize_substr_func->Name()] = std::move(tokenize_substr_func);

  auto tokenlist_concat_func =
      TokenlistConcatFunction(type_factory, catalog_name);
  function_map[tokenlist_concat_func->Name()] =
      std::move(tokenlist_concat_func);

  auto search_func = SearchFunction(type_factory, catalog_name);
  function_map[search_func->Name()] = std::move(search_func);

  auto search_substr_func = SearchSubstringFunction(type_factory, catalog_name);
  function_map[search_substr_func->Name()] = std::move(search_substr_func);

  auto score_func = ScoreFunction(type_factory, catalog_name);
  function_map[score_func->Name()] = std::move(score_func);

  auto snippet_func = SnippetFunction(type_factory, catalog_name, dialect);
  function_map[snippet_func->Name()] = std::move(snippet_func);

  auto tokenize_ngrams_func =
      TokenizeNgramsFunction(type_factory, catalog_name);
  function_map[tokenize_ngrams_func->Name()] = std::move(tokenize_ngrams_func);

  auto search_ngrams_func = SearchNgramsFunction(type_factory, catalog_name);
  function_map[search_ngrams_func->Name()] = std::move(search_ngrams_func);

  auto score_ngrams_func = ScoreNgramsFunction(type_factory, catalog_name);
  function_map[score_ngrams_func->Name()] = std::move(score_ngrams_func);

  auto tokenize_json_func =
      TokenizeJsonFunction(type_factory, catalog_name, dialect);
  function_map[tokenize_json_func->Name()] = std::move(tokenize_json_func);

  return function_map;
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
