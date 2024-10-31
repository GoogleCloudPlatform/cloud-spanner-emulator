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
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/function.h"
#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/query/search/bool_tokenizer.h"
#include "backend/query/search/exact_match_tokenizer.h"
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

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {
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

zetasql::FunctionArgumentTypeOptions GetArgumentTypeOptions(
    absl::string_view arg_name,
    zetasql::FunctionEnums::NamedArgumentKind named_argument,
    bool is_required, bool must_be_constant) {
  zetasql::FunctionArgumentTypeOptions result;
  result.set_argument_name(arg_name, named_argument);
  result.set_cardinality(is_required
                             ? zetasql::FunctionArgumentType::REQUIRED
                             : zetasql::FunctionArgumentType::OPTIONAL);
  result.set_must_be_constant(must_be_constant);
  return result;
}

zetasql::FunctionArgumentTypeOptions GetRequiredArgumentTypeOptions(
    absl::string_view arg_name, bool must_be_constant = true) {
  return GetArgumentTypeOptions(arg_name, zetasql::kPositionalOrNamed,
                                /*is_required=*/true, must_be_constant);
}

zetasql::FunctionArgumentTypeOptions GetNamedOptionalArgTypeOptions(
    absl::string_view arg_name, bool must_be_constant = true) {
  return GetArgumentTypeOptions(arg_name, zetasql::kNamedOnly,
                                /*is_required=*/false, must_be_constant);
}

absl::StatusOr<zetasql::Value> EvalToken(
    absl::Span<const zetasql::Value> args) {
  return ExactMatchTokenizer::Tokenize(args);
}

std::unique_ptr<zetasql::Function> TokenFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalToken));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* bytes_type = type_factory->get_bytes();
  const zetasql::ArrayType* string_array_type = nullptr;
  const zetasql::ArrayType* bytes_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok() ||
      !type_factory->MakeArrayType(bytes_type, &bytes_array_type).ok()) {
    // Don't expect either of them to fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<STRING> or ARRAY<BYTE> types.";
  }

  // Signature:
  //  TOKEN(string|byte|array[string|byte] value)
  return std::make_unique<zetasql::Function>(
      kTokenFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              tokenlist_type,
              {{string_type, GetRequiredArgumentTypeOptions("value", false)}},
              nullptr},
          zetasql::FunctionSignature{
              tokenlist_type,
              {{bytes_type, GetRequiredArgumentTypeOptions("value", false)}},
              nullptr},
          zetasql::FunctionSignature{
              tokenlist_type,
              {{string_array_type,
                GetRequiredArgumentTypeOptions("value", false)}},
              nullptr},
          zetasql::FunctionSignature{
              tokenlist_type,
              {{bytes_array_type,
                GetRequiredArgumentTypeOptions("value", false)}},
              nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTokenizeNumber(
    absl::Span<const zetasql::Value> args) {
  return NumericTokenizer::Tokenize(args);
}

std::unique_ptr<zetasql::Function> TokenizeNumberFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalTokenizeNumber));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* int64_type = type_factory->get_int64();
  const zetasql::Type* uint64_type = type_factory->get_uint64();
  const zetasql::Type* double_type = type_factory->get_double();

  // Signature:
  //  TOKENIZE_NUMBER(int64_t|uint64|double|array[int64|uint64_t|double] value,
  //                  string comparison_type = "all",
  //                  string algorithm = "auto",
  //                  int64_t|uint64|double min = min(type),
  //                  int64_t|uint64|double max = max(type),
  //                  int64_t|uint64|double granularity = 1,
  //                  int64_t tree_base = 2,
  //                  int64_t precision = 15)
  std::vector<const zetasql::Type*> numeric_types{int64_type, uint64_type,
                                                    double_type};

  std::vector<zetasql::FunctionSignature> signatures;
  for (auto type : numeric_types) {
    const zetasql::FunctionArgumentTypeList tokenize_number_args = {
        {string_type, GetNamedOptionalArgTypeOptions("comparison_type")},
        {string_type, GetNamedOptionalArgTypeOptions("algorithm")},
        {type, GetNamedOptionalArgTypeOptions("min")},
        {type, GetNamedOptionalArgTypeOptions("max")},
        {type, GetNamedOptionalArgTypeOptions("granularity")},
        {int64_type, GetNamedOptionalArgTypeOptions("tree_base")},
        {int64_type, GetNamedOptionalArgTypeOptions("precision")}};

    zetasql::FunctionArgumentTypeList num_arg_type_list = {
        {type, GetRequiredArgumentTypeOptions("value", false)}};
    num_arg_type_list.insert(num_arg_type_list.end(),
                             tokenize_number_args.begin(),
                             tokenize_number_args.end());
    signatures.push_back(zetasql::FunctionSignature{
        tokenlist_type, num_arg_type_list, nullptr});

    const zetasql::ArrayType* array_type;
    if (type_factory->MakeArrayType(type, &array_type).ok()) {
      zetasql::FunctionArgumentTypeList array_arg_type_list = {
          {array_type, GetRequiredArgumentTypeOptions("value", false)}};
      array_arg_type_list.insert(array_arg_type_list.end(),
                                 tokenize_number_args.begin(),
                                 tokenize_number_args.end());
      signatures.push_back(zetasql::FunctionSignature{
          tokenlist_type, array_arg_type_list, nullptr});
    }
  }

  return std::make_unique<zetasql::Function>(
      kTokenizeNumberFunctionName, catalog_name, zetasql::Function::SCALAR,
      signatures, function_options);
}

absl::StatusOr<zetasql::Value> EvalTokenizeBool(
    absl::Span<const zetasql::Value> args) {
  return BoolTokenizer::Tokenize(args);
}

std::unique_ptr<zetasql::Function> TokenizeBoolFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalTokenizeBool));

  const zetasql::Type* bool_type = type_factory->get_bool();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();

  // Signature: TOKENIZE_BOOL(bool value)
  return std::make_unique<zetasql::Function>(
      kTokenizeBoolFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          tokenlist_type,
          {{bool_type, GetRequiredArgumentTypeOptions("value", false)}},
          nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTokenizeFullText(
    absl::Span<const zetasql::Value> args) {
  return PlainFullTextTokenizer::Tokenize(args);
}

std::unique_ptr<zetasql::Function> TokenizeFullTextFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalTokenizeFullText));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::ArrayType* string_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<STRING> or ARRAY<BYTE> types.";
  }

  // Signature: TOKENIZE_FULLTEXT(string|array[string] value,
  //                              string language_tag = NULL,
  //                              string content_type = "text/plain",
  //                              string token_category = NULL)
  const zetasql::FunctionArgumentTypeList tokenize_fulltext_args = {
      {string_type, GetNamedOptionalArgTypeOptions("language_tag", false)},
      {string_type, GetNamedOptionalArgTypeOptions("content_type")},
      {string_type, GetNamedOptionalArgTypeOptions("token_category")},
  };
  zetasql::FunctionArgumentTypeList string_arg_type_list = {
      {string_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_arg_type_list.insert(string_arg_type_list.end(),
                              tokenize_fulltext_args.begin(),
                              tokenize_fulltext_args.end());
  zetasql::FunctionArgumentTypeList string_array_arg_type_list = {
      {string_array_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_array_arg_type_list.insert(string_array_arg_type_list.end(),
                                    tokenize_fulltext_args.begin(),
                                    tokenize_fulltext_args.end());

  return std::make_unique<zetasql::Function>(
      kTokenizeFullTextFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{tokenlist_type, string_arg_type_list,
                                       nullptr},
          zetasql::FunctionSignature{tokenlist_type,
                                       string_array_arg_type_list, nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTokenizeSubstring(
    absl::Span<const zetasql::Value> args) {
  return SubstringTokenizer::Tokenize(args);
}

std::unique_ptr<zetasql::Function> TokenizeSubstringFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalTokenizeSubstring));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* int_type = type_factory->get_int64();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* bool_type = type_factory->get_bool();
  const zetasql::ArrayType* string_array_type = nullptr;
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
  const zetasql::FunctionArgumentTypeList tokenize_substring_args = {
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
  zetasql::FunctionArgumentTypeList string_arg_type_list = {
      {string_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_arg_type_list.insert(string_arg_type_list.end(),
                              tokenize_substring_args.begin(),
                              tokenize_substring_args.end());
  zetasql::FunctionArgumentTypeList string_array_arg_type_list = {
      {string_array_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_array_arg_type_list.insert(string_array_arg_type_list.end(),
                                    tokenize_substring_args.begin(),
                                    tokenize_substring_args.end());
  return std::make_unique<zetasql::Function>(
      kTokenizeSubstringFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{tokenlist_type, string_arg_type_list,
                                       nullptr},
          zetasql::FunctionSignature{tokenlist_type,
                                       string_array_arg_type_list, nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalTokenlistConcat(
    absl::Span<const zetasql::Value> args) {
  return TokenlistConcat::Concat(args);
}

std::unique_ptr<zetasql::Function> TokenlistConcatFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalTokenlistConcat));

  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::ArrayType* tokenlist_array_type = nullptr;
  if (!type_factory->MakeArrayType(tokenlist_type, &tokenlist_array_type)
           .ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(FATAL) << "Fail to make ARRAY<TOKENLIST> type.";
  }

  // Signature: TOKENLIST_CONCAT(ARRAY<TOKENLIST>)
  return std::make_unique<zetasql::Function>(
      kTokenlistConcatFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
          tokenlist_type,
          {{tokenlist_array_type,
            GetRequiredArgumentTypeOptions("tokens", false)}},
          nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalSearch(
    absl::Span<const zetasql::Value> args) {
  return SearchEvaluator::Evaluate(args);
}

std::unique_ptr<zetasql::Function> SearchFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalSearch));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* bool_type = type_factory->get_bool();

  // Signature: SEARCH(tokenlist value,
  //                   string query,
  //                   bool enhance_query = false,
  //                   string language_tag = NULL)
  return std::make_unique<zetasql::Function>(
      kSearchFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              bool_type,
              {{tokenlist_type,
                GetRequiredArgumentTypeOptions("tokens", false)},
               {string_type, GetRequiredArgumentTypeOptions("query")},
               {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
               {string_type, GetNamedOptionalArgTypeOptions("language_tag")}},
              nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalSearchSubstring(
    absl::Span<const zetasql::Value> args) {
  return SearchSubstringEvaluator::Evaluate(args);
}

std::unique_ptr<zetasql::Function> SearchSubstringFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalSearchSubstring));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* bool_type = type_factory->get_bool();

  // Signature: SEARCH_SUBSTRING(tokenlist value,
  //                             string query,
  //                             string relative_search_type = NULL,
  //                             string language_tag = NULL)
  return std::make_unique<zetasql::Function>(
      kSearchSubstringFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
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

absl::StatusOr<zetasql::Value> EvalScore(
    absl::Span<const zetasql::Value> args) {
  return ScoreEvaluator::Evaluate(args);
}

std::unique_ptr<zetasql::Function> ScoreFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalScore));

  const zetasql::Type* double_type = type_factory->get_double();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* bytes_type = type_factory->get_bytes();
  const zetasql::Type* bool_type = type_factory->get_bool();
  const zetasql::Type* json_type = type_factory->get_json();

  // Signature: SCORE(tokenlist value,
  //                  string query,
  //                  bool enhance_query = false,
  //                  string language_tag = NULL,
  //                  json options = NULL)
  return std::make_unique<zetasql::Function>(
      kScoreFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
              double_type,
              {
                  {tokenlist_type,
                   GetRequiredArgumentTypeOptions("tokens", false)},
                  {string_type, GetRequiredArgumentTypeOptions("query")},
                  {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
                  {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
                  {json_type, GetNamedOptionalArgTypeOptions("options", false)},
              },
              nullptr},
          zetasql::FunctionSignature{
              double_type,
              {
                  {tokenlist_type,
                   GetRequiredArgumentTypeOptions("tokens", false)},
                  {bytes_type, GetRequiredArgumentTypeOptions("query")},
                  {bool_type, GetNamedOptionalArgTypeOptions("enhance_query")},
                  {string_type, GetNamedOptionalArgTypeOptions("language_tag")},
                  {json_type, GetNamedOptionalArgTypeOptions("options", false)},
              },
              nullptr},
      },
      function_options);
}

absl::StatusOr<zetasql::Value> EvalSnippet(
    absl::Span<const zetasql::Value> args) {
  return SnippetEvaluator::Evaluate(args);
}

std::unique_ptr<zetasql::Function> SnippetFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalSnippet));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* bool_type = type_factory->get_bool();
  const zetasql::Type* int64_type = type_factory->get_int64();
  const zetasql::Type* json_type = type_factory->get_json();

  // Signature: SNIPPET(string value,
  //                    string query,
  //                    bool enhance_query = false,
  //                    string language_tag = NULL,
  //                    int64_t max_snippet_width = 160,
  //                    int64_t max_snippets = 3,
  //                    string content_type = "text/html")
  return std::make_unique<zetasql::Function>(
      kSnippetFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{zetasql::FunctionSignature{
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

absl::StatusOr<zetasql::Value> EvalTokenizeNgrams(
    absl::Span<const zetasql::Value> args) {
  return NgramsTokenizer::Tokenize(args);
}

std::unique_ptr<zetasql::Function> TokenizeNgramsFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalTokenizeNgrams));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* int_type = type_factory->get_int64();
  const zetasql::Type* bool_type = type_factory->get_bool();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::ArrayType* string_array_type = nullptr;
  if (!type_factory->MakeArrayType(string_type, &string_array_type).ok()) {
    // Don't expect the call would fail.
    ABSL_LOG(ERROR) << "Fail to make ARRAY<STRING> type.";
  }

  // Signature: TOKENIZE_NGRAMS(string|array[string] value,
  //                               int64_t ngram_size_max = 4,
  //                               int64_t ngram_size_min = 1,
  //                               bool remove_diacritics = false)
  const zetasql::FunctionArgumentTypeList tokenize_ngrams_args = {
      {int_type, GetNamedOptionalArgTypeOptions("ngram_size_max")},
      {int_type, GetNamedOptionalArgTypeOptions("ngram_size_min")},
      {bool_type, GetNamedOptionalArgTypeOptions("remove_diacritics")}};
  zetasql::FunctionArgumentTypeList string_arg_type_list = {
      {string_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_arg_type_list.insert(string_arg_type_list.end(),
                              tokenize_ngrams_args.begin(),
                              tokenize_ngrams_args.end());
  zetasql::FunctionArgumentTypeList string_array_arg_type_list = {
      {string_array_type, GetRequiredArgumentTypeOptions("value", false)}};
  string_array_arg_type_list.insert(string_array_arg_type_list.end(),
                                    tokenize_ngrams_args.begin(),
                                    tokenize_ngrams_args.end());
  return std::make_unique<zetasql::Function>(
      kTokenizeNgramsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{tokenlist_type, string_arg_type_list,
                                       nullptr},
          zetasql::FunctionSignature{tokenlist_type,
                                       string_array_arg_type_list, nullptr}},
      function_options);
}

absl::StatusOr<zetasql::Value> EvalSearchNgrams(
    absl::Span<const zetasql::Value> args) {
  return SearchNgramsEvaluator::Evaluate(args);
}

std::unique_ptr<zetasql::Function> SearchNgramsFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(
      zetasql::FunctionEvaluator(EvalSearchNgrams));

  const zetasql::Type* string_type = type_factory->get_string();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* int_type = type_factory->get_int64();
  const zetasql::Type* double_type = type_factory->get_double();
  const zetasql::Type* bool_type = type_factory->get_bool();

  // Signature: SEARCH_NGRAMS(tokenlist value,
  //                          string query,
  //                          int64_t min_ngrams = 2,
  //                          double min_ngrams_percent = 0,
  //                          string language_tag = NULL)
  return std::make_unique<zetasql::Function>(
      kSearchNgramsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
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

absl::StatusOr<zetasql::Value> EvalScoreNgrams(
    absl::Span<const zetasql::Value> args) {
  return ScoreNgramsEvaluator::Evaluate(args);
}

std::unique_ptr<zetasql::Function> ScoreNgramsFunction(
    zetasql::TypeFactory* type_factory, const std::string& catalog_name) {
  zetasql::FunctionOptions function_options;
  function_options.set_evaluator(zetasql::FunctionEvaluator(EvalScoreNgrams));

  const zetasql::Type* double_type = type_factory->get_double();
  const zetasql::Type* tokenlist_type = type_factory->get_tokenlist();
  const zetasql::Type* string_type = type_factory->get_string();

  // Signature: SCORE_NGRAMS(tokenlist value,
  //                         string ngrams_query,
  //                         string algorithm = "trigrams",
  //                         string language_tag = NULL)
  return std::make_unique<zetasql::Function>(
      kScoreNgramsFunctionName, catalog_name, zetasql::Function::SCALAR,
      std::vector<zetasql::FunctionSignature>{
          zetasql::FunctionSignature{
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
}  // namespace

absl::flat_hash_map<std::string, std::unique_ptr<zetasql::Function>>
GetSearchFunctions(zetasql::TypeFactory* type_factory,
                   const std::string& catalog_name) {
  absl::flat_hash_map<std::string, std::unique_ptr<zetasql::Function>>
      function_map;
  auto token_func = TokenFunction(type_factory, catalog_name);
  function_map[token_func->Name()] = std::move(token_func);

  auto tokenize_number_func =
      TokenizeNumberFunction(type_factory, catalog_name);
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

  auto snippet_func = SnippetFunction(type_factory, catalog_name);
  function_map[snippet_func->Name()] = std::move(snippet_func);

  auto tokenize_ngrams_func =
      TokenizeNgramsFunction(type_factory, catalog_name);
  function_map[tokenize_ngrams_func->Name()] = std::move(tokenize_ngrams_func);

  auto search_ngrams_func = SearchNgramsFunction(type_factory, catalog_name);
  function_map[search_ngrams_func->Name()] = std::move(search_ngrams_func);

  auto score_ngrams_func = ScoreNgramsFunction(type_factory, catalog_name);
  function_map[score_ngrams_func->Name()] = std::move(score_ngrams_func);

  return function_map;
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
