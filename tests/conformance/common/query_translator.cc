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

#include "tests/conformance/common/query_translator.h"

#include <string>
#include <vector>

#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor.h"
#include "re2/re2.h"

namespace google::spanner::emulator::test {

namespace {

constexpr LazyRE2 kHintSyntaxRegex = {"@{(.+?)}"};
constexpr LazyRE2 kArgumentStringSingleQuoteRegex = {"(\'.*?\')([,)\\]])"};
constexpr LazyRE2 kArgumentStringDoubleQuoteRegex = {"\"(.*?)\"([,)\\]])"};
constexpr LazyRE2 kFunctionCallRegex = {
    "(?:([a-zA-Z0-9_]+)\\.)?([a-zA-Z0-9_]+)\\("};

constexpr int kSpannerNamespace = 50000;

void ReplaceHintSyntax(std::string& query) {
  RE2::GlobalReplace(&query, *kHintSyntaxRegex, "/*@ \\1 */");
}

void ReplaceLiteralCasts(std::string& query) {
  // TODO: Handle other types of literals.
  RE2::GlobalReplace(&query, "CAST\\(NULL AS STRING\\)", "null::text");
}

void ReplaceArgumentStringLiterals(std::string& query) {
  // Double quotes.
  RE2::GlobalReplace(&query, *kArgumentStringDoubleQuoteRegex,
                     "'\\1'::text\\2");
  // Single quotes.
  RE2::GlobalReplace(&query, *kArgumentStringSingleQuoteRegex, "\\1::text\\2");
  // Empty array literal.
  RE2::GlobalReplace(&query, "\\[\\]", "'{}'::text[]");
}

void ReplaceStringNullLiteral(std::string& query) {
  RE2::GlobalReplace(&query, "NULL", "null::text");
}

void ReplaceStringArrayLiterals(std::string& query) {
  RE2::GlobalReplace(&query, "\\[\\]", "'{}'::text[]");
}

void ReplaceNamespacedFunctions(std::string& query) {
  std::string schema_name, function_name;
  auto bootstrap_catalog = postgres_translator::GetPgBootstrapCatalog();

  // Need to copy the query string to a string_view to use
  // RE2::FindAndConsume.
  std::string initial_query = query;
  absl::string_view query_view(initial_query);
  while (RE2::FindAndConsume(&query_view, *kFunctionCallRegex, &schema_name,
                             &function_name)) {
    if (!schema_name.empty()) {
      // Explicit schema name, do nothing.
      continue;
    }

    absl::StatusOr<std::vector<postgres_translator::PgProcData>> procs_data =
        postgres_translator::GetPgProcDataFromBootstrap(
            bootstrap_catalog, absl::AsciiStrToLower(function_name));
    if (!procs_data.ok()) {
      ABSL_LOG(WARNING) << "Could not replace namespace for function "
                   << function_name << " due to error: " << procs_data.status();
      continue;
    }

    for (const auto& proc_data : *procs_data) {
      if (proc_data.pronamespace() == kSpannerNamespace) {
        RE2::Replace(&query, RE2(function_name),
                     absl::StrCat("spanner.", function_name));
        break;
      }
    }
  }
}

}  // namespace

std::string QueryTranslator::Translate(const std::string& query) {
  std::string translated_query = query;
  if (uses_hint_syntax_) {
    ReplaceHintSyntax(translated_query);
  }
  if (uses_literal_casts_) {
    ReplaceLiteralCasts(translated_query);
  }
  if (uses_argument_string_literals_) {
    ReplaceArgumentStringLiterals(translated_query);
  }
  if (uses_string_null_literal_) {
    ReplaceStringNullLiteral(translated_query);
  }
  if (uses_string_array_literals_) {
    ReplaceStringArrayLiterals(translated_query);
  }
  if (uses_namespaced_functions_) {
    ReplaceNamespacedFunctions(translated_query);
  }
  return translated_query;
};

// Option Setters.

QueryTranslator& QueryTranslator::UsesHintSyntax() {
  uses_hint_syntax_ = true;
  return *this;
};
QueryTranslator& QueryTranslator::UsesLiteralCasts() {
  uses_literal_casts_ = true;
  return *this;
}
QueryTranslator& QueryTranslator::UsesArgumentStringLiterals() {
  uses_argument_string_literals_ = true;
  return *this;
};
QueryTranslator& QueryTranslator::UsesStringNullLiteral() {
  uses_string_null_literal_ = true;
  return *this;
}
QueryTranslator& QueryTranslator::UsesStringArrayLiterals() {
  uses_string_array_literals_ = true;
  return *this;
}
QueryTranslator& QueryTranslator::UsesNamespacedFunctions() {
  uses_namespaced_functions_ = true;
  return *this;
}

}  // namespace google::spanner::emulator::test
