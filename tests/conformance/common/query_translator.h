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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_QUERY_TRANSLATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_QUERY_TRANSLATOR_H_

#include <string>

namespace google::spanner::emulator::test {

class QueryTranslator {
 public:
  QueryTranslator()
      : uses_hint_syntax_(false),
        uses_argument_string_literals_(false),
        uses_string_null_literal_(false),
        uses_string_array_literals_(false),
        uses_literal_casts_(false),
        uses_namespaced_functions_(false) {}
  ~QueryTranslator() = default;
  QueryTranslator(const QueryTranslator&) = default;
  QueryTranslator& operator=(const QueryTranslator&) = delete;

  // Translates a query from PostgreSQL to Spanner according to the set options.
  // The options are:
  //  - UsesHintSyntax:
  //      Replaces hint syntax with PostgreSQL hint syntax.
  //  - UsesLiteralCasts:
  //      Replaces literal casts with their PostgreSQL equivalents.
  //      Currently only supports NULL STRING literals.
  //  - UsesStringLiterals:
  //      Replaces string literals with their PostgreSQL equivalents.
  //  - UsesStringNullLiteral:
  //      Replaces NULL with null::text.
  //  - UsesStringArrayLiterals:
  //      Replaces empty array literals with '{}'::text[].
  //  - UsesNamespacedFunctions:
  //      Prepends "spanner." to namespaced functions.
  std::string Translate(const std::string& query);

  // Option Setters.
  QueryTranslator& UsesHintSyntax();
  QueryTranslator& UsesLiteralCasts();
  QueryTranslator& UsesArgumentStringLiterals();
  QueryTranslator& UsesStringNullLiteral();
  QueryTranslator& UsesStringArrayLiterals();
  QueryTranslator& UsesNamespacedFunctions();

 private:
  bool uses_hint_syntax_;
  bool uses_argument_string_literals_;
  bool uses_string_null_literal_;
  bool uses_string_array_literals_;
  bool uses_literal_casts_;
  bool uses_namespaced_functions_;
};

}  // namespace google::spanner::emulator::test

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_TESTS_CONFORMANCE_COMMON_QUERY_TRANSLATOR_H_
