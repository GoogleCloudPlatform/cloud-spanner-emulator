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

#include <set>
#include <string>

#include "absl/flags/parse.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_join.h"
#include "absl/strings/substitute.h"
#include "backend/schema/parser/ddl_reserved_words.h"

namespace {

std::string ToToken(absl::string_view word) {
  std::string token = absl::AsciiStrToUpper(word);
  if (token == "DEFAULT") {
    token = "DEFAULTT";
  } else if (token == "NULL") {
    token = "NULLL";
  } else if (token == "TOKEN") {
    token = "TOKENT";
  }
  return token;
}

void GenerateKeywordsRules(
    const google::spanner::emulator::backend::CaseInsensitiveStringSet&
        reserved_keywords,
    const google::spanner::emulator::backend::CaseInsensitiveStringSet&
        pseudo_reserved_keywords) {
  std::set<std::string> reserved_defs, gsql_extra_reserved_defs, pseudo_defs,
      pseudo_refs, all_refs;

  for (absl::string_view reserved : reserved_keywords) {
    const std::string upper = ToToken(reserved);
    std::string lower = absl::AsciiStrToLower(reserved);
    // Special exception: ALIAS and ALIASES are... aliases. :P
    if (lower == "alias") {
      lower = "alias\" | \"aliases";
    } else if (lower == "aliases") {
      continue;
    }
    const std::string space(std::max(1, 21 - static_cast<int>(upper.size())),
                            ' ');
    reserved_defs.insert(
        absl::Substitute("<$0:$1\"$2\">", upper, space, lower));
    all_refs.insert(absl::Substitute("<$0>", upper));
  }
  for (absl::string_view pseudo : pseudo_reserved_keywords) {
    const std::string upper = ToToken(pseudo);
    const std::string lower = absl::AsciiStrToLower(pseudo);
    const std::string space(std::max(1, 25 - static_cast<int>(upper.size())),
                            ' ');
    pseudo_defs.insert(absl::Substitute("<$0:$1\"$2\">", upper, space, lower));
    pseudo_refs.insert(absl::Substitute("<$0>", upper));
  }

  const std::string output = absl::Substitute(
      R"(
TOKEN:
{
    $0
}

TOKEN:
{
    $1
}

void pseudoReservedWord() #void :
{}
{
    $2
}

void any_reserved_word() :
{}
{
    $3
}
)",
      absl::StrJoin(reserved_defs, "\n  | "),
      absl::StrJoin(pseudo_defs, "\n  | "),
      absl::StrJoin(pseudo_refs, "\n  | "), absl::StrJoin(all_refs, "\n  | "));
  printf("%s", output.c_str());
}
}  // namespace

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);
  GenerateKeywordsRules(
      google::spanner::emulator::backend::ddl::GetReservedWords(),
      google::spanner::emulator::backend::ddl::GetPseudoReservedWords());
  return 0;
}
