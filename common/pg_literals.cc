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

#include "common/pg_literals.h"

#include <optional>
#include <string>

#include "absl/strings/str_cat.h"
#include "re2/re2.h"

namespace google {
namespace spanner {
namespace emulator {

std::string GetFullyQualifiedNameFromPgLiteral(const std::string& name) {
  std::optional<std::string> schema_name;
  std::optional<std::string> object_name;
  static const LazyRE2 kObjectNameRegex = {
      R"(^(?:\"?([^."]*)\"?\.)?\"?([^."]*)\"?$)"};
  if (RE2::FullMatch(name, *kObjectNameRegex, &schema_name, &object_name)) {
    if (schema_name.has_value() && object_name.has_value()) {
      return absl::StrCat(schema_name.value(), ".", object_name.value());
    } else if (object_name.has_value()) {
      return object_name.value();
    }
  }
  return name;
}

}  // namespace emulator
}  // namespace spanner
}  // namespace google
