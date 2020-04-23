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

#include "backend/schema/updater/global_schema_names.h"

#include "backend/schema/catalog/index.h"
#include "backend/schema/catalog/table.h"
#include "common/errors.h"
#include "common/limits.h"
#include "farmhash.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Separator between components of generated names.
constexpr absl::string_view kSeparator = "_";

// Formats the fingerprint hash value of a string.
std::string MakeFingerprint(absl::string_view signature) {
  // Add a seed to help avoid generated name algorithm collisions.
  std::string text = absl::StrCat("2695C62E33338E4B", kSeparator, signature);
  return absl::StrFormat("%016X", farmhash::Fingerprint64(text));
}

// Builds a name. Truncates the base component to ensure the final length of the
// name does not exceed the schema name limit.
std::string MakeName(absl::string_view base, absl::string_view suffix) {
  int length = base.length() + kSeparator.length() + suffix.length();
  if (length > limits::kMaxSchemaIdentifierLength) {
    base.remove_suffix(length - limits::kMaxSchemaIdentifierLength);
    if (absl::EndsWith(base, kSeparator)) {
      base.remove_suffix(kSeparator.length());
    }
  }
  return absl::StrCat(base, kSeparator, suffix);
}

}  // namespace

zetasql_base::Status GlobalSchemaNames::AddName(absl::string_view type,
                                        const std::string& name) {
  if (!names_.insert(name).second) {
    return error::SchemaObjectAlreadyExists(type, name);
  }
  return zetasql_base::OkStatus();
}

zetasql_base::StatusOr<std::string> GlobalSchemaNames::GenerateForeignKeyName(
    absl::string_view referencing_table_name,
    absl::string_view referenced_table_name) {
  ZETASQL_RET_CHECK(!referencing_table_name.empty());
  ZETASQL_RET_CHECK(!referenced_table_name.empty());
  std::string base = absl::StrJoin(
      {absl::string_view{"FK"}, referencing_table_name, referenced_table_name},
      kSeparator);
  return GenerateSequencedName(base, MakeFingerprint(base));
}

std::string GlobalSchemaNames::GenerateSequencedName(
    absl::string_view base, absl::string_view fingerprint) {
  for (std::uint64_t sequence = 1; true; ++sequence) {
    std::string suffix = absl::StrCat(fingerprint, kSeparator, sequence);
    std::string name = MakeName(base, suffix);
    if (names_.insert(name).second) {
      return name;
    }
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
