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

#include <new>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "backend/common/case.h"
#include "backend/schema/catalog/schema.h"
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

std::string MakeBaseName(absl::string_view prefix,
                         std::vector<absl::string_view> object_names) {
  std::vector<absl::string_view> to_join = {prefix};
  for (const auto& object_name : object_names) {
    to_join.push_back(SDLObjectName::GetInSchemaName(object_name));
  }

  return absl::StrJoin(to_join, kSeparator);
}

}  // namespace

absl::Status GlobalSchemaNames::AddName(absl::string_view type,
                                        const std::string& name) {
  if (!names_.insert(name).second) {
    return error::SchemaObjectAlreadyExists(type, name);
  }
  return absl::OkStatus();
}

absl::StatusOr<std::string> GlobalSchemaNames::GenerateForeignKeyName(
    absl::string_view referencing_table_name,
    absl::string_view referenced_table_name) {
  ZETASQL_RET_CHECK(!referencing_table_name.empty());
  ZETASQL_RET_CHECK(!referenced_table_name.empty());
  std::string base =
      MakeBaseName("FK", {referencing_table_name, referenced_table_name});
  return GenerateSequencedName("Foreign Key", base, MakeFingerprint(base));
}

absl::StatusOr<std::string> GlobalSchemaNames::GenerateCheckConstraintName(
    absl::string_view table_name) {
  ZETASQL_RET_CHECK(!table_name.empty());
  std::string base = MakeBaseName("CK", {table_name});
  return GenerateSequencedName("Check Constraint", base, MakeFingerprint(base));
}

std::string GlobalSchemaNames::GenerateSequencedName(
    absl::string_view type, absl::string_view base,
    absl::string_view fingerprint) {
  for (std::uint64_t sequence = 1; true; ++sequence) {
    std::string suffix = absl::StrCat(fingerprint, kSeparator, sequence);
    std::string name = MakeName(base, suffix);
    if (names_.insert(name).second) {
      ZETASQL_VLOG(1) << "Generated " << type << " name: " << name;
      return name;
    }
  }
}

absl::StatusOr<std::string> GlobalSchemaNames::GenerateManagedIndexName(
    absl::string_view table_name, const std::vector<std::string>& column_names,
    bool null_filtered, bool unique) {
  ZETASQL_RET_CHECK(!table_name.empty());
  ZETASQL_RET_CHECK(!column_names.empty());
  // Index column names.
  std::string columns = absl::StrJoin(column_names, kSeparator);
  // Base name = index prefix + table name + column names.
  std::string base = MakeBaseName("IDX", {table_name, columns});
  // Index codes, possibly empty.
  std::string codes;
  if (unique) {
    // Single code of 'U' for unique indexes whether null-filtered or not.
    absl::StrAppend(&codes, "U");
  } else if (null_filtered) {
    absl::StrAppend(&codes, "N");
  }
  absl::string_view codes_separator = codes.empty() ? "" : kSeparator;
  // Signature = index prefix + table name + columns names + index codes.
  std::string signature = absl::StrCat(base, codes_separator, codes);
  // Fingerprint is based on the full, non-truncated signature of the index.
  std::string fingerprint = MakeFingerprint(signature);
  // Suffix = index codes + fingerprint.
  std::string suffix = absl::StrCat(codes, codes_separator, fingerprint);
  // Full name = truncated(index prefix + table name + column names)
  //             + index codes + fingerprint.
  std::string name = MakeName(base, suffix);
  ZETASQL_VLOG(1) << "Generated managed index name: " << name;
  return name;
}

absl::Status GlobalSchemaNames::ValidateSchemaName(absl::string_view type,
                                                   absl::string_view name) {
  ZETASQL_RET_CHECK(!name.empty());
  const auto& [schema_part, name_part] = SDLObjectName::SplitSchemaName(name);
  if (!schema_part.empty()) {
    // Check is run for objects to ensure the named schema exists before making
    // it here, so minimal check will run on the schema_part.
    ZETASQL_RETURN_IF_ERROR(ValidateSchemaName(type, name_part));
    if (!IsSDLTypeAllowedInNamedSchema(type)) {
      return error::SchemaObjectTypeUnsupportedInNamedSchema(type, name);
    }
  }

  if (name[0] == '_') {
    return error::InvalidSchemaName(type, name);
  }
  if (name.length() > limits::kMaxSchemaIdentifierLength) {
    return error::InvalidSchemaName(type, name);
  }
  return absl::OkStatus();
}

absl::Status GlobalSchemaNames::ValidateNamedSchemaName(
    absl::string_view named_schema_name) {
  ZETASQL_RETURN_IF_ERROR(ValidateSchemaName("Schema", named_schema_name));
  if (ReservedSchemaNames().contains(std::string(named_schema_name)) ||
      // To avoid possible schema collision in the future, we block some prefix
      // below:
      // `spanner_{version}.{function}` could be used for built-in function
      // replacemet/deprecation purpose.
      absl::StartsWith(named_schema_name, "spanner_")
      // `pg_` prefix is used by couple postgresql system schemas, block whole
      // prefix for simplicity.
      || absl::StartsWith(named_schema_name, "pg_")) {
    return error::InvalidSchemaName("Schema", named_schema_name);
  }

  return absl::OkStatus();
};

absl::Status GlobalSchemaNames::ValidateConstraintName(
    absl::string_view table_name, absl::string_view constraint_type,
    absl::string_view constraint_name) {
  ZETASQL_RETURN_IF_ERROR(ValidateSchemaName(constraint_type, constraint_name));
  for (absl::string_view reserved_prefix : {"PK_", "CK_IS_NOT_NULL_"}) {
    if (absl::StartsWithIgnoreCase(constraint_name, reserved_prefix)) {
      return error::InvalidConstraintName(constraint_type, constraint_name,
                                          reserved_prefix);
    }
  }
  return absl::OkStatus();
}

const CaseInsensitiveStringSet& GlobalSchemaNames::ReservedSchemaNames() {
  // clang-format off
  // LINT.IfChange(reserved_schema_names)
  static const CaseInsensitiveStringSet* const kInstance =
      new CaseInsensitiveStringSet({
    // Provided for in the sql standard, implemented by spanner.
    "INFORMATION_SCHEMA",
    // Provided for in the googlesql language as a function prefix, although not
    // a namespace there.
    "safe",
    // Provided for in googlesql as existing function namespaces.
    "aead",
    "kll_quantiles",
    "keys",
    "hll_count",
    "net",
    "ml",
    // The following are not reserved by googlesql, but provided by spanner.
    "SPANNER_PLACEMENT",
    "SPANNER_SYS",
    // Reserved for spanner, actually used in postgres syntax variant only.
    "pg_information_schema",
    "pg_catalog",
    "pg",
    "public",
    // Not provided by googlesql or spanner, but reserved because it exists in
    // the sql standard.
    "DEFINITION_SCHEMA",
    // `default` is used on UI and FGAC to refer our default schema.
    "default",
    // Used by PostgreSQL dialect as a prefix to refer spanner specific feature,
    // like spanner.commit_timestamp, spanner.{package_name}.
    "spanner"
    // We don't need to reserve "_mt" or any future system schema names starting
    // with "_" since users are already forbidden to create such system names.
      });
  // LINT.ThenChange()
  // clang-format on
  return *kInstance;
}

bool GlobalSchemaNames::IsSDLTypeAllowedInNamedSchema(absl::string_view type) {
  return type == "Table" || type == "Synonym" || type == "View" ||
         type == "Sequence" || type == "Index" || type == "Udf";
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
