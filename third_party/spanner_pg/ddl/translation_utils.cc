//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/ddl/translation_utils.h"

#include <array>
#include <iomanip>
#include <sstream>
#include <string>

#include "absl/strings/match.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/types/optional.h"
#include "third_party/spanner_pg/src/include/pg_config.h"
#include "re2/re2.h"

namespace postgres_translator {
namespace spangres {
namespace internal {

// Additional precaution against migration errors - include below will fail to
// build if any new keyword types are added in PG.
enum KeywordType {
  UNRESERVED_KEYWORD,
  COL_NAME_KEYWORD,
  TYPE_FUNC_NAME_KEYWORD,
  RESERVED_KEYWORD
};

// To avoid importing and linking PG code, we define some of our own
// preprocessor magic to make a lookup table for keywords.
#define PG_KEYWORD(name, ignored, type, collabel) {name, type},
static auto kKeywordsMap =
    std::map<absl::string_view, KeywordType>({
#include "third_party/spanner_pg/src/include/parser/kwlist.h"
    });
#undef PG_KEYWORD

// Quotes <str> by putting it inside <delim>'s, escapes all <delim> characters
// within the <str> by doubling them.
std::string Quote(absl::string_view str, char delim) {
  std::ostringstream ss;
  ss << std::quoted(str, /*delim=*/delim, /*escape=*/delim);
  return ss.str();
}

std::string QuoteStringLiteral(absl::string_view str) {
  return Quote(str, '\'');
}

std::string QuoteIdentifier(absl::string_view identifier) {
  static_assert(PG_VERSION_NUM == 110010,
                "Please, make sure that PostgreSQL quotation logic duplicated "
                "below hasn't changed with new release");

  static const LazyRE2 safe_identifier_re = {"[a-z_][a-z0-9_]*"};
  bool need_quoting = !RE2::FullMatch(identifier, *safe_identifier_re);
  if (!need_quoting) {
    auto id_type_it = kKeywordsMap.find(identifier);
    need_quoting = id_type_it != kKeywordsMap.end() &&
                   id_type_it->second != UNRESERVED_KEYWORD;
  }

  return need_quoting ? Quote(identifier, '"') : std::string(identifier);
}

struct QuoteIdentifierFormatter {
  void operator()(std::string* out, absl::string_view in) const {
    out->append(QuoteIdentifier(in));
  }
};

std::string QuoteQualifiedIdentifier(absl::string_view identifier) {
  return absl::StrJoin(absl::StrSplit(identifier, '.'), ".",
                       QuoteIdentifierFormatter());
}

// only have to do this once to keep the possibilities of cut'n'paste errors to
// a minimum.
static std::vector<PGAlterOption> GetOptionList() {
  static std::vector<PGAlterOption> options = {
      PGAlterOption(
          PostgreSQLConstants::kSpangresOptimizerVersionName, T_Integer,
          PostgreSQLConstants::kDatabaseOptimizerVersionName, T_Integer),
      PGAlterOption(
          PostgreSQLConstants::kSpangresDatabaseVersionRetentionPeriodName,
          T_String,
          PostgreSQLConstants::kInternalDatabaseVersionRetentionPeriodName,
          T_String),
      PGAlterOption(
          PostgreSQLConstants::kSpangresDatabaseOptimizerStatisticsPackageName,
          T_String,
          PostgreSQLConstants::kDatabaseOptimizerStatisticsPackageName,
          T_String),
      PGAlterOption(
          PostgreSQLConstants::kSpangresDatabaseDefaultLeaderOptionName,
          T_String,
          PostgreSQLConstants::kInternalDatabaseDefaultLeaderOptionName,
          T_String),
      PGAlterOption(
          PostgreSQLConstants::kSpangresDatabaseWitnessLocationOptionName,
          T_String,
          PostgreSQLConstants::kInternalDatabaseWitnessLocationOptionName,
          T_String),
  };

  return options;
}

// Returns a new unowned pointer to an options object map, keyed by PG option
// name.
absl::flat_hash_map<absl::string_view, PGAlterOption> *BuildPGOptionsMap() {
  absl::flat_hash_map<absl::string_view, PGAlterOption> *options_map;

  options_map = new (absl::flat_hash_map<absl::string_view, PGAlterOption>);
  for (auto &el : GetOptionList()) {
    options_map->insert({el.PGName(), el});
  }
  return options_map;
}

// Returns a nde unowned pointer to an options object map, keyed by Spanner
// option name.
absl::flat_hash_map<absl::string_view, PGAlterOption> *BuildSpannerOptionsMap() {
  absl::flat_hash_map<absl::string_view, PGAlterOption> *options_map;

  options_map = new (absl::flat_hash_map<absl::string_view, PGAlterOption>);
  for (auto &el : GetOptionList()) {
    options_map->insert({el.SpannerName(), el});
  }
  return options_map;
}

// The PG name->options and Spanner name->options maps. Set up as globals so we
// can initialize them at startup and avoid threading issues.
absl::flat_hash_map<absl::string_view, PGAlterOption> *pg_options_map =
    BuildPGOptionsMap();
absl::flat_hash_map<absl::string_view, PGAlterOption> *spanner_options_map =
    BuildSpannerOptionsMap();

absl::optional<PGAlterOption> GetOptionByInternalName(
    absl::string_view spangres_option_name) {
  auto val = pg_options_map->find(spangres_option_name);
  if (val != pg_options_map->end()) {
    return val->second;
  }
  return absl::nullopt;
}

// This function has a list of all the PG/Spanner DB options. It's here so we
absl::optional<absl::string_view> GetInternalOptionName(
    absl::string_view spangres_option_name) {
  absl::optional<PGAlterOption> opt =
      GetOptionByInternalName(spangres_option_name);
  if (opt == absl::nullopt) {
    return absl::nullopt;
  }
  return opt->SpannerName();
}

absl::optional<absl::string_view> GetSpangresOptionName(
    absl::string_view internal_option_name) {
  auto val = spanner_options_map->find(internal_option_name);
  if (val != spanner_options_map->end()) {
    return val->second.PGName();
  }
  return absl::nullopt;
}

bool IsReservedName(absl::string_view name) {
  // These names can collide with table names in the pg_catalog schema and we
  // don't yet implement schema search path the same way as PostgreSQL does, so
  // for now avoid the collision entirely by disallowing such names.
  return absl::StartsWith(name, "pg_");
}

bool IsPostgresReservedName(absl::string_view name) {
  return absl::EqualsIgnoreCase(name, "postgres");
}

}  // namespace internal
}  // namespace spangres
}  // namespace postgres_translator
