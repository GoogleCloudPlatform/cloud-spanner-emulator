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

#include "third_party/spanner_pg/codegen/default_values_reader.h"

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/log/check.h"
#include "absl/status/statusor.h"
#include "nlohmann/json.hpp"
#include "third_party/spanner_pg/codegen/default_values_embed.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

using JSON = ::nlohmann::json;

namespace postgres_translator {

namespace {
absl::StatusOr<uint32_t> ParseProcOid(const JSON& json_entry) {
  ZETASQL_RET_CHECK(json_entry.find("proc_oid") != json_entry.end())
      << "ERROR: proc_oid entry could not be found";
  ZETASQL_RET_CHECK(json_entry.at("proc_oid").is_number_integer())
      << "ERROR: proc_oid is not an integer number (proc_oid = "
      << json_entry.at("proc_oid") << ")";

  int64_t oid = json_entry.at("proc_oid");
  ZETASQL_RET_CHECK(oid <= std::numeric_limits<uint32_t>::max())
      << "ERROR: proc_oid overflow (proc_oid = " << oid << ")";
  ZETASQL_RET_CHECK(oid >= std::numeric_limits<uint32_t>::min())
      << "ERROR: proc_oid underflow (proc_oid = " << oid << ")";

  return oid;
}

absl::StatusOr<std::vector<std::string>> ParseDefaultValues(
    const JSON& json_entry) {
  ZETASQL_RET_CHECK(json_entry.find("default_values") != json_entry.end())
      << "ERROR: default_values entry could not be found";
  ZETASQL_RET_CHECK(json_entry.at("default_values").is_array())
      << "ERROR: default_values is not an array (default_values = "
      << json_entry.at("default_values") << ")";
  ZETASQL_RET_CHECK(!json_entry.at("default_values").empty())
      << "ERROR: default_values is empty";

  std::vector<std::string> result;
  for (const auto& default_value : json_entry.at("default_values")) {
    ZETASQL_RET_CHECK(default_value.is_string())
        << "ERROR: default_values element is not a string (element = "
        << default_value << ")";
    result.push_back(default_value);
  }

  return result;
}

}  // namespace

absl::StatusOr<absl::flat_hash_map<uint32_t, std::vector<std::string>>>
GetProcsDefaultValues(std::string_view json_string) {
  absl::flat_hash_map<uint32_t, std::vector<std::string>> result;

  JSON json_document =
      JSON::parse(json_string, /*cb=*/nullptr, /*allow_exceptions=*/false,
                  /*ignore_comments=*/true);
  ZETASQL_RET_CHECK(!json_document.is_discarded())
      << "ERROR: failed to parse default values JSON: " << json_string;
  ZETASQL_RET_CHECK(json_document.is_array())
      << "ERROR: the PostgreSQL default values json document is not an array";

  for (const auto& json_entry : json_document) {
    ZETASQL_ASSIGN_OR_RETURN(uint32_t proc_oid, ParseProcOid(json_entry));
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::string> default_values,
                     ParseDefaultValues(json_entry));
    ZETASQL_RET_CHECK(!result.contains(proc_oid))
        << "ERROR: duplicate OID found when building PostgreSQL default "
           "values: "
        << proc_oid;

    result.insert({proc_oid, default_values});
  }

  return result;
}

absl::StatusOr<absl::flat_hash_map<uint32_t, std::vector<std::string>>>
GetProcsDefaultValues() {
  return GetProcsDefaultValues(kDefaultValuesEmbed);
}

absl::flat_hash_map<uint32_t, std::vector<std::string>>
GetProcsDefaultValuesOrDie() {
  const auto& result = GetProcsDefaultValues();
  ABSL_CHECK_OK(result);  // Crash OK
  return *result;
}

}  // namespace postgres_translator
