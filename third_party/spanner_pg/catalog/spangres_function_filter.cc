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

#include "third_party/spanner_pg/catalog/spangres_function_filter.h"

#include <optional>
#include <string>
#include <vector>

#include "zetasql/base/no_destructor.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/commandlineflag.h"
#include "absl/flags/reflection.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_join.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

namespace {

static zetasql_base::NoDestructor<absl::flat_hash_set<std::string>>
    kCatalogManualRegistration({
        "pg.array_all_equal",
        "pg.array_all_greater",
        "pg.array_all_greater_equal",
        "pg.array_all_less",
        "pg.array_all_less_equal",
        "pg.array_slice",
        "pg.greatest",
        "pg.least",

        "pg.cast_to_date",
        "pg.cast_to_double",
        "pg.cast_to_float",
        "pg.cast_to_int64",
        "pg.cast_to_interval",
        "pg.cast_to_numeric",
        "pg.cast_to_string",
        "pg.cast_to_timestamp",
        "pg.map_double_to_int",
        "pg.map_float_to_int",

        // Registered through make_date
        "date",
        // Registered through pg.extract
        "pg.extract_interval",
        // Registered through timestamp_seconds
        "pg.to_timestamp",
        // Registered through substr
        "pg.substring",
    });

// Returns `true` if all flags are enabled. Returns `false`, otherwise.
absl::StatusOr<bool> IsEnabledInCatalog(
    const google::protobuf::RepeatedPtrField<std::string> flags) {
  for (const auto& flag_name : flags) {
    const absl::CommandLineFlag* flag = absl::FindCommandLineFlag(flag_name);
    ZETASQL_RET_CHECK(flag != nullptr) << "Flag " << flag_name << " not found";
    std::optional<bool> is_enabled = flag->TryGet<bool>();
    ZETASQL_RET_CHECK(is_enabled.has_value())
        << "Could not get boolean value of flag " << flag_name;
    if (!is_enabled.value()) {
      return false;
    }
  }
  return true;
}

}  // namespace

absl::StatusOr<std::vector<FunctionProto>> FilterEnabledFunctionsAndSignatures(
    const std::vector<FunctionProto>& functions) {
  std::vector<FunctionProto> result;

  for (const auto& function : functions) {
    // Excludes functions manually registered
    std::string full_name =
        absl::StrJoin(function.mapped_name_path().name_path(), ".");
    if (kCatalogManualRegistration->contains(full_name)) continue;

    // Excludes functions disabled in catalog
    ZETASQL_ASSIGN_OR_RETURN(bool is_enabled_in_catalog,
                     IsEnabledInCatalog(function.enable_in_catalog()));
    if (!is_enabled_in_catalog) continue;

    FunctionProto function_proto;
    function_proto.CopyFrom(function);
    // We will only add the enabled signatures
    function_proto.clear_signatures();

    for (const auto& signature : function.signatures()) {
      // Excludes signatures disabled in catalog
      ZETASQL_ASSIGN_OR_RETURN(bool is_enabled_in_catalog,
                       IsEnabledInCatalog(signature.enable_in_catalog()));
      if (!is_enabled_in_catalog) continue;

      (*function_proto.add_signatures()) = signature;
    }

    if (!function_proto.signatures().empty()) {
      result.push_back(function_proto);
    }
  }

  return result;
}

}  // namespace postgres_translator
