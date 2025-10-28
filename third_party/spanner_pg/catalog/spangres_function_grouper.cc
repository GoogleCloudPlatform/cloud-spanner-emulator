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

#include "third_party/spanner_pg/catalog/spangres_function_grouper.h"

#include <string>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"

namespace postgres_translator {

namespace {
std::vector<std::string> FromProto(const FunctionNamePathProto& proto) {
  return std::vector<std::string>(proto.name_path().begin(),
                                  proto.name_path().end());
}

}  // namespace

absl::btree_map<NamePathKey, FunctionProto> GroupFunctionsByNamePaths(
    const std::vector<FunctionProto>& functions) {
  absl::btree_map<NamePathKey, FunctionProto> result;

  std::vector<FunctionProto> function_by_name_path;
  for (const auto& function : functions) {
    const std::vector<std::string>& mapped_name_path =
        FromProto(function.mapped_name_path());

    for (const auto& signature : function.signatures()) {
      for (const auto& pg_name_path_proto : signature.postgresql_name_paths()) {
        const std::vector<std::string>& pg_name_path =
            FromProto(pg_name_path_proto);

        NamePathKey key{mapped_name_path, pg_name_path};
        auto it = result.find(key);
        if (it == result.end()) {
          // Adds a new function with a signature with the current pg_name_path
          FunctionProto new_function;
          new_function.CopyFrom(function);
          new_function.clear_postgresql_name_paths();
          new_function.clear_signatures();
          FunctionSignatureProto new_signature;
          new_signature.CopyFrom(signature);
          new_signature.clear_postgresql_name_paths();
          (*new_signature.add_postgresql_name_paths()) = pg_name_path_proto;
          (*new_function.add_postgresql_name_paths()) = pg_name_path_proto;
          (*new_function.add_signatures()) = new_signature;

          result.insert({key, new_function});
        } else {
          // Adds the signature with the current pg_name_path to an existing
          // function
          FunctionProto& existing_function = it->second;
          FunctionSignatureProto new_signature;
          new_signature.CopyFrom(signature);
          new_signature.clear_postgresql_name_paths();
          (*new_signature.add_postgresql_name_paths()) = pg_name_path_proto;
          (*existing_function.add_signatures()) = new_signature;
        }
      }
    }
  }

  return result;
}

}  // namespace postgres_translator
