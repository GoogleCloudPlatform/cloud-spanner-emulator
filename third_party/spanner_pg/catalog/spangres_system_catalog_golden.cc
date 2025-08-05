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

#include "third_party/spanner_pg/catalog/spangres_system_catalog_golden.h"

#include <algorithm>
#include <string>
#include <vector>

#include "zetasql/public/function.pb.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/options.pb.h"
#include "absl/log/check.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.h"
#include "third_party/spanner_pg/catalog/spangres_system_catalog.pb.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "google/protobuf/json/json.h"

namespace postgres_translator {

using ::postgres_translator::spangres::test::GetSpangresTestAnalyzerOptions;
using ::postgres_translator::spangres::test::GetSpangresTestSystemCatalog;

spangres::SpangresSystemCatalog* InitializeEngineSystemCatalog(
    bool is_emulator) {
  zetasql::LanguageOptions language_options =
      GetSpangresTestAnalyzerOptions().language();

  EngineSystemCatalog* catalog = GetSpangresTestSystemCatalog(language_options);
  return static_cast<spangres::SpangresSystemCatalog*>(catalog);
}

std::string SpangresSystemCatalogGolden::Capture() {
  std::vector<PostgresFunctionArguments> functions =
      catalog_->TEST_GetFunctionsToAdd();
  spangres::SpangresSystemCatalogProto proto = ToProto(functions);
  std::string output;
  ABSL_CHECK_OK(google::protobuf::json::MessageToJsonString(  // Crash OK
      proto, &output, google::protobuf::json::PrintOptions{.add_whitespace = true}));

  return output;
}

spangres::SpangresFunctionSignatureProto SpangresSystemCatalogGolden::ToProto(
    const PostgresFunctionSignatureArguments& signature) {
  spangres::SpangresFunctionSignatureProto result;
  result.set_result_type(signature.signature().result_type().DebugString());
  for (const auto& argument : signature.signature().arguments()) {
    *result.add_arguments() = argument.DebugString();
  }
  result.set_has_mapped_function(signature.has_mapped_function());
  result.set_explicit_mapped_function_name(
      signature.explicit_mapped_function_name());
  result.set_postgres_oid(signature.postgres_proc_oid());

  return result;
}

spangres::SpangresFunctionProto SpangresSystemCatalogGolden::ToProto(
    const PostgresFunctionArguments& function) {
  spangres::SpangresFunctionProto result;
  result.set_postgres_function_name(function.postgres_function_name());
  result.set_mapped_function_name(function.mapped_function_name());

  std::vector<PostgresFunctionSignatureArguments> signatures =
      function.signature_arguments();
  std::sort(signatures.begin(), signatures.end(),
            [](const PostgresFunctionSignatureArguments& s1,
               const PostgresFunctionSignatureArguments& s2) {
              return s1.signature().DebugString() <
                     s2.signature().DebugString();
            });

  for (const auto& signature : signatures) {
    *result.add_signatures() = ToProto(signature);
  }
  result.set_mode(zetasql::FunctionEnums::Mode_Name(function.mode()));
  result.set_postgres_namespace(function.postgres_namespace());

  return result;
}

spangres::SpangresSystemCatalogProto SpangresSystemCatalogGolden::ToProto(
    std::vector<PostgresFunctionArguments>& functions) {
  std::sort(functions.begin(), functions.end(),
            [](const PostgresFunctionArguments& a,
               const PostgresFunctionArguments& b) {
              return a.postgres_function_name() < b.postgres_function_name();
            });

  spangres::SpangresSystemCatalogProto result;
  for (const auto& function : functions) {
    *result.add_functions() = ToProto(function);
  }

  return result;
}

}  // namespace postgres_translator
