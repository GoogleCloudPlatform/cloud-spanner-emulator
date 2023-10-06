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

#include "third_party/spanner_pg/catalog/builtin_rewrite_functions.h"

#include <vector>

#include "zetasql/public/function_signature.h"
#include "zetasql/public/type.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/builtin_spanner_functions.h"
#include "third_party/spanner_pg/catalog/spangres_type.h"

namespace postgres_translator {
namespace spangres {

// Constructs signature and sets FunctionSignatureRewriteOptions with
// 'REWRITE_BUILTIN_FUNCTION_INLINER' as the rewriter.
zetasql::FunctionSignature ConstructSignatureWithRewrite(
    zetasql::FunctionArgumentType&& result_type,
    zetasql::FunctionArgumentTypeList&& arguments, void* context_ptr,
    absl::string_view sql) {
  zetasql::FunctionSignature signature{result_type, arguments, context_ptr};
  zetasql::FunctionSignatureRewriteOptions options;
  options.set_sql(sql);
  options.set_rewriter(zetasql::REWRITE_BUILTIN_FUNCTION_INLINER);
  signature.mutable_options().set_rewrite_options(options);
  return signature;
}

PostgresFunctionArguments* GetExistingPostgresFunctionArguments(
    std::vector<PostgresFunctionArguments>& functions,
    absl::string_view function_name) {
  // TODO : Consider optimising this linear scan.
  for (PostgresFunctionArguments& function : functions) {
    if (function.postgres_function_name() == function_name) {
      return &function;
    }
  }
  return nullptr;
}

void AddTruncFunctionSignatures(
    std::vector<PostgresFunctionArguments>& functions) {
  auto existing_trunc_function =
      GetExistingPostgresFunctionArguments(functions, "trunc");
  ABSL_CHECK_NE(existing_trunc_function, nullptr) << "trunc function not found";
  const zetasql::Type* gsql_pg_numeric =
      postgres_translator::spangres::types::PgNumericMapping()->mapped_type();
  existing_trunc_function->add_signature(
      {ConstructSignatureWithRewrite(
           gsql_pg_numeric,
           {{gsql_pg_numeric,
             zetasql::FunctionArgumentTypeOptions().set_argument_name(
                 "arg1", zetasql::kPositionalOnly)}},
           /*context_ptr=*/nullptr, "trunc(arg1, 0)"),
       /*has_postgres_proc_oid=*/true, /*has_mapped_function=*/false});
}

void AddPgCatalogFunctions(std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_string = zetasql::types::StringType();
  functions.push_back(
      {"pg_get_viewdef",
       "pg_get_viewdef",
       {{ConstructSignatureWithRewrite(
             gsql_string,
             {{gsql_string,
               zetasql::FunctionArgumentTypeOptions().set_argument_name(
                   "arg1", zetasql::kPositionalOnly)}},
             /*context_ptr=*/nullptr,
             // The ZetaSQL rewriter operates in STRICT name resolution mode
             // so all names must be fully qualified.
             "(select pg_views.definition from pg_catalog.pg_views where "
             "pg_views.viewname = arg1)"),
         /*has_postgres_proc_oid=*/true, /*has_mapped_function=*/false}}});
}

void AddRewriteFunctions(std::vector<PostgresFunctionArguments>& functions) {
  AddTruncFunctionSignatures(functions);
  AddPgCatalogFunctions(functions);
}

}  // namespace spangres
}  // namespace postgres_translator
