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

#ifndef CATALOG_SPANGRES_SYSTEM_CATALOG_H_
#define CATALOG_SPANGRES_SYSTEM_CATALOG_H_

#include <vector>

#include "zetasql/public/language_options.h"
#include "zetasql/public/types/type.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"

namespace postgres_translator {
namespace spangres {

constexpr absl::string_view kSpangresSystemCatalogName = "pg";
constexpr char kDefaultFunctionNamespace[] = "pg_catalog";

// A derived class of EngineSystemCatalog which represents the PostgreSQL types
// and functions supported in Spanner through the PostgreSQL dialect.
class SpangresSystemCatalog : public EngineSystemCatalog {
 public:
  // Attempt to initialize the EngineSystemCatalog singleton with a
  // SpangresSystemCatalog.
  // Returns true if successful, false if EngineSystemCatalog was already
  // initialized, and an error if there was a problem initializing the catalog.
  static absl::StatusOr<bool> TryInitializeEngineSystemCatalog(
      std::unique_ptr<EngineBuiltinFunctionCatalog> builtin_function_catalog,
      const zetasql::LanguageOptions& language_options);

  // Reset the EngineSystemCatalog. The PG evaluators are defined with a time
  // zone. When we change the default time zone for a database, we need to reset
  // the EngineSystemCatalog to pick up the new time zone.
  static void ResetEngineSystemCatalog();

  const PostgresTypeMapping* GetType(const std::string& name) const override;

  absl::Status GetCustomErrorForProc(Oid proc_oid) const override;

  absl::StatusOr<FunctionAndSignature> GetPgNumericCastFunction(
      const zetasql::Type* source_type, const zetasql::Type* target_type,
      const zetasql::LanguageOptions& language_options) override;

  bool IsTransformationRequiredForComparison(
      const zetasql::ResolvedExpr& gsql_expr) override;

  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  GetResolvedExprForComparison(
      std::unique_ptr<zetasql::ResolvedExpr> gsql_expr,
      const zetasql::LanguageOptions& language_options) override;

  virtual bool IsResolvedExprForComparison(
      const zetasql::ResolvedExpr& gsql_expr) const override;

  virtual absl::StatusOr<const zetasql::ResolvedExpr*>
  GetOriginalExprFromComparisonExpr(
      const zetasql::ResolvedExpr& mapped_gsql_expr) const override;

  virtual std::optional<Oid> GetMappedOidForComparisonFuncid(
      Oid funcid) const override;

  absl::StatusOr<zetasql::TypeListView> GetExtendedTypeSuperTypes(
      const zetasql::Type* type) override;

  absl::Status FindConversion(const zetasql::Type* from_type,
                              const zetasql::Type* to_type,
                              const FindConversionOptions& options,
                              zetasql::Conversion* conversion) override;

  // Get the matching function and signature for this oid and set of input
  // argument types. Returns an error if the function call is not supported.
  absl::StatusOr<FunctionAndSignature> GetFunctionAndSignature(
      Oid proc_oid,
      const std::vector<zetasql::InputArgumentType>& input_argument_types,
      const zetasql::LanguageOptions& language_options) override;

  // Get the matching function and signature for this expr identifier and set of
  // input argument types. Returns an error if the function call is not
  // supported.
  absl::StatusOr<FunctionAndSignature> GetFunctionAndSignature(
      const PostgresExprIdentifier& expr_id,
      const std::vector<zetasql::InputArgumentType>& input_argument_types,
      const zetasql::LanguageOptions& language_options) override;

 private:

  SpangresSystemCatalog(
      std::unique_ptr<EngineBuiltinFunctionCatalog> builtin_function_catalog)
      : EngineSystemCatalog(kSpangresSystemCatalogName,
                            std::move(builtin_function_catalog)) {}

  // Add PostgreSQL types for Spanner.
  absl::Status AddTypes(
      const zetasql::LanguageOptions& language_options) override;

  // Add PostgreSQL functions for Spanner.
  absl::Status AddFunctions(
      const zetasql::LanguageOptions& language_options) override;

  absl::Status AddFunctionRegistryFunctions(
      std::vector<PostgresFunctionArguments>& functions);

  // If the input `gsql_expr` returns a double type, wraps it in a call to
  // PG.MapDoubleToInt or PG.MapFloatToInt in order to preserve float8/float4
  // ordering and equality semantics.
  absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  GetResolvedExprForFloatingPointComparison(
      std::unique_ptr<zetasql::ResolvedExpr> gsql_expr,
      const zetasql::LanguageOptions& language_options);

  // MapDoubleToInt and MapFloatToInt functions are necessary because Spanner
  // has different sort semantics for FLOAT4 and FLOAT8 types compared to
  // Postgres. Wrapping ResolvedExprs which return FLOAT4 or FLOAT8 with these
  // function allows sort and comparison semantics to be equivalent.
  absl::StatusOr<FunctionAndSignature> GetMapFloatingPointToIntFunction(
      const zetasql::Type* source_type,
      const zetasql::LanguageOptions& language_options);
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // CATALOG_SPANGRES_SYSTEM_CATALOG_H_
