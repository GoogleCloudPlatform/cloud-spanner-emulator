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

#ifndef CATALOG_ENGINE_SYSTEM_CATALOG_H_
#define CATALOG_ENGINE_SYSTEM_CATALOG_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "zetasql/public/catalog.h"
#include "zetasql/public/function.h"
#include "zetasql/public/function_signature.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/base/const_init.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/catalog/type.h"
#include "third_party/spanner_pg/interface/engine_builtin_function_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

ABSL_CONST_INIT static absl::Mutex engine_system_catalog_mutex(
    absl::kConstInit);

// An abstract class for catalogs which store the PostgresTypeMappings
// and PostgresExtendedFunctions for a storage engine. Instances of the derived
// classes should be populated in the constructor and never modified.
//
// EngineSystemCatalog can be expensive to initialize because it maps most
// function signatures to a PostgreSQL oid and to a builtin function signature.
// Storage engines which expect to use the same system catalog objects across
// queries should use GetEngineSystemCatalogPtr() to initialize the
// EngineSystemCatalog singleton before or during the first query, and then use
// GetEngineSystemCatalog() to access the EngineSystemCatalog singleton.
//
// All virtual FindX methods from the zetasql::Catalog class besides
// FindType and FindFunction will throw an error.
// All GetX methods from the zetasql::EnumerableCatalog class besides
// GetTypes and GetFunctions will also throw an error.
// FindConversion and GetConversions will be populated when
// PostgresTypeMappings have base implementations in at least one
// storage engine.
//
// Derived classes must implement AddTypes and AddFunctions to register
// PostgreSQL types and functions in the catalog.
//
// Many of the PostgreSQL types and functions have a "mapped" builtin type or
// function that we should use temporarily until the PostgreSQL types and
// functions are fully implemented in the storage engine.
// Each function in this class has a comment stating if it applies to the
// original PostgreSQL types and functions or if it applies to the mapped
// builtin types and functions.
class EngineSystemCatalog : public zetasql::EnumerableCatalog {
 public:
  static EngineSystemCatalog* GetEngineSystemCatalog();

  std::string FullName() const override { return name_; }

  // Get the PostgreSQL type by name as a zetasql::Type.
  // Sets *type to nullptr if the type is not found.
  // FindOptions is not currently supported.
  absl::Status GetType(const std::string& name, const zetasql::Type** type,
                       const FindOptions& options = FindOptions()) override;

  // Get the PostgresTypeMapping by name.
  // Returns nullptr if the type is not found.
  virtual const PostgresTypeMapping* GetType(const std::string& name) const;

  // Get the PostgresTypeMapping by oid.
  // Returns nullptr if the type is not found.
  const PostgresTypeMapping* GetType(Oid oid) const;

  // Get the `PostgresTypeMapping` for the reverse transformer.
  // - `types` specifies the type to look up in the reverse map.
  //   If the PostgreSQL type has a base implementation, type is the Postgres
  //   Extended Type.
  //   If the PostgreSQL type has a mapped builtin type, type is the mapped
  //   builtin type.
  //   Currently, all of the PostgreSQL types have mapped builtin ZetaSQL
  //   types so the type will always be the ZetaSQL mapped type.
  // - `max_length` specifies the maximum length of the type. Used just for
  //   string types: if a positive int is specified, then only varchar should
  //   be returned, even if the flag
  //   `postgres_reverse_translator_enable_random_pick_text_or_varchar` is
  //   set to true.
  // Notice that this result might be overridden by the reverse transformer for
  // RQG purpose. RQG can be configured  to randomly replace TEXT with VARCHAR
  // for increased test coverage since TEXT and VARCHAR in PostgreSQL both map
  // to STRING in ZetaSQL. In production, table columns with the ZetaSQL
  // STRING type will always be reversed transformed to the PostgreSQL TEXT type
  // in the PostgreSQL analyzer.
  const PostgresTypeMapping* GetTypeFromReverseMapping(
      const zetasql::Type* type, int max_length = 0) const;

  // Get the PostgreSQL function by name as a zetasql::Function.
  // Sets *function to nullptr if the function is not found.
  // FindOptions is not currently supported.
  absl::Status GetFunction(const std::string& name,
                           const zetasql::Function** function,
                           const FindOptions& options = FindOptions()) override;

  // Get the PostgresExtendedFunction by name.
  // Returns nullptr if the function is not found.
  const PostgresExtendedFunction* GetFunction(const std::string& name) const;

  // Get the builtin function by PostgresExprIdentifier.
  // Returns nullptr if the function_id is not found.
  const zetasql::Function* GetFunction(
      const PostgresExprIdentifier& expr_id) const;

  // Get the matching function and signature for this oid and set of input
  // argument types.
  // Returns an error if the function call is not supported.
  virtual absl::StatusOr<FunctionAndSignature> GetFunctionAndSignature(
      Oid proc_oid,
      const std::vector<zetasql::InputArgumentType>& input_argument_types,
      const zetasql::LanguageOptions& language_options);

  // Get the matching function and signature for this expr identifier and set of
  // input argument types.
  // Returns an error if the function call is not supported.
  virtual absl::StatusOr<FunctionAndSignature> GetFunctionAndSignature(
      const PostgresExprIdentifier& expr_id,
      const std::vector<zetasql::InputArgumentType>& input_argument_types,
      const zetasql::LanguageOptions& language_options);

  // Returns true if the cast should be overridden with a built-in function and
  // ResolvedFunctionCall instead of a ResolvedCast.
  // Excludes PG Numeric Functions that are soon-to-be migrated to ResolvedCast
  // instead of ResolvedFunctionCall.
  bool HasCastOverrideFunction(const zetasql::Type* source_type,
                               const zetasql::Type* target_type);

  // Get the matching function and signature for this cast override.
  // Returns an error if the cast is not overridden.
  // Excludes PG Numeric Functions that are soon-to-be migrated to ResolvedCast
  // instead of ResolvedFunctionCall.
  absl::StatusOr<FunctionAndSignature> GetCastOverrideFunctionAndSignature(
      const zetasql::Type* source_type, const zetasql::Type* target_type,
      const zetasql::LanguageOptions& language_options);

  // Get the PostgreSQL proc oid for the reverse transformer.
  // - `function name specifies the function name to look up.
  //   If the PostgreSQL function has a base implementation, function_name
  //   should be the PostgreSQL function name. If the PostgreSQL has a mapped
  //   builtin function, function_name should be the name of the mapped
  //   function. Currently, all of the PostgreSQL functions have mapped builtin
  //   functions so the name will always be the builtin function name.
  // - `input_argument_types` are the input types from the ZetaSQL resolved
  //   AST.
  absl::StatusOr<Oid> GetPgProcOidFromReverseMapping(
      const std::string& function_name,
      const std::vector<zetasql::InputArgumentType>& input_argument_types,
      const zetasql::LanguageOptions& language_options);

  // Return true if the builtin function is mapped to a PostgreSQL Expr
  // Identifier.
  bool IsGsqlFunctionMappedToPgExpr(const std::string& function_name);

  // Get the PostgreSQL Expr Identifier for the reverse transformer.
  // Should only be called if IsGsqlFunctionMappedToPgExpr is true.
  absl::StatusOr<PostgresExprIdentifier> GetPostgresExprIdentifier(
      const std::string& function_name);

  // Return true if the builtin function is mapped to a PostgreSQL Cast.
  bool IsGsqlFunctionMappedToPgCast(const std::string& function_name);

  // TODO : Enable conversions after we have base implementations
  // for PostgresTypeMappings.
  // FindOptions is not currently supported.
  absl::Status FindConversion(const zetasql::Type* from_type,
                              const zetasql::Type* to_type,
                              const FindConversionOptions& options,
                              zetasql::Conversion* conversion) override {
    return GetUnimplementedError("conversions");
  }

  // GetTypes is used by the ZetaSQL random query generator to determine
  // which builtin types can be used in the ZetaSQL resolved AST.
  // Output should be populated with the mapped builtin types.
  absl::Status GetTypes(
      absl::flat_hash_set<const zetasql::Type*>* output) const override;

  // GetFunctions is used by the ZetaSQL random query generator to determine
  // which builtin functions can be used in the ZetaSQL resolved AST.
  // Output should be populated with the mapped builtin functions.
  absl::Status GetFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const override;

  // GetConversions is used by the ZetaSQL random query generator to determine
  // which type casts between the builtin types (ZetaSQL and Postgres
  // extended) are supported.
  // TODO : enable conversions after we have base implementations
  // for PostgresTypeMappings.
  absl::Status GetConversions(absl::flat_hash_set<const zetasql::Conversion*>*
                                  output) const override {
    return GetUnimplementedError("conversions");
  }

  absl::StatusOr<bool> IsValidCast(
      const zetasql::Type* from_type, const zetasql::Type* to_type,
      const zetasql::LanguageOptions& language_options);

  absl::Status FindTable(
      const absl::Span<const std::string>& path, const zetasql::Table** table,
      const FindOptions& options = FindOptions()) override final {
    return GetUnimplementedError("tables");
  }

  absl::Status FindModel(
      const absl::Span<const std::string>& path, const zetasql::Model** model,
      const FindOptions& options = FindOptions()) override final {
    return GetUnimplementedError("models");
  }

  absl::Status FindConnection(const absl::Span<const std::string>& path,
                              const zetasql::Connection** connection,
                              const FindOptions& options) override final {
    return GetUnimplementedError("connections");
  }

  absl::Status FindTableValuedFunction(
      const absl::Span<const std::string>& path,
      const zetasql::TableValuedFunction** function,
      const FindOptions& options = FindOptions()) override final {
    return GetUnimplementedError("table valued functions");
  }

  absl::Status FindProcedure(
      const absl::Span<const std::string>& path,
      const zetasql::Procedure** procedure,
      const FindOptions& options = FindOptions()) override final {
    return GetUnimplementedError("procedures");
  }

  absl::Status FindConstantWithPathPrefix(
      const absl::Span<const std::string> path, int* num_names_consumed,
      const zetasql::Constant** constant,
      const FindOptions& options = FindOptions()) override final {
    return GetUnimplementedError("constants");
  }

  absl::Status GetCatalogs(absl::flat_hash_set<const zetasql::Catalog*>*
                               output) const override final {
    return GetUnimplementedError("sub-catalogs");
  }
  absl::Status GetTables(absl::flat_hash_set<const zetasql::Table*>* output)
      const override final {
    return GetUnimplementedError("tables");
  }

  // Uses the overridden versions of AddTypes and AddFunctions from the derived
  // classes.
  absl::Status SetUp(const zetasql::LanguageOptions& language_options) {
    ZETASQL_RETURN_IF_ERROR(AddTypes(language_options));
    ZETASQL_RETURN_IF_ERROR(AddFunctions(language_options));
    return absl::OkStatus();
  }

  zetasql::TypeFactory* type_factory() {
    return builtin_function_catalog_->type_factory();
  }

  // Like GetFunctions(), but return PostgresExtendedFunction objects
  // rather than breaking each supported PostgreSQL function
  // down into the various ZetaSQL functions that implement its different
  // type signatures.
  absl::Status GetPostgreSQLFunctions(
      absl::flat_hash_set<const PostgresExtendedFunction*>* output) const;

  // Like GetFunctions(), but returns all functions in the engine's builtin
  // function catalog, regardless of supprt in this EngineSystemCatalog.
  absl::Status GetBuiltinFunctions(
      absl::flat_hash_set<const zetasql::Function*>* output) const {
    return builtin_function_catalog_->GetFunctions(output);
  }

  // Checks whether a given expression requires transformation for comparison.
  // For some types native comparison semantics doesn't match with Postgres'
  // comparison semantics, in such cases this function allows the database to
  // know whether additional transformation is required on input ResolvedExpr so
  // the underlying database can match semantics.
  virtual bool IsTransformationRequiredForComparison(
      const zetasql::ResolvedExpr& gsql_expr) {
    return false;
  }

  // The transformer will call this function at various points during
  // transformation, as some types may have different native
  // ordering and comparison semantics which don't match Postgres' semantics.
  // This function allows a database to create a new ResolvedExpr from the input
  // ResolvedExpr so the underlying database can match semantics. Places this
  // is called from incude: ORDER BY, Comparison Expressions, CASE, IN.
  virtual absl::StatusOr<std::unique_ptr<zetasql::ResolvedExpr>>
  GetResolvedExprForComparison(
      std::unique_ptr<zetasql::ResolvedExpr> gsql_expr,
      const zetasql::LanguageOptions& language_options) {
    return gsql_expr;
  }

  // In order to perform reverse transformation, the transformer needs to detect
  // if `gsql_expr` is the result of calling GetResolvedExprForComparison. If
  // so, it should be handled seperately by calling
  // GetOriginalExprFromComparisonExpr.
  virtual bool IsResolvedExprForComparison(
      const zetasql::ResolvedExpr& gsql_expr) const {
    return false;
  }

  // Given an input `mapped_gsql_expr` return the original ResolvedExpr.
  // Expected only to be called when the input is the result of
  // GetResolvedExprForComparison.
  virtual absl::StatusOr<const zetasql::ResolvedExpr*>
  GetOriginalExprFromComparisonExpr(
      const zetasql::ResolvedExpr& mapped_gsql_expr) const {
    return absl::UnimplementedError(
        "This catalog does not implement any mappings for comparisons.");
  }

  // Given an input Oid `funcid', return a new Oid if the input is a function
  // that needs to be mapped. Returns std::nullopt if no mapping is necessary.
  virtual std::optional<Oid> GetMappedOidForComparisonFuncid(Oid funcid) const {
    return std::nullopt;
  }

 protected:
  // The EngineSystemCatalog should never be instantiated.
  explicit EngineSystemCatalog(
      absl::string_view name,
      std::unique_ptr<EngineBuiltinFunctionCatalog> builtin_function_catalog)
      : name_(name),
        builtin_function_catalog_(std::move(builtin_function_catalog)) {}

  // Defines the EngineSystemCatalog singleton.
  // Used by GetEngineSystemCatalog to read the singleton.
  // Also used by the derived classes to access and override the singleton.
  static EngineSystemCatalog** GetEngineSystemCatalogPtr()
      ABSL_SHARED_LOCKS_REQUIRED(engine_system_catalog_mutex);

  // Must be implemented by derived classes.
  virtual absl::Status AddTypes(
      const zetasql::LanguageOptions& language_options) = 0;
  virtual absl::Status AddFunctions(
      const zetasql::LanguageOptions& language_options) = 0;

  absl::Status AddType(const PostgresTypeMapping* type,
                       const zetasql::LanguageOptions& language_options);

  // Creates a PostgresExtendedFunction using the arguments and adds it to the
  // catalog.
  // For each signature, if there is an expected PostgreSQL proc oid or mapped
  // builtin function, look up the Oid or function signature and throw an error
  // if either is not found.
  absl::Status AddFunction(const PostgresFunctionArguments& function_arguments,
                           const zetasql::LanguageOptions& language_options);

  // Add a mapping between the PG Expr Identifier and a builtin function.
  absl::Status AddExprFunction(const PostgresExprIdentifier& expr_id,
                               const std::string& builtin_function_name);

  // Add a PG.NUMERIC cast function that corresponds to a PostgreSQL equivalent
  // cast function.
  absl::Status AddPgNumericCastFunction(
      const std::string& builtin_function_name);

  // Add a cast override function.
  // Excludes PG Numeric Functions that are soon-to-be migrated to ResolvedCast
  // instead of ResolvedFunctionCall.
  absl::Status AddCastOverrideFunction(
      const zetasql::Type* source_type, const zetasql::Type* target_type,
      const std::string& builtin_function_name,
      const zetasql::LanguageOptions& language_options);

  // Get a builtin function by name.
  // Returns a nullptr if the function is not found.
  absl::StatusOr<const zetasql::Function*> GetBuiltinFunction(
      const std::string& name) const;

  // Runs the ZetaSQL Function Signature matcher to see if the input arguments
  // are compatible with the signature.
  bool SignatureMatches(
      const std::vector<zetasql::InputArgumentType>& input_arguments,
      const zetasql::FunctionSignature& googlesql_signature,
      std::unique_ptr<zetasql::FunctionSignature>* result_signature,
      const zetasql::LanguageOptions& language_options);

 private:
  // Transforms a PostgreSQL type oid into a ZetaSQL FunctionArgumentType.
  // The PostgreSQL ANYOID type is transformed into a ZetaSQL ARG_TYPE_ANY_1
  // function argument type.
  // All other PostgreSQL types are either successfully transformed to a
  // supported ZetaSQL type or cause an error to be thrown.
  absl::StatusOr<zetasql::FunctionArgumentType> BuildGsqlFunctionArgumentType(
      Oid type_oid, zetasql::FunctionEnums::ArgumentCardinality cardinality);

  // Given the input type oids, output type oid, and variadic type oid of a
  // PostgreSQL proc, return the corresponding ZetaSQL FunctionSignature.
  absl::StatusOr<zetasql::FunctionSignature> BuildGsqlFunctionSignature(
      const oidvector& postgres_input_types, Oid postgres_output_type,
      Oid postgres_variadic_type);

  // Transform the PostgreSQL input type oids into ZetaSQL InputArgumentTypes.
  absl::StatusOr<std::vector<zetasql::InputArgumentType>>
  BuildGsqlInputTypeList(const oidvector& postgres_input_types);

  // Given a list PG proc candidates, a list of input arguments, and a
  // return type, look for a PG proc oid whose signature matches the input
  // argument types.
  // If no matching signature is found, return an error.
  absl::StatusOr<Oid> FindMatchingPgProcOid(
      absl::Span<const FormData_pg_proc* const> procs,
      const std::vector<zetasql::InputArgumentType>& input_argument_types,
      const zetasql::Type* return_type,
      const zetasql::LanguageOptions& language_options);

  // If the mapped_function has a matching signature, create a copy of the
  // mapped function with just this signature. Otherwise, return an error.
  absl::StatusOr<std::unique_ptr<zetasql::Function>> BuildMappedFunction(
      const zetasql::FunctionSignature& postgres_signature,
      const std::vector<zetasql::InputArgumentType>&
          postgres_input_argument_types,
      const zetasql::Function* mapped_function,
      const zetasql::LanguageOptions& language_options);

  static absl::Status GetUnimplementedError(absl::string_view not_found_type) {
    return absl::UnimplementedError(
        absl::StrCat("Engine defined ", not_found_type,
                     " are not supported in this EngineSystemCatalog."));
  }

  std::string name_;
  // Stores the PostgreSQL types which are supported in this storage engine.
  // The key is the raw PostgreSQL type name (without the "pg." prefix).
  absl::flat_hash_map<std::string, const PostgresTypeMapping*> engine_types_;
  // Stores the reverse mapping for types.
  // The key is a type instead of a type name so that the lookup is not affected
  // by the product_mode, which generates a "DOUBLE" in internal mode and
  // "FLOAT64" in external mode.
  // For PostgreSQL types with a base implementation, the key is the Postgres
  // Extended Type and the value is the raw PostgreSQL type name.
  // For PostgreSQL types without a base implementation, the key is the mapped
  // type and the value is the raw PostgreSQL type name.
  absl::flat_hash_map<const zetasql::Type*, std::string, zetasql::TypeHash,
                      zetasql::TypeEquals>
      engine_types_reverse_map_;

  // Stores the PostgreSQL functions which are supported in this storage engine.
  // The key is the PostgreSQL function name.
  absl::flat_hash_map<std::string, std::unique_ptr<PostgresExtendedFunction>>
      engine_functions_;

  // Stores a pointer to builtin functions that are accessed internally by this
  // storage engine through a PostgreSQL Expr since PostgreSQL
  // does not have an equivalent function.
  // For example, CaseExpr -> $case, CoalesceExpr -> $coalesce.
  // The pointers are stored in this map to explicitly include them in the
  // catalog rather than exposing the entire builtin_function_catalog. They
  // are also stored here so that catalog creation can verify their existence.
  // The key is the expr identifier -- typically a NodeTag (T_CoalesceExpr) but
  // sometimes a NodeTag and an additional field (T_BoolExpr + BoolExprType).
  absl::flat_hash_map<PostgresExprIdentifier, const zetasql::Function*>
      pg_expr_to_builtin_function_;

  // Store a mapping from a PostgreSQL cast to a engine builtin function.
  // Should only be used when the default type cast should be overridden with
  // a builtin function.
  // The key is std::pair<source_type, target_type>.
  // Excludes PG Numeric Functions that are soon-to-be migrated to ResolvedCast
  // instead of ResolvedFunctionCall.
  absl::flat_hash_map<std::pair<const zetasql::Type*, const zetasql::Type*>,
                      FunctionAndSignature>
      pg_cast_to_builtin_function_;

  // Stores the reverse mapping for operator functions.
  // Only used by the reverse transformer.
  // Since multiple PG operators and functions can all map to the same ZetaSQL
  // function, the reverse transformer will use the first matching function that
  // it finds. In order to guarantee that operators are tested, this map stores
  // just the set of PG operators which match a mapped function.
  //
  // For example, dpow and pow both map to the ZetaSQL power function with
  // DOUBLE inputs and outputs, but only dpow is used by the ^ operator.
  // dpow will appear in engine_function_operators_reverse_map_ and pow will
  // appear in engine_function_non_operators_reverse_map_.
  //
  // For PostgreSQL functions with a base implementation, the key is the
  // PostgreSQL function name. For PostgreSQL functions without a base
  // implementation, the key is the mapped builtin function name.
  absl::flat_hash_map<std::string, std::vector<Oid>>
      engine_function_operators_reverse_map_;
  // Stores the reverse mapping for non-operator functions.
  // Only used by the reverse transformer.
  // The same as engine_operators_reverse_map_, except that the values are
  // the Oids of functions which are not operators.
  absl::flat_hash_map<std::string, std::vector<Oid>>
      engine_function_non_operators_reverse_map_;
  // Stores the reverse mapping for expressions.
  // Only used by the reverse transformer.
  // The key is the builtin function name and value is the
  // PostgresExprIdentifier.
  absl::flat_hash_map<std::string, PostgresExprIdentifier>
      engine_function_expr_reverse_map_;
  // Lists builtin functions for the storage engine that correspond to
  // PostgreSQL cast functions. While PostgreSQL casts are usually transformed
  // to ZetaSQL ResolvedCasts, engines can choose to have casts to/from
  // engine-defined types transformed to ResolvedFunctionCalls instead.
  // Only used by the reverse transformer.
  absl::flat_hash_set<std::string> engine_cast_functions_;

  // Stores the set of builtin functions for this storage engine.
  // A subset of these functions will be exposed through the PostgreSQL
  // interface by being mapped to a PostgresExtendedFunction.
  std::unique_ptr<EngineBuiltinFunctionCatalog> builtin_function_catalog_;
};

}  // namespace postgres_translator

#endif  // CATALOG_ENGINE_SYSTEM_CATALOG_H_
