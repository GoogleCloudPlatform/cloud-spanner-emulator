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

#ifndef CATALOG_BUILTIN_FUNCTION_H_
#define CATALOG_BUILTIN_FUNCTION_H_

#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/catalog/function.h"
#include "third_party/spanner_pg/catalog/function_identifier.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

// This file contains classes and methods to easily add builtin functions
// to an EngineSystemCatalog.
// PostgresFunctionSignature and PostgresFunctionSignatureArguments are used
// to define a function in the EngineSystemCatalog.
// AddX methods group the standard builtin functions in the same way as
// ZetaSQL (see storage/googlesql/common/builtin_function_internal.h)
// Each EngineSystemCatalog should use the AddX methods to populate its
// collection of supported functions, add any storage engine specific
// functions to the collection, and then call
// EngineSystemCatalog::AddFunction on each function in the collection.

namespace postgres_translator {
// Helper class used by AddFunction to create a
// PostgresExtendedFunctionSignature.
// If has_postgres_proc_oid is true, AddFunction will verify the existence of
// a PostgreSQL proc with an identical signature and use the proc Oid when
// creating the PostgresExtendedFunctionSignature.
// If has_mapped_function is true, AddFunction will verify the
// existence of an identical FunctionSignature in the mapped builtin function.
class PostgresFunctionSignatureArguments {
 public:
  // Only explicitly set postgres proc oid for signatures that cannot be
  // validated (variadic signatures).
  // Otherwise, allow the EngineSystemCatalog to validate that the provided
  // signature is compatible with native PostgreSQL and ZetaSQL before
  // identifying the proc oid.
  PostgresFunctionSignatureArguments(
      const zetasql::FunctionSignature& signature,
      bool has_mapped_function = true,
      const std::string& explicit_mapped_function_name = "",
      Oid postgres_proc_oid = InvalidOid)
      : signature_(signature),
        has_mapped_function_(has_mapped_function),
        explicit_mapped_function_name_(explicit_mapped_function_name),
        postgres_proc_oid_(postgres_proc_oid) {}

  const zetasql::FunctionSignature& signature() const { return signature_; }
  bool has_mapped_function() const { return has_mapped_function_; }
  const std::string& explicit_mapped_function_name() const {
    return explicit_mapped_function_name_;
  }
  // Returns the postgres proc oid for unvalidated signatures, or InvalidOid
  // for validated signatures.
  Oid postgres_proc_oid() const { return postgres_proc_oid_; }

 private:
  zetasql::FunctionSignature signature_;
  bool has_mapped_function_;
  // If the mapped function name for this signature is different from the
  // general mapped function name in PostgresFunctionArguments.
  std::string explicit_mapped_function_name_;
  Oid postgres_proc_oid_;
};

// Helper class used by AddFunction to create a PostgresExtendedFunction.
// If mapped_function_name is non-empty, AddFunction will verify the existence
// of a builtin function with that name and use the function when validating
// the signature arguments.
// If mapped_function_name is empty, this function has a base implementation
// in the storage engine and does not need to reuse a builtin function.
class PostgresFunctionArguments {
 public:
  // Most function mappings should use this constructor because the mapped
  // signatures will be validated when the EngineSystemCatalog is initialized.
  PostgresFunctionArguments(
      absl::string_view postgres_function_name,
      absl::string_view mapped_function_name,
      const std::vector<PostgresFunctionSignatureArguments>&
          signature_arguments,
      zetasql::Function::Mode mode = zetasql::Function::SCALAR,
      absl::string_view postgres_namespace = "pg_catalog")
      : postgres_function_name_(postgres_function_name),
        mapped_function_name_(mapped_function_name),
        signature_arguments_(signature_arguments),
        mode_(mode),
        postgres_namespace_(postgres_namespace) {}

  const std::string& postgres_function_name() const {
    return postgres_function_name_;
  }


  const std::string& mapped_function_name() const {
    return mapped_function_name_;
  }

  const std::vector<PostgresFunctionSignatureArguments>& signature_arguments()
      const {
    return signature_arguments_;
  }

  void add_signature(PostgresFunctionSignatureArguments signature) {
    signature_arguments_.push_back(signature);
  }

  zetasql::Function::Mode mode() const { return mode_; }

  const std::string& postgres_namespace() const { return postgres_namespace_; }

 private:
  std::string postgres_function_name_;
  std::string mapped_function_name_;
  std::vector<PostgresFunctionSignatureArguments> signature_arguments_;
  zetasql::Function::Mode mode_;
  std::string postgres_namespace_;
};

// Helper functions to add common PostgreSQL functions that are storage engine
// independent. There are many functions to add so they have been split into
// multiple builtin_<category>_functions.cc files. Each function below is
// annotated with its category to easily find the function definition.

// Time.
void AddDatetimeExtractFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddDatetimeConversionFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddTimeAndDatetimeConstructionAndConversionFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddDatetimeCurrentFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddDatetimeAddSubFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddDatetimeDiffTruncLastFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddDatetimeFormatFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddDatetimeFunctions(std::vector<PostgresFunctionArguments>& functions);

// Time.
void AddIntervalFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddArithmeticFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddBitwiseFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddAggregateFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddApproxFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddStatisticalFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddAnalyticFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddBooleanFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddLogicFunctions(std::vector<PostgresFunctionArguments>& functions);

// String.
void AddStringFunctions(std::vector<PostgresFunctionArguments>& functions);

// String.
void AddRegexFunctions(std::vector<PostgresFunctionArguments>& functions);

// String.
void AddProto3ConversionFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddMiscellaneousFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// String.
void AddJSONFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddNumericFunctions(std::vector<PostgresFunctionArguments>& functions);

// Math.
void AddTrigonometricFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddNetFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddHllCountFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddKllQuantilesFunctions(
    std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddHashingFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddEncryptionFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddGeographyFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddAnonFunctions(std::vector<PostgresFunctionArguments>& functions);

// Miscellaneous.
void AddSequenceFunctions(std::vector<PostgresFunctionArguments>& functions);

// String.
void AddContainsSubstrFunction(
    std::vector<PostgresFunctionArguments>& functions);

// Expr.
// Add mappings between PG Expr types that are uniquely identified by their
// NodeTag.
void AddExprFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mappings between PG BoolExprs that are identified by the T_BoolExpr
// NodeTag and the BoolExprType.
void AddBoolExprFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mappings between PG NullTest that are identified by the T_NullTest
// NodeTag and the NullTestType.
void AddNullTestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mappings between PG MinMaxExpr that are identified by the T_MinMaxExpr
// NodeTag and the MinMaxOp.
void AddLeastGreatestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mappings between PG BooleanTests that are identified by the T_BooleanTest
// NodeTag and the BooleanTestType.
void AddBooleanTestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mappings between PG SQLValueFunctions that are identified by the
// T_SQLValueFunction NodeTag and the SQLValueFunctionOp.
void AddSQLValueFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mapping between PG SubscriptingRef and safe_array_at_ordinal function
// call.
void AddArrayAtFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mapping between PG SubscriptingRef and pg.array_slice function call.
void AddArraySliceFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Expr.
// Add mapping between PG ArrayExpr and make_array function call.
// IMPORTANT NOTE: This mapping only applies when the array contains non-Const
// element expressions (matches ZetaSQL behavior). Arrays of all Const
// elements are transformed to ResolvedLiterals.
void AddMakeArrayFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

}  // namespace postgres_translator

#endif  // CATALOG_BUILTIN_FUNCTION_H_
