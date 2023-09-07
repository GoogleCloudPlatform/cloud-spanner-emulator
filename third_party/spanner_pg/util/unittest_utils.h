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

#ifndef UTIL_UNITTEST_UTILS_H_
#define UTIL_UNITTEST_UTILS_H_

#include <string>

#include "zetasql/public/analyzer.h"
#include "zetasql/public/catalog.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator::spangres::test {

// Supported phases in the planning of a query.
// Intended to be used as a bitmask.
// The query will be run as far as the phase representing the greatest bit
// that is set.
// Each phase with a bit set will output its result.
enum PlanningPhase {
  kParse = (1 << 1),
  kAnalyze = (1 << 2),
  kTransform = (1 << 3),
  kRewrite = (1 << 4),
  kGsqlDeparse = (1 << 5),
  kReverseTransform = (1 << 6),
  kDeparse = (1 << 7),

  kAllPhases = kParse | kAnalyze | kTransform | kGsqlDeparse |
               kReverseTransform | kDeparse,
  kTransformRoundTrip = kTransform | kReverseTransform
};

// Print out a simple list of all bits set in our planning phase. Useful for
// test debugging output. If the bits of kAllPhases or kTransformRoundTrip are
// set, prints out that summary name isntead.
std::string PrintPhases(int phases);

// Parses PostgreSQL string, producing a PostgreSQL parse tree in case of
// success.
absl::StatusOr<List*> ParseFromPostgres(const std::string& sql);

// Analyzes a PostgreSQL parse tree, producing a PostgreSQL Query object in case
// of success.
//
// Before running the function, call sites are responsible for creating a
// valid thread-local `CatalogAdapter` (via `CatalogAdapterHolder`), and a valid
// thread-local `MemoryReservationManager` (via `MemoryReservationHolder`).
// The function returns an error status if either one of them is invalid.
absl::StatusOr<Query*> AnalyzeFromPostgresForTest(
    const std::string& sql, List* parse_tree,
    const zetasql::AnalyzerOptions& analyzer_options);

// Helper function to parse, analyze, and transform a single SQL statement,
// returning just the AnalyzerOutput. This is intended for testing transformer
// behaviors with different AnalyzerOptions.
absl::StatusOr<std::unique_ptr<const zetasql::AnalyzerOutput>>
ParseAnalyzeAndTransformStatement(const std::string& sql,
                                  const zetasql::AnalyzerOptions& options);

// Parses and analyzes a ZetaSQL string, producing a ZetaSQL resolved AST
// and put it into `gsql_output` in case of success.
// This function can be used in tests, it creates default `AnalyzerOptions` and
// `TypeFactory` objects.
absl::Status ParseAndAnalyzeFromZetaSQLForTest(
    const std::string& sql, zetasql::TypeFactory* type_factory,
    std::unique_ptr<const zetasql::AnalyzerOutput>* gsql_output);

// Like above but allows specifying non-default AnalyzerOptions.
absl::Status ParseAndAnalyzeFromZetaSQLForTest(
    const std::string& sql, zetasql::TypeFactory* type_factory,
    const zetasql::AnalyzerOptions& analyzer_options,
    std::unique_ptr<const zetasql::AnalyzerOutput>* gsql_output);

// Parse a single ZetaSQL/Spanner type name into the type pointer.
// Used by the golden tests, dump_query, and dump_builtin_function_data.
absl::StatusOr<const zetasql::Type*> ParseTypeName(const std::string& type);

// Parse a comma separated list of <parameter name>=<ZetaSQL/Spanner type>
// into a map of name to type. Used by the golden tests and dump_query.
absl::Status ParseParameters(
    const std::string& parameters,
    std::map<std::string, const zetasql::Type*>& result);

// Creates a PostgreSQL query tree from the input SQL string.
// As this function calls 'AnalyzeFromPostgresForTest' call sites are
// responsible for creating a valid thread-local 'CatalogAdapter' and a valid
// thread-local 'MemoryReservationManager' or else the function will return an
// error.
// This function allows for specifying non-default 'AnalyzerOptions'.
// The returned pointer is owned by CurrentMemoryContext.
absl::StatusOr<Query*> BuildPgQuery(
    const std::string& sql, const zetasql::AnalyzerOptions& analyzer_options);

// Creates a PostgreSQL query tree with the default test analyzer options.
// The same as above but passes in default analyzer options.
absl::StatusOr<Query*> BuildPgQuery(const std::string& sql);

// Runs the Spangres parser, analyzer, and forward transformer on a query and
// return the resolved AST.
// Call sites are responsible for creating a valid thread local
// 'MemoryReservationManager' or else an error will be returned. This function
// creates a local 'CatalogAdapter' already.
absl::StatusOr<std::unique_ptr<zetasql::ResolvedStatement>>
ForwardTransformQuery(const std::string& sql,
                      const zetasql::AnalyzerOptions& analyzer_options);

}  // namespace postgres_translator::spangres::test

#endif  // UTIL_UNITTEST_UTILS_H_
