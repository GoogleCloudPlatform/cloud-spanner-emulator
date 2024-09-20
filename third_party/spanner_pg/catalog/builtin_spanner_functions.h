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

#ifndef CATALOG_BUILTIN_SPANNER_FUNCTIONS_H_
#define CATALOG_BUILTIN_SPANNER_FUNCTIONS_H_

#include "third_party/spanner_pg/catalog/builtin_function.h"

namespace postgres_translator {
namespace spangres {

// In addition to adding any potential new functions for pg.numeric, existing
// functions are also augmented with appropriate signatures for pg.numeric.
void AddPgNumericFunctions(std::vector<PostgresFunctionArguments>& functions);
void AddPgJsonbFunctions(std::vector<PostgresFunctionArguments>& functions);

void AddFloatFunctions(std::vector<PostgresFunctionArguments>& functions);

void AddPgComparisonFunctions(
    std::vector<PostgresFunctionArguments>& functions);

void AddPgArrayFunctions(std::vector<PostgresFunctionArguments>& functions);

void AddPgDatetimeFunctions(std::vector<PostgresFunctionArguments>& functions);

void AddPgFormattingFunctions(
    std::vector<PostgresFunctionArguments>& functions);

void AddPgStringFunctions(std::vector<PostgresFunctionArguments>& functions);

void AddSpannerFunctions(std::vector<PostgresFunctionArguments>& functions);

void AddPgLeastGreatestFunctions(
    absl::flat_hash_map<PostgresExprIdentifier, std::string>& functions);

// Maps some Postgres' functions to custom Spanner functions for matching
// Postgres' order semantics (e.g. PG.MIN).
void RemapFunctionsForSpanner(
    std::vector<PostgresFunctionArguments>& functions);

}  // namespace spangres
}  // namespace postgres_translator

#endif  // CATALOG_BUILTIN_SPANNER_FUNCTIONS_H_
