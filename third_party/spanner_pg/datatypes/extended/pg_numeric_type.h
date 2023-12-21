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

#ifndef SPANGRES_DATATYPES_EXTENDED_PG_NUMERIC_TYPE_H_
#define SPANGRES_DATATYPES_EXTENDED_PG_NUMERIC_TYPE_H_

#include <cstdint>
#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "absl/flags/declare.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace postgres_translator::spangres::datatypes {

// Returns global static instance of PG.NUMERIC type.
const SpannerExtendedType* GetPgNumericType();

// Returns a global static instance of an ARRAY<PG.NUMERIC> type.
const zetasql::ArrayType* GetPgNumericArrayType();

// Create a zetasql::Value from the input ASCII decimal format or an ASCII
// scientific notation format `readable_numeric`.
absl::StatusOr<zetasql::Value> CreatePgNumericValue(
    absl::string_view readable_numeric);

// Create PG.NUMERIC value in a valid memory context which is required for
// calling PG code. This function is intended to be used in tests where a memory
// context is sometimes not initialized.
absl::StatusOr<zetasql::Value> CreatePgNumericValueWithMemoryContext(
    absl::string_view numeric_string);

// Retrieves an `absl::Cord` normalized representation of PG.NUMERIC value from
// the ZetaSQL `value`. Returns error if `value` doesn't contain non-NULL
// value of PG.NUMERIC.
absl::StatusOr<absl::Cord> GetPgNumericNormalizedValue(
    const zetasql::Value& value);

// Create a zetasql::Value from the input ASCII decimal format or an ASCII
// scientific notation format `readable_numeric` and precision and scale.
absl::StatusOr<zetasql::Value> CreatePgNumericValueWithPrecisionAndScale(
    absl::string_view readable_numeric, int64_t precision, int64_t scale = 0);
}  // namespace postgres_translator::spangres::datatypes
#endif  // SPANGRES_DATATYPES_EXTENDED_PG_NUMERIC_TYPE_H_
