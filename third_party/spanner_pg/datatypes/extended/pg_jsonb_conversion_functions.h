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

#ifndef DATATYPES_EXTENDED_PG_JSONB_CONVERSION_FUNCTIONS_H_
#define DATATYPES_EXTENDED_PG_JSONB_CONVERSION_FUNCTIONS_H_

#include "zetasql/public/function.h"

namespace postgres_translator::spangres {
namespace datatypes {

// PG.JSONB conversion functions
absl::StatusOr<zetasql::Value> PgJsonbToBoolConversion(
    const absl::Span<const zetasql::Value> args);
absl::StatusOr<zetasql::Value> PgJsonbToInt64Conversion(
    const absl::Span<const zetasql::Value> args);
absl::StatusOr<zetasql::Value> PgJsonbToDoubleConversion(
    const absl::Span<const zetasql::Value> args);
absl::StatusOr<zetasql::Value> PgJsonbToPgNumericConversion(
    const absl::Span<const zetasql::Value> args);
absl::StatusOr<zetasql::Value> PgJsonbToStringConversion(
    const absl::Span<const zetasql::Value> args);

const zetasql::Function* GetStringToPgJsonbConversion();
const zetasql::Function* GetPgJsonbToBoolConversion();
const zetasql::Function* GetPgJsonbToInt64Conversion();
const zetasql::Function* GetPgJsonbToDoubleConversion();
const zetasql::Function* GetPgJsonbToPgNumericConversion();
const zetasql::Function* GetPgJsonbToStringConversion();
}  // namespace datatypes
}  // namespace postgres_translator::spangres

#endif  // DATATYPES_EXTENDED_PG_JSONB_CONVERSION_FUNCTIONS_H_
