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

#ifndef DATATYPES_EXTENDED_PG_OID_TYPE_H_
#define DATATYPES_EXTENDED_PG_OID_TYPE_H_

#include <cstdint>

#include "zetasql/public/types/type.h"
#include "zetasql/public/value.h"
#include "absl/flags/declare.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/datatypes/extended/spanner_extended_type.h"

namespace postgres_translator::spangres::datatypes {

// Returns global static instance of PG.OID type.
const SpannerExtendedType* GetPgOidType();

// Returns a global static instance of an ARRAY<PG.OID> type.
const zetasql::ArrayType* GetPgOidArrayType();

// Creates a zetasql::Value from an uint32_t.
absl::StatusOr<zetasql::Value> CreatePgOidValue(uint32_t oid);

// Creates a PG.OID zetasql::Value that is null.
zetasql::Value NullPgOid();

// Retrieves int64_t representation from the ZetaSQL `value`.
// Returns error if `value` doesn't contain non-NULL value of PG.OID.
absl::StatusOr<int64_t> GetPgOidValue(const zetasql::Value& value);

}  // namespace postgres_translator::spangres::datatypes

#endif  // DATATYPES_EXTENDED_PG_OID_TYPE_H_
