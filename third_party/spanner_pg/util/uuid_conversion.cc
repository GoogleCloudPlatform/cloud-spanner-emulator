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

#include "third_party/spanner_pg/util/uuid_conversion.h"

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/status_macros.h"

absl::StatusOr<Const*> UuidStringToPgConst(absl::string_view uuid_string) {
  ZETASQL_ASSIGN_OR_RETURN(Datum uuid_datum,
                   postgres_translator::CheckedDirectFunctionCall1(
                       /*func=*/uuid_in,
                       /*arg1=*/CStringGetDatum(uuid_string.data())));

  return postgres_translator::CheckedPgMakeConst(
      /*consttype=*/UUIDOID,
      /*consttypmod=*/-1,
      /*constcollid=*/InvalidOid,
      /*constlen=*/sizeof(pg_uuid_t),
      /*constvalue=*/uuid_datum,
      /*constisnull=*/false,
      /*constbyval=*/false);
}

absl::StatusOr<absl::string_view> PgConstToUuidString(Const* pg_const) {
  ZETASQL_ASSIGN_OR_RETURN(Datum datum, postgres_translator::CheckedDirectFunctionCall1(
                                    /*func=*/uuid_out,
                                    /*arg1=*/pg_const->constvalue));

  return DatumGetCString(datum);
}
