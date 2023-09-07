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

#include "third_party/spanner_pg/catalog/udf_support.h"

#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"

namespace postgres_translator {

absl::StatusOr<const FormData_pg_proc*> GetProcByOid(
    const CatalogAdapter* adapter, Oid oid) {
  // Check in both bootstrap_catalog (builtins) and the CatalogAdapter (UDFs).
  // It doesn't matter which we check first because their Oid ranges don't
  // overlap. Lookup is cheap, so no need to optimize by hard-coding those
  // ranges here.
  const auto bootstrap_proc =
      postgres_translator::PgBootstrapCatalog::Default()->GetProc(oid);
  // kNotFound is fine, we can check the other catalog, but we don't want to
  // swallow a more serious error.
  if (bootstrap_proc.ok()) {
    return bootstrap_proc.value();
  } else if (!absl::IsNotFound(bootstrap_proc.status())) {
    return bootstrap_proc.status();
  }

  // Either we found it (Ok), it wasn't found in either catalog (NotFound), or
  // something broke (other error code). In all cases, we just return it and let
  // the caller decide what to do.
  return adapter->GetUDFProcFromOid(oid);
}

}  // namespace postgres_translator
