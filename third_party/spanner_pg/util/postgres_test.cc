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

#include "third_party/spanner_pg/util/postgres.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_namespace_d.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_type_d.h"

namespace postgres_translator::internal {
namespace {

TEST(PostgresUtilsTest, ArrayUnnestProcOid) {
  // Verify that the returned OID maps to the expected proc.
  ZETASQL_ASSERT_OK_AND_ASSIGN(Oid array_unnest_oid, GetArrayUnnestProcOid());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      const FormData_pg_proc* proc,
      PgBootstrapCatalog::Default()->GetProc(array_unnest_oid));
  EXPECT_EQ(proc->pronamespace, PG_CATALOG_NAMESPACE);
  EXPECT_STREQ(NameStr(proc->proname), "unnest");
  EXPECT_EQ(proc->proargtypes.values[0], ANYARRAYOID);
  EXPECT_EQ(proc->proargtypes.dim1, 1);  // Single arg.
  EXPECT_EQ(proc->prorettype, ANYELEMENTOID);
}

}  // namespace
}  // namespace postgres_translator::internal
