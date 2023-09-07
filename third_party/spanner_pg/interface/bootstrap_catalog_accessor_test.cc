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

#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor.h"

#include "absl/status/status.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor_structs.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace postgres_translator {

class PgTypeDataTest : public testing::TestWithParam<PgTypeData> {};

TEST_P(PgTypeDataTest, GetPgTypeDataFromBootstrapSuccess) {
  PgTypeData expected = GetParam();
  EXPECT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(),
                                         expected.typname),
              zetasql_base::testing::IsOkAndHolds(expected));
}

INSTANTIATE_TEST_SUITE_P(
  PgTypeDataTestData,
  PgTypeDataTest,
  testing::Values(
    PgTypeData{
      .oid = 16,
      .typname = "bool",
      .typlen = 1,
      .typbyval = true,
      .typtype = 'b',  // base type
      .typcategory = 'B',  // boolean
      .typispreferred = true,
      .typisdefined = true,
      .typdelim = ',',
      .typelem = 0,
      .typarray = 1000,
    },
    PgTypeData{
      .oid = 1000,
      .typname = "_bool",
      .typlen = -1,
      .typbyval = false,
      .typtype = 'b',  // base type
      .typcategory = 'A',  // array
      .typispreferred = false,
      .typisdefined = true,
      .typdelim = ',',
      .typelem = 16,
      .typarray = 0,
    }
  ));

TEST_F(PgTypeDataTest, GetPgTypeDataFromBootstrapFailure) {
  // xid is an unsupported PG type.
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "xid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace postgres_translator
