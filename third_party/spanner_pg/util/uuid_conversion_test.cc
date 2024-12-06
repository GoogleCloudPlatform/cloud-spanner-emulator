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

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

using ValidMemoryContextUuidConversionTest =
    ::postgres_translator::test::ValidMemoryContext;

TEST_F(ValidMemoryContextUuidConversionTest,
       UuidStringToPgConstAndBackToUuidString) {
  std::string uuid_string = "9a31411b-caca-4ff1-86e9-39fbd2bc3f39";
  ZETASQL_ASSERT_OK_AND_ASSIGN(Const * pg_const, UuidStringToPgConst(uuid_string));
  EXPECT_NE(pg_const, nullptr);
  pg_uuid_t* pg_uuid = DatumGetUUIDP(pg_const->constvalue);

  std::string uuid_bytes =
      "\x9a\x31\x41\x1b\xca\xca\x4f\xf1\x86\xe9\x39\xfb\xd2\xbc\x3f\x39";

  for (int i = 0; i < uuid_bytes.size(); i++) {
    EXPECT_EQ(pg_uuid->data[i], static_cast<unsigned char>(uuid_bytes[i]));
  }

  ZETASQL_ASSERT_OK_AND_ASSIGN(absl::string_view uuid_string_from_conversion,
                       PgConstToUuidString(pg_const));
  EXPECT_EQ(uuid_string_from_conversion, uuid_string);
}
