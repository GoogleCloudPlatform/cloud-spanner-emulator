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

#include "third_party/spanner_pg/util/oid_to_string.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/strings/match.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace {

TEST(OidStr, OidToStringBasic) {
  EXPECT_EQ(postgres_translator::OidToString(INT4OID), "INT4OID");
}

TEST(OidStr, OidToStringInvalid) {
  EXPECT_EQ(postgres_translator::OidToString(InvalidOid), "InvalidOid");
}

TEST(OidStr, OidToStringUnknown) {
  Oid unknown_oid = std::numeric_limits<Oid>::max();
  std::string unknown_str = absl::StrCat("<unknown:", unknown_oid, ">");
  EXPECT_EQ(postgres_translator::OidToString(unknown_oid), unknown_str);
}

TEST(OidStr, OidToStringAll) {
#define OID(x) EXPECT_EQ(postgres_translator::OidToString(x), #x);
#include "third_party/spanner_pg/util/oids.inc"
#undef OID
}

TEST(OidStr, OidToTypeStringBasic) {
  EXPECT_EQ(postgres_translator::OidToTypeString(INT4OID), "INT4");
}

TEST(OidStr, OidToTypeStringInvalid) {
  EXPECT_EQ(postgres_translator::OidToTypeString(InvalidOid), "Invalid");
}

TEST(OidStr, OidToTypeStringUnknown) {
  Oid unknown_oid = std::numeric_limits<Oid>::max();
  std::string unknown_str = absl::StrCat("<unknown:", unknown_oid, ">");
  EXPECT_EQ(postgres_translator::OidToTypeString(unknown_oid), unknown_str);
}

TEST(OidStr, OidToTypeStringAll) {
#define OID(x)                                                               \
  EXPECT_TRUE(absl::StartsWith(#x, postgres_translator::OidToTypeString(x))) \
      << "Oid " << x << " doesn't start with " << #x
#include "third_party/spanner_pg/util/oids.inc"
#undef OID
}

}  // namespace
