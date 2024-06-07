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

#include "third_party/spanner_pg/ddl/translation_utils.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator::spangres {
namespace {

using ::zetasql_base::testing::IsOkAndHolds;
using ::zetasql_base::testing::StatusIs;

TEST(TranslationUtilsTest, ObjectTypeToString) {
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TABLE),
              IsOkAndHolds("TABLE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_ACCESS_METHOD),
              IsOkAndHolds("ACCESS METHOD"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_AGGREGATE),
              IsOkAndHolds("AGGREGATE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_AMOP), IsOkAndHolds("AMOP"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_AMPROC),
              IsOkAndHolds("AMPROC"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_ATTRIBUTE),
              IsOkAndHolds("ATTRIBUTE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_CAST), IsOkAndHolds("CAST"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_CHANGE_STREAM),
              IsOkAndHolds("CHANGE STREAM"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_COLUMN),
              IsOkAndHolds("COLUMN"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_COLLATION),
              IsOkAndHolds("COLLATION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_CONVERSION),
              IsOkAndHolds("CONVERSION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_DATABASE),
              IsOkAndHolds("DATABASE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_DEFAULT),
              IsOkAndHolds("DEFAULT"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_DEFACL),
              IsOkAndHolds("DEFACL"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_DOMAIN),
              IsOkAndHolds("DOMAIN"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_DOMCONSTRAINT),
              IsOkAndHolds("DOMAIN CONSTRAINT"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_EVENT_TRIGGER),
              IsOkAndHolds("EVENT TRIGGER"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_EXTENSION),
              IsOkAndHolds("EXTENSION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_FDW),
              IsOkAndHolds("FOREIGN DATA WRAPPER"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_FOREIGN_SERVER),
              IsOkAndHolds("FOREIGN SERVER"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_FOREIGN_TABLE),
              IsOkAndHolds("FOREIGN TABLE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_FUNCTION),
              IsOkAndHolds("FUNCTION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_INDEX),
              IsOkAndHolds("INDEX"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_LANGUAGE),
              IsOkAndHolds("LANGUAGE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_LARGEOBJECT),
              IsOkAndHolds("LARGE OBJECT"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_MATVIEW),
              IsOkAndHolds("MATERIALIZED VIEW"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_OPCLASS),
              IsOkAndHolds("OPERATOR CLASS"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_OPERATOR),
              IsOkAndHolds("OPERATOR"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_OPFAMILY),
              IsOkAndHolds("OPERATOR FAMILY"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_POLICY),
              IsOkAndHolds("POLICY"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_PROCEDURE),
              IsOkAndHolds("PROCEDURE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_PUBLICATION),
              IsOkAndHolds("PUBLICATION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_PUBLICATION_REL),
              IsOkAndHolds("PUBLICATION REL"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_ROLE), IsOkAndHolds("ROLE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_ROUTINE),
              IsOkAndHolds("ROUTINE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_RULE), IsOkAndHolds("RULE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_SCHEMA),
              IsOkAndHolds("SCHEMA"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_SEQUENCE),
              IsOkAndHolds("SEQUENCE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_SUBSCRIPTION),
              IsOkAndHolds("SUBSCRIPTION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_STATISTIC_EXT),
              IsOkAndHolds("STATISTICS"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TABCONSTRAINT),
              IsOkAndHolds("TABLE CONSTRAINT"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TABLESPACE),
              IsOkAndHolds("TABLESPACE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TRANSFORM),
              IsOkAndHolds("TRANSFORM"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TRIGGER),
              IsOkAndHolds("TRIGGER"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TSCONFIGURATION),
              IsOkAndHolds("TEXT SEARCH CONFIGURATION"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TSDICTIONARY),
              IsOkAndHolds("TEXT SEARCH DICTIONARY"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TSPARSER),
              IsOkAndHolds("TEXT SEARCH PARSER"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TSTEMPLATE),
              IsOkAndHolds("TEXT SEARCH TEMPLATE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_TYPE), IsOkAndHolds("TYPE"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_USER_MAPPING),
              IsOkAndHolds("USER MAPPING"));
  EXPECT_THAT(internal::ObjectTypeToString(OBJECT_VIEW), IsOkAndHolds("VIEW"));

  // Construct an invalid (but memory-legal) ObjectType to test the negative
  // case. Since ObjectType is an enum whose underlying type is not fixed (it's
  // defined in C after all) and since there are 51 legal values, it is at
  // least represented by an 8-bit unsigned integer. So we can safely pick the
  // largest representable value in that range, 2^6-1 = 63.
  EXPECT_THAT(
      internal::ObjectTypeToString(static_cast<ObjectType>(63)),
      StatusIs(absl::StatusCode::kFailedPrecondition, "Unknown object type"));
}

}  // namespace
}  // namespace postgres_translator::spangres
