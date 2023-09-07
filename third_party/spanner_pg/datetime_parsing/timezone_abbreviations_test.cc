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

#include <cstring>

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/datetime_parsing/datetime_constants.h"

namespace postgres_translator::test {
namespace {

TEST(PGTimezoneAbbreviationsTest, AbbrevTypes) {
  // Only TZ is currently supported. DTZ and DYNTZ are not supported.
  // - TZ represents a timezone abbreviation with a fixed numerical offset.
  // - DTZ represents a timezone abbreviation with a numerical offset that is
  //   shifted by daylight savings.
  // - DYNTZ represents a timezone abbreviation that is mapped to a full
  //   timezone name.
  for (int i = 0; i < TimezoneAbbrevTableSize; ++i) {
    const datetkn& timezone_abbrev = TimezoneAbbrevTable[i];
    EXPECT_EQ(timezone_abbrev.type, TZ);
  }
}

TEST(PGTimezoneAbbreviationsTest, TimezoneAbbrevsSorted) {
  // The TimezoneAbbrevTable must be alphabetically ordered for the datebsearch
  // algorithm. Enforce the order with this unit test.
  const char* token = "";
  for (int i = 0; i < TimezoneAbbrevTableSize; ++i) {
    const datetkn& timezone_abbrev = TimezoneAbbrevTable[i];
    EXPECT_TRUE(strcmp(timezone_abbrev.token, token) > 0);
    token = timezone_abbrev.token;
  }
}

}  // namespace
}  // namespace postgres_translator::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
