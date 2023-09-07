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

#include <limits.h>

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/datetime_parsing/pgtime.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp.h"
#include "third_party/spanner_pg/datetime_parsing/timestamp_macros.h"

namespace postgres_translator::test {
namespace {

// This test ensures that sanitizers that run with the unit tests will not crash
// on integer overflow that could happen within tm2timestamp.
TEST(PGTimestampOverflowTest, MajorOverflow) {
  pg_tm test_pg_tm = {
      INT_MAX,   // tm_sec
      INT_MAX,   // tm_min
      INT_MAX,   // tm_hour
      INT_MAX,   // tm_mday
      INT_MAX,   // tm_mon
      INT_MAX,   // tm_year
      INT_MAX,   // tm_wday
      INT_MAX,   // tm_yday
      INT_MAX,   // tm_isdt
      LONG_MAX,  // tm_gmtoff
      "EST",     // tm_zone
  };

  fsec_t test_fsec = INT_MAX;
  int* test_tzp;
  int reference_tzp = INT_MAX;
  test_tzp = &reference_tzp;
  Timestamp* test_result;
  Timestamp reference_result = 0;
  test_result = &reference_result;
  EXPECT_EQ(-1, tm2timestamp(&test_pg_tm, test_fsec, test_tzp, test_result));
}

}  // namespace
}  // namespace postgres_translator::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
