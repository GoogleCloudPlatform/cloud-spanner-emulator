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

#include "third_party/spanner_pg/util/interval_helpers.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

TEST(Intervals, Intervals) {
  // Make sure that we get errors for bad intervals, and no errors for good
  // ones.
  EXPECT_THAT(IntervalToSecs("1 day"), zetasql_base::testing::IsOk());
  EXPECT_THAT(IntervalToSecs("P1Y2M3DT4H5M6S"), zetasql_base::testing::IsOk());
  EXPECT_THAT(IntervalToSecs("P-1Y2M3DT4H5M6S"), zetasql_base::testing::IsOk());
  EXPECT_THAT(IntervalToSecs("greeble"), testing::Not(zetasql_base::testing::IsOk()));
  EXPECT_THAT(IntervalToSecs("Pasdf"), testing::Not(zetasql_base::testing::IsOk()));
  EXPECT_THAT(IntervalToSecs("November 17, 1858"),
              testing::Not(zetasql_base::testing::IsOk()));

  // Test that some conversions work
  EXPECT_EQ(86400, IntervalToSecs("1 day").value());
  EXPECT_EQ(86340, IntervalToSecs("1 day - 1 minute").value());
  EXPECT_EQ(5443215, IntervalToSecs("2 months 3 days 15 seconds").value());
  EXPECT_EQ(4838415,
            IntervalToSecs("2 months 3 days 15 seconds - 1 week").value());

  // The following three intervals are identical, but in different formats.
  EXPECT_EQ(36561906,
            IntervalToSecs("1 year 2 months 3 days 4 hours 5 minutes 6 seconds")
                .value());
  EXPECT_EQ(36561906, IntervalToSecs("P1Y2M3DT4H5M6S").value());
  EXPECT_EQ(36561906, IntervalToSecs("P0001-02-03T04:05:06").value());
}
