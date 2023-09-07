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

#include "third_party/spanner_pg/shims/pg_locale_shim.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace {

using ::testing::Eq;
using ::testing::IsNull;
using ::testing::StrEq;

TEST(PgLocaleShimTest, ReturnsEnUsLocaleMonetaryAndNumericSymbols) {
  struct lconv* locale = PGLC_localeconv();
  EXPECT_THAT(locale->decimal_point, StrEq("."));
  EXPECT_THAT(locale->thousands_sep, StrEq(","));
  EXPECT_THAT(locale->grouping, StrEq("\x3\x3"));
  EXPECT_THAT(locale->int_curr_symbol, StrEq("USD "));
  EXPECT_THAT(locale->currency_symbol, StrEq("$"));
  EXPECT_THAT(locale->mon_decimal_point, StrEq("."));
  EXPECT_THAT(locale->mon_thousands_sep, StrEq(","));
  EXPECT_THAT(locale->mon_grouping, StrEq("\x3\x3"));
  EXPECT_THAT(locale->positive_sign, StrEq(""));
  EXPECT_THAT(locale->negative_sign, StrEq("-"));
  EXPECT_THAT(locale->int_frac_digits, Eq(2));
  EXPECT_THAT(locale->frac_digits, Eq(2));
  EXPECT_THAT(locale->p_cs_precedes, Eq(1));
  EXPECT_THAT(locale->n_cs_precedes, Eq(1));
  EXPECT_THAT(locale->p_sep_by_space, Eq(0));
  EXPECT_THAT(locale->n_sep_by_space, Eq(0));
  EXPECT_THAT(locale->p_sign_posn, Eq(1));
  EXPECT_THAT(locale->n_sign_posn, Eq(1));
}

TEST(PgLocaleShimTest, RestunsEnUsLocaleTimeDays) {
  std::vector<std::string> expected_abbrev_days = {"Sun", "Mon", "Tue", "Wed",
                                                   "Thu", "Fri", "Sat"};
  std::vector<std::string> expected_full_days = {
      "Sunday",   "Monday", "Tuesday", "Wednesday",
      "Thursday", "Friday", "Saturday"};
  std::vector<std::string> expected_abbrev_months = {
      "Jan", "Feb", "Mar", "Apr", "May", "Jun",
      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  std::vector<std::string> expected_full_months = {
      "January", "February", "March",     "April",   "May",      "June",
      "July",    "August",   "September", "October", "November", "December"};

  cache_locale_time();

  for (int i = 0; i < 7; ++i) {
    EXPECT_THAT(localized_abbrev_days[i], StrEq(expected_abbrev_days[i]));
    EXPECT_THAT(localized_full_days[i], StrEq(expected_full_days[i]));
  }
  EXPECT_THAT(localized_abbrev_days[7], IsNull());
  EXPECT_THAT(localized_full_days[7], IsNull());

  for (int i = 0; i < 12; ++i) {
    EXPECT_THAT(localized_abbrev_months[i], StrEq(expected_abbrev_months[i]));
    EXPECT_THAT(localized_full_months[i], StrEq(expected_full_months[i]));
  }
  EXPECT_THAT(localized_abbrev_months[12], IsNull());
  EXPECT_THAT(localized_full_months[12], IsNull());
}
}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
