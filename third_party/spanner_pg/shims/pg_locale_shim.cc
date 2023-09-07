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

// Usually this should be a C file, but we are experimenting with shimming using
// a CPP file. This is a CPP file, because we are using thread-safe static
// initialization of the localized days, months, monetary format and number
// format. This would only be possible in C if we used a mutex or thread locals
// to initialize it.
#include "third_party/spanner_pg/shims/pg_locale_shim.h"

#include <stddef.h>
#include <string.h>
#include <time.h>

#include <memory>

// Cloud Spanner does not allow for dynamic locales so it is hardcoded to the
// one set below
inline constexpr char kDefaultLocale[] = "en_US.UTF-8";
// Max size of localized time day / month
inline constexpr size_t kMaxL10NSize = 80;

// Localized days and months cache. The last array element is left as NULL for
// the convenience of outside code that wants to sequentially scan these arrays.
char* localized_abbrev_days[7 + 1];
char* localized_full_days[7 + 1];
char* localized_abbrev_months[12 + 1];
char* localized_full_months[12 + 1];

struct lconv* PGLC_localeconv(void) {
  static lconv* locale = [] {
    struct lconv* result_locale = new struct lconv;
    char* old_locale;

    // Numeric locale
    old_locale = strdup(setlocale(LC_NUMERIC, NULL));
    setlocale(LC_NUMERIC, kDefaultLocale);
    struct lconv* numeric = localeconv();
    result_locale->decimal_point = strdup(numeric->decimal_point);
    result_locale->thousands_sep = strdup(numeric->thousands_sep);
    result_locale->grouping = strdup(numeric->grouping);
    setlocale(LC_NUMERIC, old_locale);
    free(old_locale);

    // Monetary locale
    old_locale = strdup(setlocale(LC_MONETARY, NULL));
    setlocale(LC_MONETARY, kDefaultLocale);
    struct lconv* monetary = localeconv();
    result_locale->int_curr_symbol = strdup(monetary->int_curr_symbol);
    result_locale->currency_symbol = strdup(monetary->currency_symbol);
    result_locale->mon_decimal_point = strdup(monetary->mon_decimal_point);
    result_locale->mon_thousands_sep = strdup(monetary->mon_thousands_sep);
    result_locale->mon_grouping = strdup(monetary->mon_grouping);
    result_locale->positive_sign = strdup(monetary->positive_sign);
    result_locale->negative_sign = strdup(monetary->negative_sign);
    result_locale->int_frac_digits = monetary->int_frac_digits;
    result_locale->frac_digits = monetary->frac_digits;
    result_locale->p_cs_precedes = monetary->p_cs_precedes;
    result_locale->n_cs_precedes = monetary->n_cs_precedes;
    result_locale->p_sep_by_space = monetary->p_sep_by_space;
    result_locale->n_sep_by_space = monetary->n_sep_by_space;
    result_locale->p_sign_posn = monetary->p_sign_posn;
    result_locale->n_sign_posn = monetary->n_sign_posn;
    setlocale(LC_MONETARY, old_locale);
    free(old_locale);

    return result_locale;
  }();

  return locale;
}

void cache_locale_time() {
  [[maybe_unused]] static bool unused_result = [] {
    char* old_locale = strdup(setlocale(LC_TIME, NULL));
    setlocale(LC_TIME, kDefaultLocale);

    // This is used to obtain a timeinfo struct which is used below
    time_t timenow = time(NULL);
    auto timeinfo = std::make_unique<struct tm>();
    localtime_r(&timenow, timeinfo.get());

    char buffer[kMaxL10NSize];
    // Iterates over week days to obtain abbreviated / full day names
    for (int i = 0; i < 7; ++i) {
      timeinfo->tm_wday = i;
      strftime(buffer, kMaxL10NSize, "%a", timeinfo.get());
      localized_abbrev_days[i] = strdup(buffer);
      strftime(buffer, kMaxL10NSize, "%A", timeinfo.get());
      localized_full_days[i] = strdup(buffer);
    }
    localized_abbrev_days[7] = NULL;
    localized_full_days[7] = NULL;

    for (int i = 0; i < 12; ++i) {
      timeinfo->tm_mon = i;
      timeinfo->tm_mday = 1;  // make sure we don't have invalid date
      strftime(buffer, kMaxL10NSize, "%b", timeinfo.get());
      localized_abbrev_months[i] = strdup(buffer);
      strftime(buffer, kMaxL10NSize, "%B", timeinfo.get());
      localized_full_months[i] = strdup(buffer);
    }
    localized_abbrev_months[12] = NULL;
    localized_full_months[12] = NULL;

    setlocale(LC_TIME, old_locale);
    free(old_locale);

    return true;
  }();
}
