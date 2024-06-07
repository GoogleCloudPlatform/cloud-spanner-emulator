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

#include <cstdint>

#include "absl/flags/flag.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "zetasql/base/status_macros.h"

/*
 * Report an error detected by one of the datetime input processing routines.
 *
 * dterr is the error code, str is the original input string, datatype is
 * the name of the datatype we were trying to accept.
 *
 * Note: it might seem useless to distinguish DTERR_INTERVAL_OVERFLOW and
 * DTERR_TZDISP_OVERFLOW from DTERR_FIELD_OVERFLOW, but SQL99 mandates three
 * separate SQLSTATE codes, so ...
 * SPANGRES: a copy of DateTimeParseError that returns an absl::Status instead
 * of using ereport because DDL interval parsing does not have a memory context
 * that is required for ereport.
 */
static absl::Status DateTimeParseError(int dterr, absl::string_view str,
                                       const char *datatype) {
  switch (dterr) {
    case DTERR_FIELD_OVERFLOW:
    case DTERR_MD_FIELD_OVERFLOW:
      return absl::InvalidArgumentError(
          absl::StrFormat("date/time field value out of range: \"%s\"", str));
      break;
      // SPANGRES: remove the hint for datestyle settings
      // TODO: support datestyle settings
      // case DTERR_MD_FIELD_OVERFLOW:
      //        /* <nanny>same as above, but add hint about DateStyle</nanny> */
      //   return absl::InvalidArgumentError(
      //       absl::StrFormat("date/time field value out of range: \"%s\".
      //       Perhaps you need a different \"datestyle\" setting.",
      //                      str));
      //        break;
    case DTERR_INTERVAL_OVERFLOW:
      return absl::InvalidArgumentError(
          absl::StrFormat("interval field value out of range: \"%s\"", str));
      break;
    case DTERR_TZDISP_OVERFLOW:
      return absl::InvalidArgumentError(
          absl::StrFormat("time zone displacement out of range: \"%s\"", str));
      break;
    case DTERR_BAD_FORMAT:
    default:
      return absl::InvalidArgumentError(absl::StrFormat(
          "invalid input syntax for type %s: \"%s\"", datatype, str));
      break;
  }
}

/*
 * A copied version of PostgreSQL's interval_in with a few changes:
 *    - returns the number of seconds for the interval instead of an Interval*
 *    - does not handle a typmod
 *    - returns an absl::Status error for failures instead of using ereport
 * Note that ParseDateTime, DecodeInterval, DecodeISO8601Interval, and
 * tm2timestamp return an error code rather than calling ereport, so they
 * should be safe to call directly instead of through the error shim.
 */
absl::StatusOr<PGInterval> ParseInterval(absl::string_view input_string) {
  fsec_t fsec;
  struct pg_tm tt, *tm = &tt;
  int dtype;
  int nf;
  int range = INTERVAL_FULL_RANGE;
  int dterr;
  char *field[MAXDATEFIELDS];
  int ftype[MAXDATEFIELDS];
  char workbuf[256];

  dterr = ParseDateTime(input_string.data(), workbuf, sizeof(workbuf), field,
                        ftype, MAXDATEFIELDS, &nf);
  if (dterr == 0) {
    dterr = DecodeInterval(field, ftype, nf, range, &dtype, tm, &fsec);
  }

  /* if those functions think it's a bad format, try ISO8601 style */
  /*
   * SPANGRES: cast the const char* to char* because strtod requires a
   * non-const pointer. The string is not modified so this is safe.
   */
  char *non_const_input = const_cast<char *>(input_string.data());
  if (dterr == DTERR_BAD_FORMAT) {
    dterr = DecodeISO8601Interval(non_const_input, &dtype, tm, &fsec);
  }

  if (dterr != 0) {
    if (dterr == DTERR_FIELD_OVERFLOW) {
      dterr = DTERR_INTERVAL_OVERFLOW;
    }
    return DateTimeParseError(dterr, input_string, "interval");
  }

  Interval res;
  if (tm2interval(tm, 0, &res) != 0) {
    return absl::InvalidArgumentError(
        absl::StrFormat("invalid interval: \"%s\"", input_string));
  }

  return PGInterval{.months = res.month, .days = res.day, .micros = res.time};
}

absl::StatusOr<int64_t> IntervalToSecs(absl::string_view input_string) {
  ZETASQL_ASSIGN_OR_RETURN(PGInterval interval, ParseInterval(input_string));
  int64_t total;
  total = (((interval.months * 30) + interval.days) * 86400) +
          interval.micros / USECS_PER_SEC;
  return total;
}
