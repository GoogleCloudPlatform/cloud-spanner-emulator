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

#include "third_party/spanner_pg/shims/timezone_helper.h"

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/ret_check.h"

namespace postgres_translator {

constexpr inline absl::string_view gmt_timezone = "GMT";
constexpr inline absl::string_view utc_timezone = "UTC";

absl::Status InitTimezone(const char* time_zone_name) {
  ZETASQL_RET_CHECK_NE(CurrentMemoryContext, nullptr)
      << "Must set up memory contexts on this thread before initializing "
         "timezone data. See MemoryContextManager.";
  ZETASQL_RET_CHECK_EQ(session_timezone, nullptr);

  if (time_zone_name && *time_zone_name &&
      absl::string_view(time_zone_name) != gmt_timezone &&
      absl::string_view(time_zone_name) != utc_timezone) {
    ZETASQL_ASSIGN_OR_RETURN(session_timezone, CheckedPgTZSet(time_zone_name));
    log_timezone = session_timezone;
  } else {
    // Initializes session_timezone as "GMT".
    ZETASQL_RETURN_IF_ERROR(CheckedPgTimezoneInitialize());
  }
  ZETASQL_RET_CHECK_NE(session_timezone, nullptr);
  ZETASQL_RET_CHECK_NE(log_timezone, nullptr);
  return absl::OkStatus();
}

absl::Status InitTimezoneOffset(int32_t gmt_offset) {
  ZETASQL_RET_CHECK_NE(CurrentMemoryContext, nullptr)
      << "Must set up memory contexts on this thread before initializing "
         "timezone data. See MemoryContextManager.";
  ZETASQL_RET_CHECK_EQ(session_timezone, nullptr);

  ZETASQL_ASSIGN_OR_RETURN(session_timezone, CheckedPgTZOffsetSet(gmt_offset));
  log_timezone = session_timezone;

  ZETASQL_RET_CHECK_NE(session_timezone, nullptr);
  ZETASQL_RET_CHECK_NE(log_timezone, nullptr);
  return absl::OkStatus();
}

void CleanupTimezone() {
  session_timezone = nullptr;
  log_timezone = nullptr;
  ClearTimezoneHashtablePointer();
}

}  // namespace postgres_translator
