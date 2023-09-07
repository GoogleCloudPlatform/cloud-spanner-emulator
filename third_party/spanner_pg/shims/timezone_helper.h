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

#ifndef SHIMS_TIMEZONE_SHIM_H_
#define SHIMS_TIMEZONE_SHIM_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

// Helper functions to manage PostgreSQL's timezone data. These depend on
// `CurrentMemoryContext` for their backing memory. That memory is not released
// until the context is cleaned up.

// Sets up PostgreSQL's thread-local timezone data (`session_timezone`,
// `log_timezone`, and `timezone_cache`) on this thread.
absl::Status InitTimezone(const char* time_zone_name = nullptr);

// Sets up PostgreSQL's thread-local timezone data (`session_timezone`,
// `log_timezone`, and `timezone_cache`) on this thread. The offset should be
// given as the number of seconds from the GMT timezone.
// WARNING: PostgreSQL uses POSIX convention, NOT ISO. This means positive
// values are WEST of Greenwich.
absl::Status InitTimezoneOffset(int32_t gmt_offset);

// Nulls PostgreSQL's thread-local timezone data (`session_timezone`,
// `log_timezone`, and `timezone_cache`) on this thread.
void CleanupTimezone();

// This class sets the thread-local time zone during its creation and clean it
// on destruction.
class TimeZoneScope {
 public:
  static absl::StatusOr<TimeZoneScope> Create(
      const char* time_zone_name = nullptr) {
    auto init_status = InitTimezone(time_zone_name);
    if (!init_status.ok()) {
      // We might have done partial setup before we failed.
      CleanupTimezone();
      return init_status;
    }
    return TimeZoneScope();
  }

  static absl::StatusOr<TimeZoneScope> Create(absl::TimeZone time_zone) {
    return Create(time_zone.name().c_str());
  }

  // Moveable only.
  TimeZoneScope(const TimeZoneScope&) = delete;
  TimeZoneScope& operator=(const TimeZoneScope&) = delete;
  TimeZoneScope(TimeZoneScope&& other) {
    need_to_clean = other.need_to_clean;
    other.need_to_clean = false;
  }
  TimeZoneScope& operator=(TimeZoneScope&& other) {
    need_to_clean = other.need_to_clean;
    other.need_to_clean = false;
    return *this;
  }

  ~TimeZoneScope() {
    if (need_to_clean) {
      CleanupTimezone();
      need_to_clean = false;
    }
  }

 private:
  TimeZoneScope() : need_to_clean(true) {}

  bool need_to_clean = false;
};

}  // namespace postgres_translator

#endif  // SHIMS_TIMEZONE_SHIM_H_
