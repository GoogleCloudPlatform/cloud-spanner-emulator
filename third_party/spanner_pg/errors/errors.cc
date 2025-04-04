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

#include "third_party/spanner_pg/errors/errors.h"

#include "zetasql/base/logging.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/errors/errors.pb.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace spangres {
namespace error {

absl::StatusCode CanonicalCode(int pg_error_code) {
  switch (ERRCODE_TO_CATEGORY(pg_error_code)) {
    case ERRCODE_TO_CATEGORY(ERRCODE_FEATURE_NOT_SUPPORTED):
      return absl::StatusCode::kUnimplemented;
    case ERRCODE_TO_CATEGORY(ERRCODE_INSUFFICIENT_RESOURCES):
      return absl::StatusCode::kResourceExhausted;
    case ERRCODE_TO_CATEGORY(ERRCODE_UNDEFINED_SCHEMA):
      return absl::StatusCode::kInvalidArgument;
    case ERRCODE_TO_CATEGORY(ERRCODE_SYNTAX_ERROR_OR_ACCESS_RULE_VIOLATION):
      if (pg_error_code == ERRCODE_INSUFFICIENT_PRIVILEGE) {
        return absl::StatusCode::kPermissionDenied;
      } else if (pg_error_code == ERRCODE_UNDEFINED_TABLE ||
                 pg_error_code == ERRCODE_UNDEFINED_OBJECT ||
                 pg_error_code == ERRCODE_UNDEFINED_FUNCTION) {
        return absl::StatusCode::kNotFound;
      } else {
        return absl::StatusCode::kInvalidArgument;
      }
    case ERRCODE_TO_CATEGORY(ERRCODE_INTERNAL_ERROR):
      if (pg_error_code == ERRCODE_INDEX_CORRUPTED ||
          pg_error_code == ERRCODE_DATA_CORRUPTED) {
        return absl::StatusCode::kDataLoss;
      } else {
        return absl::StatusCode::kInternal;
      }
    case ERRCODE_TO_CATEGORY(ERRCODE_DATA_EXCEPTION):
      if (pg_error_code == ERRCODE_DIVISION_BY_ZERO ||
          pg_error_code == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE) {
        return absl::StatusCode::kOutOfRange;
      }
      // Failed type conversions (i.e. failing to convert a string to another
      // data type) can report this. It's used elsewhere in the engine, but for
      // us I think it's just bad syntax in the analyzer. InvalidArgument seems
      // like the best option.
      return absl::StatusCode::kInvalidArgument;
    case ERRCODE_TO_CATEGORY(ERRCODE_PROGRAM_LIMIT_EXCEEDED): {
      if (pg_error_code == ERRCODE_STATEMENT_TOO_COMPLEX) {
        return absl::StatusCode::kResourceExhausted;
      } else {
        // Exceeded some sort of artificial internal limit.
        return absl::StatusCode::kOutOfRange;
      }
    }
    case 0:
      // TODO: kUnknown is confusing, specifically for the deparser
      // errors; we need to make it more user-friendly
      // The legacy elog mechanism doesn't use sql error codes, so we won't have
      // this information. This is expected.
      return absl::StatusCode::kUnknown;
    default:
      // If we encounter an error code not covered here in a test, let's add it.
      ABSL_LOG(FATAL) << "Error code translation not implemented for error code: "
                  << pg_error_code;
      return absl::StatusCode::kUnknown;
  }
}

}  // namespace error
}  // namespace spangres
