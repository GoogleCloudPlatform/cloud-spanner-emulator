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

#ifndef SHIMS_EREPORT_SHIM_H_
#define SHIMS_EREPORT_SHIM_H_

#include "third_party/spanner_pg/postgres_includes/all.h"

// This file is included by both Google C++ code and PostgreSQL C code.
// Hide C++-specific declarations from C code.
#ifdef __cplusplus

#include <optional>
#include <string>

#include "zetasql/base/die_if_null.h"
#include "absl/status/status.h"
#include "absl/strings/string_view.h"

namespace postgres_translator {

// Encapsulates a PostgreSQL ereport() error.
// Captures any fields that we need.
//
// THIS CLASS IS INTENDED TO BE THROWN AS AN EXCEPTION!
// Inherits from std::exception to indicate that it is an exception type.
class PostgresEreportException : public std::exception {
 public:
  PostgresEreportException(
      const ErrorData* error_data,
      std::optional<absl::Status> error_status = std::nullopt) noexcept
      : error_data_(*ZETASQL_DIE_IF_NULL(error_data)),
        error_status_(std::move(error_status)) {
    CopyErrorDataToHeap();
    GenerateWhatMessage();
  }

  const ErrorData& error_data() const noexcept { return error_data_; }
  const char* what() const noexcept override { return what_.c_str(); }
  const std::optional<absl::Status>& error_status() const noexcept {
    return error_status_;
  }

 private:
  // Data that PostgreSQL emits when an error occurs.
  // Fields are often unused, in which case they will be set to nullptr, 0,
  // or some other type-appropriate value implying that the field is un-set.
  ErrorData error_data_ = {};

  // We may be passed an `ErrorData` struct that contains C strings that may or
  // may not be allocated on a heap that is as long-lived as this exception.
  // Copy those strings into an ErrorDataStrings structure.
  // A null string on ErrorData is represented here by the empty string.
  struct ErrorDataStrings {
    std::string filename;
    std::string funcname;
    std::string domain;
    std::string context_domain;
    std::string message;
    std::string detail;
    std::string detail_log;
    std::string hint;
    std::string context;
    std::string message_id;
    std::string schema_name;
    std::string table_name;
    std::string column_name;
    std::string datatype_name;
    std::string constraint_name;
    std::string internalquery;
  } error_data_strings_;

  // This is for the generated code from the error catalog. It will be returned
  // when converting from an exception to an absl::Status. When an exception is
  // caught, we will check if `error_status_` is empty. If it is empty, it means
  // that the exception comes from the original PG code. When it is not empty,
  // the exception is thrown by our generated functions. We can extract the
  // status from the exception and return it.
  const std::optional<absl::Status> error_status_;

  // Generated error string returned by `what()`
  std::string what_;

  // If error_data_ points to any strings,
  // copy the strings to error_data_strings_ and update error_data_ to point
  // to the copy stored there.
  void CopyErrorDataToHeap();

  // Generate the string that's returned by `what()`
  void GenerateWhatMessage();
};

}  // namespace postgres_translator

extern "C" {
#endif  // __cplusplus

// START C FORWARD-DECLARES

// Reports a PostgreSQL error.
//
// @throws PostgresEreportException
// THIS FUNCTION ALWAYS THROWS AN EXCEPTION!  AVOID USING IT WHENEVER POSSIBLE!
// The caller, or one of its parents, must use try/catch to catch this
// exception.
//
// PostgreSQL's original ereport() functionality is a reimplementation of
// the "exceptions" concept in C, for old compilers that don't have exceptions.
// PostgreSQL's implementation is not thread-safe (because PostgreSQL doesn't
// use threads) and does not correctly call destructors of C++ objects on the
// stack (because PostgreSQL doesn't use C++).
// While exceptions are a "code smell", PostgreSQL ereports are strictly more
// dangerous to use.
//
// This function is used internally as part of a mechanism to override
// PostgreSQL's exceptions mechanism with one that uses regular C++ exceptions,
// in places where rewriting existing PostgreSQL code would require a
// large refactor so would not be feasible.  It should ONLY be used as
// part of the (re-)implementation of PostgreSQL functions such as ereport()
// or elog().
//
// If you find yourself trying to use this function, first ask yourself:
// - Are you wrapping PostgreSQL code?  (If not, don't use this method.)
// - Could you use an absl::Status instead?
// - Could you use a call to PostgreSQL's ereport() or elog() instead?
// - Could you convince a readability reviewer that you need exceptions here?
//
// The 'throw(PostgresEreportException)' attribute is commented out because
// that attribute is not supported by our C compiler.  It would be a correct and
// useful annotation, so leaving it as a comment to catch readers' attention.
void spangres_throw_exception(ErrorData* error_data);
/* throw(PostgresEreportException) */

// Logs a PostgreSQL error.
//
// This is intended to be used instead of throwing the exception, similar to
// PostgreSQL's behavior.
void spangres_log_exception(ErrorData* error_data);

// END C FORWARD-DECLARES

#ifdef __cplusplus
}  // namespace postgres_translator
#endif

#endif  // SHIMS_EREPORT_SHIM_H_
