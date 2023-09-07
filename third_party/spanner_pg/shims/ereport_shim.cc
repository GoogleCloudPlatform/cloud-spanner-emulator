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

#include "third_party/spanner_pg/shims/ereport_shim.h"

#include <sstream>

#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

namespace postgres_translator {

namespace {
// A null pointer is not a valid C string according to std::string's
// constructor.  In that case, return the empty string.
const char* NullAsEmpty(const char* str) { return str ? str : ""; }

// Given an `elevel` error level value,
// return a string that canonincally represents that error level value.
std::string ElevelToString(int elevel) {
  // `elevel` values are generally supposed to be instances of `PgErrorLevel`.
  // (More specifically, `PgErrorLevel` is supposed to encapsulate all common
  // values and all values with special semantic meaning for `elevel`.)
  switch ((PgErrorLevel)elevel) {
    case PG_DEBUG5:
      return "DEBUG5";
    case PG_DEBUG4:
      return "DEBUG4";
    case PG_DEBUG3:
      return "DEBUG3";
    case PG_DEBUG2:
      return "DEBUG2";
    case PG_DEBUG1:
      return "DEBUG1";
    case PG_LOG:
      return "LOG";
    case PG_LOG_SERVER_ONLY:
      return "LOG_SERVER_ONLY";
    case PG_INFO:
      return "INFO";
    case PG_NOTICE:
      return "NOTICE";
    case PG_WARNING:
      return "WARNING";
    case PG_ERROR:
      return "ERROR";
    case PG_FATAL:
      return "FATAL";
    case PG_PANIC:
      return "PANIC";
  }

  // Sometimes `elevel` values are not instances of `PgErrorLevel`.
  // In that case, we don't know what the number means,
  // so just return it as an identifier of sorts.
  return absl::StrCat("(", elevel, ")");
}
}  // namespace

void PostgresEreportException::CopyErrorDataToHeap() {
  if (error_data_.filename) {
    error_data_strings_.filename = NullAsEmpty(error_data_.filename);
    error_data_.filename = error_data_strings_.filename.data();
  }
  if (error_data_.funcname) {
    error_data_strings_.funcname = NullAsEmpty(error_data_.funcname);
    error_data_.funcname = error_data_strings_.funcname.data();
  }
  if (error_data_.domain) {
    error_data_strings_.domain = NullAsEmpty(error_data_.domain);
    error_data_.domain = error_data_strings_.domain.data();
  }
  if (error_data_.context_domain) {
    error_data_strings_.context_domain =
        NullAsEmpty(error_data_.context_domain);
    error_data_.context_domain = error_data_strings_.context_domain.data();
  }
  if (error_data_.message) {
    error_data_strings_.message = NullAsEmpty(error_data_.message);
    error_data_.message = error_data_strings_.message.data();
  }
  if (error_data_.detail) {
    error_data_strings_.detail = NullAsEmpty(error_data_.detail);
    error_data_.detail = error_data_strings_.detail.data();
  }
  if (error_data_.detail_log) {
    error_data_strings_.detail_log = NullAsEmpty(error_data_.detail_log);
    error_data_.detail_log = error_data_strings_.detail_log.data();
  }
  if (error_data_.hint) {
    error_data_strings_.hint = NullAsEmpty(error_data_.hint);
    error_data_.hint = error_data_strings_.hint.data();
  }
  if (error_data_.context) {
    error_data_strings_.context = NullAsEmpty(error_data_.context);
    error_data_.context = error_data_strings_.context.data();
  }
  if (error_data_.message_id) {
    error_data_strings_.message_id = NullAsEmpty(error_data_.message_id);
    error_data_.message_id = error_data_strings_.message_id.data();
  }
  if (error_data_.schema_name) {
    error_data_strings_.schema_name = NullAsEmpty(error_data_.schema_name);
    error_data_.schema_name = error_data_strings_.schema_name.data();
  }
  if (error_data_.table_name) {
    error_data_strings_.table_name = NullAsEmpty(error_data_.table_name);
    error_data_.table_name = error_data_strings_.table_name.data();
  }
  if (error_data_.column_name) {
    error_data_strings_.column_name = NullAsEmpty(error_data_.column_name);
    error_data_.column_name = error_data_strings_.column_name.data();
  }
  if (error_data_.datatype_name) {
    error_data_strings_.datatype_name = NullAsEmpty(error_data_.datatype_name);
    error_data_.datatype_name = error_data_strings_.datatype_name.data();
  }
  if (error_data_.constraint_name) {
    error_data_strings_.constraint_name =
        NullAsEmpty(error_data_.constraint_name);
    error_data_.constraint_name = error_data_strings_.constraint_name.data();
  }
  if (error_data_.internalquery) {
    error_data_strings_.internalquery = NullAsEmpty(error_data_.internalquery);
    error_data_.internalquery = error_data_strings_.internalquery.data();
  }

  // All strings on error_data_ are now allocated on the heap.
  // Therefore, none of them are stored in any context domain.
  error_data_.assoc_context = nullptr;
}

void PostgresEreportException::GenerateWhatMessage() {
  std::stringstream ss;

  ss << "[" << ElevelToString(error_data_.elevel) << "] ";

  ss << error_data_strings_.message;

  if (!error_data_strings_.detail.empty()) {
    ss << std::endl << "Detail: " << error_data_strings_.detail;
  }

  if (!error_data_strings_.detail_log.empty()) {
    ss << std::endl << "Detail Log: " << error_data_strings_.detail_log;
  }

  if (!error_data_strings_.hint.empty()) {
    ss << std::endl << "Hint: " << error_data_strings_.hint;
  }

  if (!error_data_strings_.context.empty()) {
    ss << std::endl << "Context Message: " << error_data_strings_.context;
  }

  what_ = ss.str();
}

}  // namespace postgres_translator

extern "C" void spangres_throw_exception(ErrorData* error_data) {
  throw postgres_translator::PostgresEreportException(error_data);
}

extern "C" void spangres_log_exception(ErrorData* error_data) {
  // Generate an exception to match the string processing used in the throw
  // version, but we'll just log and then delete the exception.
  postgres_translator::PostgresEreportException exception(error_data);
  if (error_data->elevel == PG_WARNING) {
    LOG(WARNING) << exception.what();  // DO_NOT_TRANSFORM
  } else {
    LOG(INFO) << exception.what();  // DO_NOT_TRANSFORM
  }
}
