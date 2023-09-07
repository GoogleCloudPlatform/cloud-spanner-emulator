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

#include <string>

#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "third_party/spanner_pg/catalog/builtin_function.h"

namespace postgres_translator {
void AddDatetimeExtractFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddDatetimeConversionFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_int64 = zetasql::types::Int64Type();
  const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
  const zetasql::Type* gsql_date = zetasql::types::DateType();
  functions.push_back(
      {"to_timestamp",
       "timestamp_seconds",
       {{{gsql_timestamp, {gsql_int64}, /*context_ptr=*/nullptr}}}});
  functions.push_back(
      {"make_date",
       "date",
       {{{gsql_date, {gsql_int64, gsql_int64, gsql_int64},
          /*context_ptr=*/nullptr}}}});
}

void AddTimeAndDatetimeConstructionAndConversionFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddDatetimeCurrentFunctions(
    std::vector<PostgresFunctionArguments>& functions) {
  const zetasql::Type* gsql_timestamp = zetasql::types::TimestampType();
  functions.push_back({"now",
                       "current_timestamp",
                       {{{gsql_timestamp, {}, /*context_ptr=*/nullptr}}}});
}

void AddDatetimeAddSubFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddDatetimeDiffTruncLastFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddDatetimeFormatFunctions(
    std::vector<PostgresFunctionArguments>& functions) {}

void AddDatetimeFunctions(std::vector<PostgresFunctionArguments>& functions) {}

void AddIntervalFunctions(std::vector<PostgresFunctionArguments>& functions) {}

}  // namespace postgres_translator
