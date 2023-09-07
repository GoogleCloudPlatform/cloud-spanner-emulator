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

#ifndef DDL_SPANGRES_DIRECT_SCHEMA_PRINTER_IMPL_H_
#define DDL_SPANGRES_DIRECT_SCHEMA_PRINTER_IMPL_H_

#include <memory>
#include <stdint.h>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"

namespace postgres_translator {
namespace spangres {

// Returns the implementation of the SDL->PG DDL schema printer.
// TODO: this was hidden to avoid linking PG libraries. Given that it's
// no longer the case, we should refactor schema printing and pull out hidded
// definitions.
absl::StatusOr<std::unique_ptr<SpangresSchemaPrinter>>
CreateSpangresDirectSchemaPrinter();

// Turns a row deletion policy into a PG INTERVAL
// string. We can take either a regular RowDeletionPolicy proto or a decomposed
// version of it as might be lurking in other spots in spanner.
std::string RowDeletionPolicyToInterval(int64_t secs, absl::string_view column);

std::string RowDeletionPolicyToInterval(
    const google::spanner::emulator::backend::ddl::RowDeletionPolicy& policy);

}  // namespace spangres
}  // namespace postgres_translator

#endif  // DDL_SPANGRES_DIRECT_SCHEMA_PRINTER_IMPL_H_
