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

#ifndef DDL_SPANGRES_SCHEMA_PRINTER_H_
#define DDL_SPANGRES_SCHEMA_PRINTER_H_

#include <string>
#include <vector>

#include "absl/status/statusor.h"
#include "backend/schema/ddl/operations.pb.h"

namespace postgres_translator {
namespace spangres {

class SpangresSchemaPrinter {
 public:
  SpangresSchemaPrinter() = default;
  virtual ~SpangresSchemaPrinter() = default;

  // Not movable, not copyable
  SpangresSchemaPrinter(SpangresSchemaPrinter&& other) = delete;
  SpangresSchemaPrinter& operator=(SpangresSchemaPrinter&& other) = delete;
  SpangresSchemaPrinter(const SpangresSchemaPrinter&) = delete;
  SpangresSchemaPrinter& operator=(const SpangresSchemaPrinter&) = delete;

  // Prints Cloud Spanner SDL schema as PostgreSQL DDL.
  virtual absl::StatusOr<std::vector<std::string>> PrintDDLStatements(
      const google::spanner::emulator::backend::ddl::DDLStatementList& statements) const = 0;

  virtual absl::StatusOr<std::vector<std::string>> PrintDDLStatement(
      const google::spanner::emulator::backend::ddl::DDLStatement& statement) const = 0;

  virtual absl::StatusOr<std::string> PrintType(
      const google::spanner::emulator::backend::ddl::ColumnDefinition& column) const = 0;

  // Prints out the emulator's DDL statements as formatted strings. When
  // exporting Spangres DDL schema printer to the public, this function should
  // be removed and will be replaced by PrintDDLStatement.
  virtual absl::StatusOr<std::vector<std::string>> PrintDDLStatementForEmulator(
      const google::spanner::emulator::backend::ddl::DDLStatement& statement)
      const = 0;

  // Prints out the type of emulator's column definition as a formatted string.
  // When exporting Spangres DDL schema printer to the public, this function
  // should be removed and will be replaced by PrintType.
  virtual absl::StatusOr<std::string> PrintTypeForEmulator(
      const google::spanner::emulator::backend::ddl::ColumnDefinition& column)
      const = 0;

  // Prints out the type of emulator's row deletion policy as a formatted
  // string.
  virtual absl::StatusOr<std::string> PrintRowDeletionPolicyForEmulator(
      const google::spanner::emulator::backend::ddl::RowDeletionPolicy& policy)
      const = 0;
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // DDL_SPANGRES_SCHEMA_PRINTER_H_
