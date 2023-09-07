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

#include "third_party/spanner_pg/ddl/ddl_translator.h"

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/ddl/operations.pb.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres {

absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList>
PostgreSQLToSpannerDDLTranslator::Translate(
    const interfaces::ParserBatchOutput& parsed_statements,
    const TranslationOptions& options) const {
  ZETASQL_RET_CHECK_OK(parsed_statements.global_status());
  google::spanner::emulator::backend::ddl::DDLStatementList result;

  for (const absl::StatusOr<interfaces::ParserOutput>& parser_output :
       parsed_statements.output()) {
    ZETASQL_RETURN_IF_ERROR(parser_output.status())
        << "failed to parse the DDL statements.";

    if (parser_output.value().parse_tree() == nullptr) {
      return absl::InvalidArgumentError("No statement found.");
    }
    ZETASQL_RETURN_IF_ERROR(Translate(parser_output.value(), options, result));
  }

  return result;
}

absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList>
PostgreSQLToSpannerDDLTranslator::TranslateForEmulator(
    const interfaces::ParserBatchOutput& parsed_statements,
    const TranslationOptions& options) const {
  ZETASQL_ASSIGN_OR_RETURN(google::spanner::emulator::backend::ddl::DDLStatementList result,
                   Translate(parsed_statements, options));
  std::string sdl = result.SerializeAsString();
  google::spanner::emulator::backend::ddl::DDLStatementList ddl_statement_list;
  if (!ddl_statement_list.ParseFromString(sdl)) {
    return absl::InvalidArgumentError(
        "Failed to convert to the emulator DDL protocol.");
  }
  return ddl_statement_list;
}

}  // namespace postgres_translator::spangres
