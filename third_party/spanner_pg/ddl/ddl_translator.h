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

#ifndef DDL_DDL_TRANSLATOR_H_
#define DDL_DDL_TRANSLATOR_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "backend/schema/ddl/operations.pb.h"
#include "third_party/spanner_pg/interface/parser_output.h"

// This file contains some interfaces needed for DDL statement translation.
namespace postgres_translator {

namespace spangres {

struct TranslationOptions {
  // strict_mode will force translator to accept only DDL statements that
  // produce schema definitions that are fully semantically “equivalent” between
  // PostgreSQL and Spangres, for example it would require additional CHECK
  // constraints on primary keys to address differences in NaN and NULL
  // handling.
  bool strict_mode = false;
  // enable_nulls_ordering makes translator respect the differences in nulls
  // ordering with Spanner, e.g. reverse default nulls ordering compared to
  // PostgreSQL.
  bool enable_nulls_ordering = false;
  // enable_interleave_in makes translator enable translating <INTERLEAVE IN>
  // clause in addition to INTERLEAVE IN PARENT.
  bool enable_interleave_in = false;
  // enable_date_type allows translation of Spangres DATE type into the
  // corresponding DATA Spanner type.
  bool enable_date_type = true;
  // enable_arrays_type allows translation of Spangres ARRAY type into the
  // corresponding Spanner ARRAYs.
  bool enable_arrays_type = true;
  // enable_generated_column allows translation of <GENERATE ALWAYS AS> clause
  bool enable_generated_column = false;
  // enable_column_default allows translation of <DEFAULT> column
  bool enable_column_default = false;
  // enable_jsonb_type allows translation of Spangres JSONB type into the
  // corresponding PG.JSONB Spanner type.
  bool enable_jsonb_type = false;
  // enable_jsonb_type allows translation of Spangres JSONB type into the
  // corresponding PG.JSONB Spanner type.
  bool enable_array_jsonb_type = true;
  // enable_alter_statistics allows translating Spangres version of
  // ALTER STATISTICS statement.
  // TODO: remove after rollout.
  bool enable_alter_statistics = false;
  // Allows the use of ANALYZE statemtn.
  // TODO: remove after rollout.
  bool enable_analyze = false;
  // enable_create_view allows translation of <CREATE VIEW>.
  bool enable_create_view = false;
  // enable_ttl allows the use of the TTL clause on CREATE TABLE
  bool enable_ttl = false;
  // enable_expression_string allows translator to set the original_expression
  // directly using the expression string extracted from user input.
  bool enable_expression_string = false;
  // enable_if_not_exists allows the IF NOT EXISTS clause for CREATE TABLE,
  // CREATE INDEX, ALTER TABLE ADD COLUMN, as well as IF EXISTS for DROP TABLE
  // and DROP INDEX.
  bool enable_if_not_exists = false;
  // enable_change_streams allows creating/updating/deleting change streams.
  bool enable_change_streams = false;
  // enable_alter_index allows translation of <ALTER INDEX>.
  bool enable_alter_index = true;
  // enable_role_based_access allows role-based access control, e.g.
  // CREATE/DROP ROLE, GRANT/REVOKE privileges, GRANT/REVOKE role memberships.
  bool enable_role_based_access = true;
  // enable_sequence allows translation of sequence-related DDL.
  bool enable_sequence = true;
  // enable_set_drop_not_null allows translation of <SET/DROP NOT NULL> without
  // specifying a column type.
  bool enable_set_drop_not_null = false;
};

// Interface for translating PostgreSQL DDL parse tree (AST) to Spanner schema
// (SDL) parse tree.
class PostgreSQLToSpannerDDLTranslator {
 public:
  // Translates PostgreSQL DDL <parsed_statements> and add translated statements
  // to Spanner SDL <out_statements>.
  virtual absl::Status Translate(
      const interfaces::ParserOutput& parser_output,
      const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::DDLStatementList& out_statements) const = 0;

  // Helper function for Translate that takes ParserBatchOutput as an input.
  // REQUIRES: parsed_statements.global_status().ok()
  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList> Translate(
      const interfaces::ParserBatchOutput& parsed_statements,
      const TranslationOptions& options = {}) const;

  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList>
  TranslateForEmulator(const interfaces::ParserBatchOutput& parsed_statements,
                       const TranslationOptions& options = {}) const;
  absl::StatusOr<google::spanner::emulator::backend::ddl::DDLStatementList>
  TranslateForEmulator(const interfaces::ParserOutput& parser_output,
                       const TranslationOptions& options = {}) const;

  // Constructor.
  PostgreSQLToSpannerDDLTranslator() = default;

  // Destructor.
  virtual ~PostgreSQLToSpannerDDLTranslator() = default;

  // Explicitly not copyable and not movable.
  PostgreSQLToSpannerDDLTranslator(const PostgreSQLToSpannerDDLTranslator&) =
      delete;
  PostgreSQLToSpannerDDLTranslator& operator=(
      const PostgreSQLToSpannerDDLTranslator&) = delete;
  PostgreSQLToSpannerDDLTranslator(PostgreSQLToSpannerDDLTranslator&&) = delete;
  PostgreSQLToSpannerDDLTranslator& operator=(
      PostgreSQLToSpannerDDLTranslator&&) = delete;
};

}  // namespace spangres

}  // namespace postgres_translator

#endif  // DDL_DDL_TRANSLATOR_H_
