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

#include "third_party/spanner_pg/ddl/spangres_direct_schema_printer_impl.h"

#include <stddef.h>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/optional.h"
#include "backend/schema/ddl/operations.pb.h"
#include "third_party/spanner_pg/ddl/spangres_schema_printer.h"
#include "third_party/spanner_pg/ddl/translation_utils.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace spangres {
namespace {

using absl::StrAppend;
using absl::StrCat;
using absl::StrJoin;
using absl::Substitute;
using PGConstants =
    ::postgres_translator::spangres::internal::PostgreSQLConstants;
using ::postgres_translator::spangres::internal::QuoteIdentifier;
using ::postgres_translator::spangres::internal::QuoteQualifiedIdentifier;
using ::postgres_translator::spangres::internal::QuoteStringLiteral;
using ::google::spanner::emulator::backend::ddl::CheckConstraint;
using ::google::spanner::emulator::backend::ddl::ColumnDefinition;
using ::google::spanner::emulator::backend::ddl::ForeignKey;
using ::google::spanner::emulator::backend::ddl::InterleaveClause;
using ::google::spanner::emulator::backend::ddl::KeyPartClause;

static auto kTypeMap =
    std::map<google::spanner::emulator::backend::ddl::ColumnDefinition::Type,
             absl::string_view>({
    // clang-format on
    {google::spanner::emulator::backend::ddl::ColumnDefinition::BOOL, "boolean"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::BYTES, "bytea"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::DOUBLE, "double precision"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::FLOAT, "real"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::PG_NUMERIC, "numeric"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::INT64, "bigint"},
    // Forward translation maps both varchar and text to the STRING type
    // (in non-strict mode). This distinction is lost in non-strict mode,
    // so during reverse translation we map all the text fields to varchar
    // (which might lead to the reverse translated schema not matching
    // original schema exactly).
    {google::spanner::emulator::backend::ddl::ColumnDefinition::STRING, "character varying"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::DATE, "date"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::TIMESTAMP, "timestamp with time zone"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::PG_JSONB, "jsonb"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::INTERVAL, "interval"},
    {google::spanner::emulator::backend::ddl::ColumnDefinition::TOKENLIST, "spanner.tokenlist"},
});

// LINT.IfChange(supported_statements)
// If you add a new statement, make sure to handle its printing on the method
// PrintDDLStatement.

static auto kSupportedStatements =
  std::set<google::spanner::emulator::backend::ddl::DDLStatement::StatementCase>({
    // clang-format on
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateDatabase,
    google::spanner::emulator::backend::ddl::DDLStatement::kAlterDatabase,
    google::spanner::emulator::backend::ddl::DDLStatement::kAlterTable,
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateTable,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropTable,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropIndex,
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateIndex,
    google::spanner::emulator::backend::ddl::DDLStatement::kAlterIndex,
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateSchema,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropSchema,

    google::spanner::emulator::backend::ddl::DDLStatement::kAnalyze,
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateFunction,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropFunction,
    google::spanner::emulator::backend::ddl::DDLStatement::kSetColumnOptions,
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateChangeStream,
    google::spanner::emulator::backend::ddl::DDLStatement::kAlterChangeStream,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropChangeStream,

    google::spanner::emulator::backend::ddl::DDLStatement::kCreateSearchIndex,

    google::spanner::emulator::backend::ddl::DDLStatement::kDropSearchIndex,

    google::spanner::emulator::backend::ddl::DDLStatement::kCreateSequence,
    google::spanner::emulator::backend::ddl::DDLStatement::kAlterSequence,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropSequence,
    google::spanner::emulator::backend::ddl::DDLStatement::kRenameTable,
    google::spanner::emulator::backend::ddl::DDLStatement::kCreateLocalityGroup,
    google::spanner::emulator::backend::ddl::DDLStatement::kAlterLocalityGroup,
    google::spanner::emulator::backend::ddl::DDLStatement::kDropLocalityGroup,
});
// LINT.ThenChange(:print_sdl_statement)

std::string CreateNotNullCondition(const absl::string_view column) {
  return StrCat(QuoteIdentifier(column), " IS NOT NULL");
}

absl::string_view GetSchemaLocalName(absl::string_view name) {
  size_t last_dot = name.find_last_of('.');
  if (last_dot == absl::string_view::npos) {
    return name;
  } else {
    return name.substr(last_dot + 1);
  }
}

class SpangresSchemaPrinterImpl : public SpangresSchemaPrinter {
 public:
  // Prints Spanner SDL schema as PostgreSQL DDL
  absl::StatusOr<std::vector<std::string>> PrintDDLStatements(
      const google::spanner::emulator::backend::ddl::DDLStatementList& statements) const;

  absl::StatusOr<std::vector<std::string>> PrintDDLStatement(
      const google::spanner::emulator::backend::ddl::DDLStatement& statement) const;

  absl::StatusOr<std::string> PrintType(
      const google::spanner::emulator::backend::ddl::ColumnDefinition& column) const override;

  absl::StatusOr<std::vector<std::string>> PrintDDLStatementForEmulator(
      const google::spanner::emulator::backend::ddl::DDLStatement& statement)
      const;

  absl::StatusOr<std::string> PrintTypeForEmulator(
      const google::spanner::emulator::backend::ddl::ColumnDefinition& column)
      const;

  absl::StatusOr<std::string> PrintRowDeletionPolicyForEmulator(
      const google::spanner::emulator::backend::ddl::RowDeletionPolicy& policy)
      const;

 private:
  // Print<statement> methods convert Spanner <statement> into the
  // corresponding PostgreSQL DDL representation
  absl::StatusOr<std::vector<std::string>> PrintCreateDatabase(
      const google::spanner::emulator::backend::ddl::CreateDatabase& statement) const;
  absl::StatusOr<std::vector<std::string>> PrintAlterDatabase(
      const google::spanner::emulator::backend::ddl::AlterDatabase& statement) const;
  absl::StatusOr<std::string> PrintAlterTable(
      const google::spanner::emulator::backend::ddl::AlterTable& statement) const;
  absl::StatusOr<std::string> PrintCreateTable(
      const google::spanner::emulator::backend::ddl::CreateTable& statement) const;
  absl::StatusOr<std::string> PrintDropTable(
      const google::spanner::emulator::backend::ddl::DropTable& statement) const;
  absl::StatusOr<std::string> PrintDropIndex(
      const google::spanner::emulator::backend::ddl::DropIndex& statement) const;
  absl::StatusOr<std::string> PrintDropSchema(
      const google::spanner::emulator::backend::ddl::DropSchema& statement) const;
  absl::StatusOr<std::string> PrintDropFunction(
      const google::spanner::emulator::backend::ddl::DropFunction& statement) const;
  absl::StatusOr<std::string> PrintCreateSequence(
      const google::spanner::emulator::backend::ddl::CreateSequence& statement) const;
  absl::StatusOr<std::string> PrintAlterSequence(
      const google::spanner::emulator::backend::ddl::AlterSequence& statement) const;
  absl::StatusOr<std::string> PrintDropSequence(
      const google::spanner::emulator::backend::ddl::DropSequence& statement) const;
  absl::StatusOr<std::string> PrintCreateIndex(
      const google::spanner::emulator::backend::ddl::CreateIndex& statement) const;
  absl::StatusOr<std::string> PrintAlterIndex(
      const google::spanner::emulator::backend::ddl::AlterIndex& statement) const;
  absl::StatusOr<std::string> PrintCreateSchema(
      const google::spanner::emulator::backend::ddl::CreateSchema& statement) const;
  absl::StatusOr<std::string> PrintAnalyze(
      const google::spanner::emulator::backend::ddl::Analyze& statement) const;
  absl::StatusOr<std::string> PrintSQLSecurityType(
      google::spanner::emulator::backend::ddl::Function::SqlSecurity sql_security) const;
  absl::StatusOr<std::string> PrintCreateFunction(
      const google::spanner::emulator::backend::ddl::CreateFunction& statement) const;
  absl::StatusOr<std::string> PrintColumn(
      const google::spanner::emulator::backend::ddl::ColumnDefinition& column) const;
  absl::StatusOr<std::string> PrintForeignKeyAction(
      absl::string_view action_type,
      const google::spanner::emulator::backend::ddl::ForeignKey::Action action) const;
  absl::StatusOr<std::string> PrintForeignKey(
      const google::spanner::emulator::backend::ddl::ForeignKey& foreign_key) const;
  absl::StatusOr<std::string> PrintPrimaryKey(
      const google::protobuf::RepeatedPtrField<KeyPartClause>& key_parts) const;
  absl::StatusOr<std::string> PrintInterleaveDeleteAction(
      const google::spanner::emulator::backend::ddl::InterleaveClause::Action action) const;
  absl::StatusOr<std::string> PrintInterleaveClause(
      const InterleaveClause& interleave_clause) const;
  absl::StatusOr<std::string> PrintCheckConstraint(
      const CheckConstraint& check_constraint) const;
  absl::StatusOr<std::string> PrintSortOrder(KeyPartClause::Order order) const;
  absl::StatusOr<std::vector<std::string>> PrintAlterDatabaseSetOptions(
      absl::string_view quoted_db_name,
      const google::protobuf::RepeatedPtrField<google::spanner::emulator::backend::ddl::SetOption>& options) const;
  std::string PrintCreateChangeStream(
      const google::spanner::emulator::backend::ddl::CreateChangeStream& statement) const;
  std::string PrintChangeStreamForClause(
      const google::spanner::emulator::backend::ddl::ChangeStreamForClause for_clause) const;
  std::string PrintChangeStreamTrackedTables(
      const google::protobuf::RepeatedPtrField<
          google::spanner::emulator::backend::ddl::ChangeStreamForClause::TrackedTables::Entry>& list)
      const;
  std::string PrintChangeStreamSetOptions(
      const google::protobuf::RepeatedPtrField<google::spanner::emulator::backend::ddl::SetOption>& list,
      bool use_set) const;
  std::string PrintAlterChangeStream(
      const google::spanner::emulator::backend::ddl::AlterChangeStream& statement) const;
  std::string PrintDropChangeStream(
      const google::spanner::emulator::backend::ddl::DropChangeStream& statement) const;
  absl::StatusOr<std::string> PrintCreateSearchIndex(
      const google::spanner::emulator::backend::ddl::CreateSearchIndex& statement) const;
  std::string FormatOptionAsNameValuePair(
      const google::spanner::emulator::backend::ddl::SetOption& option) const;
  std::string PrintDropSearchIndex(
      const google::spanner::emulator::backend::ddl::DropSearchIndex& statement) const;

  std::string PrintCreateLocalityGroup(
      const google::spanner::emulator::backend::ddl::CreateLocalityGroup& statement) const;
  void PrintStorageOption(const google::spanner::emulator::backend::ddl::SetOption& option,
                                 std::string& out) const;
  void PrintSpillTimespanOption(const google::spanner::emulator::backend::ddl::SetOption& option,
                                       std::string& out) const;
  std::string PrintAlterLocalityGroup(
      const google::spanner::emulator::backend::ddl::AlterLocalityGroup& statement) const;
  absl::StatusOr<std::string> PrintDropLocalityGroup(
      const google::spanner::emulator::backend::ddl::DropLocalityGroup& statement) const;
  absl::StatusOr<std::string> PrintAlterColumnLocalityGroup(
      const google::spanner::emulator::backend::ddl::SetColumnOptions& statement) const;
  absl::StatusOr<std::string> PrintRenameTable(
      const google::spanner::emulator::backend::ddl::RenameTable& statement) const;

  absl::StatusOr<std::vector<std::string>> WrapOutput(
      absl::StatusOr<std::string> printing_result) const;

  // Basic translation error, used for everything translation-related
  absl::Status StatementTranslationError(absl::string_view error_mesage) const;
};

absl::StatusOr<std::vector<std::string>> SpangresSchemaPrinterImpl::WrapOutput(
    absl::StatusOr<std::string> printing_result) const {
  if (!printing_result.ok()) {
    return printing_result.status();
  }
  return std::vector({*printing_result});
}

absl::Status SpangresSchemaPrinterImpl::StatementTranslationError(
    absl::string_view error_mesage) const {
  return absl::FailedPreconditionError(error_mesage);
}

absl::StatusOr<std::vector<std::string>>
SpangresSchemaPrinterImpl::PrintDDLStatements(
    const google::spanner::emulator::backend::ddl::DDLStatementList& statements) const {
  std::vector<std::string> output;
  for (const google::spanner::emulator::backend::ddl::DDLStatement& statement : statements.statement()) {
    ZETASQL_ASSIGN_OR_RETURN(std::vector<std::string> printed,
                     PrintDDLStatement(statement));
    output.insert(output.end(), printed.begin(), printed.end());
  }
  return output;
}

absl::StatusOr<std::vector<std::string>>
SpangresSchemaPrinterImpl::PrintDDLStatement(
    const google::spanner::emulator::backend::ddl::DDLStatement& statement) const {
  // We use find() instead of contains() because the latter is not available in
  // the open-source data structure (std::set).
  if (kSupportedStatements.find(statement.statement_case()) ==
      kSupportedStatements.end()) {
    return StatementTranslationError("Unsupported statement");
  }

  // LINT.IfChange(print_sdl_statement)
  // If you need to add a new statement, make sure to update the
  // `kSupportedStatements` set.
  switch (statement.statement_case()) {
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateDatabase:
      return PrintCreateDatabase(statement.create_database());
    case google::spanner::emulator::backend::ddl::DDLStatement::kAlterDatabase:
      return PrintAlterDatabase(statement.alter_database());
    case google::spanner::emulator::backend::ddl::DDLStatement::kAlterTable:
      return WrapOutput(PrintAlterTable(statement.alter_table()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateTable:
      return WrapOutput(PrintCreateTable(statement.create_table()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropTable:
      return WrapOutput(PrintDropTable(statement.drop_table()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropIndex:
      return WrapOutput(PrintDropIndex(statement.drop_index()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateIndex:
      return WrapOutput(PrintCreateIndex(statement.create_index()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kAlterIndex:
      return WrapOutput(PrintAlterIndex(statement.alter_index()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateSchema:
      return WrapOutput(PrintCreateSchema(statement.create_schema()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropSchema:
      return WrapOutput(PrintDropSchema(statement.drop_schema()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kAnalyze:
      return WrapOutput(PrintAnalyze(statement.analyze()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateFunction:
      return WrapOutput(PrintCreateFunction(statement.create_function()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropFunction:
      return WrapOutput(PrintDropFunction(statement.drop_function()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kSetColumnOptions:
      return WrapOutput(
          PrintAlterColumnLocalityGroup(statement.set_column_options()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateChangeStream:
      return WrapOutput(
          PrintCreateChangeStream(statement.create_change_stream()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kAlterChangeStream:
      return WrapOutput(
          PrintAlterChangeStream(statement.alter_change_stream()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropChangeStream:
      return WrapOutput(PrintDropChangeStream(statement.drop_change_stream()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateSearchIndex:
      return WrapOutput(
          PrintCreateSearchIndex(statement.create_search_index()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropSearchIndex:
      return WrapOutput(PrintDropSearchIndex(statement.drop_search_index()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateSequence:
      return WrapOutput(PrintCreateSequence(statement.create_sequence()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kAlterSequence:
      return WrapOutput(PrintAlterSequence(statement.alter_sequence()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropSequence:
      return WrapOutput(PrintDropSequence(statement.drop_sequence()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kRenameTable:
      return WrapOutput(PrintRenameTable(statement.rename_table()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kCreateLocalityGroup:
      return WrapOutput(
          PrintCreateLocalityGroup(statement.create_locality_group()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kAlterLocalityGroup:
      return WrapOutput(
          PrintAlterLocalityGroup(statement.alter_locality_group()));
    case google::spanner::emulator::backend::ddl::DDLStatement::kDropLocalityGroup:
      return WrapOutput(
          PrintDropLocalityGroup(statement.drop_locality_group()));
    default:
      // Ignore unsupported statements
      return StatementTranslationError("Unsupported statement");
  }
  // LINT.ThenChange(:supported_statements)
}

absl::StatusOr<std::vector<std::string>>
SpangresSchemaPrinterImpl::PrintDDLStatementForEmulator(
    const google::spanner::emulator::backend::ddl::DDLStatement& statement)
    const {
  google::spanner::emulator::backend::ddl::DDLStatement sdl_statement;
  if (!sdl_statement.ParseFromString(statement.SerializeAsString())) {
    return absl::InvalidArgumentError(
        "Failed to convert from the emulator's DDL protocol to the SDL.");
  }
  return PrintDDLStatement(sdl_statement);
}

absl::StatusOr<std::vector<std::string>>
SpangresSchemaPrinterImpl::PrintCreateDatabase(
    const google::spanner::emulator::backend::ddl::CreateDatabase& statement) const {
  absl::optional<std::string> database_name = absl::nullopt;
  if (!database_name && statement.has_db_name()) {
    database_name = statement.db_name();
  }

  std::string quoted_db_name;
  if (database_name) {
    quoted_db_name = QuoteIdentifier(*database_name);
  } else {
    quoted_db_name = "<database_name>";
  }

  std::vector<std::string> output;
    output.push_back(absl::StrCat("CREATE DATABASE ", quoted_db_name));
  return output;
}

absl::StatusOr<std::vector<std::string>>
SpangresSchemaPrinterImpl::PrintAlterDatabase(
    const google::spanner::emulator::backend::ddl::AlterDatabase& statement) const {
  ZETASQL_RET_CHECK_EQ(statement.alter_type_case(),
               google::spanner::emulator::backend::ddl::AlterDatabase::kSetOptions);

  return PrintAlterDatabaseSetOptions(QuoteIdentifier(statement.db_name()),
                                      statement.set_options().options());
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintAlterTable(
    const google::spanner::emulator::backend::ddl::AlterTable& statement) const {
  std::string alter_table =
      StrCat("ALTER TABLE ", QuoteQualifiedIdentifier(statement.table_name()));

  switch (statement.alter_type_case()) {
    case google::spanner::emulator::backend::ddl::AlterTable::kAddColumn: {
      ZETASQL_ASSIGN_OR_RETURN(std::string printed_column,
                       PrintColumn(statement.add_column().column()));
      std::string if_not_exists = "";
      if (statement.add_column().existence_modifier() ==
          google::spanner::emulator::backend::ddl::IF_NOT_EXISTS) {
        if_not_exists = "IF NOT EXISTS ";
      }
      return StrCat(alter_table, " ADD COLUMN ", if_not_exists, printed_column);
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kDropColumn: {
      return StrCat(alter_table, " DROP COLUMN ",
                    QuoteIdentifier(statement.drop_column()));
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kAlterColumn: {
      google::spanner::emulator::backend::ddl::AlterTable::AlterColumn alter_column_sdl =
          statement.alter_column();
      ZETASQL_RET_CHECK(alter_column_sdl.has_column());
      google::spanner::emulator::backend::ddl::ColumnDefinition column = alter_column_sdl.column();
      const std::string alter_column =
          StrCat("ALTER COLUMN ", QuoteIdentifier(column.column_name()));

      if (alter_column_sdl.has_operation()) {
        switch (alter_column_sdl.operation()) {
          case google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::DROP_DEFAULT: {
            return StrCat(alter_table, " ", alter_column, " DROP DEFAULT");
          }
          case google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::SET_DEFAULT: {
            ZETASQL_RET_CHECK(column.has_column_default());
            ZETASQL_RET_CHECK(column.column_default().has_expression_origin());
            google::spanner::emulator::backend::ddl::SQLExpressionOrigin expression_origin =
                column.column_default().expression_origin();
            const std::string expression_output =
                expression_origin.has_original_expression()
                    ? StrCat(" ", expression_origin.original_expression())
                    : "";
            return StrCat(alter_table, " ", alter_column, " SET DEFAULT",
                          expression_output);
          }
          case google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::SET_NOT_NULL: {
            return StrCat(alter_table, " ", alter_column, " SET NOT NULL");
          }
          case google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::DROP_NOT_NULL: {
            return StrCat(alter_table, " ", alter_column, " DROP NOT NULL");
          }
          case google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::ALTER_IDENTITY: {
            ZETASQL_RET_CHECK(column.has_identity_column());
            if (alter_column_sdl.has_identity_alter_start_with_counter()) {
              ZETASQL_RET_CHECK(column.identity_column().has_start_with_counter());
              return absl::StrCat(
                  alter_table, " ", alter_column, " RESTART COUNTER WITH ",
                  column.identity_column().start_with_counter());
            } else if (alter_column_sdl.has_identity_alter_skip_range()) {
              if (!column.identity_column().has_skip_range_min() &&
                  !column.identity_column().has_skip_range_max()) {
                return absl::StrCat(alter_table, " ", alter_column,
                                    " SET NO SKIP RANGE");
              } else if (column.identity_column().has_skip_range_min() &&
                         column.identity_column().has_skip_range_max()) {
                return absl::StrCat(
                    alter_table, " ", alter_column, " SET SKIP RANGE ",
                    column.identity_column().skip_range_min(), " ",
                    column.identity_column().skip_range_max());
              } else {
                return absl::InvalidArgumentError(
                    "Invalid skip range for identity column.");
              }
            } else {
              return absl::InvalidArgumentError(
                  "Invalid options for <ALTER COLUMN> on identity column.");
            }
          }
          default: {
            // We need this here to allow us to add the SET_NOT_NULL and
            // DROP_NOT_NULL operations in ZetaSQL first/
            ZETASQL_RET_CHECK_FAIL()
                << "Unknown alter column operation type:"
                << static_cast<int64_t>(alter_column_sdl.operation());
          }
        }
        // Should never get here.
        ZETASQL_RET_CHECK_FAIL() << "Unknown alter column operation type:"
                         << static_cast<int64_t>(alter_column_sdl.operation());
      } else {
        ZETASQL_ASSIGN_OR_RETURN(const std::string printed_type, PrintType(column));
        if (column.has_not_null()) {
          absl::string_view not_null_action =
              column.not_null() ? "SET" : "DROP";
          return StrCat(alter_table, " ", alter_column, " TYPE ", printed_type,
                        ", ", alter_column, " ", not_null_action, " NOT NULL");
        } else {
          return StrCat(alter_table, " ", alter_column, " TYPE ", printed_type);
        }
      }
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kAddForeignKey: {
      ZETASQL_ASSIGN_OR_RETURN(
          std::string printed_foreign_key,
          PrintForeignKey(statement.add_foreign_key().foreign_key()));
      return StrCat(alter_table, " ADD ", printed_foreign_key);
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kAddCheckConstraint: {
      ZETASQL_RET_CHECK(statement.add_check_constraint().has_check_constraint())
          << "Check constraint should not be empty in <ALTER TABLE> add "
             "constraint <CHECK>.";
      ZETASQL_ASSIGN_OR_RETURN(
          std::string printed_check_constraint,
          PrintCheckConstraint(
              statement.add_check_constraint().check_constraint()));
      return StrCat(alter_table, " ADD ", printed_check_constraint);
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kDropConstraint: {
      return StrCat(alter_table, " DROP CONSTRAINT ",
                    QuoteIdentifier(statement.drop_constraint().name()));
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kDropRowDeletionPolicy: {
      return StrCat(alter_table, " DROP TTL");
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kAddRowDeletionPolicy: {
      return StrCat(
          alter_table, " ADD TTL INTERVAL ",
          RowDeletionPolicyToInterval(statement.add_row_deletion_policy()));
    }
    case google::spanner::emulator::backend::ddl::AlterTable::kAlterRowDeletionPolicy: {
      return StrCat(
          alter_table, " ALTER TTL INTERVAL ",
          RowDeletionPolicyToInterval(statement.alter_row_deletion_policy()));
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kSetOnDelete: {
      ZETASQL_ASSIGN_OR_RETURN(
          std::string action,
          PrintInterleaveDeleteAction(statement.set_on_delete().action()));
      return StrCat(alter_table, " SET ON DELETE", action);
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kSetInterleaveClause: {
      ZETASQL_ASSIGN_OR_RETURN(
          std::string interleave,
          PrintInterleaveClause(
              statement.set_interleave_clause().interleave_clause()));
      return StrCat(alter_table, " SET ", interleave);
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kRenameTo: {
      std::string rename_clause = statement.rename_to().has_synonym() ?
          " RENAME WITH SYNONYM TO " : " RENAME TO ";
      return StrCat(alter_table, rename_clause,
                    QuoteQualifiedIdentifier(statement.rename_to().name()));
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kAddSynonym: {
      return StrCat(
          alter_table, " ADD SYNONYM ",
          QuoteQualifiedIdentifier(statement.add_synonym().synonym()));
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kDropSynonym: {
      return StrCat(
          alter_table, " DROP SYNONYM ",
          QuoteQualifiedIdentifier(statement.drop_synonym().synonym()));
    }

    case google::spanner::emulator::backend::ddl::AlterTable::kSetOptions: {
      for (const google::spanner::emulator::backend::ddl::SetOption& option :
           statement.set_options().options()) {
        if (absl::EqualsIgnoreCase(option.option_name(), "locality_group")) {
          if (option.null_value()) {
            return StrCat(alter_table, " SET LOCALITY GROUP NULL");
          }
          return StrCat(alter_table, " SET LOCALITY GROUP ",
                        option.string_value());
        }
      }
      return absl::InvalidArgumentError("Invalid options for <ALTER TABLE>.");
    }

    default: {
      const google::protobuf::FieldDescriptor* field_descriptor =
          google::spanner::emulator::backend::ddl::AlterTable::GetDescriptor()->FindFieldByNumber(
              statement.alter_type_case());
      ZETASQL_RET_CHECK_FAIL() << "Unsupported alter table type: alter_type_case is: "
                       << static_cast<int64_t>(statement.alter_type_case())
                       << ", alter type name is: "
                       << (field_descriptor == nullptr
                               ? "UNKNOWN"
                               : field_descriptor->name());
    }
  }
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCreateTable(
    const google::spanner::emulator::backend::ddl::CreateTable& statement) const {
  std::vector<std::string> output;
  std::string if_not_exists = "";
  if (statement.existence_modifier() == google::spanner::emulator::backend::ddl::IF_NOT_EXISTS) {
    if_not_exists = "IF NOT EXISTS ";
  }
  std::string base =
      StrCat("CREATE TABLE ", if_not_exists,
             QuoteQualifiedIdentifier(statement.table_name()), " (");

  output.push_back(std::move(base));

  std::vector<std::string> create_table_entries;
  // Pattern with indent for each clause in create table
  const std::string_view pattern = "  $0";
  for (const ColumnDefinition& column_definition : statement.column()) {
    ZETASQL_ASSIGN_OR_RETURN(std::string column_string, PrintColumn(column_definition));
    create_table_entries.push_back(Substitute(pattern, column_string));
  }

  ZETASQL_ASSIGN_OR_RETURN(std::string primary_key_string,
                   PrintPrimaryKey(statement.primary_key()));
  create_table_entries.push_back(Substitute(pattern, primary_key_string));

  for (const ForeignKey& foreign_key : statement.foreign_key()) {
    ZETASQL_ASSIGN_OR_RETURN(std::string foreign_key_string,
                     PrintForeignKey(foreign_key));
    create_table_entries.push_back(Substitute(pattern, foreign_key_string));
  }

  for (const CheckConstraint& check_constraint : statement.check_constraint()) {
    ZETASQL_ASSIGN_OR_RETURN(std::string check_constraint_string,
                     PrintCheckConstraint(check_constraint));
    create_table_entries.push_back(
        Substitute(pattern, check_constraint_string));
  }

  if (statement.has_synonym()) {
    std::string synonym_string;
    StrAppend(&synonym_string,
              Substitute("SYNONYM($0)", QuoteIdentifier(statement.synonym())));
    create_table_entries.push_back(Substitute(pattern, synonym_string));
  }

  output.push_back(absl::StrJoin(create_table_entries, ",\n"));

  std::string after_create = ")";
  if (statement.has_interleave_clause()) {
    ZETASQL_ASSIGN_OR_RETURN(std::string interleave_in_string,
                     PrintInterleaveClause(statement.interleave_clause()));
    StrAppend(&after_create, " ", interleave_in_string);
  }

  if (statement.has_row_deletion_policy()) {
    StrAppend(&after_create, " TTL INTERVAL ",
              RowDeletionPolicyToInterval(statement.row_deletion_policy()));
  }

  std::string locality_group = "";
  for (const google::spanner::emulator::backend::ddl::SetOption& option : statement.set_options()) {
    if (option.option_name() == "locality_group") {
      StrAppend(&locality_group, " LOCALITY GROUP ");
      if (option.null_value()) {
        StrAppend(&locality_group, "NULL");
      } else {
        StrAppend(&locality_group, option.string_value());
      }
    }
  }
  StrAppend(&after_create, locality_group);

  output.push_back(std::move(after_create));

  return absl::StrJoin(output, "\n");
}

std::string SpangresSchemaPrinterImpl::PrintChangeStreamTrackedTables(
    const google::protobuf::RepeatedPtrField<
        google::spanner::emulator::backend::ddl::ChangeStreamForClause::TrackedTables::Entry>& list)
    const {
  std::string output;
  const char* sep = " ";
  for (const google::spanner::emulator::backend::ddl::ChangeStreamForClause::TrackedTables::Entry& item :
       list) {
    StrAppend(&output, sep);
    sep = ", ";
    StrAppend(&output, QuoteQualifiedIdentifier(item.table_name()));
    if (!item.has_all_columns()) {
      StrAppend(&output, "(");
      const char* sep2 = "";
      for (const std::string& column : item.tracked_columns().column_name()) {
        StrAppend(&output, sep2);
        sep2 = ", ";
        StrAppend(&output, QuoteIdentifier(column));
      }
      StrAppend(&output, ")");
    }
  }
  return output;
}

std::string SpangresSchemaPrinterImpl::PrintChangeStreamSetOptions(
    const google::protobuf::RepeatedPtrField<google::spanner::emulator::backend::ddl::SetOption>& list,
    bool use_set) const {
  std::string output;
  if (use_set) {
    StrAppend(&output, "SET");
  } else {
    StrAppend(&output, "WITH");
  }
  const char* sep = " (";
  for (const google::spanner::emulator::backend::ddl::SetOption& item : list) {
    StrAppend(&output, sep);
    sep = ", ";
    StrAppend(&output, QuoteIdentifier(item.option_name()));
    StrAppend(&output, " = ");
    if (item.null_value()) {
      StrAppend(&output, "NULL");
    } else if (item.has_bool_value()) {
      StrAppend(&output, item.bool_value() ? PGConstants::kPgTrueLiteral
                                           : PGConstants::kPgFalseLiteral);
    } else {
      StrAppend(&output, QuoteStringLiteral(item.string_value()));
    }
  }
  StrAppend(&output, ")");
  return output;
}

std::string SpangresSchemaPrinterImpl::PrintChangeStreamForClause(
    const google::spanner::emulator::backend::ddl::ChangeStreamForClause for_clause) const {
  std::string output;
  StrAppend(&output, "FOR");
  if (for_clause.has_all()) {
    StrAppend(&output, " ALL");
  } else {
    std::string change_stream_tracked_tables = PrintChangeStreamTrackedTables(
        for_clause.tracked_tables().table_entry());
    StrAppend(&output, change_stream_tracked_tables);
  }
  return output;
}

std::string SpangresSchemaPrinterImpl::PrintCreateChangeStream(
    const google::spanner::emulator::backend::ddl::CreateChangeStream& statement) const {
  std::vector<std::string> output;
  std::string base =
      StrCat("CREATE CHANGE STREAM ",
             QuoteQualifiedIdentifier(statement.change_stream_name()));

  output.push_back(std::move(base));
  if (statement.has_for_clause()) {
    std::string change_stream_for_clause =
        PrintChangeStreamForClause(statement.for_clause());
    output.push_back(change_stream_for_clause);
  }

  if (!statement.set_options().empty()) {
    std::string change_stream_set_options =
        PrintChangeStreamSetOptions(statement.set_options(), /*use_set=*/false);
    output.push_back(change_stream_set_options);
  }

  return absl::StrJoin(output, "\n");
}

std::string SpangresSchemaPrinterImpl::PrintAlterChangeStream(
    const google::spanner::emulator::backend::ddl::AlterChangeStream& statement) const {
  std::vector<std::string> output;
  std::string base =
      StrCat("ALTER CHANGE STREAM ",
             QuoteQualifiedIdentifier(statement.change_stream_name()));

  output.push_back(std::move(base));
  if (statement.has_set_for_clause()) {
    std::string set_change_stream_for_clause = "SET ";
    std::string change_stream_for_clause =
        PrintChangeStreamForClause(statement.set_for_clause());
    StrAppend(&set_change_stream_for_clause, change_stream_for_clause);
    output.push_back(set_change_stream_for_clause);
  }

  if (statement.has_drop_for_clause()) {
    std::string drop_change_stream_for_clause = "DROP ";
    std::string change_stream_for_clause =
        PrintChangeStreamForClause(statement.drop_for_clause());
    StrAppend(&drop_change_stream_for_clause, change_stream_for_clause);
    output.push_back(drop_change_stream_for_clause);
  }

  if (!statement.set_options().options().empty()) {
    std::string change_stream_set_options = PrintChangeStreamSetOptions(
        statement.set_options().options(), /*use_set=*/true);
    output.push_back(change_stream_set_options);
  }
  return absl::StrJoin(output, "\n");
}

std::string SpangresSchemaPrinterImpl::PrintDropChangeStream(
    const google::spanner::emulator::backend::ddl::DropChangeStream& statement) const {
  return Substitute("DROP CHANGE STREAM $0",
                    QuoteQualifiedIdentifier(statement.change_stream_name()));
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintDropTable(
    const google::spanner::emulator::backend::ddl::DropTable& statement) const {
  std::string if_exists = "";
  if (statement.existence_modifier() == google::spanner::emulator::backend::ddl::IF_EXISTS) {
    if_exists = "IF EXISTS ";
  }
  return Substitute("DROP TABLE $0$1", if_exists,
                    QuoteQualifiedIdentifier(statement.table_name()));
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintDropIndex(
    const google::spanner::emulator::backend::ddl::DropIndex& statement) const {
  std::string if_exists = "";
  if (statement.existence_modifier() == google::spanner::emulator::backend::ddl::IF_EXISTS) {
    if_exists = "IF EXISTS ";
  }
  return Substitute("DROP INDEX $0$1", if_exists,
                    QuoteQualifiedIdentifier(statement.index_name()));
}

std::string PrintSearchIndexClause(std::vector<std::string>& columns,
                                   absl::string_view clause) {
  std::string output = "";
  if (columns.empty()) {
    return output;
  }
  if (absl::EqualsIgnoreCase(clause, "TOKENLIST_COLUMNS")) {
    StrAppend(&output, "(", absl::StrJoin(columns, ", "), ")");
  } else if (absl::EqualsIgnoreCase(clause, "INCLUDE")) {
    StrAppend(&output, " INCLUDE (", absl::StrJoin(columns, ", "), ")");
  } else if (absl::EqualsIgnoreCase(clause, "PARTITION_BY")) {
    StrAppend(&output, " PARTITION BY ", absl::StrJoin(columns, ", "));
  } else if (absl::EqualsIgnoreCase(clause, "ORDER_BY")) {
    StrAppend(&output, " ORDER BY ", absl::StrJoin(columns, ", "));
  } else if (absl::EqualsIgnoreCase(clause, "WHERE")) {
    StrAppend(&output, " WHERE ", absl::StrJoin(columns, " AND "));
  } else if (absl::EqualsIgnoreCase(clause, "OPTIONS")) {
    StrAppend(&output, " WITH (", absl::StrJoin(columns, ", "), ")");
  }

  return output;
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCreateSearchIndex(
    const google::spanner::emulator::backend::ddl::CreateSearchIndex& statement) const {
  std::vector<std::string> statements;

  // Prints the TOKENLIST COLUMNS on which the SEARCH INDEX is created.
  std::vector<std::string> token_columns;
  for (const google::spanner::emulator::backend::ddl::TokenColumnDefinition& token :
       statement.token_column_definition()) {
    token_columns.push_back(QuoteIdentifier(token.token_column().key_name()));
  }
  statements.emplace_back(
      PrintSearchIndexClause(token_columns, "TOKENLIST_COLUMNS"));

  // Prints the INCLUDE clause.
  std::vector<std::string> storing_columns;
  for (const google::spanner::emulator::backend::ddl::StoredColumnDefinition& stored_column :
       statement.stored_column_definition()) {
    storing_columns.push_back(stored_column.name());
  }
  statements.emplace_back(PrintSearchIndexClause(storing_columns, "INCLUDE"));

  // Prints the PARTITION BY clause.
  std::vector<std::string> partition_by;
  for (const google::spanner::emulator::backend::ddl::KeyPartClause& partition :
       statement.partition_by()) {
    const std::string name = QuoteIdentifier(partition.key_name());
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view partition_order,
                     PrintSortOrder(partition.order()));
    partition_by.push_back(StrCat(name, partition_order));
  }
  statements.emplace_back(PrintSearchIndexClause(partition_by, "PARTITION_BY"));

  // Prints the ORDER BY clause.
  std::vector<std::string> order_by;
  for (const google::spanner::emulator::backend::ddl::KeyPartClause& order_by_part :
       statement.order_by()) {
    const std::string name = QuoteIdentifier(order_by_part.key_name());
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view sort_order,
                     PrintSortOrder(order_by_part.order()));
    order_by.push_back(StrCat(name, sort_order));
  }
  statements.emplace_back(PrintSearchIndexClause(order_by, "ORDER_BY"));

  // Prints the INTERLEAVE IN clause.
  std::string interleave_in_clause =
      statement.interleave_in_table().empty()
          ? ""
          : StrCat(" INTERLEAVE IN ",
                   QuoteQualifiedIdentifier(statement.interleave_in_table()));

  statements.emplace_back(interleave_in_clause);

  // Prints the WHERE clause.
  std::vector<std::string> null_filtered_columns;
  for (const std::string& null_filtered_column :
       statement.null_filtered_column()) {
    null_filtered_columns.push_back(
        CreateNotNullCondition(null_filtered_column));
  }

  statements.emplace_back(
      PrintSearchIndexClause(null_filtered_columns, "WHERE"));

  // Prints the OPTIONS clause.
  std::vector<std::string> options;
  for (const google::spanner::emulator::backend::ddl::SetOption& option : statement.set_options()) {
    options.push_back(FormatOptionAsNameValuePair(option));
  }
  statements.emplace_back(PrintSearchIndexClause(options, "OPTIONS"));

  std::string output;
  StrAppend(
      &output, "CREATE SEARCH INDEX ",
      QuoteQualifiedIdentifier(GetSchemaLocalName(statement.index_name())),
      " ON ", QuoteQualifiedIdentifier(statement.index_base_name()));
  for (std::string& statement : statements) {
    StrAppend(&output, statement);
  }
  return output;
}

std::string SpangresSchemaPrinterImpl::FormatOptionAsNameValuePair(
    const google::spanner::emulator::backend::ddl::SetOption& option) const {
  std::string output;
  StrAppend(&output, QuoteIdentifier(option.option_name()));
  StrAppend(&output, " = ");
  if (option.null_value()) {
    StrAppend(&output, "NULL");
  } else if (option.has_bool_value()) {
    StrAppend(&output, option.bool_value() ? PGConstants::kPgTrueLiteral
                                           : PGConstants::kPgFalseLiteral);
  } else if (option.has_int64_value()) {
    StrAppend(&output, option.int64_value());
  } else {
    StrAppend(&output, QuoteStringLiteral(option.string_value()));
  }
  return output;
}

std::string SpangresSchemaPrinterImpl::PrintDropSearchIndex(
    const google::spanner::emulator::backend::ddl::DropSearchIndex& statement) const {
  std::string output;
  bool exists = statement.has_existence_modifier() &&
                statement.existence_modifier() ==
                    google::spanner::emulator::backend::ddl::ExistenceModifier::IF_EXISTS;
  std::string if_exists = exists ? "IF EXISTS " : "";
  StrAppend(&output, "DROP SEARCH INDEX ", if_exists,
            QuoteQualifiedIdentifier(statement.index_name()));
  return output;
}

std::string SpangresSchemaPrinterImpl::PrintCreateLocalityGroup(
    const google::spanner::emulator::backend::ddl::CreateLocalityGroup& statement) const {
  std::vector<std::string> output;
  const std::string existence_modifier =
      statement.existence_modifier() ==
              google::spanner::emulator::backend::ddl::ExistenceModifier::IF_NOT_EXISTS
          ? "IF NOT EXISTS "
          : "";
  std::string base =
      StrCat("CREATE LOCALITY GROUP ", existence_modifier,
             QuoteQualifiedIdentifier(statement.locality_group_name()));
  output.push_back(std::move(base));

  std::string storage, ssd_to_hdd_spill_timespan;
  for (const google::spanner::emulator::backend::ddl::SetOption& option : statement.set_options()) {
    if (absl::EqualsIgnoreCase(option.option_name(),
                               internal::PostgreSQLConstants::
                                   kInternalLocalityGroupStorageOptionName)) {
      PrintStorageOption(option, storage);
    } else if (absl::EqualsIgnoreCase(
                   option.option_name(),
                   internal::PostgreSQLConstants::
                       kInternalLocalityGroupSpillTimeSpanOptionName)) {
      PrintSpillTimespanOption(option, ssd_to_hdd_spill_timespan);
    }
  }

  if (!storage.empty()) {
    output.push_back(std::move(storage));
  }
  if (!ssd_to_hdd_spill_timespan.empty()) {
    output.push_back(std::move(ssd_to_hdd_spill_timespan));
  }

  return absl::StrJoin(output, "\n");
}

void SpangresSchemaPrinterImpl::PrintStorageOption(
    const google::spanner::emulator::backend::ddl::SetOption& option, std::string& out) const {
  StrAppend(&out, "STORAGE ");
  if (option.null_value()) {
    StrAppend(&out, "NULL");
  } else if (option.bool_value()) {
    StrAppend(&out, QuoteStringLiteral("ssd"));
  } else {
    StrAppend(&out, QuoteStringLiteral("hdd"));
  }
}

void SpangresSchemaPrinterImpl::PrintSpillTimespanOption(
    const google::spanner::emulator::backend::ddl::SetOption& option, std::string& out) const {
  StrAppend(&out, "SSD_TO_HDD_SPILL_TIMESPAN ");
  if (option.null_value()) {
    StrAppend(&out, "NULL");
  } else {
    absl::string_view spill_timespan = option.string_list_value(0);
    absl::ConsumePrefix(&spill_timespan, "disk:");
    StrAppend(&out, QuoteStringLiteral(spill_timespan));
  }
}

std::string SpangresSchemaPrinterImpl::PrintAlterLocalityGroup(
    const google::spanner::emulator::backend::ddl::AlterLocalityGroup& statement) const {
  std::vector<std::string> output;
  const std::string existence_modifier =
      statement.existence_modifier() ==
              google::spanner::emulator::backend::ddl::ExistenceModifier::IF_EXISTS
          ? "IF EXISTS "
          : "";
  std::string base =
      StrCat("ALTER LOCALITY GROUP ", existence_modifier,
             QuoteQualifiedIdentifier(statement.locality_group_name()));
  output.push_back(std::move(base));

  std::string storage, ssd_to_hdd_spill_timespan;
  for (const google::spanner::emulator::backend::ddl::SetOption& option :
       statement.set_options().options()) {
    if (absl::EqualsIgnoreCase(option.option_name(),
                               internal::PostgreSQLConstants::
                                   kInternalLocalityGroupStorageOptionName)) {
      PrintStorageOption(option, storage);
    } else if (absl::EqualsIgnoreCase(
                   option.option_name(),
                   internal::PostgreSQLConstants::
                       kInternalLocalityGroupSpillTimeSpanOptionName)) {
      PrintSpillTimespanOption(option, ssd_to_hdd_spill_timespan);
    }
  }

  if (!storage.empty()) {
    output.push_back(std::move(storage));
  }
  if (!ssd_to_hdd_spill_timespan.empty()) {
    output.push_back(std::move(ssd_to_hdd_spill_timespan));
  }

  return absl::StrJoin(output, "\n");
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintDropLocalityGroup(
    const google::spanner::emulator::backend::ddl::DropLocalityGroup& statement) const {
  std::string existence_modifier =
      statement.existence_modifier() ==
              google::spanner::emulator::backend::ddl::ExistenceModifier::IF_EXISTS
          ? "IF EXISTS "
          : "";
  return Substitute("DROP LOCALITY GROUP $0$1", existence_modifier,
                    QuoteQualifiedIdentifier(statement.locality_group_name()));
}

absl::StatusOr<std::string>
SpangresSchemaPrinterImpl::PrintAlterColumnLocalityGroup(
    const google::spanner::emulator::backend::ddl::SetColumnOptions& statement) const {
  std::string locality_group;
  for (const google::spanner::emulator::backend::ddl::SetOption& option : statement.options()) {
    if (option.option_name() == "locality_group") {
      if (option.null_value()) {
        StrAppend(&locality_group, "NULL");
      } else {
        StrAppend(&locality_group, option.string_value());
      }
    }
  }
  if (locality_group.empty()) {
    return absl::InvalidArgumentError(
        "Locality group name is missing in the <ALTER COLUMN ... SET LOCALITY "
        "GROUP> statement.");
  }

  return Substitute(
      "ALTER TABLE $0 ALTER COLUMN $1 SET LOCALITY GROUP $2",
      QuoteQualifiedIdentifier(statement.column_path().at(0).table_name()),
      QuoteQualifiedIdentifier(statement.column_path().at(0).column_name()),
      locality_group);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintDropSchema(
    const google::spanner::emulator::backend::ddl::DropSchema& statement) const {
  std::vector<std::string> drop_schema_clauses;
  drop_schema_clauses.push_back("DROP SCHEMA");
  if (statement.has_if_exists() && statement.if_exists()) {
    drop_schema_clauses.push_back("IF EXISTS");
  }
  drop_schema_clauses.push_back(QuoteIdentifier(statement.schema_name()));
  return absl::StrJoin(drop_schema_clauses, " ");
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintDropFunction(
    const google::spanner::emulator::backend::ddl::DropFunction& statement) const {
  std::string if_exists = "";
  if (statement.existence_modifier() == google::spanner::emulator::backend::ddl::IF_EXISTS) {
    if_exists = "IF EXISTS ";
  }
  switch (statement.function_kind()) {
    case google::spanner::emulator::backend::ddl::Function::VIEW:
      return Substitute("DROP VIEW $0$1", if_exists,
                        QuoteQualifiedIdentifier(statement.function_name()));
    case google::spanner::emulator::backend::ddl::Function::INVALID_KIND:
      ZETASQL_RET_CHECK_FAIL() << "Only VIEW is supported as a function kind";
  }
  // Should never get here.
  ZETASQL_RET_CHECK_FAIL() << "Unknown Function type:"
                   << static_cast<int64_t>(statement.function_kind());
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintRenameTable(
    const google::spanner::emulator::backend::ddl::RenameTable& statement) const {
  std::string output;
  const char* sep = "";
  for (const google::spanner::emulator::backend::ddl::RenameTable::RenameOp& op : statement.rename_op()) {
    StrAppend(&output, sep, "ALTER TABLE ",
              QuoteQualifiedIdentifier(op.from_name()), " RENAME TO ",
              QuoteQualifiedIdentifier(op.to_name()));
    sep = ", ";
  }
  return output;
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintAlterIndex(
    const google::spanner::emulator::backend::ddl::AlterIndex& statement) const {
  std::string alter_type;
  std::string column_name;
  switch (statement.alter_type_case()) {
    case google::spanner::emulator::backend::ddl::AlterIndex::kAddStoredColumn: {
      alter_type = " ADD INCLUDE COLUMN ";
      column_name =
          QuoteIdentifier(statement.add_stored_column().column_name());
      break;
    }
    case google::spanner::emulator::backend::ddl::AlterIndex::kDropStoredColumn: {
      alter_type = " DROP INCLUDE COLUMN ";
      column_name = QuoteIdentifier(statement.drop_stored_column());
      break;
    }
    case google::spanner::emulator::backend::ddl::AlterIndex::kSetOptions: {
      alter_type = " SET LOCALITY GROUP ";
      for (const google::spanner::emulator::backend::ddl::SetOption& option :
           statement.set_options().options()) {
        if (option.option_name() == "locality_group") {
          if (option.null_value()) {
            column_name = "NULL";
          } else {
            column_name = option.string_value();
          }
          break;
        }
      }
      if (column_name.empty()) {
        return absl::InvalidArgumentError(
            "Locality group option not found in <ALTER INDEX ... SET LOCALITY "
            "GROUP> statement.");
      }
      break;
    }
    default: {
      return absl::UnimplementedError(
          StrCat("ALTER INDEX does not support alter type:",
                 statement.alter_type_case()));
    }
  }

  return StrCat("ALTER INDEX ",
                QuoteQualifiedIdentifier(statement.index_name()), alter_type,
                column_name);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCreateIndex(
    const google::spanner::emulator::backend::ddl::CreateIndex& statement) const {
  std::string modifier = "";
  if (statement.unique()) {
    modifier = "UNIQUE ";
  }

  std::string if_not_exists = "";
  if (statement.existence_modifier() == google::spanner::emulator::backend::ddl::IF_NOT_EXISTS) {
    if_not_exists = "IF NOT EXISTS ";
  }

  std::vector<std::string> key_parts;
  for (const google::spanner::emulator::backend::ddl::KeyPartClause& index_part : statement.key()) {
    std::string key_part;
    const std::string name = QuoteIdentifier(index_part.key_name());
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view sort_order,
                     PrintSortOrder(index_part.order()));
    key_parts.push_back(StrCat(name, sort_order));
  }

  std::vector<std::string> include_columns;
  for (const google::spanner::emulator::backend::ddl::StoredColumnDefinition& col :
       statement.stored_column_definition()) {
    include_columns.push_back(QuoteIdentifier(col.name()));
  }
  std::string include =
      include_columns.empty()
          ? ""
          : StrCat(" INCLUDE (", absl::StrJoin(include_columns, ", "), ")");

  std::string locality_group = "";
  for (const google::spanner::emulator::backend::ddl::SetOption& option : statement.set_options()) {
    if (option.option_name() == "locality_group") {
      StrAppend(&locality_group, " LOCALITY GROUP ");
      if (option.null_value()) {
        StrAppend(&locality_group, "NULL");
      } else {
        StrAppend(&locality_group, option.string_value());
      }
    }
  }

  // Interleave in should be put before <WHERE> clause, otherwise it may cause
  // postgresql parser error.
  std::string interleave_in = "";
  if (statement.has_interleave_in_table()) {
    interleave_in =
        StrCat(" INTERLEAVE IN ",
               QuoteQualifiedIdentifier(statement.interleave_in_table()));
  }

  std::vector<std::string> conditions;

  // Null filtered indexes are not supported in Spangres, therefore it should
  // not be possible for them to appear in schema.
  ZETASQL_RET_CHECK(!statement.null_filtered());

  for (const std::string& column : statement.null_filtered_column()) {
    conditions.push_back(CreateNotNullCondition(column));
  }

  std::string where = "";
  if (!conditions.empty()) {
    where = Substitute(" WHERE ($0)", absl::StrJoin(conditions, " AND "));
  }

  return StrCat("CREATE ", modifier, "INDEX ", if_not_exists,
                QuoteIdentifier(GetSchemaLocalName(statement.index_name())),
                " ON ", QuoteQualifiedIdentifier(statement.index_base_name()),
                " (", absl::StrJoin(key_parts, ", "), ")", include,
                locality_group, interleave_in, where);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCreateSchema(
    const google::spanner::emulator::backend::ddl::CreateSchema& statement) const {
  std::vector<std::string> create_schema_clauses;
  create_schema_clauses.push_back("CREATE SCHEMA");
  switch (statement.existence_modifier()) {
    case google::spanner::emulator::backend::ddl::CreateSchema::NONE:
      break;
    case google::spanner::emulator::backend::ddl::CreateSchema::IF_NOT_EXISTS:
      create_schema_clauses.push_back("IF NOT EXISTS");
      break;
    default:
      return absl::UnimplementedError(
          StrCat("CREATE SCHEMA existence modifier ",
                 statement.existence_modifier(), " not supported."));
  }
  create_schema_clauses.push_back(
      QuoteIdentifier(statement.schema_name()));
  return absl::StrJoin(create_schema_clauses, " ");
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCreateSequence(
    const google::spanner::emulator::backend::ddl::CreateSequence& statement) const {
  std::vector<std::string> create_statement_clauses;
  create_statement_clauses.push_back("CREATE SEQUENCE");
  const bool if_not_exits = statement.has_existence_modifier() &&
                            statement.existence_modifier() ==
                                google::spanner::emulator::backend::ddl::ExistenceModifier::IF_NOT_EXISTS;
  if (if_not_exits) {
    create_statement_clauses.push_back("IF NOT EXISTS");
  }
  create_statement_clauses.push_back(
      QuoteQualifiedIdentifier(statement.sequence_name()));
  std::optional<int64_t> skip_range_min, skip_range_max;
  for (const ::google::spanner::emulator::backend::ddl::SetOption& option : statement.set_options()) {
    if (option.option_name() == "sequence_kind") {
      ZETASQL_RET_CHECK_EQ(option.string_value(), "bit_reversed_positive");
      create_statement_clauses.push_back("BIT_REVERSED_POSITIVE");
    } else if (option.option_name() == "start_with_counter") {
      create_statement_clauses.push_back(
          absl::Substitute("START COUNTER WITH $0", option.int64_value()));
    } else if (option.option_name() == "skip_range_min") {
      skip_range_min = option.int64_value();
    } else if (option.option_name() == "skip_range_max") {
      skip_range_max = option.int64_value();
    }
    // Skip other unsupported options.
  }

  if (skip_range_min.has_value()) {
    ZETASQL_RET_CHECK(skip_range_max.has_value());
    create_statement_clauses.push_back(absl::Substitute(
        "SKIP RANGE $0 $1", skip_range_min.value(), skip_range_max.value()));
  }
  return absl::StrJoin(create_statement_clauses, " ");
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintAlterSequence(
    const google::spanner::emulator::backend::ddl::AlterSequence& statement) const {
  std::vector<std::string> alter_statement_clauses;
  alter_statement_clauses.push_back("ALTER SEQUENCE");
  const bool if_exists = statement.has_existence_modifier() &&
                        statement.existence_modifier() ==
                            google::spanner::emulator::backend::ddl::ExistenceModifier::IF_EXISTS;
  if (if_exists) {
    alter_statement_clauses.push_back("IF EXISTS");
  }
  alter_statement_clauses.push_back(
      QuoteQualifiedIdentifier(statement.sequence_name()));

  std::optional<int64_t> skip_range_min, skip_range_max;
  if (statement.has_set_options()) {
    for (const ::google::spanner::emulator::backend::ddl::SetOption& option :
         statement.set_options().options()) {
      if (option.option_name() == "start_with_counter") {
        alter_statement_clauses.push_back(
            absl::Substitute("RESTART COUNTER WITH $0", option.int64_value()));
      } else if (option.option_name() == "skip_range_min") {
        skip_range_min = option.int64_value();
      } else if (option.option_name() == "skip_range_max") {
        skip_range_max = option.int64_value();
      }
      // Skip other non-supported options.
    }

    if (skip_range_min.has_value() && skip_range_max.has_value()) {
      alter_statement_clauses.push_back(absl::Substitute(
          "SKIP RANGE $0 $1", skip_range_min.value(), skip_range_max.value()));
    }
  }

  return absl::StrJoin(alter_statement_clauses, " ");
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintDropSequence(
    const google::spanner::emulator::backend::ddl::DropSequence& statement) const {
  return absl::Substitute("DROP SEQUENCE $0",
                          QuoteQualifiedIdentifier(statement.sequence_name()));
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintAnalyze(
    const google::spanner::emulator::backend::ddl::Analyze& statement) const {
  return "ANALYZE";
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintSQLSecurityType(
    google::spanner::emulator::backend::ddl::Function::SqlSecurity sql_security) const {
  switch (sql_security) {
    case google::spanner::emulator::backend::ddl::Function::INVOKER:
      return "INVOKER";
      break;
    case google::spanner::emulator::backend::ddl::Function::UNSPECIFIED_SQL_SECURITY:
      ZETASQL_RET_CHECK_FAIL() << "Only SQL SECURITY INVOKER or DEFINER is supported.";
  }
  ZETASQL_RET_CHECK_FAIL() << "Unsupported sql security type: "
                   << static_cast<int64_t>(sql_security);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCreateFunction(
    const google::spanner::emulator::backend::ddl::CreateFunction& statement) const {
  switch (statement.function_kind()) {
    case google::spanner::emulator::backend::ddl::Function_Kind::Function_Kind_VIEW: {
      std::string view_template =
          statement.is_or_replace()
              ? "CREATE OR REPLACE VIEW $0 SQL SECURITY $1 AS $2"
              : "CREATE VIEW $0 SQL SECURITY $1 AS $2";

      ZETASQL_ASSIGN_OR_RETURN(absl::string_view security_type,
                       PrintSQLSecurityType(statement.sql_security()));

      return Substitute(
          view_template, QuoteQualifiedIdentifier(statement.function_name()),
          security_type, statement.sql_body_origin().original_expression());
    }
    case google::spanner::emulator::backend::ddl::Function_Kind::Function_Kind_INVALID_KIND:
      ZETASQL_RET_CHECK_FAIL()
          << "Only VIEW and scalar FUNCTION are supported as function types.";
  }
  // Should never get here.
  ZETASQL_RET_CHECK_FAIL() << "Unknown Function type:"
                   << static_cast<int64_t>(statement.function_kind());
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintType(
    const google::spanner::emulator::backend::ddl::ColumnDefinition& column) const {
  std::string array = "";
  google::spanner::emulator::backend::ddl::ColumnDefinition::Type type;
  int length;

  // For array types, actual type will be stored as pseudo-ColumnDefinition in
  // the array_subtype field. To avoid conditionals down below, we just
  // extract actual type and actual length for further use.
  if (column.type() == google::spanner::emulator::backend::ddl::ColumnDefinition::ARRAY) {
    array = "[]";
    type = column.array_subtype().type();
    length = column.array_subtype().length();
  } else {
    type = column.type();
    length = column.length();
  }

  if (column.set_options_size() == 1 &&
      column.set_options(0).option_name() ==
          PGConstants::kInternalCommitTimestampOptionName) {
    ZETASQL_RET_CHECK(column.set_options(0).has_bool_value() &&
              column.set_options(0).bool_value());
    ZETASQL_RET_CHECK_EQ(column.type(), google::spanner::emulator::backend::ddl::ColumnDefinition::TIMESTAMP);

    return "spanner.commit_timestamp";
  }

  auto type_name_it = kTypeMap.find(type);
  if (type_name_it == kTypeMap.end()) {
    // type not found -> schema is not a valid Spangres schema
    return StatementTranslationError(
        StrCat("Spanner type <",
               google::spanner::emulator::backend::ddl::ColumnDefinition::Type_Name(type),
               "> is not supported."));
  }
  absl::string_view type_name = type_name_it->second;
  std::string type_modifiers = "";

  // Translate type modifiers:
  //   - for STRING->VARCHAR, just set length of output field to the length of
  //     the input field, or omit completely if set to MAX
  //   - for BYTES->BYTEA, PostgreSQL does not support setting max length for
  //     the BYTEA, so we just check and enforce input field to be of max
  //     length
  //   - for NUMERIC->NUMERIC, set output field precision and scale to the
  //   ones
  //     used in Spanner by default
  switch (type) {
    case google::spanner::emulator::backend::ddl::ColumnDefinition::STRING: {
      if (length != 0 && length != PGConstants::kMaxStringLength) {
        type_modifiers = StrCat("(", length, ")");
      }
      break;
    }

    // There is no way to limit the size of bytea in PostgreSQL, so we enforce
    // all BYTES columns in Spangres databases to be of MAX size
    case google::spanner::emulator::backend::ddl::ColumnDefinition::BYTES: {
      if (length != 0 && length != PGConstants::kMaxBytesLength) {
        return StatementTranslationError(
            "Translation is supported only for <BYTES> columns of <MAX> "
            "size.");
      }
      break;
    }

    case google::spanner::emulator::backend::ddl::ColumnDefinition::PG_NUMERIC: {
      // For PG.NUMERIC with no scale and precision modifier, breaking out is
      // fine. This won't work for fixed precision PG.NUMERIC columns, which
      // are not yet supported.
      // TODO: Add support for fixed-precision numeric DDL.
      break;
    }

    default:
      // no type modifiers for the rest
      break;
  }

  return StrCat(type_name, type_modifiers, array);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintTypeForEmulator(
    const google::spanner::emulator::backend::ddl::ColumnDefinition& column)
    const {
  google::spanner::emulator::backend::ddl::ColumnDefinition sdl_column;
  if (!sdl_column.ParseFromString(column.SerializeAsString())) {
    return absl::InvalidArgumentError(
        "Failed to convert from the emulator's ColumnDefinition to the "
        "SDL-equivalent.");
  }
  return PrintType(sdl_column);
}

absl::StatusOr<std::string>
SpangresSchemaPrinterImpl::PrintRowDeletionPolicyForEmulator(
    const google::spanner::emulator::backend::ddl::RowDeletionPolicy& policy)
    const {
  google::spanner::emulator::backend::ddl::RowDeletionPolicy sdl_policy;
  if (!sdl_policy.ParseFromString(policy.SerializeAsString())) {
    return absl::InvalidArgumentError(
        "Failed to convert from the emulator's RowDeletionPolicy to the "
        "SDL-equivalent.");
  }
  return StrCat("INTERVAL ",
                RowDeletionPolicyToInterval(sdl_policy.older_than().unit() *
                                                sdl_policy.older_than().count(),
                                            sdl_policy.column_name()));
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintColumn(
    const google::spanner::emulator::backend::ddl::ColumnDefinition& column) const {
  std::string constraint;
  if (column.has_generated_column()) {
    ZETASQL_RET_CHECK(column.generated_column().has_expression_origin());
    const google::spanner::emulator::backend::ddl::SQLExpressionOrigin& generated_column =
        column.generated_column().expression_origin();
    if (column.generated_column().has_stored() &&
        column.generated_column().stored()) {
      constraint =
        StrCat(" GENERATED ALWAYS AS (",
               generated_column.original_expression(), ")", " STORED");
    } else {
      constraint =
        StrCat(" GENERATED ALWAYS AS (",
               generated_column.original_expression(), ")", " VIRTUAL");
    }
  }

  if (column.has_column_default()) {
    ZETASQL_RET_CHECK(column.column_default().has_expression_origin());
    const google::spanner::emulator::backend::ddl::SQLExpressionOrigin& default_column =
        column.column_default().expression_origin();
    const std::string expression_output =
        default_column.has_original_expression()
            ? StrCat(" ", default_column.original_expression())
            : "";
    constraint = StrCat(" DEFAULT", expression_output);
  }

  if (column.has_identity_column()) {
    ZETASQL_RET_CHECK(!column.has_column_default() && !column.has_generated_column());
    StrAppend(&constraint, " GENERATED BY DEFAULT AS IDENTITY");
    std::vector<std::string> identity_column_options;
    if (column.identity_column().has_type()) {
      identity_column_options.push_back(
          ColumnDefinition::IdentityColumnDefinition::Type_Name(
              column.identity_column().type()));
    }
    if (column.identity_column().has_skip_range_min() &&
        column.identity_column().has_skip_range_max()) {
      identity_column_options.push_back(
          StrCat("SKIP RANGE ", column.identity_column().skip_range_min(), " ",
                 column.identity_column().skip_range_max()));
    } else if (column.identity_column().has_skip_range_min() !=
               column.identity_column().has_skip_range_max()) {
      return absl::InvalidArgumentError(
          "Skip range must specify both min and max values.");
    }
    if (column.identity_column().has_start_with_counter()) {
      identity_column_options.push_back(
          StrCat("START COUNTER WITH ",
                 column.identity_column().start_with_counter()));
    }
    if (!identity_column_options.empty()) {
      StrAppend(&constraint, " (", absl::StrJoin(identity_column_options, " "),
                ")");
    }
    // Ignore NO MINVALUE | NO MAXVALUE | NO CYCLE
  }

  if (column.has_vector_length()) {
    const int64_t vector_length = column.vector_length();
      constraint = absl::Substitute(" VECTOR LENGTH $0", vector_length);
  }

  StrAppend(&constraint, column.not_null() ? " NOT NULL" : "");
  StrAppend(&constraint, column.hidden() ? " HIDDEN" : "");
  for (const google::spanner::emulator::backend::ddl::SetOption& option : column.set_options()) {
    if (option.option_name() == "locality_group") {
      StrAppend(&constraint, " LOCALITY GROUP ");
      if (option.null_value()) {
        StrAppend(&constraint, "NULL");
      } else {
        StrAppend(&constraint, option.string_value());
      }
    }
  }
  ZETASQL_ASSIGN_OR_RETURN(absl::string_view printed_type, PrintType(column));
  return StrCat(QuoteIdentifier(column.column_name()), " ", printed_type,
                constraint);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintForeignKeyAction(
    absl::string_view action_type,
    const google::spanner::emulator::backend::ddl::ForeignKey::Action action) const {
  switch (action) {
    case google::spanner::emulator::backend::ddl::ForeignKey::ACTION_UNSPECIFIED:
    case google::spanner::emulator::backend::ddl::ForeignKey::NO_ACTION:
      return "";
    case google::spanner::emulator::backend::ddl::ForeignKey::CASCADE:
      return StrCat(" ", action_type, " CASCADE");
    default:
      ZETASQL_RET_CHECK_FAIL() << "Unknown foreign key action type:"
                       << static_cast<int64_t>(action);
  }
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintForeignKey(
    const google::spanner::emulator::backend::ddl::ForeignKey& foreign_key) const {
  // Not supported by Cloud Spanner
  ZETASQL_RET_CHECK(foreign_key.enforced());

  std::string constraint_name = "";
  if (foreign_key.has_constraint_name()) {
    constraint_name = StrCat(
        "CONSTRAINT ", QuoteIdentifier(foreign_key.constraint_name()), " ");
  }

  std::vector<std::string> constrained_columns;
  for (const std::string& fk_column : foreign_key.constrained_column_name()) {
    constrained_columns.push_back(QuoteIdentifier(fk_column));
  }

  std::vector<std::string> referenced_columns;
  for (const std::string& pk_column : foreign_key.referenced_column_name()) {
    referenced_columns.push_back(QuoteIdentifier(pk_column));
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::string_view printed_delete_action,
                   PrintForeignKeyAction("ON DELETE", foreign_key.on_delete()));
  return StrCat(constraint_name, "FOREIGN KEY (",
                absl::StrJoin(constrained_columns, ", "), ") REFERENCES ",
                QuoteQualifiedIdentifier(foreign_key.referenced_table_name()),
                "(", absl::StrJoin(referenced_columns, ", "), ")",
                printed_delete_action);
}

absl::StatusOr<std::string>
SpangresSchemaPrinterImpl::PrintInterleaveDeleteAction(
    const google::spanner::emulator::backend::ddl::InterleaveClause::Action action) const {
  switch (action) {
    case google::spanner::emulator::backend::ddl::InterleaveClause::NO_ACTION:
      return " NO ACTION";
    case google::spanner::emulator::backend::ddl::InterleaveClause::CASCADE:
      return " CASCADE";
  }
  // Should never get here.
  ZETASQL_RET_CHECK_FAIL() << "Unknown Interleave delete action type:"
                   << static_cast<int64_t>(action);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintInterleaveClause(
    const InterleaveClause& interleave_clause) const {
  std::string output = "INTERLEAVE IN ";
  if (interleave_clause.type() == InterleaveClause::IN_PARENT) {
    StrAppend(&output, "PARENT ");
  }
  StrAppend(&output, QuoteQualifiedIdentifier(interleave_clause.table_name()));

  if (interleave_clause.has_on_delete()) {
    StrAppend(&output, " ON DELETE");

    ZETASQL_ASSIGN_OR_RETURN(std::string action, PrintInterleaveDeleteAction(
                                             interleave_clause.on_delete()));
    StrAppend(&output, action);
  }
  return output;
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintCheckConstraint(
    const CheckConstraint& check_constraint) const {
  std::string output;
  if (!check_constraint.name().empty()) {
    StrAppend(&output, Substitute("CONSTRAINT $0 ",
                                  QuoteIdentifier(check_constraint.name())));
  }

  StrAppend(
      &output,
      Substitute("CHECK($0)",
                 check_constraint.expression_origin().original_expression()));
  return output;
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintSortOrder(
    KeyPartClause::Order order) const {
  switch (order) {
    case KeyPartClause::ASC:
      return " NULLS FIRST";
    case KeyPartClause::DESC:
      return " DESC NULLS LAST";
    case KeyPartClause::ASC_NULLS_LAST:
      return "";
    case KeyPartClause::DESC_NULLS_FIRST:
      return " DESC";
  }

  // Should never get here.
  ZETASQL_RET_CHECK_FAIL() << "Unknown order type:" << static_cast<int64_t>(order);
}

absl::StatusOr<std::string> SpangresSchemaPrinterImpl::PrintPrimaryKey(
    const google::protobuf::RepeatedPtrField<KeyPartClause>& key_parts) const {
  std::string output = "PRIMARY KEY(";

  const char* separator = "";
  for (const KeyPartClause& key : key_parts) {
    if (key.has_order() && key.order() != KeyPartClause::ASC_NULLS_LAST) {
      return StatementTranslationError(
          "Non-default ordering is not supported for primary keys.");
    }

    StrAppend(&output, separator);
    separator = ", ";
    StrAppend(&output, QuoteIdentifier(key.key_name()));
  }

  StrAppend(&output, ")");
  return output;
}

absl::StatusOr<std::vector<std::string>>
SpangresSchemaPrinterImpl::PrintAlterDatabaseSetOptions(
    absl::string_view quoted_db_name,
    const google::protobuf::RepeatedPtrField<google::spanner::emulator::backend::ddl::SetOption>& options) const {
  std::vector<std::string> output;
  std::string base = StrCat("ALTER DATABASE ", quoted_db_name);
  std::vector<std::string> printed_options;

  for (const google::spanner::emulator::backend::ddl::SetOption& option : options) {
    absl::optional<absl::string_view> pg_option_name =
        internal::GetSpangresOptionName(option.option_name());
    // Options not whitelisted for customer use are not printed
    if (!pg_option_name) {
      continue;
    }
    if (option.has_null_value()) {
      output.push_back(
          StrCat(base, " RESET ", QuoteIdentifier(*pg_option_name)));
      continue;
    } else {
      std::string value;
      if (option.has_string_value()) {
        value = QuoteStringLiteral(option.string_value());
      } else if (option.has_int64_value()) {
        value = std::to_string(option.int64_value());
      } else {
        ZETASQL_RET_CHECK_FAIL();
      }
      output.push_back(StrCat(base, " SET ", QuoteIdentifier(*pg_option_name),
                              " = ", value));
    }
  }
  return output;
}

}  // namespace

absl::StatusOr<std::unique_ptr<SpangresSchemaPrinter>>
CreateSpangresDirectSchemaPrinter() {
  return std::make_unique<SpangresSchemaPrinterImpl>();
}

// RowDeletionPolicyToInterval generates a Postgres-format interval string from
// a row deletion policy proto. The conversion always uses the largest possible
// units in its conversion, which may result in a string different (but always
// equivalent) to what the user provided. That is, if the user gave us "14
// DAYS", the conversion here would return "2 WEEKS". This is the best we can
// do, since we don't have access to the original interval string.
std::string RowDeletionPolicyToInterval(int64_t secs,
                                        absl::string_view column) {
  // These are Postgres' ideas of how long each of these are. They represent
  // abstract versions of these, so no leap seconds, leap years, or months that
  // may have 28 or 31 (or 29...) days.
  constexpr int64_t kSecPerMinute = 60;
  constexpr int64_t kSecPerHour = kSecPerMinute * 60;
  constexpr int64_t kSecPerDay = kSecPerHour * 24;
  constexpr int64_t kSecPerWeek = kSecPerDay * 7;
  constexpr int64_t kSecPerMonth = kSecPerDay * 30;

  std::vector<std::string> output;
  // Go build our interval.
  if (secs > kSecPerMonth) {
    output.push_back(StrCat(int(secs / kSecPerMonth), " MONTHS"));
    secs = secs % kSecPerMonth;
  }
  if (secs > kSecPerWeek) {
    output.push_back(StrCat(int(secs / kSecPerWeek), " WEEKS"));
    secs = secs % kSecPerWeek;
  }
  if (secs > kSecPerDay) {
    output.push_back(StrCat(int(secs / kSecPerDay), " DAYS"));
    secs = secs % kSecPerDay;
  }
  if (secs > kSecPerHour) {
    output.push_back(StrCat(int(secs / kSecPerHour), " HOURS"));
    secs = secs % kSecPerHour;
  }
  if (secs > kSecPerMinute) {
    output.push_back(StrCat(int(secs / kSecPerMinute), " MINUTES"));
    secs = secs % kSecPerMinute;
  }
  if (secs > 0) {
    output.push_back(StrCat(secs, " SECONDS"));
  }

  // If we haven't added anything it means we have no seconds, which turns ot 0
  // DAYS.
  if (output.empty()) {
    output.push_back("0 DAYS");
  }

  return StrCat("'", StrJoin(output, " "), "' ON ", QuoteIdentifier(column));
}

std::string RowDeletionPolicyToInterval(
    const google::spanner::emulator::backend::ddl::RowDeletionPolicy& policy) {
  return RowDeletionPolicyToInterval(
      policy.older_than().unit() * policy.older_than().count(),
      policy.column_name());
}

std::vector<google::spanner::emulator::backend::ddl::DDLStatement::StatementCase>
TEST_GetSupportedDDLStatements() {
  return std::vector<google::spanner::emulator::backend::ddl::DDLStatement::StatementCase>(
      kSupportedStatements.begin(), kSupportedStatements.end());
}

}  // namespace spangres
}  // namespace postgres_translator
