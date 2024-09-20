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

#include "third_party/spanner_pg/ddl/pg_to_spanner_ddl_translator.h"

#include <string.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/flag.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/optional.h"
#include "third_party/spanner_pg/ddl/ddl_translator.h"
#include "third_party/spanner_pg/ddl/pg_parse_tree_validator.h"
#include "third_party/spanner_pg/ddl/translation_utils.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/interval_helpers.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "google/protobuf/repeated_ptr_field.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::spangres {

typedef google::protobuf::RepeatedPtrField<google::spanner::emulator::backend::ddl::SetOption> OptionList;

namespace {
using PGConstants = internal::PostgreSQLConstants;
using internal::FieldTypeChecker;
using ::postgres_translator::internal::PostgresCastToNode;

// Translator from PostgreSQL DDL to Spanner schema dialect.
class PostgreSQLToSpannerDDLTranslatorImpl
    : public PostgreSQLToSpannerDDLTranslator {
 public:
  absl::Status Translate(
      const interfaces::ParserOutput& parser_output,
      const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::DDLStatementList& out_statements) const override;

 private:
  class Visitor {
   public:
    Visitor(const PostgreSQLToSpannerDDLTranslatorImpl& ddl_translator,
            const TranslationOptions& options,
            google::spanner::emulator::backend::ddl::DDLStatementList& output)
        : ddl_translator_(ddl_translator), options_(options), output_(output) {}
    absl::Status Visit(const List& parse_tree);
    absl::Status Visit(const RawStmt& raw_statement);

   private:
    void AddStatementToOutput(google::spanner::emulator::backend::ddl::DDLStatement&& statement) {
      *output_.add_statement() = std::move(statement);
    }

    template <typename PgStatementT, typename SchemaStatementT>
    absl::Status TranslateAndAddStatement(
        const PgStatementT& intput_statement,
        std::function<absl::Status(const PgStatementT& pg_statement,
                                   SchemaStatementT& output)>
            translate_function,
        std::function<SchemaStatementT*(google::spanner::emulator::backend::ddl::DDLStatement&)>
            statement_member_selector) {
      google::spanner::emulator::backend::ddl::DDLStatement statement;
      ZETASQL_RETURN_IF_ERROR(translate_function(
          intput_statement, *statement_member_selector(statement)));
      AddStatementToOutput(std::move(statement));
      return absl::OkStatus();
    }

    const PostgreSQLToSpannerDDLTranslatorImpl& ddl_translator_;
    const TranslationOptions& options_;
    google::spanner::emulator::backend::ddl::DDLStatementList& output_;
  };

  friend class Visitor;

  // Helper class grouping functionality related to CREATE INDEX statement
  // translation.
  class CreateIndexStatementTranslator {
   public:
    CreateIndexStatementTranslator(
        const CreateIndexStatementTranslator& other) = delete;
    CreateIndexStatementTranslator& operator=(
        const CreateIndexStatementTranslator& other) = delete;

    CreateIndexStatementTranslator(
        const CreateIndexStatementTranslator&& other) = delete;
    CreateIndexStatementTranslator& operator=(
        CreateIndexStatementTranslator&& other) = delete;

    CreateIndexStatementTranslator(
        const PostgreSQLToSpannerDDLTranslatorImpl& ddl_translator,
        const TranslationOptions& options)
        : ddl_translator_(ddl_translator), options_(options) {}

    absl::Status Translate(const IndexStmt& create_index_statement,
                           google::spanner::emulator::backend::ddl::CreateIndex& out,
                           const TranslationOptions& options);

    absl::Status Translate(const AlterTableStmt& alter_index_statement,
                           google::spanner::emulator::backend::ddl::AlterIndex& out);

   protected:
    absl::StatusOr<absl::string_view> GetColumnName(
        const NullTest& null_test, absl::string_view parent_statement) const;

    absl::Status TranslateWhereNode(
        const Node* node, std::set<absl::string_view>* null_filtered_columns,
        absl::string_view parent_statement);
    absl::Status TranslateNullTest(
        const NullTest* null_test,
        std::set<absl::string_view>* null_filtered_columns,
        absl::string_view parent_statement);
    absl::Status TranslateBoolExpr(
        const BoolExpr* bool_expr,
        std::set<absl::string_view>* null_filtered_columns,
        absl::string_view parent_statement);

    absl::StatusOr<absl::string_view> TranslateIndexElem(
        const IndexElem& index_elem,
        google::spanner::emulator::backend::ddl::KeyPartClause::Order& ordering,
        const TranslationOptions& options);

    absl::StatusOr<google::spanner::emulator::backend::ddl::KeyPartClause::Order> ProcessOrdering(
        const SortByDir& index_elem_ordering,
        const SortByNulls& index_elem_nulls_ordering,
        const TranslationOptions& options) const;

    // We reuse the data structure "InterleaveSpec", originally created for
    // <CREATE TABLE>, for <CREATE INDEX>. Even the parser can guard on grammar
    // level. We still double check the data in translator for safety purpose.
    absl::Status CheckInterleaveInParserOutput(
        const InterleaveSpec* pg_interleave) const;

    absl::Status UnsupportedTranslationError(
        absl::string_view error_mesage) const {
      return ddl_translator_.UnsupportedTranslationError(error_mesage);
    }

    absl::Status WhereClauseTranslationError(
        absl::string_view parent_statement) const {
      return ddl_translator_.UnsupportedTranslationError(absl::Substitute(
          "<WHERE> clause of <$0> statement supports only "
          "conjunction of <column IS NOT NULL> expressions using <AND>, where "
          "<column> is part of an index.",
          parent_statement));
    }

    const PostgreSQLToSpannerDDLTranslatorImpl& ddl_translator_;
    const TranslationOptions& options_;

    // All null filtered columns of the index (meaning WHERE <column> IS NOT
    // NULL predicate applied to them) go here.
    std::set<absl::string_view> null_filtered_columns_;
  };

  friend class CreateIndexStatementTranslator;

  // Class holding the context for CREATE/ALTER TABLE operations, e.g. primary
  // keys, other constraints, etc.
  struct TableContext;

  static absl::StatusOr<google::spanner::emulator::backend::ddl::InterleaveClause::Type>
  GetInterleaveClauseType(InterleaveInType type);

  // Converts PostgreSQL column <TypeName> into Spanner type, honoring extra
  // attributes like length, array size, etc.
  absl::Status ProcessColumnTypeWithAttributes(
      const TypeName& type, const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;

  // Translate<statement> methods convert PostgreSQL <statement> node into the
  // corresponding spanner sdl construct. Methods take resulting statement by
  // reference to allow for more efficient use of protobuf via add*/mutable*
  // methods.
  absl::Status TranslateAlterTable(const AlterTableStmt& alter_statement,
                                   const TranslationOptions& options,
                                   google::spanner::emulator::backend::ddl::AlterTable& out) const;
  absl::Status TranslateCreateTable(const CreateStmt& create_statement,
                                    const TranslationOptions& options,
                                    google::spanner::emulator::backend::ddl::CreateTable& out) const;
  absl::Status TranslateDropStatement(const DropStmt& drop_statement,
                                      const TranslationOptions& options,
                                      google::spanner::emulator::backend::ddl::DDLStatement& out) const;
  absl::Status TranslateDropTable(const DropStmt& drop_statement,
                                  google::spanner::emulator::backend::ddl::DropTable& out,
                                  const TranslationOptions& options) const;
  absl::Status TranslateDropIndex(const DropStmt& drop_statement,
                                  google::spanner::emulator::backend::ddl::DropIndex& out,
                                  const TranslationOptions& options) const;
  absl::Status TranslateDropSchema(const DropStmt& drop_statement,
                                   google::spanner::emulator::backend::ddl::DropSchema& out) const;
  absl::Status TranslateCreateIndex(const IndexStmt& create_index_statement,
                                    const TranslationOptions& options,
                                    google::spanner::emulator::backend::ddl::CreateIndex& out) const;

  absl::Status TranslateAlterIndex(const AlterTableStmt& alter_index_statement,
                                   const TranslationOptions& options,
                                   google::spanner::emulator::backend::ddl::AlterIndex& out) const;
  absl::Status TranslateCreateSchema(const CreateSchemaStmt& create_statement,
                                     google::spanner::emulator::backend::ddl::CreateSchema& out) const;
  absl::Status TranslateVacuum(const VacuumStmt& vacuum_statement,
                               const TranslationOptions& options,
                               google::spanner::emulator::backend::ddl::Analyze& out) const;
  absl::Status TranslateCreateView(const ViewStmt& view_statement,
                                   const TranslationOptions& options,
                                   google::spanner::emulator::backend::ddl::CreateFunction& out) const;
  absl::Status TranslateDropView(const DropStmt& drop_statement,
                                 google::spanner::emulator::backend::ddl::DropFunction& out) const;
  absl::Status TranslateDropChangeStream(
      const DropStmt& drop_change_stream_statement,
      const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::DropChangeStream& out) const;

  // Updates table translation <context> with the information about translated
  // table <constraint>. If constraint is defined on column level, the
  // pointer to this column is given in <target_column>. Otherwise (constraint
  // is defined on the table level), <target_column> is nullptr.
  //
  // Currently supported constraints: NULL, NOT NULL, PRIMARY KEY, FOREIGN KEY.
  absl::Status ProcessTableConstraint(
      const Constraint& constraint, const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::ColumnDefinition* target_column,
      TableContext& context) const;

  absl::Status TranslateInterleaveIn(
      const InterleaveSpec* pg_interleave,
      google::spanner::emulator::backend::ddl::InterleaveClause* interleave_out,
      const TranslationOptions& options) const;

  // Translates <FOREIGN KEY> <constraint> and adds information about it to the
  // table translation <context>. If <FOREIGN KEY> clause is attached to
  // the column, the pointer to this column is given in <target_column>.
  // Otherwise (clause is defined on the table level), <target_column> is
  // nullptr.
  absl::Status TranslateForeignKey(
      const Constraint& constraint,
      const google::spanner::emulator::backend::ddl::ColumnDefinition* target_column,
      google::spanner::emulator::backend::ddl::ForeignKey& out) const;

  absl::Status TranslateCheckConstraint(
      const Constraint& constraint, const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::CheckConstraint& out) const;

  absl::Status TranslateGeneratedColumn(
      const Constraint& generated, const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;

  absl::Status TranslateColumnDefault(
      const Constraint& column_default, const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;

  absl::Status TranslateVectorLength(
      const Constraint& vector_length, const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;

  // Translates PostgreSQL <column_definition> used with <CREATE TABLE> and
  // <ALTER TABLE> statements. Updates table translation state stored in
  // <context> with information about processed columns.
  absl::Status TranslateColumnDefinition(
      const ColumnDef& column_definition, const TranslationOptions& options,
      TableContext& context, google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;
  absl::Status TranslateCreateDatabase(const CreatedbStmt& create_statement,
                                       google::spanner::emulator::backend::ddl::CreateDatabase& out) const;
  absl::StatusOr<internal::PGAlterOption> TranslateOption(
      absl::string_view option_name, const TranslationOptions& options) const;

  absl::Status UnsupportedTranslationError(
      absl::string_view error_mesage) const;

  absl::StatusOr<google::spanner::emulator::backend::ddl::InterleaveClause::Action>
  GetInterleaveParentDeleteActionType(char action) const;

  absl::StatusOr<google::spanner::emulator::backend::ddl::ForeignKey::Action> GetForeignKeyAction(
      char action) const;

  absl::StatusOr<std::string> GetSchemaName(
      const String& value, absl::string_view parent_statement_type) const;

  absl::StatusOr<std::string> GetTableName(
      const RangeVar& range_var, absl::string_view parent_statement_type) const;

  absl::StatusOr<std::string> GetChangeStreamTableName(
      const RangeVar& range_var, absl::string_view parent_statement_type) const;

  absl::StatusOr<std::string> GetFunctionName(
      const ObjectWithArgs& object_with_args, bool is_grant,
      absl::string_view parent_statement_type) const;

  // Parses PG column type represented as a List into schema name and type name.
  absl::Status ParseColumnType(const TypeName& type, std::string& schema_name,
                               std::string& type_name) const;

  // Converts PostgreSQL type with type modifiers into Spanner type.
  absl::Status ProcessPostgresType(absl::string_view type_name,
                                   const TypeName& type,
                                   const TranslationOptions& options,
                                   google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;

  // Converts extended type into Spanner type, honoring all the extra options.
  absl::Status ProcessExtendedType(absl::string_view type_name,
                                   const TypeName& type,
                                   google::spanner::emulator::backend::ddl::ColumnDefinition& out) const;

  // Take a Ttl node and apply it to the provided RowDeletionPolicy proto.
  absl::Status TranslateRowDeletionPolicy(
      const Ttl& ttl,
      google::spanner::emulator::backend::ddl::RowDeletionPolicy& row_deletion_policy) const;

  // Translate the CREATE CHANGE STREAM statement.
  absl::Status TranslateCreateChangeStream(
      const CreateChangeStreamStmt& create_change_stream_stmt,
      const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::CreateChangeStream& out) const;

  absl::Status PopulateChangeStreamForClause(
      const List* opt_for_or_drop_for_tables, bool for_or_drop_for_all,
      absl::string_view parent_statement,
      google::spanner::emulator::backend::ddl::ChangeStreamForClause* change_stream_for_clause) const;

  absl::Status PopulateChangeStreamOptions(
      List* opt_options, absl::string_view parent_statement,
      OptionList* options_out, const TranslationOptions& options) const;

  // Translate the ALTER CHANGE STREAM statement.
  absl::Status TranslateAlterChangeStream(
      const AlterChangeStreamStmt& alter_change_stream_stmt,
      const TranslationOptions& options,
      google::spanner::emulator::backend::ddl::AlterChangeStream& out) const;

  // SEQUENCE translation statements.
  absl::Status ProcessSequenceSkipRangeOption(
      const DefElem& skip_range,
      ::google::spanner::emulator::backend::ddl::SetOption& skip_range_min_option,
      ::google::spanner::emulator::backend::ddl::SetOption& skip_range_max_option) const;
  absl::Status TranslateCreateSequence(const CreateSeqStmt& create_statement,
                                       const TranslationOptions& options,
                                       google::spanner::emulator::backend::ddl::CreateSequence& out) const;
  absl::Status TranslateDropSequence(const DropStmt& drop_statement,
                                     const TranslationOptions& options,
                                     google::spanner::emulator::backend::ddl::DropSequence& out) const;
  absl::Status TranslateAlterSequence(const AlterSeqStmt& alter_statement,
                                      const TranslationOptions& options,
                                      google::spanner::emulator::backend::ddl::AlterSequence& out) const;

  // Rename translation statement.
  absl::Status TranslateRenameStatement(const RenameStmt& rename_statement,
                                        const TranslationOptions& options,
                                        google::spanner::emulator::backend::ddl::AlterTable& out) const;
  // Rename Chain translation statement.
  absl::Status TranslateTableChainedRenameStatement(
      const TableChainedRenameStmt& table_chained_rename_statement,
      const TranslationOptions& options, google::spanner::emulator::backend::ddl::RenameTable& out) const;
};

// IntervalString extracts the interval from a TTL struct, or returns an
// error if the chain of nodes from the TTL struct aren't valid.
absl::StatusOr<char*> IntervalString(const Ttl& node) {
  ZETASQL_ASSIGN_OR_RETURN(const TypeCast* tcnode,
                   (DowncastNode<TypeCast, T_TypeCast>(node.interval)));
  if (tcnode->arg == nullptr) {
    return UnsupportedTranslationError("null arg field in ttl struct");
  }
  ZETASQL_ASSIGN_OR_RETURN(const A_Const* acnode,
                   (DowncastNode<A_Const, T_A_Const>(tcnode->arg)));
  return acnode->val.sval.sval;
}

// Returns true if PG list is empty or nullptr
bool IsListEmpty(const List* list) { return list_length(list) == 0; }

const absl::flat_hash_map<absl::string_view,
                          absl::optional<google::spanner::emulator::backend::ddl::ColumnDefinition::Type>>&
GetTypeMap() {
  static auto* map = new absl::flat_hash_map<
      absl::string_view, absl::optional<google::spanner::emulator::backend::ddl::ColumnDefinition::Type>>{
      // Both <bool> and <boolean> are named <bool> in PG AST.
      {"bool", google::spanner::emulator::backend::ddl::ColumnDefinition::BOOL},
      {"bytea", google::spanner::emulator::backend::ddl::ColumnDefinition::BYTES},
      // Both <float8> and <double precision> are named <float8> in PG AST.
      {"float4", google::spanner::emulator::backend::ddl::ColumnDefinition::FLOAT},
      {"float8", google::spanner::emulator::backend::ddl::ColumnDefinition::DOUBLE},
      // Both <int8_t> and <bigint> are named <int8_t> in PG AST.
      {"int8", google::spanner::emulator::backend::ddl::ColumnDefinition::INT64},
      {"text", google::spanner::emulator::backend::ddl::ColumnDefinition::STRING},
      // Both <varchar> and <character varying> are named <varchar> in PG AST.
      {"varchar", google::spanner::emulator::backend::ddl::ColumnDefinition::STRING},
      {"date", google::spanner::emulator::backend::ddl::ColumnDefinition::DATE},
      // Both <timestamp with time zone> and <timestamptz> are named
      // <timestamptz> in PG AST.
      // Only timestamp with timezone is supported
      {"timestamptz", google::spanner::emulator::backend::ddl::ColumnDefinition::TIMESTAMP},
      {"numeric", google::spanner::emulator::backend::ddl::ColumnDefinition::PG_NUMERIC},
      {"decimal", google::spanner::emulator::backend::ddl::ColumnDefinition::PG_NUMERIC},
      {"jsonb", google::spanner::emulator::backend::ddl::ColumnDefinition::PG_JSONB},
      // Unsupported standard types from pg_catalog schema
      {"aclitem", absl::nullopt},
      {"any", absl::nullopt},
      {"anyarray", absl::nullopt},
      {"anycompatible", absl::nullopt},
      {"anycompatiblearray", absl::nullopt},
      {"anycompatiblenonarray", absl::nullopt},
      {"anycompatiblerange", absl::nullopt},
      {"anyelement", absl::nullopt},
      {"anyenum", absl::nullopt},
      {"anynonarray", absl::nullopt},
      {"anyrange", absl::nullopt},
      {"bit", absl::nullopt},
      {"box", absl::nullopt},
      {"bpchar", absl::nullopt},
      {"char", absl::nullopt},
      {"cid", absl::nullopt},
      {"cidr", absl::nullopt},
      {"circle", absl::nullopt},
      {"cstring", absl::nullopt},
      {"date", absl::nullopt},
      {"daterange", absl::nullopt},
      {"event_trigger", absl::nullopt},
      {"fdw_handler", absl::nullopt},
      {"gtsvector", absl::nullopt},
      {"index_am_handler", absl::nullopt},
      {"inet", absl::nullopt},
      {"int2", absl::nullopt},
      {"int2vector", absl::nullopt},
      {"int4", absl::nullopt},
      {"int4range", absl::nullopt},
      {"int8range", absl::nullopt},
      {"internal", absl::nullopt},
      // copybara::insert_begin(interval-emulator-support)
      {"interval", absl::nullopt},
      // copybara::insert_end
      {"json", absl::nullopt},
      {"jsonpath", absl::nullopt},
      {"language_handler", absl::nullopt},
      {"line", absl::nullopt},
      {"lseg", absl::nullopt},
      {"macaddr", absl::nullopt},
      {"macaddr8", absl::nullopt},
      {"money", absl::nullopt},
      {"name", absl::nullopt},
      {"numrange", absl::nullopt},
      {"oid", absl::nullopt},
      {"oidvector", absl::nullopt},
      {"path", absl::nullopt},
      {"pg_ddl_command", absl::nullopt},
      {"pg_dependencies", absl::nullopt},
      {"pg_lsn", absl::nullopt},
      {"pg_mcv_list", absl::nullopt},
      {"pg_ndistinct", absl::nullopt},
      {"pg_node_tree", absl::nullopt},
      {"pg_snapshot", absl::nullopt},
      {"point", absl::nullopt},
      {"polygon", absl::nullopt},
      {"record", absl::nullopt},
      {"refcursor", absl::nullopt},
      {"regclass", absl::nullopt},
      {"regcollation", absl::nullopt},
      {"regconfig", absl::nullopt},
      {"regdictionary", absl::nullopt},
      {"regnamespace", absl::nullopt},
      {"regoper", absl::nullopt},
      {"regoperator", absl::nullopt},
      {"regproc", absl::nullopt},
      {"regprocedure", absl::nullopt},
      {"regrole", absl::nullopt},
      {"regtype", absl::nullopt},
      {"table_am_handler", absl::nullopt},
      {"tid", absl::nullopt},
      {"time", absl::nullopt},
      {"timestamp", absl::nullopt},
      {"timestamptz", absl::nullopt},
      {"timetz", absl::nullopt},
      {"trigger", absl::nullopt},
      {"tsm_handler", absl::nullopt},
      {"tsquery", absl::nullopt},
      {"tsrange", absl::nullopt},
      {"tstzrange", absl::nullopt},
      {"tsvector", absl::nullopt},
      {"txid_snapshot", absl::nullopt},
      {"unknown", absl::nullopt},
      {"uuid", absl::nullopt},
      {"varbit", absl::nullopt},
      {"varchar", absl::nullopt},
      {"void", absl::nullopt},
      {"xid", absl::nullopt},
      {"xid8", absl::nullopt},
      {"xml", absl::nullopt},
      // These 6 variants of SERIAL types are pseudo types supported in
      // transformColumnDefinition()
      // third_party/spanner_pg/src/backend/parser/parse_utilcmd.c
      {"bigserial", absl::nullopt},
      {"serial8", absl::nullopt},
      {"serial", absl::nullopt},
      {"serial4", absl::nullopt},
      {"smallserial", absl::nullopt},
      {"serial2", absl::nullopt},
  };

  return *map;
}

// Suggest a type for unsupported PostgreSQL types based on
// https://cloud.google.com/spanner/docs/migrating-postgres-spanner-pgcompat#data-types
const absl::flat_hash_map<absl::string_view, std::vector<absl::string_view>>&
GetSuggestedTypeMap() {
  static auto* map = new absl::flat_hash_map<absl::string_view,
                                             std::vector<absl::string_view>>{
      {"bigserial", {"bigint", "int8"}},
      {"serial8", {"bigint", "int8"}},
      {"cidr", {"text"}},
      {"inet", {"text"}},
      {"integer", {"bigint", "int8"}},
      {"int4", {"bigint", "int8"}},
      {"json", {"jsonb"}},
      {"macaddr", {"text"}},
      {"money", {"numeric", "decimal"}},
      {"realfloat4", {"double precision", "float8"}},
      {"smallint", {"bigint", "int8"}},
      {"int2", {"bigint", "int8"}},
      {"smallserial", {"bigint", "int8"}},
      {"serial2", {"bigint", "int8"}},
      {"serial", {"bigint, int8"}},
      {"serial4", {"bigint", "int8"}},
      {"uuid", {"text", "bytea"}},
      {"xml", {"text"}}};

  return *map;
}

// Returns the first and only element of the single-item list cast to
// <NodeType*>. Checks that element's type is equal to NodeTypeTag.
template <typename NodeType, const NodeTag NodeTypeTag>
absl::StatusOr<const NodeType*> SingleItemListAsNode(const List* list) {
  ZETASQL_RET_CHECK_NE(list, nullptr);
  ZETASQL_RET_CHECK_EQ(list->type, T_List);
  ZETASQL_RET_CHECK_EQ(list_length(list), 1);

  NodeType* node = static_cast<NodeType*>(linitial(list));

  ZETASQL_RET_CHECK_NE(node, nullptr);
  ZETASQL_RET_CHECK_EQ(node->type, NodeTypeTag);

  return node;
}

// Returns the nth element of the list cast to <NodeType*>. Checks that
// element's type is equal to NodeTypeTag.
template <typename NodeType, const NodeTag NodeTypeTag>
absl::StatusOr<const NodeType*> GetListItemAsNode(const List* list, int n) {
  ZETASQL_RET_CHECK_NE(list, nullptr);
  ZETASQL_RET_CHECK_EQ(list->type, T_List);
  ZETASQL_RET_CHECK_GT(list_length(list), n);

  // PostgreSQL now (13+) stores lists as arrays of cells with O(1) access to
  // any one of them via new helper functions.
  NodeType* node = static_cast<NodeType*>(list_nth(list, n));

  ZETASQL_RET_CHECK_NE(node, nullptr);
  ZETASQL_RET_CHECK_EQ(node->type, NodeTypeTag);

  return node;
}

// PostgreSQL parse tree uses C-style struct hierarchy, with the first field
// of every node being <type> (thus everything is guaranteed to 'inherit'
// from Node). This means that the only way to cast a generic <Node> to
// actual type is via <reinterpret_cast>, as <down_cast> requires types to be
// connected to one another via C++ inheritance, which is not true in this case.
template <typename NodeType, const NodeTag NodeTypeTag, typename FromNodeType>
absl::StatusOr<const NodeType*> DowncastNode(const FromNodeType* node) {
  ZETASQL_RET_CHECK_NE(node, nullptr);
  // Extra precaution: nothing prevents reinterpret_cast from casting Node to
  // any type available, with a subsequent segfaults, so we pass expected type
  // tag explicitly and make sure the node has this type before casting.
  ZETASQL_RET_CHECK_EQ(node->type, NodeTypeTag);

  return reinterpret_cast<const NodeType*>(node);
}

// Represents table translation context. This context helps to perform
// post-translation validation and enforce translation rules that depend on
// table state (e.g. attaching <NOT NULL> constraints to primary key columns).
struct PostgreSQLToSpannerDDLTranslatorImpl::TableContext {
  // Names of the columns to make not nullable. Include both columns with
  // NOT NULL constraint and PRIMARY KEY columns. Actual strings key point to
  // are part of PostgreSQL parse tree and live in the memory arena, thus are
  // guaranteed to stay alive for the duration of the translation.
  absl::flat_hash_set<absl::string_view> not_null_columns;
  // Names of the columns with NULL constraint (columns that are nullable by
  // default are not included). Actual strings key point to are part of
  // PostgreSQL parse tree and live in the memory arena, thus are guaranteed to
  // stay alive for the duration of the translation.
  absl::flat_hash_set<absl::string_view> nullable_columns;
  std::vector<google::spanner::emulator::backend::ddl::ForeignKey> foreign_keys;
  std::vector<google::spanner::emulator::backend::ddl::KeyPartClause> primary_key_columns;
  std::vector<google::spanner::emulator::backend::ddl::CheckConstraint> check_constraints;
};

absl::StatusOr<google::spanner::emulator::backend::ddl::InterleaveClause::Type>
PostgreSQLToSpannerDDLTranslatorImpl::GetInterleaveClauseType(
    const InterleaveInType type) {
  switch (type) {
    case INTERLEAVE_IN_PARENT: {
      return google::spanner::emulator::backend::ddl::InterleaveClause::IN_PARENT;
    }
    case INTERLEAVE_IN: {
      return google::spanner::emulator::backend::ddl::InterleaveClause::IN;
    }
  }

  ZETASQL_RET_CHECK_FAIL() << "Unknown value of InterleaveInType " << type << ".";
}

absl::StatusOr<internal::PGAlterOption>
PostgreSQLToSpannerDDLTranslatorImpl::TranslateOption(
    absl::string_view option_name, const TranslationOptions& options) const {
  ZETASQL_RET_CHECK(!option_name.empty()) << "Option name is missing.";

  absl::optional<internal::PGAlterOption> option =
      internal::GetOptionByInternalName(option_name);
  if (!option) {
    return UnsupportedTranslationError(
        absl::StrCat("Database option <", option_name, "> is not supported."));
  }

  return *option;
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::ParseColumnType(
    const TypeName& type, std::string& schema_name,
    std::string& type_name) const {
  // We expect type in a form of [ [<catalog>,] <type_name> ], so there could
  // either be 1 (just name) or 2 (schema name and type name) parts.
  bool type_name_contains_schema_part = list_length(type.names) == 2;

  schema_name = "";

  for (const String* type_name_part : StructList<String*>(type.names)) {
    if (type_name_contains_schema_part) {
      schema_name = type_name_part->sval;
      type_name_contains_schema_part = false;
    } else {
      type_name = type_name_part->sval;
    }
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::ProcessTableConstraint(
    const Constraint& constraint, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::ColumnDefinition* target_column,
    TableContext& context) const {
  ZETASQL_RETURN_IF_ERROR(
      ValidateParseTreeNode(constraint, /*add_in_alter=*/false, options));

  switch (constraint.contype) {
    case CONSTR_PRIMARY: {
      if (!context.primary_key_columns.empty()) {
        return UnsupportedTranslationError(
            "Primary key can be only defined once.");
      }

      if (constraint.conname != nullptr) {
        return UnsupportedTranslationError(
            "Setting a name of a <PRIMARY KEY> constraint is not supported.");
      }

      std::vector<absl::string_view> primary_key_column_names;

      if (target_column != nullptr) {
        // Constraint is defined directly on the column.
        ZETASQL_RET_CHECK(target_column->has_column_name());

        primary_key_column_names.push_back(target_column->column_name());
      } else {
        // Constraint is defined as a separate expression.
        ZETASQL_RET_CHECK(!IsListEmpty(constraint.keys));
        for (const String* primary_key_part :
             StructList<String*>(constraint.keys)) {
          ZETASQL_RET_CHECK_EQ(primary_key_part->type, T_String);
          ZETASQL_RET_CHECK_NE(primary_key_part->sval, nullptr);

          primary_key_column_names.push_back(primary_key_part->sval);
        }
      }

      for (const absl::string_view key_part_name : primary_key_column_names) {
        google::spanner::emulator::backend::ddl::KeyPartClause key_part;

        key_part.set_key_name(key_part_name);
        if (options.enable_nulls_ordering) {
          key_part.set_order(google::spanner::emulator::backend::ddl::KeyPartClause::ASC_NULLS_LAST);
        }
        context.primary_key_columns.push_back(std::move(key_part));

        // All the columns included in primary key should have NOT NULL
        // constraint applied to them to be compatible with Postgresql.
        context.not_null_columns.insert(key_part_name);
      }

      return absl::OkStatus();
    }

    case CONSTR_NOTNULL: {
      // NOT NULL can be defined only as column constraint
      ZETASQL_RET_CHECK_NE(target_column, nullptr);
      ZETASQL_RET_CHECK(target_column->has_column_name());
      if (constraint.conname != nullptr) {
        return UnsupportedTranslationError(
            "Setting a name of a <NOT NULL> constraint is not supported.");
      }

      context.not_null_columns.insert(target_column->column_name());
      return absl::OkStatus();
    }

    case CONSTR_NULL: {
      // NULL can be defined only as column constraint
      ZETASQL_RET_CHECK_NE(target_column, nullptr);
      ZETASQL_RET_CHECK(target_column->has_column_name());
      if (constraint.conname != nullptr) {
          return UnsupportedTranslationError(
              "Setting a name of a <NULL> constraint is not supported.");
      }

      context.nullable_columns.insert(target_column->column_name());
      return absl::OkStatus();
    }

    case CONSTR_FOREIGN: {
      google::spanner::emulator::backend::ddl::ForeignKey foreign_key;
      ZETASQL_RETURN_IF_ERROR(
          TranslateForeignKey(constraint, target_column, foreign_key));
      context.foreign_keys.push_back(std::move(foreign_key));

      return absl::OkStatus();
    }

    case CONSTR_CHECK: {
      google::spanner::emulator::backend::ddl::CheckConstraint check_constraint;
      ZETASQL_RETURN_IF_ERROR(
          TranslateCheckConstraint(constraint, options, check_constraint));
      context.check_constraints.push_back(std::move(check_constraint));
      return absl::OkStatus();
    }

    case CONSTR_GENERATED: {
      if (!options.enable_generated_column) {
        return UnsupportedTranslationError(
            "<GENERATED> constraint type is not supported.");
      }
      ZETASQL_RETURN_IF_ERROR(
          TranslateGeneratedColumn(constraint, options, *target_column));
      return absl::OkStatus();
    }

    case CONSTR_DEFAULT: {
      if (!options.enable_column_default) {
        return UnsupportedTranslationError(
            "<DEFAULT> constraint type is not supported.");
      }
      ZETASQL_RETURN_IF_ERROR(
          TranslateColumnDefault(constraint, options, *target_column));
      return absl::OkStatus();
    }

    case CONSTR_VECTOR_LENGTH: {
      if (!options.enable_vector_length) {
        return UnsupportedTranslationError(
            "<VECTOR LENGTH> constraint type is not supported.");
      }
      // VECTOR LENGTH can be defined only as column constraint.
      ZETASQL_RET_CHECK_NE(target_column, nullptr);
      ZETASQL_RET_CHECK(target_column->has_column_name());
      ZETASQL_RETURN_IF_ERROR(
          TranslateVectorLength(constraint, options, *target_column));
      return absl::OkStatus();
    }

    case CONSTR_ATTR_IMMEDIATE:
    case CONSTR_ATTR_NOT_DEFERRABLE: {
      // This means NOT DEFERRABLE and/or INITIALLY IMMEDIATE is set on
      // column-level constraint. This is default behavior, we just skip this
      // definition.
      return absl::OkStatus();
    }

    default:
      ZETASQL_RET_CHECK_FAIL() << "Constraint type should have been validated "
                          "separately in ValidateParseTreeNode";
  }

  return absl::OkStatus();
}

absl::StatusOr<google::spanner::emulator::backend::ddl::InterleaveClause::Action>
PostgreSQLToSpannerDDLTranslatorImpl::GetInterleaveParentDeleteActionType(
    const char action) const {
  switch (action) {
    case FKCONSTR_ACTION_NOACTION: {
      return google::spanner::emulator::backend::ddl::InterleaveClause::NO_ACTION;
    }
    case FKCONSTR_ACTION_CASCADE: {
      return google::spanner::emulator::backend::ddl::InterleaveClause::CASCADE;
    }
    default:
      return UnsupportedTranslationError(
          "Only <NO ACTION> and <CASCADE> are supported in <ON DELETE> clause "
          "of <INTERLEAVE IN PARENT>.");
  }
}

absl::StatusOr<std::string> PostgreSQLToSpannerDDLTranslatorImpl::GetSchemaName(
    const String& value, absl::string_view parent_statement_type) const {
  if (value.sval == nullptr) {
    return absl::InvalidArgumentError(absl::Substitute(
        "Schema name not specified in $0 statement.", parent_statement_type));
  }
  if (strcmp(value.sval, "public") != 0) {
    return value.sval;
  }
  // Return empty string for public schema.
  return "";
}

absl::StatusOr<std::string> PostgreSQLToSpannerDDLTranslatorImpl::GetTableName(
    const RangeVar& range_var, absl::string_view parent_statement_type) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(range_var, parent_statement_type));
  if (range_var.schemaname != nullptr &&
      strcmp(range_var.schemaname, "public") != 0) {
    return absl::Substitute("$0.$1", range_var.schemaname, range_var.relname);
  }
  return range_var.relname;
}

absl::StatusOr<std::string> PostgreSQLToSpannerDDLTranslatorImpl::
    GetChangeStreamTableName(
    const RangeVar& range_var, absl::string_view parent_statement_type) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(range_var, parent_statement_type));
  if (range_var.schemaname != nullptr &&
      strcmp(range_var.schemaname, "public") != 0) {
    return absl::InvalidArgumentError(absl::Substitute(
        "Schema name can only be 'public' in <$0> statement.",
        parent_statement_type));
  }
  return range_var.relname;
}

absl::StatusOr<std::string>
PostgreSQLToSpannerDDLTranslatorImpl::GetFunctionName(
    const ObjectWithArgs& object_with_args, bool is_grant,
    absl::string_view parent_statement_type) const {
  ZETASQL_RETURN_IF_ERROR(
      ValidateParseTreeNode(object_with_args, /*is_grant=*/is_grant));
  bool uses_spanner_namespace = false;
  if (list_length(object_with_args.objname) == 2) {
    absl::string_view function_namespace =
        static_cast<String*>(linitial(object_with_args.objname))->sval;
    if (function_namespace == "spanner") {
      uses_spanner_namespace = true;
    }
  }
  if (!uses_spanner_namespace) {
    return absl::InvalidArgumentError(absl::Substitute(
        "Namespace 'spanner' must be specified in <$0> statement on functions.",
        parent_statement_type));
  }
  std::string function_name =
      static_cast<String*>(lsecond(object_with_args.objname))->sval;
  return function_name;
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateInterleaveIn(
    const InterleaveSpec* pg_interleave,
    google::spanner::emulator::backend::ddl::InterleaveClause* interleave_out,
    const TranslationOptions& options) const {
  ZETASQL_RET_CHECK_NE(pg_interleave, nullptr);
  ZETASQL_RET_CHECK_NE(interleave_out, nullptr);
  ZETASQL_RET_CHECK_NE(pg_interleave->parent, nullptr);

  ZETASQL_ASSIGN_OR_RETURN(auto relname,
                   GetTableName(*pg_interleave->parent, "INTERLEAVE"));
  interleave_out->set_table_name(relname);

  ZETASQL_ASSIGN_OR_RETURN(google::spanner::emulator::backend::ddl::InterleaveClause::Type type,
                   GetInterleaveClauseType(pg_interleave->interleavetype));

  switch (type) {
    case google::spanner::emulator::backend::ddl::InterleaveClause::IN: {
      if (!options.enable_interleave_in) {
        return UnsupportedTranslationError(
            "<INTERLEAVE IN> clause is not supported. Consider using "
            "<INTERLEAVE IN PARENT>.");
      }
    }
    [[fallthrough]];
    case google::spanner::emulator::backend::ddl::InterleaveClause::IN_PARENT: {
      if (pg_interleave->on_delete_action) {
        ZETASQL_ASSIGN_OR_RETURN(
            google::spanner::emulator::backend::ddl::InterleaveClause::Action action,
            GetInterleaveParentDeleteActionType(
                pg_interleave->on_delete_action));
        interleave_out->set_on_delete(action);
      }
      break;
    }

    default:
      // Should never get here
      ZETASQL_RET_CHECK_FAIL() << "Unknown interleave type.";
  }
  interleave_out->set_type(type);

  return absl::OkStatus();
}

absl::StatusOr<google::spanner::emulator::backend::ddl::ForeignKey::Action>
PostgreSQLToSpannerDDLTranslatorImpl::GetForeignKeyAction(
    const char action) const {
  switch (action) {
    case FKCONSTR_ACTION_NOACTION: {
      // Spanner ForeignKey_Action_ACTION_UNSPECIFIED equals to NOACTION
      return google::spanner::emulator::backend::ddl::ForeignKey::ACTION_UNSPECIFIED;
    }
    case FKCONSTR_ACTION_CASCADE: {
      return google::spanner::emulator::backend::ddl::ForeignKey::CASCADE;
    }
    default:
      return UnsupportedTranslationError(
          "Requested foreign key action is not supported.");
  }
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCheckConstraint(
    const Constraint& check_constraint, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::CheckConstraint& out) const {
  ZETASQL_RET_CHECK_EQ(check_constraint.contype, CONSTR_CHECK);
  ZETASQL_RET_CHECK_NE(check_constraint.raw_expr, nullptr)
      << "parse tree must be present for CHECK constraints.";

  out.set_enforced(true);

  if (check_constraint.conname && *check_constraint.conname != '\0') {
    out.set_name(check_constraint.conname);
  }

  google::spanner::emulator::backend::ddl::SQLExpressionOrigin* expression_origin =
      out.mutable_expression_origin();

  ZETASQL_ASSIGN_OR_RETURN(std::string serialized_expression,
                   CheckedPgNodeToString(check_constraint.raw_expr));
  expression_origin->set_serialized_parse_tree(serialized_expression);
  if (options.enable_expression_string) {
    expression_origin->set_original_expression(
        check_constraint.constraint_expr_string);
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateColumnDefault(
    const Constraint& column_default, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  ZETASQL_RET_CHECK_EQ(column_default.contype, CONSTR_DEFAULT);
  ZETASQL_RET_CHECK_NE(column_default.raw_expr, nullptr)
      << "parse tree must be present for column DEFAULT constraints.";

  google::spanner::emulator::backend::ddl::ColumnDefinition::ColumnDefaultDefinition* column_default_def =
      out.mutable_column_default();

  google::spanner::emulator::backend::ddl::SQLExpressionOrigin* expression_origin =
      column_default_def->mutable_expression_origin();

  ZETASQL_ASSIGN_OR_RETURN(std::string serialized_expression,
                   CheckedPgNodeToString(column_default.raw_expr));
  expression_origin->set_serialized_parse_tree(serialized_expression);
  if (options.enable_expression_string) {
    expression_origin->set_original_expression(
        column_default.constraint_expr_string);
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateGeneratedColumn(
    const Constraint& generated, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  ZETASQL_RET_CHECK_EQ(generated.contype, CONSTR_GENERATED);
  ZETASQL_RET_CHECK_NE(generated.raw_expr, nullptr)
      << "parse tree must be present for GENERATED column constraints.";

  // Postgresql also support GENERATED BY DEFAULT for identity column which we
  // do not support yet.
  if (generated.generated_when != ATTRIBUTE_IDENTITY_ALWAYS) {
    return UnsupportedTranslationError(
        "Only <GENERATED ALWAYS> is supported for generated column.");
  }

  google::spanner::emulator::backend::ddl::ColumnDefinition::GeneratedColumnDefinition*
      generated_column_def = out.mutable_generated_column();
  // PostgreSQL's generated column only support `stored`. Spangres support
  // non-stored generated column.
  if (generated.stored_kind == GENERATED_COL_NON_STORED) {
    if (!options.enable_virtual_generated_column) {
      return UnsupportedTranslationError(
          "<VIRTUAL> is not supported for generated column."
          "Only <STORED> is supported.");
    }
    generated_column_def->set_stored(false);
  }  else {
    generated_column_def->set_stored(true);
  }
  // `expression` is `required` field for spanner, set as empty here;
  generated_column_def->set_expression("");

  google::spanner::emulator::backend::ddl::SQLExpressionOrigin* expression_origin =
      generated_column_def->mutable_expression_origin();

  ZETASQL_ASSIGN_OR_RETURN(std::string serialized_expression,
                   CheckedPgNodeToString(generated.raw_expr));
  expression_origin->set_serialized_parse_tree(serialized_expression);
  if (options.enable_expression_string) {
    expression_origin->set_original_expression(
        generated.constraint_expr_string);
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateVectorLength(
    const Constraint& length, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  ZETASQL_RET_CHECK_EQ(length.contype, CONSTR_VECTOR_LENGTH);
  out.set_vector_length(length.vector_length);
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateForeignKey(
    const Constraint& constraint,
    const google::spanner::emulator::backend::ddl::ColumnDefinition* target_column,
    google::spanner::emulator::backend::ddl::ForeignKey& out) const {
  ZETASQL_RET_CHECK_EQ(constraint.contype, CONSTR_FOREIGN);
  ZETASQL_RET_CHECK_NE(constraint.pktable, nullptr)
      << "Referenced table name is missing.";

  if (IsListEmpty(constraint.pk_attrs)) {
    return UnsupportedTranslationError(
        "At least one referenced column is required in foreign key.");
  }

  // All PG foreign keys are enforced
  // TODO: add option to allow creating non-enforced fks
  out.set_enforced(true);

  if (constraint.conname && *constraint.conname != '\0') {
    out.set_constraint_name(constraint.conname);
  }

  if (target_column != nullptr) {
    // Constraint is defined directly on the column -> we use column name as
    // constrained column
    ZETASQL_RET_CHECK(IsListEmpty(constraint.fk_attrs));
    ZETASQL_RET_CHECK(target_column->has_column_name());

    out.add_constrained_column_name(target_column->column_name());
  } else {
    // Constraint is defined at table level -> we use data from constraint
    // itself to set constrained columns
    ZETASQL_RET_CHECK(!IsListEmpty(constraint.fk_attrs));
    for (String* attr_name : StructList<String*>(constraint.fk_attrs)) {
      ZETASQL_RET_CHECK_NE(attr_name, nullptr);
      ZETASQL_RET_CHECK_EQ(attr_name->type, T_String);

      out.add_constrained_column_name(attr_name->sval);
    }
  }

  for (String* attr_name : StructList<String*>(constraint.pk_attrs)) {
    ZETASQL_RET_CHECK_NE(attr_name, nullptr);
    ZETASQL_RET_CHECK_EQ(attr_name->type, T_String);
    out.add_referenced_column_name(attr_name->sval);
  }

  ZETASQL_ASSIGN_OR_RETURN(std::string pk_table_name,
                   GetTableName(*constraint.pktable, "FOREIGN KEY"));

  out.set_referenced_table_name(pk_table_name);

  ZETASQL_ASSIGN_OR_RETURN(google::spanner::emulator::backend::ddl::ForeignKey::Action del_action,
                   GetForeignKeyAction(constraint.fk_del_action));
  out.set_on_delete(del_action);

  ZETASQL_ASSIGN_OR_RETURN(google::spanner::emulator::backend::ddl::ForeignKey::Action upd_action,
                   GetForeignKeyAction(constraint.fk_upd_action));
  out.set_on_update(upd_action);

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateColumnDefinition(
    const ColumnDef& column_definition, const TranslationOptions& options,
    TableContext& context, google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  ZETASQL_RETURN_IF_ERROR(
      ValidateParseTreeNode(column_definition, /*alter_column=*/false));

  *out.mutable_column_name() = column_definition.colname;

  ZETASQL_RETURN_IF_ERROR(ProcessColumnTypeWithAttributes(*column_definition.typeName,
                                                  options, out));

  if (column_definition.is_not_null) {
    context.not_null_columns.insert(column_definition.colname);
  }

  if (column_definition.constraints != nullptr) {
    for (const Constraint* constraint :
         StructList<Constraint*>(column_definition.constraints)) {
      ZETASQL_RETURN_IF_ERROR(
          ProcessTableConstraint(*constraint, options, &out, context));
    }
  }

  return absl::OkStatus();
}

absl::Status
PostgreSQLToSpannerDDLTranslatorImpl::ProcessColumnTypeWithAttributes(
    const TypeName& type, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(type));

  // This checking could be removed soon after 28.1 released and merged to
  // mainline. There is on branch test depends on this.
  if (!options.enable_arrays_type && !IsListEmpty(type.arrayBounds)) {
    return UnsupportedTranslationError("Arrays are not supported.");
  }

  std::string schema_name, type_name;
  ZETASQL_RETURN_IF_ERROR(ParseColumnType(type, schema_name, type_name));
  std::string full_type_name = schema_name.empty() ? "" : schema_name + ".";
  full_type_name += type_name;

  if (schema_name.empty() || schema_name == "pg_catalog") {
    // Postgres integral types
    return ProcessPostgresType(type_name, type, options, out);
  }
  if (schema_name == "spanner") {
    // Extended type. Currently only commit_timestamp is supported.

    if (!IsListEmpty(type.arrayBounds)) {
      return UnsupportedTranslationError(absl::StrCat(
          "Arrays are not supported for the type <", type_name, ">."));
    }
    return ProcessExtendedType(type_name, type, out);
  }
  return UnsupportedTranslationError(absl::StrCat(
      "Types from namespace <", schema_name, "> are not supported."));
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::ProcessPostgresType(
    const absl::string_view type_name, const TypeName& type,
    const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  google::spanner::emulator::backend::ddl::ColumnDefinition* where_to_set_type = &out;
  // Spanner accepts array types in a peculiar way, necessitating the creation
  // of separate column definition describing actual type for arrays
  if (!IsListEmpty(type.arrayBounds)) {
    out.set_type(google::spanner::emulator::backend::ddl::ColumnDefinition::ARRAY);
    where_to_set_type = out.mutable_array_subtype();
  }
  if (!options.enable_date_type && type_name == "date") {
    return UnsupportedTranslationError(
        absl::StrCat("Type <date> is not supported."));
  }

  if (!options.enable_jsonb_type && type_name == "jsonb") {
    return UnsupportedTranslationError(
        absl::StrCat("Type <jsonb> is not supported."));
  }

  if (!options.enable_array_jsonb_type && type_name == "jsonb" &&
      !IsListEmpty(type.arrayBounds)) {
    return UnsupportedTranslationError(
        absl::StrCat("Type Array of <jsonb> is not supported."));
  }

  auto type_it = GetTypeMap().find(type_name);
  if (type_it == GetTypeMap().end()) {
    // type not found
    // TODO: properly handle case when type keyword isn't
    // translated into actual type by parser
    return UnsupportedTranslationError(
        absl::StrCat("Type <", type_name, "> does not exist."));
  }
  if (!type_it->second.has_value()) {
    // type not supported
    auto suggested_type_it = GetSuggestedTypeMap().find(type_name);
    if (suggested_type_it != GetSuggestedTypeMap().end()) {
      // Type <bigserial> is unsupported; use bigint or int8_t instead.
      return UnsupportedTranslationError(
          absl::StrFormat(
              "Type <%s> is not supported; use %s instead.", type_name,
              absl::StrJoin(suggested_type_it->second, " or ")));
    }
    return UnsupportedTranslationError(
        absl::StrCat("Type <", type_name, "> is not supported."));
  }

  google::spanner::emulator::backend::ddl::ColumnDefinition::Type spanner_type = type_it->second.value();
  where_to_set_type->set_type(spanner_type);

  // Type modifiers present
  if (!IsListEmpty(type.typmods)) {
    static constexpr absl::string_view type_modifier_not_supported =
        "Type modifier is not supported for type <$0>.";
    // Type modifiers mean different things for different types
    switch (spanner_type) {
      case google::spanner::emulator::backend::ddl::ColumnDefinition::STRING: {
        if (type_name == "text") {
          return UnsupportedTranslationError(
              absl::Substitute(type_modifier_not_supported, type_name));
        } else if (type_name == "varchar") {
          if (list_length(type.typmods) > 1) {
            return UnsupportedTranslationError(
                absl::Substitute(type_modifier_not_supported, type_name));
          }
          // This is called only if length is specified for the VARCHAR type.
          // Otherwise (if length is not specified, or for TEXT types) 'length'
          // is left not set on the target STRING type, meaning it defaults to
          // MAX.
          ZETASQL_ASSIGN_OR_RETURN(
              const A_Const* length,
              (SingleItemListAsNode<A_Const, T_A_Const>)(type.typmods));
          if (length->val.ival.ival > PGConstants::kMaxStringLength) {
            return UnsupportedTranslationError(
                absl::StrCat("Maximum length for <varchar> fields is ",
                             PGConstants::kMaxStringLength, "."));
          }
          where_to_set_type->set_length(length->val.ival.ival);
        } else {
          // should never get here
          ZETASQL_RET_CHECK_FAIL() << "Unexpected type name: " << type_name << ".";
        }
        break;
      }
      default:
        // TODO: Add support for fixed-precision numeric DDL.
        return UnsupportedTranslationError(
            absl::Substitute(type_modifier_not_supported, type_name));
    }
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::ProcessExtendedType(
    const absl::string_view type_name, const TypeName& type,
    google::spanner::emulator::backend::ddl::ColumnDefinition& out) const {
  if (type_name != "commit_timestamp") {
    return UnsupportedTranslationError(
        absl::StrCat("Type <", type_name, "> does not exist."));
  }
  if (!IsListEmpty(type.typmods)) {
    return UnsupportedTranslationError(absl::StrCat(
        "Type modifiers are not supported for the type <", type_name, ">."));
  }

  out.set_type(google::spanner::emulator::backend::ddl::ColumnDefinition::TIMESTAMP);
  google::spanner::emulator::backend::ddl::SetOption* commit_ts_option = out.add_set_options();
  commit_ts_option->set_option_name(
      PGConstants::kInternalCommitTimestampOptionName);
  commit_ts_option->set_bool_value(true);

  return absl::OkStatus();
}

const int kDefaultRowDeletionInterval = 3;
const int kSecsPerDay = 86400;

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateRowDeletionPolicy(
    const Ttl& ttl,
    google::spanner::emulator::backend::ddl::RowDeletionPolicy& row_deletion_policy) const {
  row_deletion_policy.set_column_name(ttl.name);

  google::spanner::emulator::backend::ddl::DDLTimeLengthProto* interval =
      row_deletion_policy.mutable_interval();
  interval->set_count(kDefaultRowDeletionInterval);
  interval->set_unit(google::spanner::emulator::backend::ddl::DDLTimeLengthProto_Unit_DAYS);

  google::spanner::emulator::backend::ddl::DDLTimeLengthProto* older_than =
      row_deletion_policy.mutable_older_than();
  ZETASQL_ASSIGN_OR_RETURN(char* interval_str, IntervalString(ttl));
  ZETASQL_ASSIGN_OR_RETURN(int64_t ttl_secs, IntervalToSecs(interval_str));

  if (ttl_secs < 0) {
    return UnsupportedTranslationError(
        "TTL interval must be greater than or equal to zero");
  }

  // If we're here then we don't allow fine-grained time, so that means checking
  // to see if we're on a day boundary.
  if (ttl_secs % kSecsPerDay) {
    return absl::InvalidArgumentError(
        "TTL interval must be a whole number of days");
  }
  older_than->set_unit(google::spanner::emulator::backend::ddl::DDLTimeLengthProto_Unit_DAYS);
  older_than->set_count(ttl_secs / kSecsPerDay);
  return absl::OkStatus();
}

absl::Status
PostgreSQLToSpannerDDLTranslatorImpl::ProcessSequenceSkipRangeOption(
    const DefElem& skip_range, ::google::spanner::emulator::backend::ddl::SetOption& skip_range_min_option,
    ::google::spanner::emulator::backend::ddl::SetOption& skip_range_max_option) const {
  ZETASQL_ASSIGN_OR_RETURN(const List* args,
                   (DowncastNode<List, T_List>(skip_range.arg)));
  // Can not use the DowncastNode because the type tag could be either
  // T_INTEGER or T_FLOAT.
  char min_name[] = "skip_range_min";
  ZETASQL_ASSIGN_OR_RETURN(
      DefElem * min_elem,
      CheckedPgMakeDefElem(min_name, PostgresCastToNode(linitial(args)),
                           skip_range.location));
  ZETASQL_ASSIGN_OR_RETURN(int64_t min_int64, CheckedPgDefGetInt64(min_elem));
  skip_range_min_option.set_option_name(min_name);
  skip_range_min_option.set_int64_value(min_int64);

  // Can not use the DowncastNode because the type tag could be either
  // T_INTEGER or T_FLOAT.
  char max_name[] = "skip_range_max";
  ZETASQL_ASSIGN_OR_RETURN(
      DefElem * max_elem,
      CheckedPgMakeDefElem(max_name, PostgresCastToNode(lsecond(args)),
                           skip_range.location));
  ZETASQL_ASSIGN_OR_RETURN(int64_t max_int64, CheckedPgDefGetInt64(max_elem));
  skip_range_max_option.set_option_name(max_name);
  skip_range_max_option.set_int64_value(max_int64);

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateSequence(
    const CreateSeqStmt& create_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::CreateSequence& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(create_statement, options));

  absl::string_view sequence_name;
  ZETASQL_ASSIGN_OR_RETURN(sequence_name,
                   GetTableName(*create_statement.sequence, "CREATE SEQUENCE"));
  out.set_sequence_name(sequence_name);

  List* opts = create_statement.options;

  // This function originally translated and added `sequence_kind` option first
  // because it is mandatory. The option is now optional after the support
  // of `default_sequence_kind` database option but we still want to keep this
  // ordering of options; hence, we add an extra for loop to search and add
  // `bit_reversed_positive` instead of just searching for it in the original
  // for loop.
  for (int i = 0; i < list_length(opts); ++i) {
    DefElem* elem = ::postgres_translator::internal::PostgresCastNode(
        DefElem, opts->elements[i].ptr_value);
    absl::string_view name = elem->defname;
    if (name == "bit_reversed_positive") {
      out.set_type(google::spanner::emulator::backend::ddl::CreateSequence::BIT_REVERSED_POSITIVE);
      ::google::spanner::emulator::backend::ddl::SetOption* sequence_type = out.add_set_options();
      sequence_type->set_option_name(PGConstants::kSequenceKindOptionName);
      sequence_type->set_string_value(
          PGConstants::kSequenceKindBitReversedPositive);
      break;
    }
  }

  // Default `start_with_counter` value is `1`.
  ::google::spanner::emulator::backend::ddl::SetOption* start_counter = out.add_set_options();
  start_counter->set_option_name("start_with_counter");
  start_counter->set_int64_value(1);

  for (int i = 0; i < list_length(opts); ++i) {
    DefElem* elem = ::postgres_translator::internal::PostgresCastNode(
        DefElem, opts->elements[i].ptr_value);
    absl::string_view name = elem->defname;
    if (name == "start_counter") {
      ZETASQL_ASSIGN_OR_RETURN(int64_t value, CheckedPgDefGetInt64(elem));
      start_counter->set_int64_value(value);
    } else if (name == "skip_range") {
      ZETASQL_RETURN_IF_ERROR(ProcessSequenceSkipRangeOption(
          *elem, *out.add_set_options(), *out.add_set_options()));
    }
  }

  if (create_statement.if_not_exists) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_NOT_EXISTS);
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateAlterSequence(
    const AlterSeqStmt& alter_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::AlterSequence& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(alter_statement, options));

  absl::string_view sequence_name;
  ZETASQL_ASSIGN_OR_RETURN(sequence_name,
                   GetTableName(*alter_statement.sequence, "ALTER SEQUENCE"));
  out.set_sequence_name(sequence_name);

  List* opts = alter_statement.options;
  for (int i = 0; i < list_length(opts); ++i) {
    DefElem* elem = ::postgres_translator::internal::PostgresCastNode(
        DefElem, opts->elements[i].ptr_value);
    absl::string_view name = elem->defname;
    if (name == "restart_counter") {
      ZETASQL_ASSIGN_OR_RETURN(int64_t value, CheckedPgDefGetInt64(elem));
      auto* opt = out.mutable_set_options()->add_options();
      opt->set_option_name("start_with_counter");
      opt->set_int64_value(value);
      continue;
    } else if (name == "skip_range") {
      ZETASQL_RETURN_IF_ERROR(ProcessSequenceSkipRangeOption(
          *elem, *out.mutable_set_options()->add_options(),
          *out.mutable_set_options()->add_options()));
    }
  }

  if (alter_statement.missing_ok) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_EXISTS);
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropSequence(
    const DropStmt& drop_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::DropSequence& out) const {
  ZETASQL_RET_CHECK_EQ(drop_statement.removeType, OBJECT_SEQUENCE);
  if (!options.enable_sequence) {
    return UnsupportedTranslationError("DROP SEQUENCE is not supported");
  }

  ZETASQL_ASSIGN_OR_RETURN(
      const List* sequence_to_drop_list,
      (SingleItemListAsNode<List, T_List>(drop_statement.objects)));

  // Caller should have called ValidateParseTreeNode, which ensures the size of
  // sequence_to_drop_list is 1 or 2.
  if (sequence_to_drop_list->length == 2) {
    ZETASQL_ASSIGN_OR_RETURN(
        const String* schema_to_drop_node,
        (GetListItemAsNode<String, T_String>)(sequence_to_drop_list, 0));
    ZETASQL_ASSIGN_OR_RETURN(
        const String* sequence_to_drop_node,
        (GetListItemAsNode<String, T_String>)(sequence_to_drop_list, 1));
    if (strcmp(schema_to_drop_node->sval, "public") != 0) {
      *out.mutable_sequence_name() =
          absl::Substitute("$0.$1", schema_to_drop_node->sval,
                           sequence_to_drop_node->sval);
    } else {
      *out.mutable_sequence_name() =
          absl::StrCat(sequence_to_drop_node->sval);
    }
  } else {
    ZETASQL_RET_CHECK_EQ(sequence_to_drop_list->length, 1)
        << "Incorrect number of name components in drop target.";
    ZETASQL_ASSIGN_OR_RETURN(
        const String* sequence_to_drop_node,
        (SingleItemListAsNode<String, T_String>)(sequence_to_drop_list));
    *out.mutable_sequence_name() = sequence_to_drop_node->sval;
  }

  if (drop_statement.missing_ok) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_EXISTS);
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateTable(
    const CreateStmt& create_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::CreateTable& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(create_statement, options));

  absl::string_view table_name;
  ZETASQL_ASSIGN_OR_RETURN(table_name,
                   GetTableName(*create_statement.relation, "CREATE TABLE"));
  if (internal::IsReservedName(table_name)) {
    return absl::InvalidArgumentError(
        "'pg_' is not supported as a prefix for a table name.");
  }
  *out.mutable_table_name() = table_name;

  TableContext context;

  // <tableElts> contains a List that can include ColumnDef, Constraint or
  // T_TableLikeClause. See transformCreateStmt for details.
  for (const Node* node : StructList<Node*>(create_statement.tableElts)) {
    ZETASQL_RET_CHECK_NE(node, nullptr);
    switch (node->type) {
      case T_ColumnDef: {
        ZETASQL_ASSIGN_OR_RETURN(const ColumnDef* column,
                         (DowncastNode<ColumnDef, T_ColumnDef>(node)));
        ZETASQL_RETURN_IF_ERROR(TranslateColumnDefinition(*column, options, context,
                                                  *out.add_column()));
        break;
      }

      case T_Constraint: {
        ZETASQL_ASSIGN_OR_RETURN(const Constraint* constraint,
                         (DowncastNode<Constraint, T_Constraint>(node)));
        ZETASQL_RETURN_IF_ERROR(
            ProcessTableConstraint(*constraint, options, nullptr, context));
        break;
      }

      case T_SynonymClause: {
        ZETASQL_ASSIGN_OR_RETURN(const SynonymClause* synonym_clause,
                         (DowncastNode<SynonymClause, T_SynonymClause>(node)));
        out.set_synonym(synonym_clause->name);
        break;
      }

      case T_TableLikeClause: {
        return UnsupportedTranslationError(
            "<LIKE> clause is not supported in <CREATE TABLE> statement.");
      }

      default: {
        return UnsupportedTranslationError(
            "Unkown clause in <CREATE TABLE> statement.");
      }
    }
  }

  if (create_statement.constraints != nullptr) {
    // <constraints> contains list of Constraint nodes.
    for (const Constraint* constraint :
         StructList<Constraint*>(create_statement.constraints)) {
      ZETASQL_RETURN_IF_ERROR(
          ProcessTableConstraint(*constraint, options, nullptr, context));
    }
  }

  if (create_statement.interleavespec != nullptr) {
    ZETASQL_RETURN_IF_ERROR(TranslateInterleaveIn(create_statement.interleavespec,
                                          out.mutable_interleave_clause(),
                                          options));
  }

  if (create_statement.ttl != nullptr) {
    ZETASQL_RETURN_IF_ERROR(TranslateRowDeletionPolicy(
        *create_statement.ttl, *out.mutable_row_deletion_policy()));
  }

  if (create_statement.if_not_exists) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_NOT_EXISTS);
  }

  // Post processing phase: add all the constraints (PRIMARY KEY, FOREIGN KEY,
  // NULL, NOT NULL).
  for (int i = 0; i < out.column_size(); i++) {
    google::spanner::emulator::backend::ddl::ColumnDefinition* column = out.mutable_column(i);

    // Explicitly check that primary key columns don't have NULL constraints on
    // them
    if (context.nullable_columns.contains(column->column_name()) &&
        context.not_null_columns.contains(column->column_name())) {
      return UnsupportedTranslationError(
          "Nullable <PRIMARY KEY> columns are not supported.");
    }

    if (context.nullable_columns.contains(column->column_name())) {
      column->set_not_null(false);
    }
    if (context.not_null_columns.contains(column->column_name())) {
      column->set_not_null(true);
    }
  }

  for (google::spanner::emulator::backend::ddl::ForeignKey& foreign_key : context.foreign_keys) {
    *out.add_foreign_key() = std::move(foreign_key);
  }

  for (google::spanner::emulator::backend::ddl::KeyPartClause& primary_key_part :
       context.primary_key_columns) {
    *out.add_primary_key() = std::move(primary_key_part);
  }

  for (google::spanner::emulator::backend::ddl::CheckConstraint& check_constraint :
       context.check_constraints) {
    *out.add_check_constraint() = std::move(check_constraint);
  }

  if (out.primary_key_size() == 0) {
    return UnsupportedTranslationError(absl::StrCat(
        "Primary key must be defined for table \"", table_name, "\"."));
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateAlterTable(
    const AlterTableStmt& alter_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::AlterTable& out) const {
  ZETASQL_RET_CHECK_EQ(alter_statement.objtype, OBJECT_TABLE);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(alter_statement, options));

  ZETASQL_ASSIGN_OR_RETURN(*out.mutable_table_name(),
                   GetTableName(*alter_statement.relation, "ALTER TABLE"));

  ZETASQL_ASSIGN_OR_RETURN(const AlterTableCmd* first_cmd,
                   (GetListItemAsNode<AlterTableCmd, T_AlterTableCmd>(
                       alter_statement.cmds, 0)));

  switch (first_cmd->subtype) {
    case AT_AddColumn: {
      TableContext context;

      ZETASQL_ASSIGN_OR_RETURN(const ColumnDef* column,
                       (DowncastNode<ColumnDef, T_ColumnDef>(first_cmd->def)));
      ZETASQL_RETURN_IF_ERROR(TranslateColumnDefinition(
          *column, options, context,
          *out.mutable_add_column()->mutable_column()));

      absl::string_view column_name = out.add_column().column().column_name();

      // It is not possible to create table without primary key, therefore
      // any new primary key will be invalid
      if (!context.primary_key_columns.empty()) {
        return UnsupportedTranslationError(
            "Primary keys cannot be added via <ALTER TABLE> "
            "statement.");
      }

      // TODO: Spanner SDL requires separate alter statement for
      // a constraint (foreign key, check, etc.) which might pose atomicity
      // issues; support for this requires further investigation
      if (!context.foreign_keys.empty()) {
        return UnsupportedTranslationError(
            "Foreign key creation is not supported in <ALTER TABLE ADD COLUMN> "
            "statement.");
      }
      if (!context.check_constraints.empty()) {
        return UnsupportedTranslationError(
            "Check constraint creation is not supported in <ALTER TABLE ADD "
            "COLUMN> statement");
      }

      if (context.not_null_columns.contains(column_name)) {
        out.mutable_add_column()->mutable_column()->set_not_null(true);
      }

      if (first_cmd->missing_ok) {
        if (!options.enable_if_not_exists) {
          return UnsupportedTranslationError(
              "IF NOT EXISTS is not supported in <ALTER TABLE ADD "
              "COLUMN> statement");
        }
        out.mutable_add_column()->set_existence_modifier(
            google::spanner::emulator::backend::ddl::IF_NOT_EXISTS);
      }

      return absl::OkStatus();
    }

    case AT_DropColumn: {
      out.set_drop_column(first_cmd->name);

      return absl::OkStatus();
    }

    case AT_AddConstraint: {
      ZETASQL_ASSIGN_OR_RETURN(
          const Constraint* constraint,
          (DowncastNode<Constraint, T_Constraint>(first_cmd->def)));
      ZETASQL_RETURN_IF_ERROR(
          ValidateParseTreeNode(*constraint, /*add_in_alter=*/true, options));

      if (!constraint->initially_valid && constraint->skip_validation) {
        return UnsupportedTranslationError(
            "<NOT VALID> clause is not supported in <ALTER TABLE ADD> "
            "statement.");
      }

      switch (constraint->contype) {
        case CONSTR_FOREIGN:
          ZETASQL_RETURN_IF_ERROR(TranslateForeignKey(
              *constraint, /*target_column=*/nullptr,
              *out.mutable_add_foreign_key()->mutable_foreign_key()));
          break;

        case CONSTR_CHECK:
          ZETASQL_RETURN_IF_ERROR(TranslateCheckConstraint(
              *constraint, options,
              *out.mutable_add_check_constraint()->mutable_check_constraint()));
          break;

        default:
          ZETASQL_RET_CHECK_FAIL() << "Constraint type should have been validated "
                              "separately in ValidateParseTreeNode";
      }

      return absl::OkStatus();
    }

    case AT_DropConstraint: {
      out.mutable_drop_constraint()->set_name(first_cmd->name);

      return absl::OkStatus();
    }

    case AT_AlterColumnType: {
      google::spanner::emulator::backend::ddl::ColumnDefinition* out_column =
          out.mutable_alter_column()->mutable_column();

      out_column->set_column_name(first_cmd->name);
      ZETASQL_ASSIGN_OR_RETURN(const ColumnDef* column_def,
                       (DowncastNode<ColumnDef, T_ColumnDef>(first_cmd->def)));
      const TypeName* t = column_def->typeName;
      ZETASQL_RETURN_IF_ERROR(
          ProcessColumnTypeWithAttributes(*t, options, *out_column));
      if (list_length(alter_statement.cmds) == 2) {
        ZETASQL_ASSIGN_OR_RETURN(const AlterTableCmd* second_cmd,
                         (GetListItemAsNode<AlterTableCmd, T_AlterTableCmd>(
                             alter_statement.cmds, 1)));
        out_column->set_not_null(second_cmd->subtype == AT_SetNotNull);
      }
      return absl::OkStatus();
    }

    case AT_ColumnDefault: {
      google::spanner::emulator::backend::ddl::AlterTable::AlterColumn* alter_column =
          out.mutable_alter_column();
      alter_column->mutable_column()->set_column_name(first_cmd->name);

      // `type` is required in ColumnDefinition, so set it to NONE here.
      // It will be replaced later using the type from current schema when
      // applying the schema in analyzer.
      alter_column->mutable_column()->set_type(
          google::spanner::emulator::backend::ddl::ColumnDefinition::NONE);

      if (first_cmd->def == nullptr) {
        alter_column->set_operation(
            google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::DROP_DEFAULT);
      } else {
        alter_column->set_operation(
            google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::SET_DEFAULT);
        ZETASQL_ASSIGN_OR_RETURN(std::string serialized_expression,
                         CheckedPgNodeToString(first_cmd->def));
        google::spanner::emulator::backend::ddl::SQLExpressionOrigin* expression_origin =
            alter_column->mutable_column()
                ->mutable_column_default()
                ->mutable_expression_origin();
        expression_origin->set_serialized_parse_tree(serialized_expression);
        if (options.enable_expression_string) {
          expression_origin->set_original_expression(
              first_cmd->raw_expr_string);
        }
      }
      return absl::OkStatus();
    }

    case AT_SetNotNull: {
      google::spanner::emulator::backend::ddl::AlterTable::AlterColumn* alter_column =
          out.mutable_alter_column();
      alter_column->mutable_column()->set_column_name(first_cmd->name);
      alter_column->set_operation(
          google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::SET_NOT_NULL);
      // `type` is required in ColumnDefinition, so set it to NONE here.
      // It will be replaced later using the type from current schema when
      // applying the schema in analyzer.
      alter_column->mutable_column()->set_type(
          google::spanner::emulator::backend::ddl::ColumnDefinition::NONE);

      google::spanner::emulator::backend::ddl::ColumnDefinition* out_column =
          out.mutable_alter_column()->mutable_column();
      out_column->set_not_null(true);
      return absl::OkStatus();
    }

    case AT_DropNotNull: {
      google::spanner::emulator::backend::ddl::AlterTable::AlterColumn* alter_column =
          out.mutable_alter_column();
      alter_column->mutable_column()->set_column_name(first_cmd->name);
      alter_column->set_operation(
          google::spanner::emulator::backend::ddl::AlterTable::AlterColumn::DROP_NOT_NULL);
      // `type` is required in ColumnDefinition, so set it to NONE here.
      // It will be replaced later using the type from current schema when
      // applying the schema in analyzer.
      alter_column->mutable_column()->set_type(
          google::spanner::emulator::backend::ddl::ColumnDefinition::NONE);

      google::spanner::emulator::backend::ddl::ColumnDefinition* out_column =
          out.mutable_alter_column()->mutable_column();
      out_column->set_not_null(false);
      return absl::OkStatus();
    }

    case AT_DropTtl: {
      // The row deletion policy drop message is empty, so just call the mutable
      // accessor and ignore what it returns. That'll instantiate and set this
      // field in the oneof, which is what we want.
      out.mutable_drop_row_deletion_policy();
      return absl::OkStatus();
    }

    case AT_AddTtl: {
      ZETASQL_ASSIGN_OR_RETURN(const Ttl* ttl,
                       (DowncastNode<Ttl, T_Ttl>(first_cmd->def)));
      return TranslateRowDeletionPolicy(*ttl,
                                        *out.mutable_add_row_deletion_policy());
    }
    case AT_AlterTtl: {
      ZETASQL_ASSIGN_OR_RETURN(const Ttl* ttl,
                       (DowncastNode<Ttl, T_Ttl>(first_cmd->def)));
      return TranslateRowDeletionPolicy(
          *ttl, *out.mutable_alter_row_deletion_policy());
    }

    case AT_SetOnDeleteCascade: {
      out.mutable_set_on_delete()->set_action(
          google::spanner::emulator::backend::ddl::InterleaveClause::CASCADE);
      return absl::
          OkStatus();
    }
    case AT_SetOnDeleteNoAction: {
      out.mutable_set_on_delete()->set_action(
          google::spanner::emulator::backend::ddl::InterleaveClause::NO_ACTION);
      return absl::OkStatus();
    }
    case AT_SetInterleaveIn: {
      if (!options.enable_interleave_in) {
        return UnsupportedTranslationError(
            "<ALTER TABLE ... SET INTERLEAVE IN> statement is not supported.");
      }

      ZETASQL_ASSIGN_OR_RETURN(
          const InterleaveSpec* interleavespec,
          (DowncastNode<InterleaveSpec, T_InterleaveSpec>(first_cmd->def)));

      ZETASQL_RETURN_IF_ERROR(TranslateInterleaveIn(
          interleavespec,
          out.mutable_set_interleave_clause()->mutable_interleave_clause(),
          options));
      return absl::OkStatus();
    }

    case AT_AddSynonym: {
      out.mutable_add_synonym()->set_synonym(first_cmd->name);
      return absl::OkStatus();
    }
    case AT_DropSynonym: {
      out.mutable_drop_synonym()->set_synonym(first_cmd->name);
      return absl::OkStatus();
    }

    default: {
      ZETASQL_RET_CHECK_FAIL() << "Unknown command type in <ALTER TABLE> statement.";
    }
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::UnsupportedTranslationError(
    absl::string_view error_mesage) const {
  return absl::FailedPreconditionError(error_mesage);
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateDatabase(
    const CreatedbStmt& create_statement,
    google::spanner::emulator::backend::ddl::CreateDatabase& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(create_statement));

  *out.mutable_db_name() = create_statement.dbname;
  ZETASQL_RET_CHECK(!out.db_name().empty());

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropStatement(
    const DropStmt& drop_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::DDLStatement& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(drop_statement, options));

  switch (drop_statement.removeType) {
    case OBJECT_TABLE:
      return TranslateDropTable(drop_statement, *out.mutable_drop_table(),
                                options);

    case OBJECT_INDEX:
      return TranslateDropIndex(drop_statement, *out.mutable_drop_index(),
                                options);

    case OBJECT_SCHEMA:
      return TranslateDropSchema(drop_statement, *out.mutable_drop_schema());

    case OBJECT_VIEW:
      return TranslateDropView(drop_statement, *out.mutable_drop_function());

    case OBJECT_CHANGE_STREAM:
      return TranslateDropChangeStream(
          drop_statement, options, *out.mutable_drop_change_stream());

    case OBJECT_SEQUENCE:
      return TranslateDropSequence(drop_statement, options,
                                   *out.mutable_drop_sequence());
    default:
      ZETASQL_RET_CHECK_FAIL()
          << "removeType should have been validated in ValidateTreeNode.";
  }
}
absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropChangeStream(
    const DropStmt& drop_change_stream_statement,
    const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::DropChangeStream& out) const {
  if (!options.enable_change_streams) {
    return UnsupportedTranslationError(
        "<DROP CHANGE STREAM> statement is not supported.");
  }

  ZETASQL_RET_CHECK_EQ(drop_change_stream_statement.removeType, OBJECT_CHANGE_STREAM);
  ZETASQL_ASSIGN_OR_RETURN(
      const RangeVar* changestream_to_drop_node,
      (SingleItemListAsNode<RangeVar, T_RangeVar>(
          drop_change_stream_statement.objects)));
  ZETASQL_RET_CHECK(changestream_to_drop_node->relname &&
            *changestream_to_drop_node->relname != '\0');

  ZETASQL_ASSIGN_OR_RETURN(
      std::string change_stream_name,
      GetChangeStreamTableName(*changestream_to_drop_node,
                               "DROP CHANGE STREAM"));
  out.set_change_stream_name(change_stream_name);
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropView(
    const DropStmt& drop_statement, google::spanner::emulator::backend::ddl::DropFunction& out) const {
  ZETASQL_RET_CHECK_EQ(drop_statement.removeType, OBJECT_VIEW);

  ZETASQL_ASSIGN_OR_RETURN(
      const List* view_to_drop_list,
      (SingleItemListAsNode<List, T_List>)(drop_statement.objects));

  out.set_function_kind(google::spanner::emulator::backend::ddl::Function::VIEW);

  if (view_to_drop_list->length == 2) {
    ZETASQL_ASSIGN_OR_RETURN(
        const String* schema_to_drop_node,
        (GetListItemAsNode<String, T_String>)(view_to_drop_list, 0));
    ZETASQL_ASSIGN_OR_RETURN(
        const String* view_to_drop_node,
        (GetListItemAsNode<String, T_String>)(view_to_drop_list, 1));
    if (strcmp(schema_to_drop_node->sval, "public") != 0) {
      *out.mutable_function_name() = absl::Substitute(
          "$0.$1", schema_to_drop_node->sval, view_to_drop_node->sval);
    } else {
      *out.mutable_function_name() = view_to_drop_node->sval;
    }
  } else if (view_to_drop_list->length == 1) {
    ZETASQL_ASSIGN_OR_RETURN(
        const String* view_to_drop_node,
        (SingleItemListAsNode<String, T_String>)(view_to_drop_list));
    *out.mutable_function_name() = view_to_drop_node->sval;
  } else {
    // Caller should have called ValidateParseTreeNode, which ensures the size
    // of view_to_drop_list is 1 or 2. So this should never be reached.
    ZETASQL_RET_CHECK_FAIL() << "Incorrect number of name components in drop target: "
                     << view_to_drop_list->length;
  }

  if (drop_statement.missing_ok) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_EXISTS);
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropTable(
    const DropStmt& drop_statement, google::spanner::emulator::backend::ddl::DropTable& out,
    const TranslationOptions& options) const {
  ZETASQL_RET_CHECK_EQ(drop_statement.removeType, OBJECT_TABLE);

  ZETASQL_ASSIGN_OR_RETURN(
      const List* table_to_drop_list,
      (SingleItemListAsNode<List, T_List>(drop_statement.objects)));

  // Caller should have called ValidateParseTreeNode, which ensures the size of
  // table_to_drop_list is 1 or 2.
  if (table_to_drop_list->length == 2) {
    ZETASQL_ASSIGN_OR_RETURN(const String* schema_to_drop_node,
                     (GetListItemAsNode<String, T_String>)(
                         table_to_drop_list, 0));
    ZETASQL_ASSIGN_OR_RETURN(const String* table_to_drop_node,
                     (GetListItemAsNode<String, T_String>)(
                         table_to_drop_list, 1));
    if (strcmp(schema_to_drop_node->sval, "public") != 0) {
      *out.mutable_table_name() = absl::Substitute("$0.$1",
                                                  schema_to_drop_node->sval,
                                                  table_to_drop_node->sval);
    } else {
      *out.mutable_table_name() = absl::StrCat(table_to_drop_node->sval);
    }
  } else {
    ZETASQL_RET_CHECK_EQ(table_to_drop_list->length, 1)
        << "Incorrect number of name components in drop target.";
    ZETASQL_ASSIGN_OR_RETURN(const String* table_to_drop_node,
                     (SingleItemListAsNode<String, T_String>)(
                         table_to_drop_list));
    *out.mutable_table_name() = table_to_drop_node->sval;
  }

  if (drop_statement.missing_ok && options.enable_if_not_exists) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_EXISTS);
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateIndex(
    const IndexStmt& create_index_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::CreateIndex& out) const {
  CreateIndexStatementTranslator translator(*this, options);
  ZETASQL_RETURN_IF_ERROR(translator.Translate(create_index_statement, out, options));
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateAlterIndex(
    const AlterTableStmt& alter_index_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::AlterIndex& out) const {
  if (!options.enable_alter_index) {
    return UnsupportedTranslationError("<ALTER INDEX> is not supported.");
  }

  ZETASQL_RET_CHECK_EQ(alter_index_statement.objtype, OBJECT_INDEX);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(alter_index_statement, options));

  CreateIndexStatementTranslator translator(*this, options);
  ZETASQL_RETURN_IF_ERROR(translator.Translate(alter_index_statement, out));
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropIndex(
    const DropStmt& drop_statement, google::spanner::emulator::backend::ddl::DropIndex& out,
    const TranslationOptions& options) const {
  ZETASQL_RET_CHECK_EQ(drop_statement.removeType, OBJECT_INDEX);

  ZETASQL_ASSIGN_OR_RETURN(
      const List* index_to_drop_list,
      (SingleItemListAsNode<List, T_List>)(drop_statement.objects));

  // Caller should have called ValidateParseTreeNode, which ensures the size of
  // index_to_drop_list is 1 or 2.
  if (index_to_drop_list->length == 2) {
    ZETASQL_ASSIGN_OR_RETURN(const String* schema_to_drop_node,
                     (GetListItemAsNode<String, T_String>)(
                         index_to_drop_list, 0));
    ZETASQL_ASSIGN_OR_RETURN(const String* index_to_drop_node,
                     (GetListItemAsNode<String, T_String>)(
                         index_to_drop_list, 1));
    if (strcmp(schema_to_drop_node->sval, "public") != 0) {
      *out.mutable_index_name() = absl::Substitute("$0.$1",
                                                  schema_to_drop_node->sval,
                                                  index_to_drop_node->sval);
    } else {
      *out.mutable_index_name() = absl::StrCat(index_to_drop_node->sval);
    }
  } else {
    ZETASQL_RET_CHECK_EQ(index_to_drop_list->length, 1)
        << "Incorrect number of name components in drop target.";
    ZETASQL_ASSIGN_OR_RETURN(const String* index_to_drop_node,
                     (SingleItemListAsNode<String, T_String>)(
                         index_to_drop_list));
    *out.mutable_index_name() = index_to_drop_node->sval;
  }

  if (drop_statement.missing_ok && options.enable_if_not_exists) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_EXISTS);
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateSchema(
    const CreateSchemaStmt& create_statement,
    google::spanner::emulator::backend::ddl::CreateSchema& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(create_statement));

  if (internal::IsReservedName(create_statement.schemaname)) {
    return absl::InvalidArgumentError(
        "'pg_' is not supported as a prefix for a schema name.");
  }
  if (absl::StartsWith(create_statement.schemaname, "spanner_")) {
    // "spanner_*" namespaces are mainly reserved for functions; it's a place to
    // put old versions of functions whose behavior we need to modify. It would
    // be spanner_{version}.{function_name}.
    return absl::InvalidArgumentError(
        "'spanner_' is not supported as a prefix for a schema name.");
  }
  if (create_statement.if_not_exists) {
    out.set_existence_modifier(google::spanner::emulator::backend::ddl::CreateSchema::IF_NOT_EXISTS);
  }
  out.set_schema_name(create_statement.schemaname);
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateDropSchema(
    const DropStmt& drop_statement, google::spanner::emulator::backend::ddl::DropSchema& out) const {
  ZETASQL_RET_CHECK_EQ(drop_statement.removeType, OBJECT_SCHEMA);

  ZETASQL_ASSIGN_OR_RETURN(
      const String* schema_to_drop_node,
      (SingleItemListAsNode<String, T_String>)(drop_statement.objects));

  *out.mutable_schema_name() = schema_to_drop_node->sval;

  if (drop_statement.missing_ok) {
    out.set_if_exists(true);
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateVacuum(
    const VacuumStmt& vacuum_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::Analyze& out) const {
  if (!options.enable_analyze) {
    return UnsupportedTranslationError("<ANALYZE> statement is not supported.");
  }

  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(vacuum_statement));
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateView(
    const ViewStmt& view_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::CreateFunction& out) const {
  if (!options.enable_create_view) {
    return UnsupportedTranslationError(
        "<CREATE VIEW> statement is not supported.");
  }

  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(view_statement));

  out.set_function_kind(google::spanner::emulator::backend::ddl::Function_Kind_VIEW);
  out.set_is_or_replace(view_statement.replace);
  out.set_sql_security(
      google::spanner::emulator::backend::ddl::Function_SqlSecurity::Function_SqlSecurity_INVOKER);
  out.set_function_name(
      GetTableName(*view_statement.view, "CREATE VIEW").value());
  out.mutable_sql_body_origin()->set_original_expression(
      view_statement.query_string);
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateAlterChangeStream(
    const AlterChangeStreamStmt& alter_change_stream_stmt,
    const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::AlterChangeStream& out) const {
  if (!options.enable_change_streams) {
    return UnsupportedTranslationError(
        "<ALTER CHANGE STREAM> statement is not supported.");
  }
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(alter_change_stream_stmt));

  // Populate the change stream name.
  ZETASQL_ASSIGN_OR_RETURN(std::string change_stream_name,
                   GetChangeStreamTableName(
                       *alter_change_stream_stmt.change_stream_name,
                       "ALTER CHANGE STREAM"));
  out.set_change_stream_name(change_stream_name);

  // Change stream set for clause.
  if (alter_change_stream_stmt.opt_for_tables != nullptr ||
      alter_change_stream_stmt.for_all) {
    ZETASQL_RETURN_IF_ERROR(PopulateChangeStreamForClause(
        alter_change_stream_stmt.opt_for_tables,
        alter_change_stream_stmt.for_all, "ALTER CHANGE STREAM",
        out.mutable_set_for_clause()));
  }

  // Change stream drop for clause.
  if (alter_change_stream_stmt.opt_drop_for_tables != nullptr) {
    return absl::InvalidArgumentError(
        "Only DROP FOR ALL is supported for now.");
  }
  if (alter_change_stream_stmt.drop_for_all) {
    ZETASQL_RETURN_IF_ERROR(PopulateChangeStreamForClause(
        alter_change_stream_stmt.opt_drop_for_tables,
        alter_change_stream_stmt.drop_for_all, "ALTER CHANGE STREAM",
        out.mutable_drop_for_clause()));
  }

  // Populate the change stream options, if any.
  if (alter_change_stream_stmt.opt_options != nullptr) {
    ZETASQL_RETURN_IF_ERROR(PopulateChangeStreamOptions(
        alter_change_stream_stmt.opt_options, "ALTER CHANGE STREAM",
        out.mutable_set_options()->mutable_options(), options));
  }

  // Populate the change stream reset options, if any.
  if (alter_change_stream_stmt.opt_reset_options != nullptr) {
    for (const String* option_name :
         StructList<String*>(alter_change_stream_stmt.opt_reset_options)) {
      ZETASQL_RET_CHECK_EQ(option_name->type, T_String);
      ZETASQL_RET_CHECK_NE(option_name->sval, nullptr);
      ZETASQL_RET_CHECK_NE(*option_name->sval, '\0');
      std::string option_string = option_name->sval;
      if (option_string == internal::PostgreSQLConstants::
          kChangeStreamValueCaptureTypeOptionName) {
        google::spanner::emulator::backend::ddl::SetOption* option_out =
            out.mutable_set_options()->mutable_options()->Add();
        option_out->set_option_name(internal::PostgreSQLConstants::
                                    kChangeStreamValueCaptureTypeOptionName);
        option_out->set_string_value("OLD_AND_NEW_VALUES");
      } else if (option_string == internal::PostgreSQLConstants::
                 kChangeStreamRetentionPeriodOptionName) {
        google::spanner::emulator::backend::ddl::SetOption* option_out =
            out.mutable_set_options()->mutable_options()->Add();
        option_out->set_option_name(internal::PostgreSQLConstants::
                                    kChangeStreamRetentionPeriodOptionName);
        option_out->set_string_value("24h");
      } else if (option_string == internal::PostgreSQLConstants::
                                      kChangeStreamExcludeInsertOptionName ||
                 option_string == internal::PostgreSQLConstants::
                                      kChangeStreamExcludeUpdateOptionName ||
                 option_string == internal::PostgreSQLConstants::
                                      kChangeStreamExcludeDeleteOptionName ||
                 option_string ==
                     internal::PostgreSQLConstants::
                         kChangeStreamExcludeTtlDeletesOptionName ||
                 option_string ==
                     internal::PostgreSQLConstants::
                         kChangeStreamAllowTxnExclusionOptionName) {
        google::spanner::emulator::backend::ddl::SetOption* option_out =
            out.mutable_set_options()->mutable_options()->Add();
        option_out->set_option_name(option_string);
        option_out->set_bool_value(false);
      } else {
        return absl::InvalidArgumentError(
            "Invalid change stream option in <ALTER CHANGE STREAM> statement.");
      }
    }
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateCreateChangeStream(
    const CreateChangeStreamStmt& create_change_stream_stmt,
    const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::CreateChangeStream& out) const {
  if (!options.enable_change_streams) {
    return UnsupportedTranslationError(
        "<CREATE CHANGE STREAM> statement is not supported.");
  }
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(create_change_stream_stmt));

  // Populate the change stream name.
  ZETASQL_ASSIGN_OR_RETURN(std::string change_stream_name,
                   GetChangeStreamTableName(
                       *create_change_stream_stmt.change_stream_name,
                       "CREATE CHANGE STREAM"));
  out.set_change_stream_name(change_stream_name);

  // Change stream for clause.
  if (create_change_stream_stmt.opt_for_tables != nullptr ||
      create_change_stream_stmt.for_all) {
    ZETASQL_RETURN_IF_ERROR(PopulateChangeStreamForClause(
        create_change_stream_stmt.opt_for_tables,
        create_change_stream_stmt.for_all, "CREATE CHANGE STREAM",
        out.mutable_for_clause()));
  }

  // Populate the change stream options, if any.
  if (create_change_stream_stmt.opt_options != nullptr) {
    ZETASQL_RETURN_IF_ERROR(PopulateChangeStreamOptions(
        create_change_stream_stmt.opt_options, "CREATE CHANGE STREAM",
        out.mutable_set_options(), options));
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::PopulateChangeStreamOptions(
    List* opt_options, absl::string_view parent_statement,
    OptionList* options_out, const TranslationOptions& options) const {
  ABSL_DCHECK(opt_options != nullptr);
  absl::flat_hash_set<std::string> seen_options;
  for (DefElem* def_elem : StructList<DefElem*>(opt_options)) {
    if (def_elem == nullptr || def_elem->arg == nullptr || !def_elem->defname ||
        *def_elem->defname == '\0') {
      return absl::InvalidArgumentError(absl::Substitute(
          "Failed to parse change stream option correctly in <$0> statement.",
          parent_statement));
    }
    if (seen_options.contains(def_elem->defname)) {
      return absl::InvalidArgumentError(absl::Substitute(
          "Contains duplicate change stream option '$0' in <$1> statement.",
          def_elem->defname, parent_statement));
    }
    seen_options.insert(def_elem->defname);
    if (def_elem->defname ==
        internal::PostgreSQLConstants::
        kChangeStreamValueCaptureTypeOptionName ||
        def_elem->defname == internal::PostgreSQLConstants::
        kChangeStreamRetentionPeriodOptionName) {
      if (def_elem->arg->type != T_String) {
        return absl::InvalidArgumentError(absl::Substitute(
            "Failed to provide valid option value for '$0' in <$1> statement.",
            def_elem->defname, parent_statement));
      }
      ZETASQL_ASSIGN_OR_RETURN(const String* arg_value,
                       (DowncastNode<String, T_String>(def_elem->arg)));
      std::string value = arg_value->sval;
      google::spanner::emulator::backend::ddl::SetOption* option_out = options_out->Add();
      option_out->set_option_name(def_elem->defname);
      if (absl::AsciiStrToLower(value) == "null") {
        option_out->set_null_value(true);
      } else {
        option_out->set_string_value(value);
      }
    } else if (def_elem->defname == internal::PostgreSQLConstants::
                                        kChangeStreamExcludeInsertOptionName ||
               def_elem->defname == internal::PostgreSQLConstants::
                                        kChangeStreamExcludeUpdateOptionName ||
               def_elem->defname == internal::PostgreSQLConstants::
                                        kChangeStreamExcludeDeleteOptionName ||
               def_elem->defname ==
                   internal::PostgreSQLConstants::
                       kChangeStreamExcludeTtlDeletesOptionName ||
               def_elem->defname ==
                   internal::PostgreSQLConstants::
                       kChangeStreamAllowTxnExclusionOptionName) {
      if (def_elem->defname == internal::PostgreSQLConstants::
                                   kChangeStreamExcludeTtlDeletesOptionName &&
          !options.enable_change_streams_ttl_deletes_filter_option) {
        return UnsupportedTranslationError(
            "Option exclude_ttl_deletes is not supported yet.");
      } else if (def_elem->defname ==
                     internal::PostgreSQLConstants::
                         kChangeStreamAllowTxnExclusionOptionName &&
                 !options.enable_change_streams_allow_txn_exclusion_option) {
        return UnsupportedTranslationError(
            "Option allow_txn_exclusion is not supported yet.");
      } else if (!options.enable_change_streams_mod_type_filter_options) {
        return UnsupportedTranslationError(
            "Options exclude_insert, exclude_update, and exclude_delete are "
            "not supported yet.");
      }
      if (def_elem->arg->type != T_String) {
        return absl::InvalidArgumentError(absl::Substitute(
            "Failed to provide valid option value for '$0' in <$1> statement.",
            def_elem->defname, parent_statement));
      }
      ZETASQL_ASSIGN_OR_RETURN(const String* arg_value,
                       (DowncastNode<String, T_String>(def_elem->arg)));
      std::string value = arg_value->sval;
      google::spanner::emulator::backend::ddl::SetOption* option_out = options_out->Add();
      option_out->set_option_name(def_elem->defname);
      if (absl::AsciiStrToLower(value) == "null") {
        option_out->set_null_value(true);
      } else if (value == PGConstants::kPgTrueLiteral) {
        option_out->set_bool_value(true);
      } else if (value == PGConstants::kPgFalseLiteral) {
        option_out->set_bool_value(false);
      } else {
        return UnsupportedTranslationError(
            absl::Substitute("Unsupported option value in change stream option "
                             "'$0' in <$1> statement.",
                             def_elem->defname, parent_statement));
      }
    } else {
      return absl::InvalidArgumentError(absl::Substitute(
          "Invalid change stream option '$0' in <$1> statement.",
          def_elem->defname, parent_statement));
    }
  }
  return absl::OkStatus();
}

absl::Status
PostgreSQLToSpannerDDLTranslatorImpl::PopulateChangeStreamForClause(
    const List* opt_for_or_drop_for_tables, bool for_or_drop_for_all,
    absl::string_view parent_statement,
    google::spanner::emulator::backend::ddl::ChangeStreamForClause* change_stream_for_clause) const {
  if (opt_for_or_drop_for_tables != nullptr) {
    for (ChangeStreamTrackedTable* tracked_table :
         StructList<ChangeStreamTrackedTable*>(opt_for_or_drop_for_tables)) {
      google::spanner::emulator::backend::ddl::ChangeStreamForClause::TrackedTables::Entry*
          tracked_table_out = change_stream_for_clause->mutable_tracked_tables()
                                  ->add_table_entry();
      // Set the table name.
      ZETASQL_ASSIGN_OR_RETURN(
          std::string table_name,
          GetTableName(*tracked_table->table_name, parent_statement));

      tracked_table_out->set_table_name(table_name);

      // Populate tracked columns, if any.
      ::google::spanner::emulator::backend::ddl::ChangeStreamForClause_TrackedTables_Entry_TrackedColumns*
          columns = tracked_table_out->mutable_tracked_columns();
      if (tracked_table->columns != nullptr) {
        // If there are explicitly tracked columns, for_all_columns cannot
        // be set to true.
        ZETASQL_RET_CHECK_EQ(tracked_table->for_all_columns, false);
        for (String* column_value :
             StructList<String*>(tracked_table->columns)) {
          const char* column_name = column_value->sval;
          ZETASQL_RET_CHECK(column_name && *column_name != '\0');
          columns->add_column_name(column_name);
        }
      } else if (tracked_table->for_all_columns == true) {
        tracked_table_out->set_all_columns(true);
      }
    }
  } else if (for_or_drop_for_all == true) {
    change_stream_for_clause->set_all(true);
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::TranslateRenameStatement(
    const RenameStmt& rename_statement, const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::AlterTable& out) const {
  ZETASQL_RET_CHECK_EQ(rename_statement.renameType, OBJECT_TABLE);

  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(rename_statement, options));

  ZETASQL_ASSIGN_OR_RETURN(*out.mutable_table_name(),
                   GetTableName(*rename_statement.relation, "RENAME TABLE"));

  *out.mutable_rename_to()->mutable_name() = rename_statement.newname;

  if (rename_statement.addSynonym) {
    *out.mutable_rename_to()->mutable_synonym() = out.table_name();
  }

  return absl::OkStatus();
}

absl::Status
PostgreSQLToSpannerDDLTranslatorImpl::TranslateTableChainedRenameStatement(
    const TableChainedRenameStmt& table_chained_rename_statement,
    const TranslationOptions& options, google::spanner::emulator::backend::ddl::RenameTable& out) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(table_chained_rename_statement, options));
  for (TableRenameOp* rename_op :
       StructList<TableRenameOp*>(table_chained_rename_statement.ops)) {
    auto op = out.add_rename_op();
    ZETASQL_ASSIGN_OR_RETURN(*op->mutable_from_name(),
                     GetTableName(*rename_op->fromName, "RENAME TABLE"));
    *op->mutable_to_name() = rename_op->toName;
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::Visitor::Visit(
    const RawStmt& raw_statement) {
  // Make sure that if RawStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(raw_statement,
                         FieldTypeChecker<Node*>(raw_statement.stmt),
                         FieldTypeChecker<int>(raw_statement.stmt_location),
                         FieldTypeChecker<int>(raw_statement.stmt_len),
                         FieldTypeChecker<List*>(raw_statement.statementHints));

  // Statement-level query hints are unsupported in DDL statements, but the
  // grammar doesn't differentiate. Check and reject them here.
  if (raw_statement.statementHints != nullptr) {
    return absl::InvalidArgumentError(
        "Statement hints are not supported in DDL.");
  }

  ZETASQL_RET_CHECK_EQ(raw_statement.type, T_RawStmt);
  ZETASQL_RET_CHECK_NE(raw_statement.stmt, nullptr);

  google::spanner::emulator::backend::ddl::DDLStatement result_statement;

  switch (raw_statement.stmt->type) {
    //LINT.IfChange
    case T_CreatedbStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const CreatedbStmt* statement,
          (DowncastNode<CreatedbStmt, T_CreatedbStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateDatabase(
          *statement, *result_statement.mutable_create_database()));

      break;
    }

    case T_CreateStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const CreateStmt* statement,
          (DowncastNode<CreateStmt, T_CreateStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateTable(
          *statement, options_, *result_statement.mutable_create_table()));

      break;
    }

    case T_AlterTableStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const AlterTableStmt* statement,
          (DowncastNode<AlterTableStmt, T_AlterTableStmt>(raw_statement.stmt)));

      switch (statement->objtype) {
        case OBJECT_TABLE: {
          ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateAlterTable(
              *statement, options_, *result_statement.mutable_alter_table()));
          break;
        }

        case OBJECT_INDEX: {
          ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateAlterIndex(
              *statement, options_, *result_statement.mutable_alter_index()));
          break;
        }

        default:
          return absl::InvalidArgumentError(
              "Object type is not supported in <ALTER> statement.");
      }
      break;
    }

    case T_DropStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const DropStmt* statement,
          (DowncastNode<DropStmt, T_DropStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateDropStatement(
          *statement, options_, result_statement));

      break;
    }

    case T_IndexStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const IndexStmt* statement,
          (DowncastNode<IndexStmt, T_IndexStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateIndex(
          *statement, options_, *result_statement.mutable_create_index()));

      break;
    }

    case T_CreateSchemaStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const CreateSchemaStmt* statement,
          (DowncastNode<CreateSchemaStmt, T_CreateSchemaStmt>(
              raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateSchema(
          *statement, *result_statement.mutable_create_schema()));

      break;
    }

    case T_VacuumStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const VacuumStmt* statement,
          (DowncastNode<VacuumStmt, T_VacuumStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateVacuum(
          *statement, options_, *result_statement.mutable_analyze()));

      break;
    }

    case T_ViewStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const ViewStmt* statement,
          (DowncastNode<ViewStmt, T_ViewStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateView(
          *statement, options_, *result_statement.mutable_create_function()));

      break;
    }

    case T_CreateChangeStreamStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const CreateChangeStreamStmt* statement,
          (DowncastNode<CreateChangeStreamStmt, T_CreateChangeStreamStmt>(
              raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateChangeStream(
          *statement, options_,
          *result_statement.mutable_create_change_stream()));

      break;
    }

    case T_AlterChangeStreamStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const AlterChangeStreamStmt* statement,
          (DowncastNode<AlterChangeStreamStmt, T_AlterChangeStreamStmt>(
              raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateAlterChangeStream(
          *statement, options_,
          *result_statement.mutable_alter_change_stream()));

      break;
    }

    case T_CreateSeqStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const CreateSeqStmt* statement,
          (DowncastNode<CreateSeqStmt, T_CreateSeqStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateCreateSequence(
          *statement, options_, *result_statement.mutable_create_sequence()));
      break;
    }

    case T_AlterSeqStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const AlterSeqStmt* statement,
          (DowncastNode<AlterSeqStmt, T_AlterSeqStmt>(raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateAlterSequence(
          *statement, options_, *result_statement.mutable_alter_sequence()));
      break;
    }

    case T_RenameStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const RenameStmt* statement,
          (DowncastNode<RenameStmt, T_RenameStmt>(raw_statement.stmt)));
      if (statement->renameType != ObjectType::OBJECT_TABLE) {
        return ddl_translator_.UnsupportedTranslationError(
            "Only <TABLE> is supported for renaming.");
      }
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateRenameStatement(
          *statement, options_, *result_statement.mutable_alter_table()));

      break;
    }
    case T_TableChainedRenameStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const TableChainedRenameStmt* statement,
          (DowncastNode<TableChainedRenameStmt, T_TableChainedRenameStmt>(
              raw_statement.stmt)));
      ZETASQL_RETURN_IF_ERROR(ddl_translator_.TranslateTableChainedRenameStatement(
          *statement, options_, *result_statement.mutable_rename_table()));

      break;
    }
    case T_AlterOwnerStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const AlterOwnerStmt* statement,
          (DowncastNode<AlterOwnerStmt, T_AlterOwnerStmt>(raw_statement.stmt)));
      ZETASQL_ASSIGN_OR_RETURN(std::string object_name,
                       internal::ObjectTypeToString(statement->objectType));
      return ddl_translator_.UnsupportedTranslationError(
          absl::StrCat("<ALTER ", object_name, " OWNER> is not supported"));
      break;
    }
    case T_AlterStatsStmt: {
      return ddl_translator_.UnsupportedTranslationError(
          "<ALTER STATISTICS SET STATISTICS> is not supported");
      break;
    }
    case T_AlterObjectSchemaStmt: {
      ZETASQL_ASSIGN_OR_RETURN(
          const AlterObjectSchemaStmt* statement,
          (DowncastNode<AlterObjectSchemaStmt, T_AlterObjectSchemaStmt>(
              raw_statement.stmt)));
      ZETASQL_ASSIGN_OR_RETURN(std::string object_name,
                       internal::ObjectTypeToString(statement->objectType));
      return ddl_translator_.UnsupportedTranslationError(absl::StrCat(
          "<ALTER ", object_name, " SET SCHEMA> is not supported"));
      break;
    }
    default:
      return ddl_translator_.UnsupportedTranslationError(
          "Statement is not supported.");
    // LINT.ThenChange(../transformer/forward_query.cc)
  }

  AddStatementToOutput(std::move(result_statement));
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::Visitor::Visit(
    const List& parse_tree) {
  for (const RawStmt* raw_statement : StructList<RawStmt*>(&parse_tree)) {
    ZETASQL_RETURN_IF_ERROR(Visit(*raw_statement));
  }

  return absl::OkStatus();
}

absl::StatusOr<google::spanner::emulator::backend::ddl::KeyPartClause::Order>
PostgreSQLToSpannerDDLTranslatorImpl::CreateIndexStatementTranslator::
    ProcessOrdering(const SortByDir& index_elem_ordering,
                    const SortByNulls& index_elem_nulls_ordering,
                    const TranslationOptions& options) const {
  // This is a bit clunky, but ultimately more readable than checking if nulls
  // ordering enabled inside branches
  if (options.enable_nulls_ordering) {
    switch (index_elem_ordering) {
        // Spanner default nulls ordering is reverse to the PostgreSQL. This
        // means
        // that we cannot rely on defaults and have to explicitly set it.
      case SORTBY_DEFAULT:
      case SORTBY_ASC:
        switch (index_elem_nulls_ordering) {
          case SORTBY_NULLS_DEFAULT:
          case SORTBY_NULLS_LAST:
            return google::spanner::emulator::backend::ddl::KeyPartClause::ASC_NULLS_LAST;
          case SORTBY_NULLS_FIRST:
            return google::spanner::emulator::backend::ddl::KeyPartClause::ASC;
          default:
            return UnsupportedTranslationError(
                "NULLS order for index key is not supported in <CREATE INDEX> "
                "statement.");
        }
      case SORTBY_DESC:
        switch (index_elem_nulls_ordering) {
          case SORTBY_NULLS_DEFAULT:
          case SORTBY_NULLS_FIRST:
            return google::spanner::emulator::backend::ddl::KeyPartClause::DESC_NULLS_FIRST;
          case SORTBY_NULLS_LAST:
            return google::spanner::emulator::backend::ddl::KeyPartClause::DESC;
          default:
            return UnsupportedTranslationError(
                "NULLS order for index key is not supported in <CREATE INDEX> "
                "statement.");
        }
      default:
        return UnsupportedTranslationError(
            "Sort order for index key is not supported in <CREATE INDEX> "
            "statement.");
    }
  } else {
    switch (index_elem_ordering) {
        // Default sort order in Spanner is ASC
      case SORTBY_DEFAULT:
      case SORTBY_ASC:
        if (index_elem_nulls_ordering == SORTBY_NULLS_DEFAULT ||
            index_elem_nulls_ordering == SORTBY_NULLS_FIRST) {
          return google::spanner::emulator::backend::ddl::KeyPartClause::ASC;
        }
        return UnsupportedTranslationError(
            "<ASC NULLS LAST> is not supported in <CREATE INDEX> statement.");
      case SORTBY_DESC:
        if (index_elem_nulls_ordering == SORTBY_NULLS_DEFAULT ||
            index_elem_nulls_ordering == SORTBY_NULLS_LAST) {
          return google::spanner::emulator::backend::ddl::KeyPartClause::DESC;
        }
        return UnsupportedTranslationError(
            "<DESC NULLS FIRST> is not supported in <CREATE INDEX> statement.");
      default:
        return UnsupportedTranslationError(
            "Sort order for index key is not supported in <CREATE INDEX> "
            "statement.");
    }
  }
}

absl::Status
PostgreSQLToSpannerDDLTranslatorImpl::CreateIndexStatementTranslator::Translate(
    const IndexStmt& create_index_statement,
    google::spanner::emulator::backend::ddl::CreateIndex& create_index_out,
    const TranslationOptions& options) {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(create_index_statement, options));

  create_index_out.set_unique(create_index_statement.unique);

  ZETASQL_ASSIGN_OR_RETURN(std::string table_name,
                   ddl_translator_.GetTableName(
                       *create_index_statement.relation, "CREATE INDEX"));
  create_index_out.set_index_base_name(table_name);
  if (create_index_statement.relation->schemaname != nullptr &&
      strcmp(create_index_statement.relation->schemaname, "public") != 0) {
    create_index_out.set_index_name(
        absl::Substitute("$0.$1", create_index_statement.relation->schemaname,
                         create_index_statement.idxname));
  } else {
    create_index_out.set_index_name(create_index_statement.idxname);
  }

  // Process key columns
  for (IndexElem* index_elem :
       StructList<IndexElem*>(create_index_statement.indexParams)) {
    google::spanner::emulator::backend::ddl::KeyPartClause::Order ordering;
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view key_name,
                     TranslateIndexElem(*index_elem, ordering, options_));
    google::spanner::emulator::backend::ddl::KeyPartClause* key_part = create_index_out.add_key();
    key_part->set_key_name(key_name);
    key_part->set_order(ordering);

    // UNIQUE indexes should have all the keys as null filtered to account for
    // PG treating NULLs as not equal
    if (create_index_statement.unique) {
      null_filtered_columns_.insert(key_name);
    }
  }

  // Process INCLUDE columns that are translated into STORED columns
  for (IndexElem* index_elem :
       StructList<IndexElem*>(create_index_statement.indexIncludingParams)) {
    google::spanner::emulator::backend::ddl::KeyPartClause::Order ordering;
    ZETASQL_ASSIGN_OR_RETURN(absl::string_view key_name,
                     TranslateIndexElem(*index_elem, ordering, options_));
    google::spanner::emulator::backend::ddl::StoredColumnDefinition* stored_column =
        create_index_out.add_stored_column_definition();
    stored_column->set_name(key_name);
  }

  if (create_index_statement.whereClause != nullptr) {
    ZETASQL_RETURN_IF_ERROR(TranslateWhereNode(create_index_statement.whereClause,
                                       &null_filtered_columns_,
                                       "CREATE INDEX"));
  }

  for (absl::string_view key_name : null_filtered_columns_) {
    create_index_out.add_null_filtered_column(key_name);
  }

  if (create_index_statement.interleavespec != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        CheckInterleaveInParserOutput(create_index_statement.interleavespec));
    ZETASQL_ASSIGN_OR_RETURN(std::string relname, ddl_translator_.GetTableName(
        *create_index_statement.interleavespec->parent, "INTERLEAVE"));
    create_index_out.set_interleave_in_table(relname);
  }

  if (create_index_statement.if_not_exists && options.enable_if_not_exists) {
    create_index_out.set_existence_modifier(google::spanner::emulator::backend::ddl::IF_NOT_EXISTS);
  }

  return absl::OkStatus();
}

absl::Status
PostgreSQLToSpannerDDLTranslatorImpl::CreateIndexStatementTranslator::Translate(
    const AlterTableStmt& alter_index_statement,
    google::spanner::emulator::backend::ddl::AlterIndex& out) {
  // alter_index_statement has been verified
  ZETASQL_RET_CHECK_EQ(list_length(alter_index_statement.cmds), 1);
  ZETASQL_ASSIGN_OR_RETURN(const AlterTableCmd* first_cmd,
                   (GetListItemAsNode<AlterTableCmd, T_AlterTableCmd>(
                       alter_index_statement.cmds, 0)));
  ZETASQL_ASSIGN_OR_RETURN(std::string relname,
                   ddl_translator_.GetTableName(*alter_index_statement.relation,
                                                "ALTER INDEX"));

  out.set_index_name(relname);
  switch (first_cmd->subtype) {
    case AT_AddIndexIncludeColumn: {
      *out.mutable_add_stored_column()->mutable_column_name() = first_cmd->name;
      break;
    }
    case AT_DropIndexIncludeColumn: {
      *out.mutable_drop_stored_column() = first_cmd->name;
      break;
    }
    default:
      return UnsupportedTranslationError(
          "<ALTER INDEX> only supports actions to add or drop an include "
          "column");
  }

  return absl::OkStatus();
}

absl::StatusOr<absl::string_view> PostgreSQLToSpannerDDLTranslatorImpl::
    CreateIndexStatementTranslator::TranslateIndexElem(
        const IndexElem& index_elem,
        google::spanner::emulator::backend::ddl::KeyPartClause::Order& ordering,
        const TranslationOptions& options) {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(index_elem));

  ZETASQL_ASSIGN_OR_RETURN(
      ordering,
      ProcessOrdering(index_elem.ordering, index_elem.nulls_ordering, options));

  return index_elem.name;
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::
    CreateIndexStatementTranslator::TranslateBoolExpr(
        const BoolExpr* bool_expr,
        std::set<absl::string_view>* null_filtered_columns,
        absl::string_view parent_statement) {
  ZETASQL_RET_CHECK_NE(bool_expr, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*bool_expr));

  // Only support conjunction of <column IS NOT NULL> expressions using <AND>
  if (bool_expr->boolop != AND_EXPR) {
    return WhereClauseTranslationError(parent_statement);
  }

  for (Node* node : StructList<Node*>(bool_expr->args)) {
    ZETASQL_RETURN_IF_ERROR(
        TranslateWhereNode(node, null_filtered_columns, parent_statement));
  }

  return absl::OkStatus();
}

absl::StatusOr<absl::string_view> PostgreSQLToSpannerDDLTranslatorImpl::
    CreateIndexStatementTranslator::GetColumnName(
        const NullTest& null_test, absl::string_view parent_statement) const {
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(null_test));

  const Expr* column_expr = null_test.arg;
  if (nodeTag(column_expr) != T_ColumnRef) {
    return WhereClauseTranslationError(parent_statement);
  }

  ZETASQL_ASSIGN_OR_RETURN(const ColumnRef* column_ref,
                   (DowncastNode<ColumnRef, T_ColumnRef, Expr>(column_expr)));
  const List* column_list = column_ref->fields;
  if (list_length(column_list) != 1) {
    return WhereClauseTranslationError(parent_statement);
  }

  ZETASQL_ASSIGN_OR_RETURN(const String* value,
                   (SingleItemListAsNode<String, T_String>(column_list)));
  const char* column_name = value->sval;
  ZETASQL_RET_CHECK(column_name && *column_name != '\0');

  return column_name;
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::
    CreateIndexStatementTranslator::TranslateNullTest(
        const NullTest* null_test,
        std::set<absl::string_view>* null_filtered_columns,
        absl::string_view parent_statement) {
  ZETASQL_RET_CHECK_NE(null_test, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*null_test));

  // Only support IS_NOT_NULL
  if (null_test->nulltesttype != IS_NOT_NULL) {
    return WhereClauseTranslationError(parent_statement);
  }

  ZETASQL_ASSIGN_OR_RETURN(absl::string_view column_name,
                   GetColumnName(*null_test, parent_statement));
  null_filtered_columns->insert(column_name);

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::
    CreateIndexStatementTranslator::CheckInterleaveInParserOutput(
        const InterleaveSpec* pg_interleave) const {
  ZETASQL_RET_CHECK_NE(pg_interleave, nullptr);

  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*pg_interleave, "CREATE INDEX"));

  ZETASQL_ASSIGN_OR_RETURN(google::spanner::emulator::backend::ddl::InterleaveClause::Type type,
                   GetInterleaveClauseType(pg_interleave->interleavetype));

  switch (type) {
    case google::spanner::emulator::backend::ddl::InterleaveClause::IN_PARENT: {
      return UnsupportedTranslationError(
          "<INTERLEAVE IN PARENT> clause is not supported for index, Consider "
          "using <INTERLEAVE IN>.");
      break;
    }
    case google::spanner::emulator::backend::ddl::InterleaveClause::IN: {
      break;
    }
    default:
      // Should never get here
      ZETASQL_RET_CHECK_FAIL() << "Unknown interleave type.";
  }

  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::
    CreateIndexStatementTranslator::TranslateWhereNode(
        const Node* node, std::set<absl::string_view>* null_filtered_columns,
        absl::string_view parent_statement) {
  ZETASQL_RET_CHECK_NE(node, nullptr);

  switch (nodeTag(node)) {
    case T_BoolExpr: {
      ZETASQL_ASSIGN_OR_RETURN(const BoolExpr* bool_expr,
                       (DowncastNode<BoolExpr, T_BoolExpr>(node)));
      ZETASQL_RETURN_IF_ERROR(TranslateBoolExpr(bool_expr, null_filtered_columns,
                                        parent_statement));
      break;
    }
    case T_NullTest: {
      ZETASQL_ASSIGN_OR_RETURN(const NullTest* null_test,
                       (DowncastNode<NullTest, T_NullTest>(node)));
      ZETASQL_RETURN_IF_ERROR(TranslateNullTest(null_test, null_filtered_columns,
                                        parent_statement));
      break;
    }
    default:
      return WhereClauseTranslationError(parent_statement);
  }
  return absl::OkStatus();
}

absl::Status PostgreSQLToSpannerDDLTranslatorImpl::Translate(
    const interfaces::ParserOutput& parser_output,
    const TranslationOptions& options,
    google::spanner::emulator::backend::ddl::DDLStatementList& out_statements) const {
  Visitor visitor(*this, options, out_statements);

  const List* parse_tree = parser_output.parse_tree();
  ZETASQL_RET_CHECK_NE(parse_tree, nullptr);
  return visitor.Visit(*parse_tree);
}

}  // namespace

absl::StatusOr<std::unique_ptr<PostgreSQLToSpannerDDLTranslator>>
CreatePostgreSQLToSpannerDDLTranslator() {
  return std::make_unique<PostgreSQLToSpannerDDLTranslatorImpl>();
}

}  // namespace postgres_translator::spangres
