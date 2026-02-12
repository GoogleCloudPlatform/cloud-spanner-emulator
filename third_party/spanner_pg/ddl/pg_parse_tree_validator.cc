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

#include "third_party/spanner_pg/ddl/pg_parse_tree_validator.h"

#include <stdbool.h>
#include <string.h>

#include <array>
#include <cstdint>
#include <set>
#include <string>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/flags/declare.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "third_party/spanner_pg/ddl/ddl_translator.h"
#include "third_party/spanner_pg/ddl/translation_utils.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

ABSL_DECLARE_FLAG(bool, spangres_enable_udf_unnamed_parameters);

namespace postgres_translator {
namespace spangres {

absl::Status ValidateSequenceOption(
    const List* options,
    bool is_create,
    bool is_identity_column,
    const TranslationOptions& translation_options);

absl::Status ValidateSequenceOption(
    const DefElem& elem,
    bool is_create,
    bool is_identity_column
);

namespace {
using PGConstants = internal::PostgreSQLConstants;
using internal::FieldTypeChecker;

// Returns true if PG list is empty or nullptr
bool IsListEmpty(const List* list) { return list_length(list) == 0; }

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

absl::Status ValidateAlterColumnType(const AlterTableStmt& node,
                                     const AlterTableCmd* first_cmd,
                                     const TranslationOptions& options) {
  ZETASQL_RET_CHECK_EQ(first_cmd->subtype, AT_AlterColumnType);
  if (list_length(node.cmds) > 2) {
    return UnsupportedTranslationError(
        "<ALTER TABLE ALTER COLUMN> statement can only modify one column "
        "at a time and only support <ALTER COLUMN SET DATA TYPE>, <ALTER "
        "COLUMN {SET|DROP} NOT NULL>, <ALTER COLUMN {SET|DROP} DEFAULT>, "
        "<ALTER COLUMN SET>, and <ALTER COLUMN RESTART COUNTER WITH>.");
  }
  if (list_length(node.cmds) == 2) {
    ZETASQL_ASSIGN_OR_RETURN(
        const AlterTableCmd* second_cmd,
        (GetListItemAsNode<AlterTableCmd, T_AlterTableCmd>(node.cmds, 1)));

    ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*second_cmd, node.objtype, options));

    if (second_cmd->subtype != AT_SetNotNull &&
        second_cmd->subtype != AT_DropNotNull) {
      return UnsupportedTranslationError(
          "Only <SET/DROP NOT NULL> action is allowed after <ALTER "
          "COLUMN> "
          "action of <ALTER TABLE> statement.");
    }
    ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*second_cmd, node.objtype, options));

    if (strcmp(first_cmd->name, second_cmd->name)) {
      return UnsupportedTranslationError(
          "Both actions (<SET DATA TYPE> and <{SET|DROP} NOT NULL>) of "
          "<ALTER "
          "TABLE ALTER COLUMN> statement should be applied to the same "
          "column.");
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status ValidateParseTreeNode(const Ttl& node) {
  if (node.name == nullptr) {
    return UnsupportedTranslationError("TTL requires a column name");
  }
  if (node.interval == nullptr) {
    return UnsupportedTranslationError("TTL requires an INTERVAL");
  }
  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const ColumnDef& node,
                                   const bool alter_column) {
  // Make sure that if ColumnDef structure changes we update the translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<char*>(node.colname),
      FieldTypeChecker<TypeName*>(node.typeName),
      FieldTypeChecker<char*>(node.compression),
      FieldTypeChecker<int>(node.inhcount),
      FieldTypeChecker<bool>(node.is_local),
      FieldTypeChecker<bool>(node.is_not_null),
      FieldTypeChecker<bool>(node.is_from_type),
      FieldTypeChecker<char>(node.storage),
      FieldTypeChecker<Node*>(node.raw_default),
      FieldTypeChecker<Node*>(node.cooked_default),
      FieldTypeChecker<char>(node.identity),
      FieldTypeChecker<RangeVar*>(node.identitySequence),
      FieldTypeChecker<char>(node.generated),
      FieldTypeChecker<CollateClause*>(node.collClause),
      FieldTypeChecker<Oid>(node.collOid),
      FieldTypeChecker<List*>(node.constraints),
      FieldTypeChecker<List*>(node.fdwoptions),
      FieldTypeChecker<LocalityGroupOption*>(node.locality_group_name),
      FieldTypeChecker<int>(node.location));

  ZETASQL_RET_CHECK_EQ(node.type, T_ColumnDef);

  // `colname` defines name of the column. Mandatory in <CREATE TABLE>, not used
  // in <ALTER TABLE ALTER TYPE>
  if (!alter_column) {
    ZETASQL_RET_CHECK_NE(node.colname, nullptr);
    ZETASQL_RET_CHECK_NE(*node.colname, '\0');
  }

  // `typeName` defines type of the column, validated separately.
  ZETASQL_RET_CHECK_NE(node.typeName, nullptr);

  // `compression` defines the method to compress the given column. Not
  // supported.
  ZETASQL_RET_CHECK_EQ(node.compression, nullptr);

  // `inhcount` shows how many times the column is inherited. Not used during
  // parsing.
  ZETASQL_RET_CHECK_EQ(node.inhcount, 0);

  // `is_local` is true if column is not inherited. Not used during parsing. Not
  // set for <ALTER TABLE ALTER TYPE>.
  if (!alter_column) {
    ZETASQL_RET_CHECK(node.is_local);
  }

  // `is_not_null` is true if IS NOT NULL is set for the column.

  // `is_from_type` is used for typed tables (i.e. defined as <OF type_name>).
  // Not supported.
  ZETASQL_RET_CHECK(!node.is_from_type);

  // `storage` defines storage type for oversized attributes (e.g. BLOBs, large
  // varchar/text, etc. - see TOAST). Not used during parsing.
  ZETASQL_RET_CHECK_EQ(node.storage, '\0');

  // `raw_default` contains untransformed parse tree for the DEFAULT expression.
  // Not supported.
  ZETASQL_RET_CHECK_EQ(node.raw_default, nullptr);

  // `cooked_default` contains transformed parse tree for the DEFAULT
  // expression. Not used during parsing.
  ZETASQL_RET_CHECK_EQ(node.cooked_default, nullptr);

  // `identity` is set for IDENTITY columns. Not supported.
  if (node.identity != '\0') {
    return UnsupportedTranslationError("<IDENTITY> columns are not supported.");
  }

  // `identitySequence` is used to store sequence name for identity columns or
  // serial types. Not supported.
  ZETASQL_RET_CHECK_EQ(node.identitySequence, nullptr);

  // `generated` is set to non-zero for generated columns. Not used during
  // parsing, not supported.
  ZETASQL_RET_CHECK_EQ(node.generated, '\0');

  // `collClause` is present if COLLATE clause is provided. Not supported.
  if (node.collClause != nullptr) {
    return UnsupportedTranslationError(
        "<COLLATE> clause is not supported in column definition.");
  }

  // `collOid` defines oid for collation. Not used during parsing, not
  // supported.
  ZETASQL_RET_CHECK_EQ(node.collOid, 0);

  // `constraints` contains list of constraints defined for the column.
  // Validated separately.

  // `fdwoptions` contains options set via <WITH OPTION> clause. PostgreSQL
  // allows it for certain kinds of <CREATE TABLE> statement (mainly
  // inheritance-related), none of which are supported by Spangres.
  ZETASQL_RET_CHECK(IsListEmpty(node.fdwoptions));

  // `location` defines parsed token location of the text in the input, no
  // validation required.

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterTableCmd& node,
                                   const ObjectType object_type,
                                   const TranslationOptions& options) {
  // Make sure that if AlterTableCmd structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<AlterTableType>(node.subtype),
      FieldTypeChecker<char*>(node.name), FieldTypeChecker<int16_t>(node.num),
      FieldTypeChecker<RoleSpec*>(node.newowner),
      FieldTypeChecker<Node*>(node.def),
      FieldTypeChecker<DropBehavior>(node.behavior),
      FieldTypeChecker<bool>(node.missing_ok),
      FieldTypeChecker<LocalityGroupOption*>(node.locality_group_name),
      FieldTypeChecker<char*>(node.raw_expr_string));

  ZETASQL_RET_CHECK_EQ(node.type, T_AlterTableCmd);

  // `subtype` defines the kind of ALTER TABLE statement (ALTER COLUMN, ADD
  // CONSTRAINT, etc.). Validated in AlterTableStmt validation method above.
  switch (object_type) {
    case OBJECT_TABLE: {
      switch (node.subtype) {
        case AT_AddColumn:
        case AT_AddConstraint: {
          ZETASQL_RET_CHECK_EQ(node.name, nullptr);
          ZETASQL_RET_CHECK_NE(node.def, nullptr);
          break;
        }
        case AT_DropColumn:
        case AT_DropConstraint:
        case AT_SetNotNull:
        case AT_DropNotNull: {
          ZETASQL_RET_CHECK_NE(node.name, nullptr);
          ZETASQL_RET_CHECK_EQ(node.def, nullptr);
          break;
        }
        case AT_AlterColumnType: {
          ZETASQL_RET_CHECK_NE(node.name, nullptr);
          ZETASQL_RET_CHECK_NE(node.def, nullptr);
          ZETASQL_ASSIGN_OR_RETURN(const ColumnDef* column_def,
                           (DowncastNode<ColumnDef, T_ColumnDef>(node.def)));
          // `raw_default` is used to store expression in USING clause, e.g.
          // ALTER TABLE t ALTER c TYPE varchar USING (c::varchar)
          if (column_def->raw_default != nullptr) {
            return UnsupportedTranslationError(
                "<USING> clause is not supported in <ALTER TABLE ALTER COLUMN "
                "TYPE> statement");
          }
          ZETASQL_RETURN_IF_ERROR(
              ValidateParseTreeNode(*column_def, /*alter_column=*/true));
          break;
        }
        case AT_SetIdentity: {
          ZETASQL_RET_CHECK_NE(node.name, nullptr);
          ZETASQL_RET_CHECK_NE(node.def, nullptr);
          // We currently support altering only one option at a time.
          ZETASQL_ASSIGN_OR_RETURN(const List* alter_options,
                           (DowncastNode<List, T_List>(node.def)));
          if (list_length(alter_options) != 1) {
            return absl::InvalidArgumentError(
                "Only single option is allowed for <ALTER COLUMN> on identity "
                "column.");
          }
          // The supported options are: NO MINVALUE | NO MAXVALUE | NO SKIP
          // RANGE | SKIP RANGE skip_range_min skip_range_max | NO CYCLE. We
          // reuse the validation for sequence option.
          ZETASQL_ASSIGN_OR_RETURN(
              const DefElem* elem,
              (GetListItemAsNode<DefElem, T_DefElem>(alter_options, 0)));
          ZETASQL_RET_CHECK_NE(elem, nullptr);
          ZETASQL_RETURN_IF_ERROR(ValidateSequenceOption(*elem, /*is_create=*/false,
                                                 /*is_identity_column=*/true));
          break;
        }
        case AT_RestartCounter: {
          ZETASQL_RET_CHECK_NE(node.name, nullptr);
          ZETASQL_RET_CHECK_NE(node.def, nullptr);
          // Pg integer-looking string will get lexed as T_Float if the value is
          // too large to fit in an 'int'.
          ZETASQL_RET_CHECK(node.def->type == T_Integer || node.def->type == T_Float);
          break;
        }
        case AT_ColumnDefault:
        case AT_ColumnOnUpdate: {
          ZETASQL_RET_CHECK_NE(node.name, nullptr);
          break;
        }
        case AT_DropTtl:
          break;
        case AT_AddTtl:
        case AT_AlterTtl: {
          ZETASQL_RET_CHECK_NE(node.def, nullptr);
          ZETASQL_ASSIGN_OR_RETURN(const Ttl* ttl,
                           (DowncastNode<Ttl, T_Ttl>(node.def)));
          ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*ttl));
          break;
        }
        case AT_SetOnDeleteCascade:
        case AT_SetOnDeleteNoAction:
          break;
        case AT_SetInterleaveIn:
          break;
        case AT_AddSynonym:
        case AT_DropSynonym: {
          ZETASQL_RET_CHECK_NE(node.name, nullptr);
          break;
        }
        case AT_SetLocalityGroup: {
          ZETASQL_RET_CHECK_NE(node.locality_group_name, nullptr);
          break;
        }
        default: {
          return UnsupportedTranslationError(
              "Operation is not supported in <ALTER TABLE> statement.");
        }
      }
      break;
    }
    case OBJECT_INDEX: {
      switch (node.subtype) {
        case AT_AddIndexIncludeColumn:
        case AT_DropIndexIncludeColumn:
          break;
        case AT_SetLocalityGroup: {
          if (!options.enable_locality_groups) {
            return UnsupportedTranslationError(
                "Locality groups are not supported in <ALTER INDEX> "
                "statement.");
          }
          ZETASQL_RET_CHECK_NE(node.locality_group_name, nullptr);
          break;
        }
        default: {
          return UnsupportedTranslationError(
              "Operation is not supported in <ALTER INDEX> statement.");
        }
      }
      break;
    }
    default: {
      return UnsupportedTranslationError(
          "Object type is not supported in <ALTER> statement.");
    }
  }

  // `name` is used for operations on existing entities and holds the name
  // of such entity, e.g. column, constraints, etc.
  // Should be nullptr for all ADD operations.

  // `num` is used when referencing column by number, which is allowed only in
  // ALTER TABLE ALTER COLUMN SET STATISTICS statement, which is not supported.
  ZETASQL_RET_CHECK_EQ(node.num, 0);

  // `newowner` defines new owner in ALTER TABLE OWNER TO statement, which is
  // not supported.
  ZETASQL_RET_CHECK_EQ(node.newowner, nullptr);

  // `def` contains the definition of new object in ALTER TABLE ADD/ALTER
  // COLUMN/CONSTRAINT (and some unsupported) command types.

  // `behavior` could be defined for ALTER TABLE DROP ... statements. It
  // could be either RESTRICT or CASCADE, out of which only RESTRICT is
  // supported.
  if (node.behavior != DROP_RESTRICT) {
    return UnsupportedTranslationError(
        "Only <RESTRICT> drop mode is supported in <ALTER> statement "
        "operations.");
  }

  // `missing_ok` is set if IF EXISTS/IF NOT EXISTS clause is present. Not
  // supported, except for ALTER TABLE ADD COLUMN.
  if (node.missing_ok) {
    if (!(object_type == OBJECT_TABLE && node.subtype == AT_AddColumn &&
          options.enable_if_not_exists)) {
      return UnsupportedTranslationError(
          "<IF [NOT] EXISTS> is not supported in <ALTER> statement "
          "operations.");
    }
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const VariableSetStmt& node,
                                   absl::string_view parent_statement_type) {
  // Make sure that if VariableSetStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<VariableSetKind>(node.kind),
                         FieldTypeChecker<char*>(node.name),
                         FieldTypeChecker<List*>(node.args),
                         FieldTypeChecker<bool>(node.is_local));

  ZETASQL_RET_CHECK_EQ(node.type, T_VariableSetStmt);

  // `kind` defines the type of operation on the option and is validated in
  // TranslateAlterDatabase method.

  // `name` is mandatory only for certain kinds of SET statement and is
  // therefore validated in TranslateOptionName.

  // `args` might be empty for certain kinds of SET statement and is therefore
  // validated in TranslateAlterDatabase method.
  if (list_length(node.args) > 1) {
    return UnsupportedTranslationError(
        absl::Substitute("List values for options are not supported in <$0> "
                         "statement.",
                         parent_statement_type));
  }

  // `is_local` represents use of SET LOCAL and is not supported by ALTER
  // DATABASE.
  ZETASQL_RET_CHECK(!node.is_local);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreatedbStmt& node) {
  // Make sure that if CreatedbStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<char*>(node.dbname),
                         FieldTypeChecker<List*>(node.options));

  ZETASQL_RET_CHECK_EQ(node.type, T_CreatedbStmt);

  // `dbname` defines the name of the database to create.
  ZETASQL_RET_CHECK(node.dbname) << "Database name is missing.";

  // `options` defines options to set on the newly created database. Not
  // supported, options are expected to be set via separate ALTER DATABASE SET
  // statement after the db is created.
  if (!IsListEmpty(node.options)) {
    return UnsupportedTranslationError("Database options are not supported.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterDatabaseSetStmt& node) {
  // Make sure that if AlterDatabaseSetStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<char*>(node.dbname),
                         FieldTypeChecker<VariableSetStmt*>(node.setstmt));

  ZETASQL_RET_CHECK_EQ(node.type, T_AlterDatabaseSetStmt);

  // `dbname` defines the name of the database to set options for. It's largely
  // ignored because we already know the database statement is being executed
  // for.
  ZETASQL_RET_CHECK(node.dbname) << "Database name is missing.";

  // `setstmt` defines what value to assign to which database option.
  ZETASQL_RET_CHECK_NE(node.setstmt, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.setstmt, "ALTER DATABASE"));

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const Constraint& node, bool add_in_alter,
                                   const TranslationOptions& options) {
  // Make sure that if Constraint structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<ConstrType>(node.contype),
                         FieldTypeChecker<char*>(node.conname),
                         FieldTypeChecker<bool>(node.deferrable),
                         FieldTypeChecker<bool>(node.initdeferred),
                         FieldTypeChecker<int>(node.location),
                         FieldTypeChecker<bool>(node.is_no_inherit),
                         FieldTypeChecker<Node*>(node.raw_expr),
                         FieldTypeChecker<char*>(node.cooked_expr),
                         FieldTypeChecker<char>(node.generated_when),
                         FieldTypeChecker<GeneratedColStoreOpt>(node.stored_kind),
                         FieldTypeChecker<bool>(node.nulls_not_distinct),
                         FieldTypeChecker<List*>(node.keys),
                         FieldTypeChecker<List*>(node.including),
                         FieldTypeChecker<List*>(node.exclusions),
                         FieldTypeChecker<List*>(node.options),
                         FieldTypeChecker<char*>(node.indexname),
                         FieldTypeChecker<char*>(node.indexspace),
                         FieldTypeChecker<bool>(node.reset_default_tblspc),
                         FieldTypeChecker<char*>(node.access_method),
                         FieldTypeChecker<Node*>(node.where_clause),
                         FieldTypeChecker<RangeVar*>(node.pktable),
                         FieldTypeChecker<List*>(node.fk_attrs),
                         FieldTypeChecker<List*>(node.pk_attrs),
                         FieldTypeChecker<char>(node.fk_matchtype),
                         FieldTypeChecker<char>(node.fk_upd_action),
                         FieldTypeChecker<char>(node.fk_del_action),
                         FieldTypeChecker<List*>(node.fk_del_set_cols),
                         FieldTypeChecker<List*>(node.old_conpfeqop),
                         FieldTypeChecker<Oid>(node.old_pktable_oid),
                         FieldTypeChecker<bool>(node.skip_validation),
                         FieldTypeChecker<int>(node.vector_length),
                         FieldTypeChecker<bool>(node.initially_valid),
                         FieldTypeChecker<char*>(node.constraint_expr_string));

  ZETASQL_RET_CHECK_EQ(node.type, T_Constraint);

  // `contype` defines a type of constraint (UNIQUE, ABSL_CHECK, FOREIGN KEY,
  // etc.). Validate that it's supported before anything else to make sure we
  // return "not supported" for unsupported types.
  //
  // add_in_alter is true if constraint is being added as part of ALTER TABLE
  // ADD..., which has narrower set of supported constraint types compared to
  // regular CREATE TABLE (or even ALTER TABLE ADD COLUMN).
  if (add_in_alter) {
    if (node.contype != CONSTR_CHECK && node.contype != CONSTR_FOREIGN) {
      return UnsupportedTranslationError("Constraint type is not supported.");
    }
  } else {
    switch (node.contype) {
      // The only supported constraint types
      case CONSTR_PRIMARY:
      case CONSTR_NOTNULL:
      case CONSTR_NULL:
      case CONSTR_FOREIGN:
      case CONSTR_CHECK:
      case CONSTR_ATTR_IMMEDIATE:
      case CONSTR_ATTR_NOT_DEFERRABLE:
      case CONSTR_GENERATED:
      case CONSTR_DEFAULT:
      case CONSTR_HIDDEN:
      case CONSTR_IDENTITY:
      case CONSTR_ON_UPDATE:
        break;
      case CONSTR_VECTOR_LENGTH:
        if (node.vector_length < 0) {
          return absl::InvalidArgumentError(absl::Substitute(
          "Vector length cannot be negative: $0.", node.vector_length));
        }
        break;

      case CONSTR_UNIQUE:
        // TODO: create unique index in separate statement in batch
        return UnsupportedTranslationError(
            "<UNIQUE> constraint is not supported, create a unique index "
            "instead.");

      case CONSTR_ATTR_DEFERRABLE:
      case CONSTR_ATTR_DEFERRED:
        return UnsupportedTranslationError(
            "<DEFERRABLE> constraints are not supported.");

      default:
        return UnsupportedTranslationError("Constraint type is not supported.");
    }
  }

  // `conname` defines the name of the constraint. Validated in
  // ProcessTableConstraint.

  // `deferrable` is not supported.
  if (node.deferrable) {
    return UnsupportedTranslationError(
        "<DEFERRABLE> constraints are not supported.");
  }

  // `initdeferred` is impossible without deferred initialization.
  ZETASQL_RET_CHECK(!node.initdeferred)
      << "Non-deferrable constraint cannot have deferred initialization.";

  // `location` holds the offset in the original source text for error
  // formatting and debug purposes. It's not used by Spangres yet, so ignored in
  // validation.

  // `is_no_inherit`
  if (node.is_no_inherit) {
    return UnsupportedTranslationError(
        "<NO INHERIT> clause is not supported in constraints.");
  }

  // `raw_expr` contains untransformed parse tree for ABSL_CHECK constraints.
  // Validated in TranslateCheckConstraint.

  // `cooked_expr` contains serialized `raw_expr`, currently unused by either
  // Spangres or PostgreSQL.

  // `generated_when` is checked based on constraint type.

  // `keys` defines key columns, used in PRIMARY KEY constraints (also in
  // UNIQUE, but they're not supported). It might be either required or not
  // allowed, depending on where the constraint is defined (column-level or
  // table-level), so validated in ProcessTableConstraint.

  // `including` contains referenced non-key columns for PRIMARY KEY (and
  // unsupported UNIQUE and EXCLUDE) constraints.
  if (!IsListEmpty(node.including)) {
    return UnsupportedTranslationError(
        "<INCLUDE> clause is not supported in constraints.");
  }

  // `exclusions` is used for EXCLUDE constraints which are not supported.
  ZETASQL_RET_CHECK_EQ(node.exclusions, nullptr);

  if (node.contype == CONSTR_IDENTITY) {
    List* opts = node.options;
    ZETASQL_RETURN_IF_ERROR(ValidateSequenceOption(opts, /*is_create=*/true,
                                           /*is_identity_column=*/true,
                                           options));
  } else {
    // `options` are defined in WITH clause, which is not supported.
    if (!IsListEmpty(node.options)) {
      return UnsupportedTranslationError(
          "<WITH> clause is not supported in constraint definitions.");
    }
  }

  // `indexname` is used in ALTER TABLE ADD [UNIQUE | PRIMARY KEY] USING INDEX.
  // This type of ALTER statement is not supported.
  ZETASQL_RET_CHECK_EQ(node.indexname, nullptr);

  // `indexspace` is used in USING INDEX TABLESPACE for PRIMARY KEY (and
  // unsupported UNIQUE and EXCLUDE) constraints.
  if (node.indexspace != nullptr) {
    return UnsupportedTranslationError(
        "<USING INDEX TABLESPACE> clause is not supported in constraint "
        "definitions.");
  }

  // `reset_default_tblsp` is currently unused.

  // `access_method` (B-Tree, hash, etc.) for UNIQUE/PRIMARY KEY constraints.
  // Currently not supported by PG, and not supported by Spangres either.
  if (node.access_method != nullptr) {
    return UnsupportedTranslationError(
        "Setting access method is not supported for constraints.");
  }

  // `where_clause` defines partial predicate for EXCLUDE constraints, which are
  // not supported. However, there is some indication that it will be expanded
  // to other contypes, so to be on the safe side we're returning error instead
  // of ZETASQL_RET_CHECK.
  if (node.where_clause != nullptr) {
    return UnsupportedTranslationError(
        "Partial predicates are not supported for constraints.");
  }

  // `pktable` defines referenced table for foreign key constraints. Validated
  // in TranslateForeignKey.

  // `fk_attrs` defines constrained columns for foreign key constraints.
  // Mandatory if defined on table level, but not allowed if defined on column
  // level, so validated in TranslateForeignKey.

  // `pk_attrs` defines referenced columns for foreign key constraints.
  // Validated in TranslateForeignKey.

  // `fk_matchtype` defines match type (SIMPLE/FULL) for foreign key
  // constraints. Validated in TranslateForeignKey.

  // `fk_upd_action` defines ON UPDATE action for foreign key constraints.
  // Validated in GetForeignKeyAction.

  // `fk_del_action` defines ON DELETE action for foreign key constraints.
  // Validated in GetForeignKeyAction.

  // `old_conpfeqop` isn't documented, but doesn't seem to be used anywhere
  // relevant for Spangres.

  // `old_pktable_oid` isn't documented; seems to define OID of referenced
  // table, which is of no use for the translation purposes.

  // `skip_validation` seems to be an internal field, dependent on
  // `initially_valid` value, constraint type and other factors. Validated in
  // TranslateAlterTable.

  // `initially_valid` defines if constraint is valid on creation. This field
  // makes sense only if constraint is being added via ALTER TABLE ADD and is
  // not supported by Spangres. Validated in TranslateAlterTable.

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const RangeVar& node,
                                   absl::string_view parent_statement_type) {
  // Make sure that if RangeVar structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<char*>(node.catalogname),
                         FieldTypeChecker<char*>(node.schemaname),
                         FieldTypeChecker<char*>(node.relname),
                         FieldTypeChecker<bool>(node.inh),
                         FieldTypeChecker<char>(node.relpersistence),
                         FieldTypeChecker<Alias*>(node.alias),
                         FieldTypeChecker<int>(node.location),
                         FieldTypeChecker<List*>(node.tableHints));

  ZETASQL_RET_CHECK_EQ(node.type, T_RangeVar);

  if (node.catalogname != nullptr) {
    return UnsupportedTranslationError(absl::Substitute(
        "Catalog in the name is not supported in <$0> statement.",
        parent_statement_type));
  }

  if (node.schemaname != nullptr) {
    ZETASQL_RET_CHECK(*node.schemaname != '\0');
  }
  if (node.schemaname != nullptr && absl::StrContains(node.schemaname, ".")) {
    return UnsupportedTranslationError(absl::Substitute(
        "Dot(.) is not supported in schema name: $0.", node.schemaname));
  }

  ZETASQL_RET_CHECK(node.relname && *node.relname != '\0');
  if (absl::StrContains(node.relname, ".")) {
    return UnsupportedTranslationError(absl::Substitute(
        "Dot(.) is not supported in object name: $0.", node.relname));
  }

  // `inh` (inheritance) defines if the relation only for the table or should
  // also include its parent. Setting this is not supported. `true` is the
  // default value.
  if (!node.inh) {
    return UnsupportedTranslationError(absl::Substitute(
        "<ONLY> for table inheritance is not supported in <$0> statement.",
        parent_statement_type));
  }

  // `relpersistence` defines the persistency property of the table, its value
  // could be PERMANENT, UNLOGGED and TEMP. Setting this is not supported,
  // PERMANENT is the default value.
  if (node.relpersistence != RELPERSISTENCE_PERMANENT) {
    return UnsupportedTranslationError(
        absl::Substitute("TEMP/TEMPORARY or UNLOGGED objects are not supported "
                         "in <$0> statement.",
                         parent_statement_type));
  }

  // `alias` defines table alias. This is not used by DDL.
  ZETASQL_RET_CHECK_EQ(node.alias, nullptr);

  // `location` defines parsed token location of the text in the input, no
  // validation required.

  // `tableHints` defines extra query options to facilitate DQL/DML handling.
  // This is not supported by DDL.
  if (node.tableHints != nullptr) {
    return UnsupportedTranslationError(
        absl::Substitute("Table hints are not supported in <$0> statement.",
                         parent_statement_type));
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const InterleaveSpec& node,
                                   absl::string_view parent_statement_type) {
  // Make sure that if InterleaveSpec structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<InterleaveInType>(node.interleavetype),
      FieldTypeChecker<RangeVar*>(node.parent),
      FieldTypeChecker<char>(node.on_delete_action),
      FieldTypeChecker<int>(node.location));

  ZETASQL_RET_CHECK_EQ(node.type, T_InterleaveSpec);

  // `parent` defines the target to store/interleave the data.
  ZETASQL_RET_CHECK_NE(node.parent, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(
      *node.parent, absl::StrCat(parent_statement_type, " INTERLEAVE")));

  // `on_delete_action` defines the action when parent is deleted. Validated in
  // `GetInterleaveParentDeleteActionType()`

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterTableStmt& node,
                                   const TranslationOptions& options) {
  // Make sure that if AlterTableStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<RangeVar*>(node.relation),
                         FieldTypeChecker<List*>(node.cmds),
                         FieldTypeChecker<ObjectType>(node.objtype),
                         FieldTypeChecker<bool>(node.missing_ok));

  ZETASQL_RET_CHECK_EQ(node.type, T_AlterTableStmt);

  // `relation` defines the table to alter.
  ZETASQL_RET_CHECK_NE(node.relation, nullptr);

  // `cmds` defines clauses to execute. Only one clause is supported. Usually we
  // support just one clause here, with the exception of ALTER TABLE ALTER
  // COLUMN, in which case we require two clauses: both ALTER COLUMN SET DATA
  // TYPE and ALTER COLUMN {SET|DROP} NOT NULL. Actual command is validated the
  // last to make sure we fail on more important errors first.
  ZETASQL_RET_CHECK(!IsListEmpty(node.cmds));

  switch (node.objtype) {
    case OBJECT_TABLE: {
      // Validating command here to allow failures for statements like ALTER
      // INDEX..., ALTER TABLE IF EXISTS... happen first.
      ZETASQL_ASSIGN_OR_RETURN(
          const AlterTableCmd* first_cmd,
          (GetListItemAsNode<AlterTableCmd, T_AlterTableCmd>(node.cmds, 0)));
      ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*first_cmd, node.objtype, options));

      if (first_cmd->subtype == AT_AlterColumnType) {
        return ValidateAlterColumnType(node, first_cmd, options);
      }

      if (list_length(node.cmds) > 1) {
        return UnsupportedTranslationError(
            "For the provided action type of the <ALTER TABLE> statement "
            "only single action per statement is allowed.");
      }

      break;
    }
    case OBJECT_INDEX: {
      if (list_length(node.cmds) != 1) {
        return UnsupportedTranslationError(
            "<ALTER INDEX> only supports one action at a time.");
      }

      ZETASQL_ASSIGN_OR_RETURN(
          const AlterTableCmd* first_cmd,
          (GetListItemAsNode<AlterTableCmd, T_AlterTableCmd>(node.cmds, 0)));
      ZETASQL_RET_CHECK_NE(first_cmd, nullptr);
      ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*first_cmd, node.objtype, options));
      break;
    }
    default: {
      return UnsupportedTranslationError(
          "Object type is not supported in <ALTER> statement.");
    }
  }

  // `objtype` defines the type of object to alter (and the type of ALTER, e.g.
  // ALTER TABLE, ALTER INDEX, ALTER VIEW, etc.). Only tables or indexes are
  // supported (e.g. only ALTER TABLE).
  if (!(node.objtype == OBJECT_TABLE || node.objtype == OBJECT_INDEX)) {
    return UnsupportedTranslationError(
        "Object type is not supported in <ALTER> statement.");
  }

  // `missing_ok` is set if IF EXISTS/IF NOT EXISTS clause is present. We are
  // supporting IF EXISTS only for ALTER TABLE statements.
  if (node.missing_ok) {
      return UnsupportedTranslationError(
          "<IF [NOT] EXISTS> is not supported in <ALTER> statement.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const DropStmt& node,
                                   const TranslationOptions& options) {
  // Make sure that if DropStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.objects),
                         FieldTypeChecker<ObjectType>(node.removeType),
                         FieldTypeChecker<DropBehavior>(node.behavior),
                         FieldTypeChecker<bool>(node.missing_ok),
                         FieldTypeChecker<bool>(node.concurrent));

  ZETASQL_RET_CHECK_EQ(node.type, T_DropStmt);

  // `objects` contains the list of objects (tables or indexes) to drop. Only
  // one object is allowed here. Validated last to prioritize other kinds of
  // errors first.
  ZETASQL_RET_CHECK(!IsListEmpty(node.objects));
  if (list_length(node.objects) > 1) {
    return UnsupportedTranslationError(
        "<DROP> statements support deletion of only single object per "
        "statement.");
  }
  // `removeType` defines the type of object to remove. Only certain types
  // are supported.
  if (node.removeType != OBJECT_TABLE && node.removeType != OBJECT_INDEX &&
      node.removeType != OBJECT_SCHEMA && node.removeType != OBJECT_VIEW &&
      node.removeType != OBJECT_CHANGE_STREAM &&
      node.removeType != OBJECT_SEQUENCE
      && node.removeType != OBJECT_SEARCH_INDEX
      && node.removeType != OBJECT_LOCALITY_GROUP
      // TODO: expose when queue is implemented.
    ) {
    auto object_type = internal::ObjectTypeToString(node.removeType);
    const std::string error_message =
        "Only <DROP TABLE>, <DROP INDEX>, <DROP SCHEMA>, <DROP VIEW>, "
        "<DROP SEQUENCE>, "
        "<DROP SEARCH INDEX>, "
        "<DROP LOCALITY GROUP>, "
        // TODO: expose when queue is implemented.
        "and <DROP CHANGE STREAM> statements are supported.";
    if (!object_type.ok()) {
      return UnsupportedTranslationError(error_message);
    }
    return UnsupportedTranslationError("<DROP " + *object_type +
                                       "> is not supported. " + error_message);
  }

  if (node.removeType == OBJECT_CHANGE_STREAM) {
    ZETASQL_RET_CHECK_EQ(1, node.objects->length);
    // `change_stream_name` defines the name of the change stream.
    ZETASQL_ASSIGN_OR_RETURN(
        const RangeVar* changestream_to_drop_node,
        (SingleItemListAsNode<RangeVar, T_RangeVar>(node.objects)));
    ZETASQL_RET_CHECK(changestream_to_drop_node->relname &&
              *changestream_to_drop_node->relname != '\0');
  } else if (node.removeType == OBJECT_SEARCH_INDEX) {
    ZETASQL_ASSIGN_OR_RETURN(const List* search_index_to_drop_list,
                     (SingleItemListAsNode<List, T_List>(node.objects)));
    ZETASQL_RET_CHECK(!IsListEmpty(search_index_to_drop_list))
        << "Empty search index object name to drop provided.";
    if (list_length(search_index_to_drop_list) > 2) {
      return UnsupportedTranslationError(
          "Object name catalog qualifiers are not supported in <DROP> "
          "statement.");
    }
    for (int i = 0; i < list_length(search_index_to_drop_list); ++i) {
      ZETASQL_ASSIGN_OR_RETURN(
          const String* search_index_to_drop_node,
          (GetListItemAsNode<String, T_String>)(search_index_to_drop_list, i));

      char* search_index_to_drop = search_index_to_drop_node->sval;
      // Expected to see non-empty object name to drop.
      ZETASQL_RET_CHECK(search_index_to_drop && *search_index_to_drop != '\0');
    }
  } else if (node.removeType == OBJECT_LOCALITY_GROUP) {
    ZETASQL_RET_CHECK_EQ(1, node.objects->length);
    ZETASQL_ASSIGN_OR_RETURN(
        const RangeVar* locality_group_to_drop_node,
        (SingleItemListAsNode<RangeVar, T_RangeVar>(node.objects)));
    ZETASQL_RET_CHECK(locality_group_to_drop_node->relname &&
              *locality_group_to_drop_node->relname != '\0');
  // TODO: expose when queue is implemented.
  } else if (node.removeType != OBJECT_SCHEMA) {
    ZETASQL_ASSIGN_OR_RETURN(const List* object_to_drop_list,
                     (SingleItemListAsNode<List, T_List>(node.objects)));
    ZETASQL_RET_CHECK(!IsListEmpty(object_to_drop_list))
        << "Empty object name to drop provided.";
    if (node.removeType == OBJECT_TABLE || node.removeType == OBJECT_INDEX ||
        node.removeType == OBJECT_VIEW || node.removeType == OBJECT_SEQUENCE) {
      if (list_length(object_to_drop_list) > 2) {
        return UnsupportedTranslationError(
            "Object name catalog qualifiers are not supported in <DROP> "
            "statement.");
      }
      for (int i = 0; i < list_length(object_to_drop_list); ++i) {
        ZETASQL_ASSIGN_OR_RETURN(
            const String* object_to_drop_node,
            (GetListItemAsNode<String, T_String>)(object_to_drop_list, i));

        char* object_to_drop = object_to_drop_node->sval;
        // Expected to see non-empty object name to drop.
        ZETASQL_RET_CHECK(object_to_drop && *object_to_drop != '\0');
      }
    } else {
      if (list_length(object_to_drop_list) > 1) {
        return UnsupportedTranslationError(
            "Object name schema qualifiers are not supported in <DROP> "
            "statement.");
      }
      ZETASQL_ASSIGN_OR_RETURN(
          const String* object_to_drop_node,
          (SingleItemListAsNode<String, T_String>)(object_to_drop_list));

      char* object_to_drop = object_to_drop_node->sval;
      // Expected to see non-empty object name to drop.
      ZETASQL_RET_CHECK(object_to_drop && *object_to_drop != '\0');
    }
  } else {
    ZETASQL_ASSIGN_OR_RETURN(const String* object_to_drop_node,
                     (SingleItemListAsNode<String, T_String>)(node.objects));

    char* object_to_drop = object_to_drop_node->sval;
    // Expected to see non-empty object name to drop.
    ZETASQL_RET_CHECK(object_to_drop && *object_to_drop != '\0');
  }

  // `behavior` defines if RESTRICT or CASCADE drop mode is requested. Only
  // RESTRICT is supported.
  if (node.behavior != DROP_RESTRICT) {
    return UnsupportedTranslationError(
        "Only <RESTRICT> behavior is supported by <DROP> statement.");
  }

  // `missing_ok` is set if IF EXISTS clause is present. Not supported.
  if (node.missing_ok) {
    // if not enabled, or not a table or index or sequence or view or schema or
    // change stream then an error.
    if (!options.enable_if_not_exists ||
        !(node.removeType == OBJECT_TABLE || node.removeType == OBJECT_INDEX ||
          node.removeType == OBJECT_SEQUENCE ||
          node.removeType == OBJECT_VIEW || node.removeType == OBJECT_SCHEMA ||
          node.removeType == OBJECT_CHANGE_STREAM
          || node.removeType == OBJECT_SEARCH_INDEX ||
          node.removeType == OBJECT_LOCALITY_GROUP
          // TODO: expose when queue is implemented.
          )) {
      return UnsupportedTranslationError(
          "<IF EXISTS> is not supported by <DROP> statement.");
    }

    if (!options.enable_change_streams_if_not_exists &&
        node.removeType == OBJECT_CHANGE_STREAM) {
      return UnsupportedTranslationError(
          "<IF EXISTS> clause is not supported by <DROP CHANGE STREAM> "
          "statement.");
    }
  }

  // `concurrent` is set for <DROP INDEX CONCURRENTLY> statements, which are not
  // supported.
  if (node.concurrent) {
    return UnsupportedTranslationError(
        "<CONCURRENTLY> is not supported by <DROP> statement.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const TypeName& node) {
  // Make sure that if DropStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.names),
                         FieldTypeChecker<Oid>(node.typeOid),
                         FieldTypeChecker<bool>(node.setof),
                         FieldTypeChecker<bool>(node.pct_type),
                         FieldTypeChecker<List*>(node.typmods),
                         FieldTypeChecker<int32_t>(node.typemod),
                         FieldTypeChecker<List*>(node.arrayBounds),
                         FieldTypeChecker<int>(node.location));

  ZETASQL_RET_CHECK_EQ(node.type, T_TypeName);

  // `names` defines the type name. Might contain one (type name) or two
  // (catalog name and type name) elements.
  ZETASQL_RET_CHECK(!IsListEmpty(node.names));
  if (list_length(node.names) > 2) {
    return UnsupportedTranslationError(
        "Type names with more than two parts are not supported.");
  }
  for (const String* type_name_part : StructList<String*>(node.names)) {
    ZETASQL_RET_CHECK_EQ(type_name_part->type, T_String);
    ZETASQL_RET_CHECK_NE(type_name_part->sval, nullptr);
    ZETASQL_RET_CHECK_NE(*type_name_part->sval, '\0');
  }

  // `typeOid` is used when type is defined internally via oid. Not used during
  // parsing.
  ZETASQL_RET_CHECK_EQ(node.typeOid, 0);

  // `setof` is set if type is defined as SETOF. Used when defining functions,
  // not supported in Spangres.
  ZETASQL_RET_CHECK(!node.setof);

  // `pct_type` is set if `names` contains field name instead of type (in which
  // case type should be looked up). Used only when defining functions, which is
  // not supported by Spangres.
  ZETASQL_RET_CHECK(!node.pct_type);

  // `typmods` contains type modifiers. Currently supported for varchar only.
  // `typemod` is used to store type modifiers when type is defined internally
  // via oid. Not used during parsing.
  ZETASQL_RET_CHECK_EQ(node.typemod, -1);

  // `arrayBounds` contains the size for the bounded or -1 for unbounded array
  // types as well as dimensionality information. PostgreSQL ignores both of
  // these limits, and does not store them in the catalog. We will honor this
  // behavior for length limits, but still fail if a user specifies a multi-
  // dimensional array.
  if (!IsListEmpty(node.arrayBounds)) {
    if (list_length(node.arrayBounds) > 1) {
      return UnsupportedTranslationError(
          "Multi-dimensional arrays are not supported.");
    }
  }

  // `location` defines parsed token location of the text in the input, no
  // validation required.

  return absl::OkStatus();
}

// Validate the option for both <CREATE SEQUENCE> and <ALTER SEQUENCE>.
// Also validate the option for <CREATE TABLE> and <ALTER TABLE> when
// is_identity_column is true.
absl::Status ValidateSequenceOption(const DefElem& elem, bool is_create,
                                    bool is_identity_column) {
  absl::string_view name = elem.defname;
  absl::string_view parent_statement_type =
      is_identity_column ? (is_create ? "CREATE TABLE" : "ALTER COLUMN ... SET")
                         : (is_create ? "CREATE SEQUENCE" : "ALTER SEQUENCE");
  ZETASQL_RET_CHECK(!name.empty());
  if (name == "minvalue") {
    if (elem.arg != nullptr) {
      return UnsupportedTranslationError(absl::Substitute(
          "Optional clause <MINVALUE> is not supported in <$0> statement.",
          parent_statement_type));
    }
  } else if (name == "maxvalue") {
    if (elem.arg != nullptr) {
      return UnsupportedTranslationError(absl::Substitute(
          "Optional clause <MAXVALUE> is not supported in <$0> statement.",
          parent_statement_type));
    }
  } else if (name == "skip_range") {
    // `skip_range` name is used by `SKIP RANGE min max` and `NO SKIP RANGE`.
    // Below we check that if `NO SKIP RANGE` is found, i.e. elem.arg==nullptr,
    // return error if it is not `ALTER COLUMN ... SET NO SKIP RANGE`.
    if (elem.arg == nullptr) {
      if (is_create || !is_identity_column) {
        return UnsupportedTranslationError(absl::Substitute(
          "Optional clause <NO SKIP RANGE> is not supported in <$0> statement.",
          parent_statement_type));
      }
    } else {
      ZETASQL_RET_CHECK_NE(elem.arg, nullptr);
      ZETASQL_ASSIGN_OR_RETURN(const List* range_values,
                      (DowncastNode<List, T_List>(elem.arg)));
      ZETASQL_RET_CHECK_EQ(list_length(range_values), 2);
    }
  } else if (name == "start_counter" && is_create) {
    ZETASQL_RET_CHECK_NE(elem.arg, nullptr);
    // Pg integer-looking string will get lexed as T_Float if the value is
    // too large to fit in an 'int'.
    ZETASQL_RET_CHECK(elem.arg->type == T_Integer || elem.arg->type == T_Float);
  } else if (name == "restart_counter" && !is_create) {
    if (is_identity_column) {
      // Spangres supports RESTART COUNTER WITH as follows:
      // - Correct: ALTER COLUMN ... RESTART COUNTER WITH ...
      // - Incorrect: ALTER COLUMN ... SET RESTART COUNTER WITH ...
      return UnsupportedTranslationError(absl::Substitute(
          "Optional clause <RESTART COUNTER WITH> is not supported "
          "in <$0> statement. Use <ALTER COLUMN ... RESTART COUNTER WITH> "
          "instead.",
          parent_statement_type));
    }
    ZETASQL_RET_CHECK_NE(elem.arg, nullptr);
    // Pg integer-looking string will get lexed as T_Float if the value is
    // too large to fit in an 'int'.
    ZETASQL_RET_CHECK(elem.arg->type == T_Integer || elem.arg->type == T_Float);
  } else if (name == "owned_by" && !is_identity_column) {
    // Spangres does not support OWNED BY for identity column, so only
    // validating "owned_by" for sequence.
    ZETASQL_ASSIGN_OR_RETURN(const List* names, (DowncastNode<List, T_List>(elem.arg)));
    ZETASQL_RET_CHECK_GE(list_length(names), 1);
    if (list_length(names) == 1) {
      ZETASQL_ASSIGN_OR_RETURN(const String* name,
                       (SingleItemListAsNode<String, T_String>(names)));
      absl::string_view name_str = name->sval;
      if (name_str == "none") {
        return absl::OkStatus();
      }
    }
    return UnsupportedTranslationError(
        "Optional clause <owned_by> only support `NONE` as the value.");
  } else if (name == "cycle") {
    ZETASQL_ASSIGN_OR_RETURN(const Boolean* has_cycle,
                     (DowncastNode<Boolean, T_Boolean>(elem.arg)));
    if (has_cycle->boolval) {
      return UnsupportedTranslationError(
          "Optional clause <cycle> only support `NO CYCLE` as the value.");
    }
  } else {
    return UnsupportedTranslationError(absl::Substitute(
        "Optional clause <$0> is not supported in <$1> statement.", name,
        parent_statement_type));
  }
  return absl::OkStatus();
}

// Validate the options for:
// * sequence in <CREATE SEQUENCE> and <ALTER SEQUENCE>, and
// * identity column in <CREATE TABLE> and <ALTER TABLE>.
absl::Status ValidateSequenceOption(
    const List* options, bool is_create, bool is_identity_column,
    const TranslationOptions& translation_options) {
  bool seen_bit_reversed_positive = false;
  for (int i = 0; i < list_length(options); ++i) {
    DefElem* elem = ::postgres_translator::internal::PostgresCastNode(
        DefElem, options->elements[i].ptr_value);
    absl::string_view name = elem->defname;
    ZETASQL_RET_CHECK(!name.empty());
    if (name == "bit_reversed_positive" && is_create) {
      seen_bit_reversed_positive = true;
    } else {
      ZETASQL_RETURN_IF_ERROR(
          ValidateSequenceOption(*elem, is_create, is_identity_column));
    }
  }

  if (!translation_options.enable_default_sequence_kind) {
    // Sequence kind is mandatory when default sequence kind is disabled.
    if (!seen_bit_reversed_positive && is_create) {
      if (is_identity_column) {
        return UnsupportedTranslationError(
            "Only BIT_REVERSED_POSITIVE identity columns are supported. Use "
            "`GENERATED BY DEFAULT AS IDENTITY (BIT_REVERSED_POSITIVE ...)` to "
            "define identity column.");
      }
      return UnsupportedTranslationError(
          "Only BIT_REVERSED_POSITIVE sequences are supported. Use `CREATE "
          "SEQUENCE <name> BIT_REVERSED_POSITIVE ...` to create sequence.");
    }
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreateSeqStmt& node,
                                   const TranslationOptions& options) {
  AssertPGNodeConsistsOf(node, FieldTypeChecker<RangeVar*>(node.sequence),
                         FieldTypeChecker<List*>(node.options),
                         FieldTypeChecker<Oid>(node.ownerId),
                         FieldTypeChecker<bool>(node.for_identity),
                         FieldTypeChecker<bool>(node.if_not_exists));

  ZETASQL_RET_CHECK_EQ(node.type, T_CreateSeqStmt);

  if (!options.enable_sequence) {
    return UnsupportedTranslationError(
        "<CREATE SEQUENCE> statement is not supported.");
  }

  // `sequence` defines the name of the new sequence.
  ZETASQL_RET_CHECK_NE(node.sequence, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.sequence, "CREATE SEQUENCE"));

  // `options` defines the property of the sequence. Spangres only supports a
  // few portions of native PostgreSQL's options.
  List* opts = node.options;
  ZETASQL_RETURN_IF_ERROR(ValidateSequenceOption(opts, /*is_create=*/true,
                                         /*is_identity_column=*/false,
                                         options));

  // `ownerId` should be the default value after parsing.
  ZETASQL_RET_CHECK_EQ(node.ownerId, InvalidOid);

  // `for_identity` true when it created for SERIAL
  ZETASQL_RET_CHECK_EQ(node.for_identity, false);

  // `if_not_exists` do nothing when the sequence exists if it is true.

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterSeqStmt& node,
                                   const TranslationOptions& options) {
  AssertPGNodeConsistsOf(node, FieldTypeChecker<RangeVar*>(node.sequence),
                         FieldTypeChecker<List*>(node.options),
                         FieldTypeChecker<bool>(node.for_identity),
                         FieldTypeChecker<bool>(node.missing_ok));

  ZETASQL_RET_CHECK_EQ(node.type, T_AlterSeqStmt);

  // Sequences have quite a few options in the base Postgres grammar. Make sure
  // the ones we support have proper parameters, and error out if the user
  // specified options we don't (currently) allow.
  List* opts = node.options;
  if (opts != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateSequenceOption(opts, /*is_create=*/false,
                                           /*is_identity_column=*/false,
                                           options));
  }

  // `for_identity` true when it created for SERIAL
  ZETASQL_RET_CHECK_EQ(node.for_identity, false);

  // `missing_ok` do nothing when the sequence exists if value is true.

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const RenameStmt& node,
                                   const TranslationOptions& options) {
  // Make sure that if RenameStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<ObjectType>(node.renameType),
                         FieldTypeChecker<ObjectType>(node.relationType),
                         FieldTypeChecker<RangeVar*>(node.relation),
                         FieldTypeChecker<Node*>(node.object),
                         FieldTypeChecker<char*>(node.subname),
                         FieldTypeChecker<char*>(node.newname),
                         FieldTypeChecker<DropBehavior>(node.behavior),
                         FieldTypeChecker<bool>(node.missing_ok),
                         FieldTypeChecker<bool>(node.addSynonym));

  ZETASQL_RET_CHECK_EQ(node.type, T_RenameStmt);

  // `renameType` defines the type of the object to rename.
  if (node.renameType != OBJECT_TABLE) {
    return UnsupportedTranslationError(
        "Object type is not supported in <RENAME TO> statement.");
  }

  // `relationType` is not used for table rename.

  // `relation` defines the table to alter.
  ZETASQL_RET_CHECK_NE(node.relation, nullptr);

  // `object` is not used for table rename.
  // `subname` is not used for table rename.

  // `newname` defines the new table name to change to.
  ZETASQL_RET_CHECK_NE(node.newname, nullptr);

  // `behavior` is not used for table rename.

   // `missing_ok` is set if IF EXISTS/IF NOT EXISTS clause is present.
  // TODO: Add support for IF EXISTS
  if (node.missing_ok) {
      return UnsupportedTranslationError(
          "<IF EXISTS> clause is not supported in <RENAME TO> statement.");
  }

  // `addSynonym` defines whether to additionally create a synonym.
  // Both cases are supported.

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const TableChainedRenameStmt& node,
                                   const TranslationOptions& options) {
  // Make sure that if TableChainedRenameStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.ops));

  ZETASQL_RET_CHECK_EQ(node.type, T_TableChainedRenameStmt);

  if (list_length(node.ops) < 2) {
    return UnsupportedTranslationError(
        "Chained <RENAME TABLE> statement must have at least two rename ops.");
  }
  for (int i = 0; i < list_length(node.ops); ++i) {
    TableRenameOp* op = ::postgres_translator::internal::PostgresCastNode(
        TableRenameOp, node.ops->elements[i].ptr_value);
    ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*op));
  }
  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const TableRenameOp& node) {
  // Make sure that if TableRenameOp structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<RangeVar*>(node.fromName),
                         FieldTypeChecker<char*>(node.toName));

  ZETASQL_RET_CHECK_EQ(node.type, T_TableRenameOp);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreateStmt& node,
                                   const TranslationOptions& options) {
  // Make sure that if CreateStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<RangeVar*>(node.relation),
      FieldTypeChecker<List*>(node.tableElts),
      FieldTypeChecker<List*>(node.inhRelations),
      FieldTypeChecker<PartitionBoundSpec*>(node.partbound),
      FieldTypeChecker<PartitionSpec*>(node.partspec),
      FieldTypeChecker<TypeName*>(node.ofTypename),
      FieldTypeChecker<List*>(node.constraints),
      FieldTypeChecker<List*>(node.options),
      FieldTypeChecker<OnCommitAction>(node.oncommit),
      FieldTypeChecker<char*>(node.tablespacename),
      FieldTypeChecker<char*>(node.accessMethod),
      FieldTypeChecker<bool>(node.if_not_exists),
      FieldTypeChecker<InterleaveSpec*>(node.interleavespec),
      FieldTypeChecker<Ttl*>(node.ttl),
      FieldTypeChecker<LocalityGroupOption*>(node.locality_group_name));

  ZETASQL_RET_CHECK_EQ(node.type, T_CreateStmt);

  // `relation` defines the name of the new table.
  ZETASQL_RET_CHECK_NE(node.relation, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.relation, "CREATE TABLE"));

  // If there's a TTL here make sure it's correct.
  if (node.ttl != nullptr) {
    ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.ttl));
  }

  // `tableElts` contains column definitions for the new table.
  if (IsListEmpty(node.tableElts)) {
    return UnsupportedTranslationError(
        "<CREATE TABLE> statement must have at least one column defined.");
  }

  // `inhRelations` is used for inherited relations, which are not supported.
  if (!IsListEmpty(node.inhRelations)) {
    return UnsupportedTranslationError(
        "<INHERITS> clause is not supported in <CREATE TABLE> statement.");
  }

  // `partbound` contains <FOR VALUES> clause. PostgreSQL allows it for
  // partitioned tables, but it's not supported in Spangres.
  if (node.partbound != nullptr) {
    return UnsupportedTranslationError(
        "<FOR VALUES> clause is not supported in <CREATE TABLE> statement.");
  }

  // `partspec` contains the partition specificatin for <PARTITION BY> clause.
  // Not supported.
  if (node.partspec != nullptr) {
    return UnsupportedTranslationError(
        "<PARTITION BY> clause is not supported in <CREATE TABLE> statement.");
  }

  // `ofTypename` is set if table is created as <OF typename>, which is not
  // supported.
  if (node.ofTypename != nullptr) {
    return UnsupportedTranslationError(
        "<OF type_name> clause is not supported in <CREATE TABLE> statement.");
  }

  // `constraints` contains list of table constraints. Validated separately.

  // `oncommit` defines what to do with temporary tables on commit. Not
  // supported.
  if (node.oncommit != ONCOMMIT_NOOP) {
    return UnsupportedTranslationError(
        "<ON COMMIT> clause is not supported in <CREATE TABLE> statement.");
  }

  // `tablespacename` contains name of the tablespace to use. Not supported.
  if (node.tablespacename != nullptr) {
    return UnsupportedTranslationError(
        "<TABLESPACE> clause is not supported in <CREATE TABLE> statement.");
  }

  // `if_not_exists` is set if IF NOT EXISTS clause is present. May not be
  // supported.
  if (node.if_not_exists && !options.enable_if_not_exists) {
    return UnsupportedTranslationError(
        "<IF NOT EXISTS> clause is not supported in <CREATE TABLE> statement.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreateSchemaStmt& node) {
  if (node.schemaElts != nullptr) {
    return UnsupportedTranslationError(
        "Schema elements are not supported in <CREATE SCHEMA> statement.");
  }
  if (node.authrole != nullptr) {
    return UnsupportedTranslationError(
        "Authorized roles are not supported in <CREATE SCHEMA> statement.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterSpangresStatsStmt& node) {
  // Make sure that if AlterSpangresStatsStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.package_name),
                         FieldTypeChecker<VariableSetStmt*>(node.setstmt));

  // `setstmt` defines what value to assign to which package option.
  ZETASQL_RET_CHECK_NE(node.setstmt, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.setstmt, "ALTER STATISTICS"));

  if (list_length(node.package_name) != 2) {
    return UnsupportedTranslationError(
        "<ALTER STATISTICS> statement expects package name in format "
        "spanner.<name>");
  }

  ZETASQL_ASSIGN_OR_RETURN(const String* package_namespace,
                   (GetListItemAsNode<String, T_String>(node.package_name, 0)));
  if (std::string(package_namespace->sval) != PGConstants::kNamespace) {
    return UnsupportedTranslationError(absl::StrCat(
        "<ALTER STATISTICS> statement requires package name to be prefixed by "
        "'",
        PGConstants::kNamespace, "'"));
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const VacuumStmt& node) {
  // VACUUM is not supported.
  if (node.is_vacuumcmd) {
    return UnsupportedTranslationError("VACUUM is not supported.");
  }
  if (node.options != nullptr) {
    return UnsupportedTranslationError(
        "Option is not supported by <ANALYZE> statement.");
  }
  if (node.rels) {
    ZETASQL_ASSIGN_OR_RETURN(
        const VacuumRelation* relation,
        (GetListItemAsNode<VacuumRelation, T_VacuumRelation>(node.rels, 0)));
    if (relation->relation != nullptr || relation->va_cols != nullptr) {
      return UnsupportedTranslationError(
          "Table or column is not supported by <ANALYZE> statement.");
    }
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const ViewStmt& node) {
  // Make sure that if ViewStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<RangeVar*>(node.view),
      FieldTypeChecker<List*>(node.aliases),
      FieldTypeChecker<Node*>(node.query), FieldTypeChecker<bool>(node.replace),
      FieldTypeChecker<List*>(node.options),
      FieldTypeChecker<ViewCheckOption>(node.withCheckOption),
      FieldTypeChecker<bool>(node.is_definer),
      FieldTypeChecker<ViewSecurityType>(node.view_security_type),
      FieldTypeChecker<char*>(node.query_string));

  // `view` defines the name of the new view.
  ZETASQL_RET_CHECK_NE(node.view, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.view, "CREATE VIEW"));

  // 'aliases' defines names to be used for columns of the view.
  if (node.aliases != nullptr) {
    return UnsupportedTranslationError(
        "Setting column names in <CREATE VIEW> statement is not supported.");
  }

  // 'options' used to set view options
  if (node.options != nullptr) {
    for (DefElem* def_elem : StructList<DefElem*>(node.options)) {
      // We currently only support the "security_invoker" WITH option.
      if (def_elem->defname !=
          internal::PostgreSQLConstants::kSecurityViewOptionName) {
        return UnsupportedTranslationError(absl::Substitute(
            "<$0> option is not supported in <WITH> clause of <CREATE VIEW> "
            "statement.",
            def_elem->defname));
      }
    }
  }

  // 'withCheckOption' used for updatable views which is not supported.
  if (node.withCheckOption != NO_CHECK_OPTION) {
    return UnsupportedTranslationError(
        "<CHECK OPTION> clause is not supported in <CREATE VIEW> statement.");
  }

  // 'query_string' defines the raw string format of the query field.
  ZETASQL_RET_CHECK_NE(node.query_string, nullptr);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterChangeStreamStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_AlterChangeStreamStmt);

  // Make sure that if AlterSpangresStatsStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node,
                         FieldTypeChecker<RangeVar*>(node.change_stream_name),
                         FieldTypeChecker<List*>(node.opt_options),
                         FieldTypeChecker<List*>(node.opt_for_tables),
                         FieldTypeChecker<List*>(node.opt_drop_for_tables),
                         FieldTypeChecker<bool>(node.for_all),
                         FieldTypeChecker<bool>(node.drop_for_all),
                         FieldTypeChecker<List*>(node.opt_reset_options));

  // `change_stream_name` defines the change stream name.
  ZETASQL_RET_CHECK_NE(node.change_stream_name, nullptr);

  // `opt_options` defines the user-configured options applied to the change
  // streams. Optional attribute. If any,
  // validated later during translation.
  int opt_options_int = node.opt_options ? 1 : 0;
  // `opt_for_tables` defines the tables the change stream should be altered
  // to track. Optional attribute. If any,
  // validated later during translation.
  int opt_for_tables_int = node.opt_for_tables ? 1 : 0;
  // `opt_drop_for_tables` defines the tables the change stream should be
  // altered to stop tracking. Optional attribute. If any,
  // validated later during translation.
  int opt_drop_for_tables_int = node.opt_drop_for_tables ? 1 : 0;
  // `for_all` defines whether the change stream should be altered to track
  // all tables. Optional attribute. If any,
  // validated later during translation.
  int for_all_int = node.for_all ? 1 : 0;
  // `drop_for_all` defines whether the change stream should be altered to stop
  // tracking all tables. Optional attribute. If any,
  // validated later during translation.
  int drop_for_all_int = node.drop_for_all ? 1 : 0;
  // `opt_reset_options` defines the options the change stream should be
  // altered to reset to default values. Optional attribute. If any,
  // validated later during translation.
  int opt_reset_options_int = node.opt_reset_options ? 1 : 0;

  // Exactly one of the above fields should be set at all times.
  ZETASQL_RET_CHECK_EQ(for_all_int + drop_for_all_int + opt_for_tables_int +
                   opt_drop_for_tables_int + opt_options_int +
                   opt_reset_options_int,
               1)
      << "Exactly one of the <SET FOR> clause, <DROP FOR> clause, "
      << "<SET> clause, or <RESET> clause should be populated in "
      << "ALTER CHANGE STREAM>.";

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreateChangeStreamStmt& node,
                                   const TranslationOptions& options) {
  ZETASQL_RET_CHECK_EQ(node.type, T_CreateChangeStreamStmt);

  // Make sure that if CreateChangeStreamStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node,
                         FieldTypeChecker<RangeVar*>(node.change_stream_name),
                         FieldTypeChecker<List*>(node.opt_options),
                         FieldTypeChecker<List*>(node.opt_for_tables),
                         FieldTypeChecker<bool>(node.for_all));

  // `change_stream_name` defines the change stream name.
  ZETASQL_RET_CHECK_NE(node.change_stream_name, nullptr);

  // `opt_options` defines the user-configured options applied to the change
  // streams. Optional attribute. If any, validated later during translation.

  if (node.opt_for_tables || node.for_all) {
    // `opt_for_tables` defines the tables the change stream should be created
    // to track. Optional attribute. If any, validated later during translation.
    int opt_for_tables_int = node.opt_for_tables ? 1 : 0;

    // `for_all` defines whether the change stream should be created to track
    // all tables. Optional attribute. If any, validated later during
    // translation.
    int for_all_int = node.for_all ? 1 : 0;
    ZETASQL_RET_CHECK_EQ(for_all_int + opt_for_tables_int, 1)
        << "Invalid <FOR> clause for <CREATE CHANGE STREAM>";
  }

  if (!options.enable_change_streams_if_not_exists && node.if_not_exists) {
    return UnsupportedTranslationError(
        "<IF NOT EXISTS> clause is not supported in <CREATE CHANGE STREAM> "
        "statement.");
  }

  return absl::OkStatus();
}

// TODO: expose when queue is implemented.

absl::Status ValidateParseTreeNode(const CreateSearchIndexStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_CreateSearchIndexStmt);
  // Make sure that if CreateSearchIndexStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<char*>(node.search_index_name),
                         FieldTypeChecker<RangeVar*>(node.table_name),
                         FieldTypeChecker<List*>(node.token_columns),
                         FieldTypeChecker<List*>(node.storing),
                         FieldTypeChecker<List*>(node.partition),
                         FieldTypeChecker<List*>(node.order),
                         FieldTypeChecker<Node*>(node.null_filters),
                         FieldTypeChecker<InterleaveSpec*>(node.interleave),
                         FieldTypeChecker<List*>(node.options));

  // `search_index_name` defines the name of the index.
  if (node.search_index_name == nullptr) {
    return UnsupportedTranslationError(
        "Index name is mandatory in <CREATE SEARCH INDEX> statement.");
  }

  // `table_name` defines a table to build index on.
  ZETASQL_RET_CHECK_NE(node.table_name, nullptr);
  ZETASQL_RETURN_IF_ERROR(
      ValidateParseTreeNode(*node.table_name, "CREATE SEARCH INDEX"));

  // `token_columns` defines the columns to index.
  ZETASQL_RET_CHECK(!IsListEmpty(node.token_columns));

  // `interleave in` validation.
  if (node.interleave != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateParseTreeNode(*node.interleave, "CREATE SEARCH INDEX"));
  }

  /*
    `include` columns are checked implicitly by ProcessOrdering().
    `partition by` is a list of string values and does not need explicit
     handling.
    `ordering` defines ORDER BY for a column and is checked implicitly
     by `ProcessOrdering()`
    `where` defines  NULL FILTERED columns in the form of Col1 NOT NULL AND Col2
     NOT NULL. This is checked by `TranslateWhereNode`.
    `options` can be sort_order_sharding = <bool>
     and disable_automatic_uid = <bool>`.
     This is validated in `PopulateSearchIndexOptions()`
  */
  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterSearchIndexCmd& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_AlterSearchIndexCmd);
  // Make sure that if AlterSearchIndexCmd structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<AlterSearchIndexCmdType>(node.cmd_type),
      FieldTypeChecker<char*>(node.column_name));

  ZETASQL_RET_CHECK_NE(node.column_name, nullptr);
  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreateLocalityGroupStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_CreateLocalityGroupStmt);

  // Make sure that if CreateLocalityGroupStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<RangeVar*>(node.locality_group_name),
      FieldTypeChecker<LocalityGroupOption*>(node.storage),
      FieldTypeChecker<LocalityGroupOption*>(node.ssd_to_hdd_spill_timespan),
      FieldTypeChecker<bool>(node.if_not_exists));

  if (node.storage != nullptr) {
    if (!node.storage->is_null && strcmp(node.storage->value, "ssd") != 0 &&
        strcmp(node.storage->value, "hdd") != 0) {
      return UnsupportedTranslationError(absl::StrFormat(
          "Invalid storage '%s'. Must be one of 'ssd', 'hdd' or NULL.",
          node.storage->value));
    }
  }
  if (node.ssd_to_hdd_spill_timespan != nullptr) {
    if (!node.ssd_to_hdd_spill_timespan->is_null &&
        internal::ParseSchemaTimeSpec(node.ssd_to_hdd_spill_timespan->value) ==
            -1) {
      return UnsupportedTranslationError(
          absl::StrFormat("Invalid ssd_to_hdd_spill_timespan '%s'. Must be a "
                          "valid timespan or NULL.",
                          node.ssd_to_hdd_spill_timespan->value));
    }
  }
  ZETASQL_RET_CHECK_NE(node.locality_group_name, nullptr);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterLocalityGroupStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_AlterLocalityGroupStmt);

  // Make sure that if AlterLocalityGroupStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<RangeVar*>(node.locality_group_name),
      FieldTypeChecker<LocalityGroupOption*>(node.storage),
      FieldTypeChecker<LocalityGroupOption*>(node.ssd_to_hdd_spill_timespan),
      FieldTypeChecker<bool>(node.if_exists));

  if (node.storage == nullptr && node.ssd_to_hdd_spill_timespan == nullptr) {
    return UnsupportedTranslationError(
        "At least one of STORAGE or SSD_TO_HDD_SPILL_TIMESPAN must be "
        "specified in <ALTER LOCALITY GROUP> statement.");
  }

  if (node.storage != nullptr) {
    if (!node.storage->is_null && strcmp(node.storage->value, "ssd") != 0 &&
        strcmp(node.storage->value, "hdd") != 0) {
      return UnsupportedTranslationError(absl::StrFormat(
          "Invalid storage '%s'. Must be one of 'ssd', 'hdd' or NULL.",
          node.storage->value));
    }
  }
  if (node.ssd_to_hdd_spill_timespan != nullptr) {
    if (!node.ssd_to_hdd_spill_timespan->is_null &&
        internal::ParseSchemaTimeSpec(node.ssd_to_hdd_spill_timespan->value) ==
            -1) {
      return UnsupportedTranslationError(
          absl::StrFormat("Invalid ssd_to_hdd_spill_timespan '%s'. Must be a "
                          "valid timespan or NULL.",
                          node.ssd_to_hdd_spill_timespan->value));
    }
  }
  ZETASQL_RET_CHECK_NE(node.locality_group_name, nullptr);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AlterColumnLocalityGroupStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_AlterColumnLocalityGroupStmt);

  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<RangeVar*>(node.relation),
      FieldTypeChecker<char*>(node.column),
      FieldTypeChecker<LocalityGroupOption*>(node.locality_group_name));

  ZETASQL_RET_CHECK_NE(node.relation, nullptr);
  ZETASQL_RET_CHECK_NE(node.column, nullptr);
  ZETASQL_RET_CHECK_NE(node.locality_group_name, nullptr);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const CreateRoleStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_CreateRoleStmt);

  // Make sure that if CreateRoleStmt structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<RoleStmtType>(node.stmt_type),
                         FieldTypeChecker<char*>(node.role),
                         FieldTypeChecker<List*>(node.options));

  // Check for old variants of CREATE ROLE.
  ZETASQL_RETURN_IF_ERROR(([&node]() -> absl::Status {
    switch (node.stmt_type) {
      case ROLESTMT_ROLE:
        return absl::OkStatus();
      case ROLESTMT_USER:
        return UnsupportedTranslationError(
            "<CREATE USER> statement is not supported.");
      case ROLESTMT_GROUP:
        return UnsupportedTranslationError(
            "<CREATE GROUP> statement is not supported.");
    }
    ZETASQL_RET_CHECK_FAIL() << "Unknown value of RoleStmtType " << node.stmt_type;
  })());

  // Certain role names should not be created.
  if (internal::IsReservedName(node.role)) {
    return UnsupportedTranslationError(
        "'pg_' is not supported as a prefix for a role name.");
  }
  if (internal::IsPostgresReservedName(node.role)) {
    return UnsupportedTranslationError(
        "'postgres' is not supported as a role name.");
  }

  // `options` is used to set role options, which are not supported.
  if (!IsListEmpty(node.options)) {
    return UnsupportedTranslationError(
        "<WITH> clause is not supported in <CREATE ROLE> statement.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const DropRoleStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_DropRoleStmt);

  // Make sure that if DropRoleStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.roles),
                         FieldTypeChecker<bool>(node.missing_ok));

  // Drop only one role.
  if (list_length(node.roles) > 1) {
    return UnsupportedTranslationError(
        "<DROP ROLE> supports deletion of only a single role per statement.");
  }
  ZETASQL_ASSIGN_OR_RETURN(const RoleSpec* role_spec,
                   (SingleItemListAsNode<RoleSpec, T_RoleSpec>)(node.roles));
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*role_spec));

  if (node.missing_ok) {
    return UnsupportedTranslationError(
        "<IF EXISTS> clause is not supported in <DROP ROLE> statement.");
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const GrantStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_GrantStmt);

  // Make sure that if GrantStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<bool>(node.is_grant),
                         FieldTypeChecker<GrantTargetType>(node.targtype),
                         FieldTypeChecker<ObjectType>(node.objtype),
                         FieldTypeChecker<List*>(node.objects),
                         FieldTypeChecker<List*>(node.privileges),
                         FieldTypeChecker<List*>(node.grantees),
                         FieldTypeChecker<bool>(node.grant_option),
                         FieldTypeChecker<RoleSpec*>(node.grantor),
                         FieldTypeChecker<DropBehavior>(node.behavior));

  absl::string_view grant_type;
  if (node.is_grant) {
    grant_type = "GRANT";
  } else {
    grant_type = "REVOKE";
  }

  // Certain object types are not supported.
  absl::string_view object_type;
  switch (node.objtype) {
    case ObjectType::OBJECT_TABLE:
      object_type = "TABLES";
      break;
    case ObjectType::OBJECT_CHANGE_STREAM:
      object_type = "CHANGE STREAMS";
      break;
    case ObjectType::OBJECT_FUNCTION:
      object_type = "FUNCTIONS";
      break;
    case ObjectType::OBJECT_ROUTINE:
      object_type = "ROUTINES";
      break;
    case ObjectType::OBJECT_SEQUENCE:
      object_type = "SEQUENCES";
      break;
    case ObjectType::OBJECT_SCHEMA:
      object_type = "SCHEMAS";
      break;
    default:
      return UnsupportedTranslationError(absl::Substitute(
          "Object type is not supported in <$0> statement.", grant_type));
  }

  // Privileges must be granted by naming the targets individually.
  ZETASQL_RETURN_IF_ERROR(([&node, grant_type, object_type]() -> absl::Status {
    switch (node.targtype) {
      case ACL_TARGET_OBJECT:
        return absl::OkStatus();
      case ACL_TARGET_ALL_IN_SCHEMA:
        return absl::OkStatus();
      case ACL_TARGET_DEFAULTS:
        return UnsupportedTranslationError(
            "DEFAULT PRIVILEGES is not supported.");
    }
    ZETASQL_RET_CHECK_FAIL() << "Unknown value of GrantTargetType " << node.targtype
                     << ".";
  }()));

  if (node.privileges == nullptr) {
    return UnsupportedTranslationError(absl::Substitute(
        "<ALL PRIVILEGES> clause is not supported in <$0> statement.",
        grant_type));
  }

  // `grant_option` is used by GRANT OPTION, which is not supported.
  if (node.grant_option) {
    absl::string_view clause;
    if (node.is_grant) {
      clause = "WITH GRANT OPTION";
    } else {
      clause = "GRANT OPTION";
    }
    return UnsupportedTranslationError(absl::Substitute(
        "<$0> clause is not supported in <$1> statement.", clause, grant_type));
  }

  // Check drop behavior.
  if (node.is_grant) {
    ZETASQL_RET_CHECK_EQ(node.behavior, DROP_RESTRICT);
  } else {
    ZETASQL_RETURN_IF_ERROR(([&node, grant_type]() -> absl::Status {
      switch (node.behavior) {
        case DROP_RESTRICT:
          return absl::OkStatus();
        case DROP_CASCADE:
          return UnsupportedTranslationError(absl::Substitute(
              "<CASCADE> clause is not supported in <$0> statement.",
              grant_type));
      }
      ZETASQL_RET_CHECK_FAIL() << "Unknown value of DropBehavior " << node.behavior
                       << ".";
    }()));
  }

  // `grantor` is not supported
  if (node.grantor) {
    return UnsupportedTranslationError(absl::Substitute(
        "<GRANT BY> clause is not supported in <$0> statement.", grant_type));
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const AccessPriv& node, bool is_grant,
                                   bool is_privilege) {
  ZETASQL_RET_CHECK_EQ(node.type, T_AccessPriv);

  // Make sure that if AccessPriv structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<char*>(node.priv_name),
                         FieldTypeChecker<List*>(node.cols));

  // Both GrantStmt and GrantRoleStmt use AccessPriv to represent the
  // privilege or role being granted. For both statements, priv_name is not
  // validated here, and it should be validated and manipulated in the
  // translator.

  absl::string_view grant_type;
  if (is_grant) {
    grant_type = "GRANT ROLE";
  } else {
    grant_type = "REVOKE ROLE";
  }

  // Catch a statement like `GRANT foo(bar) TO r`.
  if (!is_privilege && node.cols != nullptr) {
    return UnsupportedTranslationError(absl::Substitute(
        "Column names are not supported in <$0> statement.", grant_type));
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const ObjectWithArgs& node, bool is_grant) {
  ZETASQL_RET_CHECK_EQ(node.type, T_ObjectWithArgs);

  // Make sure that if ObjectWithArgs structure changes we update the
  // translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.objname),
                         FieldTypeChecker<List*>(node.objargs),
                         FieldTypeChecker<List*>(node.objfuncargs),
                         FieldTypeChecker<bool>(node.args_unspecified));

  // `objname` must contain an object name, and it can optionally contain a
  // schema name and catalog name.
  ZETASQL_RET_CHECK_NE(node.objname, nullptr);
  ZETASQL_RET_CHECK_GT(list_length(node.objname), 0);
  if (list_length(node.objname) > 3) {
    return UnsupportedTranslationError(
        "Function contains too many dotted names.");
  }

  // `objfuncargs` contains the full specification of the parameter list (if not
  // NULL). This is not supported.
  ZETASQL_RET_CHECK_EQ(node.objfuncargs, nullptr);

  // `args_unspecified` and `objargs` must indicate that no function arguments
  // are provided.
  if (!node.args_unspecified) {
    std::string grant_type = is_grant ? "GRANT" : "REVOKE";
    return UnsupportedTranslationError(absl::Substitute(
        "Function arguments are not supported in <$0> statement.", grant_type));
  }
  ZETASQL_RET_CHECK_EQ(node.objargs, nullptr);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const RoleSpec& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_RoleSpec);

  // Make sure that if RoleSpec structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<RoleSpecType>(node.roletype),
                         FieldTypeChecker<char*>(node.rolename),
                         FieldTypeChecker<int>(node.location));

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const GrantRoleStmt& node) {
  ZETASQL_RET_CHECK_EQ(node.type, T_GrantRoleStmt);

  // Make sure that if GrantRoleStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(node, FieldTypeChecker<List*>(node.granted_roles),
                         FieldTypeChecker<List*>(node.grantee_roles),
                         FieldTypeChecker<bool>(node.is_grant),
                         FieldTypeChecker<bool>(node.admin_opt),
                         FieldTypeChecker<RoleSpec*>(node.grantor),
                         FieldTypeChecker<DropBehavior>(node.behavior));

  absl::string_view grant_type;
  if (node.is_grant) {
    grant_type = "GRANT ROLE";
  } else {
    grant_type = "REVOKE ROLE";
  }

  // `admin_opt` is used by ADMIN OPTION, which is not supported.
  if (node.admin_opt) {
    absl::string_view clause;
    if (node.is_grant) {
      clause = "WITH ADMIN OPTION";
    } else {
      clause = "ADMIN OPTION";
    }
    return UnsupportedTranslationError(absl::Substitute(
        "<$0> clause is not supported in <$1> statement.", clause, grant_type));
  }

  // `grantor` is used by GRANTED BY, which is not supported.
  if (node.grantor != nullptr) {
    ZETASQL_RET_CHECK(node.is_grant);
    return UnsupportedTranslationError(absl::Substitute(
        "<GRANTED BY> clause is not supported in <$0> statement.", grant_type));
  }

  // Check drop behavior.
  if (node.is_grant) {
    ZETASQL_RET_CHECK_EQ(node.behavior, DROP_RESTRICT);
  } else {
    ZETASQL_RETURN_IF_ERROR(([&node, grant_type]() -> absl::Status {
      switch (node.behavior) {
        case DROP_RESTRICT:
          return absl::OkStatus();
        case DROP_CASCADE:
          return UnsupportedTranslationError(absl::Substitute(
              "<CASCADE> clause is not supported in <$0> statement.",
              grant_type));
      }
      ZETASQL_RET_CHECK_FAIL() << "Unknown value of DropBehavior " << node.behavior
                       << ".";
    }()));
  }

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const IndexStmt& node,
                                   const TranslationOptions& options) {
  // Make sure that if IndexStmt structure changes we update the translator.
  AssertPGNodeConsistsOf(
      node, FieldTypeChecker<char*>(node.idxname),
      FieldTypeChecker<RangeVar*>(node.relation),
      FieldTypeChecker<char*>(node.accessMethod),
      FieldTypeChecker<char*>(node.tableSpace),
      FieldTypeChecker<LocalityGroupOption*>(node.locality_group_name),
      FieldTypeChecker<List*>(node.indexParams),
      FieldTypeChecker<List*>(node.indexIncludingParams),
      FieldTypeChecker<List*>(node.options),
      FieldTypeChecker<InterleaveSpec*>(node.interleavespec),
      FieldTypeChecker<Node*>(node.whereClause),
      FieldTypeChecker<List*>(node.excludeOpNames),
      FieldTypeChecker<char*>(node.idxcomment),
      FieldTypeChecker<Oid>(node.indexOid), FieldTypeChecker<Oid>(node.oldNode),
      FieldTypeChecker<SubTransactionId>(node.oldCreateSubid),
      FieldTypeChecker<SubTransactionId>(node.oldFirstRelfilenodeSubid),
      FieldTypeChecker<bool>(node.unique),
      FieldTypeChecker<bool>(node.nulls_not_distinct),
      FieldTypeChecker<bool>(node.primary),
      FieldTypeChecker<bool>(node.isconstraint),
      FieldTypeChecker<bool>(node.deferrable),
      FieldTypeChecker<bool>(node.initdeferred),
      FieldTypeChecker<bool>(node.transformed),
      FieldTypeChecker<bool>(node.concurrent),
      FieldTypeChecker<bool>(node.if_not_exists),
      FieldTypeChecker<bool>(node.reset_default_tblspc));

  ZETASQL_RET_CHECK_EQ(node.type, T_IndexStmt);

  // `idxname` defines the name of the index.
  if (node.idxname == nullptr || *node.idxname == '\0') {
    return UnsupportedTranslationError(
        "Index name is mandatory in <CREATE INDEX> statement.");
  }

  // `relation` defines a table to build index on.
  ZETASQL_RET_CHECK_NE(node.relation, nullptr);
  ZETASQL_RETURN_IF_ERROR(ValidateParseTreeNode(*node.relation, "CREATE INDEX"));

  // `accessMethod` only allows specifying a vector index type.
  // Setting it to any other value, including the default (btree), is not
  // supported.
  if (strcmp(node.accessMethod, DEFAULT_INDEX_TYPE) != 0) {
    return UnsupportedTranslationError(
        "Setting access method is not supported in <CREATE INDEX> statement.");
  }

  // `tablespace` defines space in which to create the index. Setting this is
  // not supported.
  if (node.tableSpace != nullptr) {
    return UnsupportedTranslationError(
        "Tablespaces are not supported in <CREATE INDEX> statement.");
  }

  // `indexParams` defines the columns to index.
  ZETASQL_RET_CHECK(!IsListEmpty(node.indexParams));

  // `indexIncludingParams` defines columns provided in INCLUDE clause. They are
  // additional columns store with index columns not used for search. They are
  // optional and will be translated into Spanner STORING clause if present.

  // `options` defines storage parameters for index in WITH clause. e.g.
  // CREATE INDEX title_idx ON films (title) WITH (deduplicate_items = off);
  // This is only supported for vector indexes.
  if (!IsListEmpty(node.options)) {
    return UnsupportedTranslationError(
        "Index options are not supported in <CREATE INDEX> statement.");
  }

  // `interleavespec` defines index placement/interleave information.
  if (node.interleavespec != nullptr) {
    ZETASQL_RETURN_IF_ERROR(
        ValidateParseTreeNode(*node.interleavespec, "CREATE INDEX"));
  }

  // `excludeOpNames` defines exclusion operator name. This is used during
  // analyzer phase internally when analyzing exclustion constraint.
  ZETASQL_RET_CHECK_EQ(node.excludeOpNames, nullptr);

  // `idxcomment` defines the comment for an index. The value is from catalog,
  // not used by parser.
  ZETASQL_RET_CHECK_EQ(node.idxcomment, nullptr);

  // `indexOid` defines the OID of the index. This is used during analyzer phase
  // internally.
  ZETASQL_RET_CHECK_EQ(node.indexOid, InvalidOid);

  // `oldNode` defines the object id on storage layer. This is used in PG
  // analyzer phase internally.
  ZETASQL_RET_CHECK_EQ(node.oldNode, InvalidOid);

  // `oldCreateSubid` and `oldFirstRelfilenodeSubid` define the object id on
  // storage layer of an existed index. They are used in PG analyzer phase
  // internally.
  ZETASQL_RET_CHECK_EQ(node.oldCreateSubid, InvalidSubTransactionId);
  ZETASQL_RET_CHECK_EQ(node.oldFirstRelfilenodeSubid, InvalidSubTransactionId);

  // `unique` defines if index should be UNIQUE.

  // `primary`, `isconstraint`, `deferrable` and `initdeferred` define the
  // properties of the index created internally by PG analyzer for constraints
  // e.g. PRIMARY KEY/UNIQUE. They are not used for parser phase and set to
  // `false` by default.
  ZETASQL_RET_CHECK(!node.primary);       // is index a primary key?
  ZETASQL_RET_CHECK(!node.isconstraint);  // is it for a pkey/unique constraint?
  ZETASQL_RET_CHECK(!node.deferrable);    // is the constraint DEFERRABLE?
  ZETASQL_RET_CHECK(!node.initdeferred);  // is the constraint INITIALLY DEFERRED?

  // `transformed` defines the state whether an index has been processed
  // internally by PG's analyzer. It is `false` during parsing phase.
  ZETASQL_RET_CHECK(!node.transformed);

  // `concurrent` defines the way to build up index concurrently or not.
  // This is not supported.
  if (node.concurrent) {
    return UnsupportedTranslationError(
        "Concurrent index creation is not supported in <CREATE INDEX> "
        "statement.");
  }

  // `if_not_exists` is set if IF NOT EXISTS clause is present. Not supported.
  if (node.if_not_exists && !options.enable_if_not_exists) {
    return UnsupportedTranslationError(
        "<IF NOT EXISTS> is not supported in <CREATE INDEX> statement.");
  }

  // `reset_default_tblspc` defines a boolean to reset default table space.
  // This is not supported, default value is false.
  ZETASQL_RET_CHECK(!node.reset_default_tblspc);

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const IndexElem& node) {
  AssertPGNodeConsistsOf(node, FieldTypeChecker<char*>(node.name),
                         FieldTypeChecker<Node*>(node.expr),
                         FieldTypeChecker<char*>(node.expr_string),
                         FieldTypeChecker<char*>(node.indexcolname),
                         FieldTypeChecker<List*>(node.collation),
                         FieldTypeChecker<List*>(node.opclass),
                         FieldTypeChecker<List*>(node.opclassopts),
                         FieldTypeChecker<SortByDir>(node.ordering),
                         FieldTypeChecker<SortByNulls>(node.nulls_ordering));

  ZETASQL_RET_CHECK_EQ(node.type, T_IndexElem);

  // `name` defines the column name to index
  if (node.expr == nullptr && (node.name == nullptr || *node.name == '\0')) {
    return UnsupportedTranslationError(
        "Column name is mandatory in <CREATE INDEX> statement.");
  }

  // `expr` defines the expressions applied to the column. e.g.
  // CREATE INDEX ON films ((lower(title))); This is not supported.
  if (node.expr != nullptr) {
   return UnsupportedTranslationError(
       "Expressions are not supported in <CREATE INDEX> statement.");
  }

  // `indexcolname` defines the name of the index when the index is created
  // using  expression. It is an internal field used set by the analyer after
  // the parsing phase. During parsing phase, it is not used.
  ZETASQL_RET_CHECK_EQ(node.indexcolname, nullptr);

  // `collation` define name of collation: e.g. CREATE INDEX title_idx_german ON
  // films (title COLLATE "de_DE"); Default value is `NULL`, setting this is not
  // supported.
  if (!IsListEmpty(node.collation)) {
    return UnsupportedTranslationError(
        "Setting collation is not supported in <CREATE INDEX> statement.");
  }

  // `opclass` defines the name of an operator class for the column of an index.
  // It identifies the operators to be used by the index for that column: e.g.
  // CREATE INDEX test_index ON test_table (col varchar_pattern_ops);
  // Setting this is not supported.
  if (!IsListEmpty(node.opclass)) {
    return UnsupportedTranslationError(
        "Setting operator class is not supported in <CREATE INDEX> statement.");
  }

  // `opclassopts` defines options/parameters of an operator class.
  if (!IsListEmpty(node.opclassopts)) {
    return UnsupportedTranslationError(
        "Setting operator class parameter is not supported in <CREATE INDEX> "
        "statement");
  }

  // `ordering` defines ORDER BY for a column. This is simple enum and the valid
  // value is checked implicitly by `ProcessOrdering()`

  // `nulls_ordering` defines NULLS FIRST/LAST in column ordering. This is
  // simple enum and valid value is checked implicitly by `nulls_ordering()`

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const BoolExpr& node) {
  AssertStructConsistsOf(node, FieldTypeChecker<Expr>(node.xpr),
                         FieldTypeChecker<BoolExprType>(node.boolop),
                         FieldTypeChecker<List*>(node.args),
                         FieldTypeChecker<int>(node.location));
  // `xpr` defines expression type
  ZETASQL_RET_CHECK_EQ(node.xpr.type, T_BoolExpr);

  // `boolop` defines logical operator type to apply to the arguments. Validated
  // in `TranslateBoolExpr()`

  // `args` defines the arguments of the logical operator.
  ZETASQL_RET_CHECK(!IsListEmpty(node.args));

  // `location` defines parsed token location of the text in the input, no
  // validation required.

  return absl::OkStatus();
}

absl::Status ValidateParseTreeNode(const NullTest& node) {
  AssertStructConsistsOf(node, FieldTypeChecker<Expr>(node.xpr),
                         FieldTypeChecker<Expr*>(node.arg),
                         FieldTypeChecker<NullTestType>(node.nulltesttype),
                         FieldTypeChecker<bool>(node.argisrow),
                         FieldTypeChecker<int>(node.location));

  // `xpr` defines expression type
  ZETASQL_RET_CHECK_EQ(node.xpr.type, T_NullTest);

  // `arg` defines the expression in the null test.
  ZETASQL_RET_CHECK_NE(node.arg, nullptr);

  // `argisrow` True to perform field-by-field null checks for a row type value.
  // This value is set during analyzer phase. No validation needed.

  return absl::OkStatus();
}

}  // namespace spangres
}  // namespace postgres_translator
