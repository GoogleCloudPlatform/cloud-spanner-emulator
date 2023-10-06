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

// Portions derived from source copyright Postgres Global Development Group in
// accordance with the following notice:
//------------------------------------------------------------------------------
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
// Portions Copyright 2022 Google LLC
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

#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim.h"
#include "third_party/spanner_pg/shims/parser_shim.h"
#include "third_party/spanner_pg/util/nodetag_to_string.h"

// Match type for foreign keys:
//   - kFull = MATCH FULL will not allow one column of a multicolumn foreign
//     key to be null unless all foreign key columns are null.
//   - kSimple = MATCH SIMPLE allows some foreign key columns to be null while
//     other parts of the foreign key are not null.
enum MatchType {
  kMatchFull = 'f',
  kMatchSimple = 's',
};

// Action for foreign key's <ON UPDATE>/<ON DELETE> clauses
// The only supported action for now is kNoAction=NO ACTION
enum ForeignKeyAction {
  kActionNoAction = 'a',
  kRestrict = 'r',
  kCascade = 'c',
  kSetNull = 'n',
  kSetDefault = 'd'
};

static char* err_msg_where_clause_create_index =
    "<WHERE> clause of <CREATE INDEX> statement only supports <column IS "
    "NOT NULL> expressions and their <AND> conjunctions.";

// We made these functions non-static to call from here. Forward declare
// them to limit the scope.
void appendContextKeyword(deparse_context* context, const char* str,
                          int indentBefore, int indentAfter, int indentPlus);
void simple_quote_literal(StringInfo buf, const char* val);

char* GetStringValue(Value* val) {
  if (val->type != T_String) {
    elog(ERROR, "invalid parse tree: string value expected, but got %s",
         CNodeTagToNodeString(val->type));
  }

  return val->val.str;
}

int GetIntValue(Value* val) {
  if (val->type != T_Integer) {
    elog(ERROR, "invalid parse tree: integer value expected, but got %s",
         CNodeTagToNodeString(val->type));
  }

  return val->val.ival;
}

// Deparse Value node and print it as literal depending on type:
//   - for int nodes, just print as is
//   - for string nodes, quote the value before printing
void GetValueDef(Value* val, deparse_context* context) {
  StringInfo buf = context->buf;

  switch (val->type) {
    case T_Integer:
      appendStringInfo(buf, "%d", val->val.ival);
      break;

    case T_String:
      simple_quote_literal(buf, val->val.str);
      break;

    // TODO: add test case and support for T_Float, T_Null values
    default:
      elog(ERROR, "unexpected value type %s", CNodeTagToNodeString(val->type));
  }
}

// Deparse Value node and print it as identifier
void GetIdentifierValueDef(Value* val, deparse_context* context) {
  StringInfo buf = context->buf;

  if (val->type != T_String || val->val.str == NULL) {
    elog(ERROR, "invalid identifier type");
  }

  appendStringInfoString(buf, quote_identifier(val->val.str));
}

// Deparse RangeVar and append it to buf.
void AppendRangeVarToStringInfo(StringInfo buf, RangeVar* stmt) {
  if (stmt == NULL || stmt->relname == NULL || *stmt->relname == '\0') {
    elog(ERROR, "invalid parse tree: table name is missing");
  }
  if (stmt->schemaname != NULL) {
    appendStringInfoString(buf, quote_identifier(stmt->schemaname));
    appendStringInfoString(buf, ".");
  }
  appendStringInfoString(buf, quote_identifier(stmt->relname));
}

// Deparse column type
void GetTypeNameDef(TypeName* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  if (stmt->typemod != -1) {
    elog(ERROR, "Support for prespecified type modifiers is not implemented");
  }

  // We expect name to be <[schema_name.]type_name>
  if (list_length(stmt->names) > 2) {
    elog(ERROR,
         "invalid parse tree: no more than 2 name components are expected, but "
         "got %u",
         list_length(stmt->names));
  }

  char* schema_name = NULL;
  ListCell* names_to_process = list_head(stmt->names);
  if (lnext(stmt->names, names_to_process)) {
    schema_name = strVal(lfirst(names_to_process));
    appendStringInfo(buf, "%s.", quote_identifier(schema_name));
    names_to_process = lnext(stmt->names, names_to_process);
  }
  char* type_name = strVal(lfirst(names_to_process));
  appendStringInfoString(buf, type_name);

  if (list_length(stmt->typmods) > 0) {
    if (schema_name != NULL && strcmp(schema_name, "pg_catalog") != 0) {
      // schema==NULL implies default (pg_catalog) schema
      elog(ERROR,
           "type modifiers are supported only for types from the pg_catalog "
           "schema, but got: %s",
           schema_name);
    }

    appendStringInfoString(buf, "(");
    if (strcmp(type_name, "varchar") == 0 || strcmp(type_name, "bpchar") == 0 ||
        strcmp(type_name, "varbit") == 0 || strcmp(type_name, "bit") == 0 ||
        strcmp(type_name, "timestamp") == 0 ||
        strcmp(type_name, "timestamptz") == 0) {
      if (list_length(stmt->typmods) != 1) {
        elog(ERROR, "invalid parse tree: only one modifier allowed for type %s",
             type_name);
      }
      A_Const* typmod_node = castNode(A_Const, linitial(stmt->typmods));
      GetValueDef(&typmod_node->val, context);
    } else if (strcmp(type_name, "numeric") == 0) {
      if (list_length(stmt->typmods) != 2) {
        elog(ERROR,
             "invalid parse tree: two modifiers must be present for type %s",
             type_name);
      }
      A_Const* precision_node = castNode(A_Const, linitial(stmt->typmods));
      A_Const* scale_node = castNode(A_Const, lsecond(stmt->typmods));
      GetValueDef(&precision_node->val, context);
      appendStringInfoString(buf, ", ");
      GetValueDef(&scale_node->val, context);
    } else {
      elog(ERROR, "type modifiers are not allowed for type %s", type_name);
    }
    appendStringInfoString(buf, ")");
  }

  for (ListCell* bound_it = list_head(stmt->arrayBounds); bound_it;
       bound_it = lnext(stmt->arrayBounds, bound_it)) {
    int bound = intVal(lfirst(bound_it));

    if (bound != -1) {
      appendStringInfo(buf, "[%d]", bound);
    } else {
      appendStringInfoString(buf, "[]");
    }
  }
}

// Deparse NOTIFY statements
void GetNotifyStmtDef(NotifyStmt* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  appendContextKeyword(context, "", 0, POSTGRES_PRETTYINDENT_STD, 1);
  appendStringInfo(buf, "NOTIFY %s", quote_identifier(stmt->conditionname));

  if (stmt->payload) {
    appendStringInfoString(buf, ", ");
    simple_quote_literal(buf, stmt->payload);
  }
}

// Deparse CREATE DATABASE statements
void GetCreatedbStmtDef(CreatedbStmt* stmt, deparse_context* context) {
  if (list_length(stmt->options) != 0) {
    elog(ERROR,
         "options support is not implemented in <CREATE DATABASE> statement");
  }

  StringInfo buf = context->buf;

  appendStringInfo(buf, "CREATE DATABASE %s", quote_identifier(stmt->dbname));
}


// Deparse IS NOT NULL in WHERE clause
void GetNullTestDef(NullTest* node, deparse_context* context) {
  StringInfo buf = context->buf;
  if (node->nulltesttype != IS_NOT_NULL || nodeTag(node->arg) != T_ColumnRef) {
    elog(ERROR, err_msg_where_clause_create_index);
  }

  ColumnRef* column_ref = castNode(ColumnRef, node->arg);

  const List* column_list = column_ref->fields;
  if (list_length(column_list) != 1) {
    elog(ERROR, err_msg_where_clause_create_index);
  }

  const char* column_name = strVal(linitial(column_list));
  appendStringInfo(buf, "%s IS NOT NULL", quote_identifier(column_name));
}

void ValidateIndexElem(IndexElem* index_elem) {
  if (!index_elem->name || *index_elem->name == '\0') {
    elog(ERROR, "column name is missing in <CREATE INDEX> statement");
  }
  if (index_elem->expr != NULL) {
    elog(ERROR,
         "expressions support is not implemented in <CREATE INDEX> "
         "statement");
  }
  if (index_elem->collation != NULL) {
    elog(ERROR,
         "setting collation is not implemented in <CREATE INDEX> statement");
  }
  if (index_elem->opclass != NULL) {
    elog(ERROR,
         "setting operator class is not implemented in <CREATE INDEX> "
         "statement");
  }
}

// Deparse CREATE INDEX statements
void GetIndexStmtDef(IndexStmt* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  if (stmt->primary || stmt->isconstraint || stmt->deferrable ||
      stmt->initdeferred || stmt->excludeOpNames != NULL) {
    elog(ERROR,
         "invalid parse tree: constraints based on existing indexes cannot be "
         "created via <CREATE INDEX> statement");
  }
  if (stmt->idxcomment != NULL) {
    elog(ERROR,
         "index comments support is not implemented in <CREATE INDEX> "
         "statement");
  }
  if (stmt->tableSpace != NULL) {
    elog(ERROR,
         "tablespaces support is not implemented in <CREATE INDEX> statement");
  }
  if (stmt->concurrent) {
    elog(ERROR,
         "consurrent index creation support is not implemented in <CREATE "
         "INDEX> statement");
  }
  if (stmt->if_not_exists) {
    elog(ERROR,
         "<IF NOT EXISTS> support is not implemented in <CREATE INDEX> "
         "statement");
  }

  if (!stmt->idxname || *stmt->idxname == '\0') {
    elog(ERROR, "index name is missing in <CREATE INDEX> statement");
  }

  appendStringInfoString(buf, "CREATE");
  if (stmt->unique) {
    appendStringInfoString(buf, " UNIQUE");
  }
  appendStringInfo(buf, " INDEX %s ON ", quote_identifier(stmt->idxname));
  AppendRangeVarToStringInfo(buf, stmt->relation);
  appendStringInfoString(buf, " (");

  for (ListCell* index_elem_it = list_head(stmt->indexParams); index_elem_it;
       index_elem_it = lnext(stmt->indexParams, index_elem_it)) {
    IndexElem* index_elem = lfirst_node(IndexElem, index_elem_it);

    ValidateIndexElem(index_elem);
    appendStringInfo(buf, "%s", quote_identifier(index_elem->name));
    if (index_elem->ordering == SORTBY_DESC) {
      appendStringInfoString(buf, " DESC");
    }

    switch (index_elem->nulls_ordering) {
      case SORTBY_NULLS_FIRST: {
        appendStringInfoString(buf, " NULLS FIRST");
        break;
      }

      case SORTBY_NULLS_LAST: {
        appendStringInfoString(buf, " NULLS LAST");
        break;
      }

      default: {
        // avoid extra verbosity and rely on default ordering
        break;
      }
    }

    if (lnext(stmt->indexParams, index_elem_it) != NULL) {
      appendStringInfo(buf, ", ");
    }
  }
  appendStringInfoString(buf, ")");

  if (list_length(stmt->indexIncludingParams) > 0) {
    appendStringInfoString(buf, " INCLUDE (");

    for (ListCell* index_elem_it = list_head(stmt->indexIncludingParams);
         index_elem_it; index_elem_it = lnext(stmt->indexIncludingParams, index_elem_it)) {
      IndexElem* index_elem = lfirst_node(IndexElem, index_elem_it);

      ValidateIndexElem(index_elem);

      appendStringInfo(buf, "%s", quote_identifier(index_elem->name));

      if (lnext(stmt->indexIncludingParams, index_elem_it) != NULL) {
        appendStringInfo(buf, ", ");
      }
    }

    appendStringInfoString(buf, ")");
  }

  Node* whereClause = stmt->whereClause;

  if (whereClause != NULL) {
    appendStringInfoString(buf, " WHERE (");

    switch (nodeTag(whereClause)) {
      case T_NullTest: {
        NullTest* null_test = castNode(NullTest, whereClause);
        GetNullTestDef(null_test, context);
        break;
      }
      case T_BoolExpr: {
        BoolExpr* bool_expr = castNode(BoolExpr, whereClause);
        if (bool_expr->boolop != AND_EXPR) {
          elog(ERROR, err_msg_where_clause_create_index);
        }

        for (ListCell* null_test = list_head(bool_expr->args); null_test;
             null_test = lnext(bool_expr->args, null_test)) {
          GetNullTestDef(lfirst_node(NullTest, null_test), context);
          if (lnext(bool_expr->args, null_test) != NULL) {
            appendStringInfoString(buf, " AND ");
          }
        }
        break;
      }
      default: {
        elog(ERROR, err_msg_where_clause_create_index);
      }
    }
    appendStringInfoString(buf, ")");
  }
}

void GetForeignKeyAction(char action, deparse_context* context) {
  StringInfo buf = context->buf;
  switch (action) {
    case FKCONSTR_ACTION_NOACTION: {
      appendStringInfoString(buf, " NO ACTION");
      break;
    }
    case FKCONSTR_ACTION_CASCADE: {
      appendStringInfoString(buf, " CASCADE");
      break;
    }
    case FKCONSTR_ACTION_RESTRICT: {
      appendStringInfoString(buf, " RESTRICT");
      break;
    }
    case FKCONSTR_ACTION_SETNULL: {
      appendStringInfoString(buf, " SET NULL");
      break;
    }
    default: {
      elog(ERROR, "support for foreign key action %c is not implemented",
           action);
    }
  }
}

// Deparse constraint
void GetConstraintDef(Constraint* stmt, deparse_context* context) {
  StringInfo buf = context->buf;
  if (stmt->deferrable) {
    elog(ERROR, "support for <DEFERRABLE> constraints is not implemented");
  }
  // It's not possible for constraint to not be deferrable, but have
  // deferred initialization
  if (stmt->initdeferred) {
    elog(ERROR,
         "invalid parse tree: deferred initialization for non-deferrable "
         "constraint");
  }
  if (stmt->skip_validation) {
    elog(ERROR, "support for <NOT VALID> constraints is not implemented");
  }

  if (stmt->conname != NULL && *stmt->conname != '\0') {
    appendStringInfo(buf, "CONSTRAINT %s ", quote_identifier(stmt->conname));
  }

  switch (stmt->contype) {
    case CONSTR_PRIMARY: {
      appendStringInfoString(buf, "PRIMARY KEY");

      if (list_length(stmt->keys) > 0) {
        // Table-level constraint: have to output list of column names
        appendStringInfoString(buf, " (");
        for (ListCell* pk_it = list_head(stmt->keys); pk_it;
             pk_it = lnext(stmt->keys, pk_it)) {
          GetIdentifierValueDef((Value*)lfirst(pk_it), context);

          if (lnext(stmt->keys, pk_it) != NULL) {
            appendStringInfo(buf, ", ");
          }
        }
        appendStringInfoString(buf, ")");
      }

      break;
    }

    case CONSTR_NOTNULL: {
      appendStringInfo(buf, "NOT NULL");
      break;
    }

    case CONSTR_NULL: {
      appendStringInfo(buf, "NULL");
      break;
    }

    case CONSTR_UNIQUE: {
      elog(ERROR, "support for <UNIQUE> constraints is not implemented");
      break;
    }

    case CONSTR_FOREIGN: {
      if (list_length(stmt->fk_attrs) > 0) {
        // Table-level constraint: have to output FOREIGN KEY (columns)
        appendStringInfoString(buf, "FOREIGN KEY (");

        for (ListCell* fk_it = list_head(stmt->fk_attrs); fk_it;
             fk_it = lnext(stmt->fk_attrs, fk_it)) {
          GetIdentifierValueDef((Value*)lfirst(fk_it), context);

          if (lnext(stmt->fk_attrs, fk_it) != NULL) {
            appendStringInfo(buf, ", ");
          }
        }
        appendStringInfoString(buf, ") ");
      }

      appendStringInfoString(buf, "REFERENCES ");
      AppendRangeVarToStringInfo(buf, stmt->pktable);
      appendStringInfoString(buf, "(");

      for (ListCell* pk_it = list_head(stmt->pk_attrs); pk_it;
           pk_it = lnext(stmt->pk_attrs, pk_it)) {
        GetIdentifierValueDef((Value*)lfirst(pk_it), context);

        if (lnext(stmt->pk_attrs, pk_it) != NULL) {
          appendStringInfo(buf, ", ");
        }
      }
      appendStringInfoString(buf, ")");

      switch (stmt->fk_matchtype) {
        case kMatchSimple: {
          // do nothing: MATCH SIMPLE is default
          break;
        }
        case kMatchFull: {
          appendStringInfo(buf, " MATCH FULL");
          break;
        }
        default: {
          elog(ERROR, "support for <MATCH> type %c is not implemented",
               stmt->fk_matchtype);
        }
      }

      if (stmt->fk_upd_action != FKCONSTR_ACTION_NOACTION) {
        appendStringInfo(buf, " ON UPDATE");
        GetForeignKeyAction(stmt->fk_upd_action, context);
      }

      if (stmt->fk_del_action != FKCONSTR_ACTION_NOACTION) {
        appendStringInfo(buf, " ON DELETE");
        GetForeignKeyAction(stmt->fk_del_action, context);
      }
      break;
    }

    default: {
      elog(ERROR, "support for given constraint type is not implemented");
    }
  }
}

// Deparse column definition
void GetColumnDefDef(ColumnDef* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  if (stmt->colname == NULL) {
    elog(ERROR, "invalid parse tree: missing column name");
  }
  if (stmt->typeName == NULL) {
    elog(ERROR, "invalid parse tree: missing column type");
  };
  // unused/unsupported by PostgreSQL
  if (stmt->is_from_type) {
    elog(ERROR, "support for columns from table type is not implemented");
  }
  if (!stmt->is_local) {
    elog(ERROR, "support for non-local column definitions is not implemented");
  }
  if (stmt->collClause != NULL) {
    elog(ERROR,
         "support for <COLLATE> clause in column definition is not "
         "implemented");
  }
  if (stmt->cooked_default != NULL) {
    elog(ERROR,
         "support for <DEFAULT> clause in column definition is not "
         "implemented");
  }
  if (stmt->fdwoptions != NULL) {
    elog(ERROR,
         "support for foreign data wrapper options in column definition is not "
         "implemented");
  }
  if (stmt->identity != 0) {
    elog(ERROR, "support for identity columns is not implemented");
  }

  appendStringInfo(buf, "%s ", quote_identifier(stmt->colname));
  GetTypeNameDef(stmt->typeName, context);

  for (ListCell* constraint_it = list_head(stmt->constraints); constraint_it;
       constraint_it = lnext(stmt->constraints, constraint_it)) {
    Constraint* constraint = lfirst_node(Constraint, constraint_it);
    appendStringInfoString(buf, " ");
    GetConstraintDef(constraint, context);
  }
}

// Deparse ALTER DATABASE SET statements
void GetAlterDatabaseSetStmtDef(AlterDatabaseSetStmt* stmt,
                                deparse_context* context) {
  StringInfo buf = context->buf;

  appendStringInfo(buf, "ALTER DATABASE %s", quote_identifier(stmt->dbname));

  const VariableSetStmt* set_statement = stmt->setstmt;
  switch (set_statement->kind) {
    case VAR_SET_VALUE: {
      appendStringInfo(buf,
                       " SET %s = ", quote_identifier(set_statement->name));
      if (list_length(set_statement->args) != 1) {
        elog(ERROR,
             "support for multiple options is not implemented in <ALTER "
             "DATABASE SET> statement, only one option is supported per "
             "statement");
      }

      A_Const* const_node = castNode(A_Const, linitial(set_statement->args));
      GetValueDef(&const_node->val, context);

      return;
    }

    case VAR_SET_DEFAULT: {
      appendStringInfo(buf, " SET %s TO DEFAULT",
                       quote_identifier(set_statement->name));
      return;
    }

    case VAR_RESET: {
      appendStringInfo(buf, " RESET %s", quote_identifier(set_statement->name));
      return;
    }

    case VAR_SET_CURRENT:
    case VAR_SET_MULTI:
    case VAR_RESET_ALL: {
      elog(ERROR,
           "support for the operation is not implemented in <ALTER DATABASE> "
           "statement");
    }
  }

  elog(ERROR, "invalid enum value in <ALTER DATABASE> statement");
}

// Deparse DROP (TABLE/INDEX/SCHEMA) statements
void GetDropStmtDef(DropStmt* stmt, deparse_context* context) {
  if (stmt->missing_ok) {
    elog(ERROR, "<IF EXISTS> support is not implemented in <DROP> statement");
  }
  if (stmt->concurrent) {
    elog(ERROR,
         "concurrent dropping support is not implemented in <DROP> statement");
  }
  if (stmt->behavior != DROP_RESTRICT) {
    elog(ERROR,
         "<CASCADE> drop behavior support is not implemented in <DROP> "
         "statement");
  }

  StringInfo buf = context->buf;

  switch (stmt->removeType) {
    case OBJECT_TABLE: {
      appendStringInfoString(buf, "DROP TABLE ");
      break;
    }

    case OBJECT_INDEX: {
      appendStringInfoString(buf, "DROP INDEX ");
      break;
    }

    case OBJECT_SCHEMA: {
      appendStringInfoString(buf, "DROP SCHEMA ");
      break;
    }

    default: {
      elog(ERROR,
           "support for object type %u is not implemented in <DROP> statement",
           stmt->removeType);
    }
  }

  if (list_length(stmt->objects) != 1) {
    elog(ERROR,
         "support for multiple identifiers in <DROP> statement is not "
         "implemented");
  }

  List* name_components = castNode(List, linitial(stmt->objects));
  if (list_length(name_components) == 0) {
    elog(ERROR, "missing value");
  }
  if (list_length(name_components) > 2) {
    elog(ERROR,
         "catalog qualifiers support is not implemented in <DROP> statement");
  } else if (list_length(name_components) == 2) {
    GetIdentifierValueDef((Value*)list_nth(name_components, 0), context);
    appendStringInfoString(buf, ".");
    GetIdentifierValueDef((Value*)list_nth(name_components, 1), context);
    return;
  }
  GetIdentifierValueDef((Value*)linitial(name_components), context);
}

void GetAlterTableCmdDropColumn(AlterTableCmd* cmd, deparse_context* context) {
  StringInfo buf = context->buf;

  if (cmd->def != NULL) {
    elog(ERROR,
         "invalid parse tree: new column definition present in <ALTER "
         "TABLE DROP COLUMN> statement");
  }
  if (cmd->num != 0) {
    elog(ERROR,
         "invalid parse tree: columns referenced by number are not "
         "supported in <ALTER TABLE DROP COLUMN> statement");
  }
  if (cmd->name == NULL || *cmd->name == '\0') {
    elog(ERROR, "missing column name in <ALTER TABLE DROP COLUMN> statement");
  }
  if (cmd->missing_ok) {
    elog(ERROR,
         "<IF EXISTS> support is not implemented in <ALTER TABLE DROP "
         "COLUMN> statement");
  }
  if (cmd->behavior != DROP_RESTRICT) {
    elog(ERROR,
         "<CASCADE> drop behavior support is not implemented in <ALTER "
         "TABLE DROP COLUMN> statement");
  }

  appendStringInfo(buf, " DROP COLUMN %s", quote_identifier(cmd->name));
}

void GetAlterTableCmdDropConstraint(AlterTableCmd* cmd,
                                    deparse_context* context) {
  StringInfo buf = context->buf;

  if (cmd->def != NULL) {
    elog(ERROR,
         "invalid parse tree: new column definition present in <ALTER "
         "TABLE DROP CONSTRAINT> statement");
  }
  if (cmd->num != 0) {
    elog(ERROR,
         "invalid parse tree: columns referenced by number are not "
         "supported in <ALTER TABLE DROP CONSTRAINT> statement");
  }
  if (cmd->name == NULL || *cmd->name == '\0') {
    elog(ERROR,
         "missing column name in <ALTER TABLE DROP CONSTRAINT> statement");
  }
  if (cmd->missing_ok) {
    elog(ERROR,
         "<IF EXISTS> support is not implemented in <ALTER TABLE DROP "
         "CONSTRAINT> statement");
  }
  if (cmd->behavior != DROP_RESTRICT) {
    elog(ERROR,
         "<CASCADE> drop behavior support is not implemented in <ALTER "
         "TABLE DROP CONSTRAINT> statement");
  }

  appendStringInfo(buf, " DROP CONSTRAINT %s", quote_identifier(cmd->name));
}

void GetAlterTableCmdAddConstraint(AlterTableCmd* cmd,
                                   deparse_context* context) {
  StringInfo buf = context->buf;

  appendStringInfoString(buf, " ADD ");
  GetConstraintDef(castNode(Constraint, cmd->def), context);
}

void GetAlterTableCmdAddColumn(AlterTableCmd* cmd, deparse_context* context) {
  StringInfo buf = context->buf;

  // <name> is used for operations on existing entities and holds the name
  // of such entity, e.g. column, constraints, etc.
  if (cmd->name != NULL) {
    elog(ERROR, "invalid parse tree: name cannot be set for ADD operations");
  }

  // <num> is a (currently officially unsupported by PostgreSQL) way to
  // reference columns by index as opposed to usual name.
  if (cmd->num != 0) {
    elog(ERROR,
         "invalid parse tree: referencing columns by number is unsupported");
  }

  if (cmd->missing_ok) {
    elog(ERROR,
         "<IF NOT EXISTS> support is not implemented in <ALTER TABLE ADD "
         "COLUMN> statement");
  }

  ColumnDef* column = castNode(ColumnDef, cmd->def);

  appendStringInfoString(buf, " ADD COLUMN ");
  GetColumnDefDef(column, context);
}

void GetAlterTableCmdSetNotNull(AlterTableCmd* cmd, deparse_context* context) {
  elog(ERROR,
       "support for <ALTER COLUMN SET NOT NULL> is not implemented in <ALTER "
       "TABLE> statement");
}

void GetAlterTableCmdDropNotNull(AlterTableCmd* cmd, deparse_context* context) {
  elog(ERROR,
       "support for <ALTER COLUMN DROP NOT NULL> is not implemented in <ALTER "
       "TABLE> statement");
}

void GetAlterTableCmdAlterColumnType(AlterTableCmd* cmd,
                                     deparse_context* context) {
  elog(ERROR,
       "support for <ALTER COLUMN SET DATA TYPE> is not implemented in <ALTER "
       "TABLE> statement");
}

// Deparse ALTER TABLE statements
void GetAlterTableStmtDef(AlterTableStmt* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  if (stmt->objtype != OBJECT_TABLE) {
    elog(ERROR,
         "invalid parse tree: only tables are supported in <ALTER TABLE> "
         "statement");
  }
  if (stmt->missing_ok) {
    elog(ERROR, "<IF EXISTS> support is not implemented in <ALTER TABLE>");
  }

  appendStringInfoString(buf, "ALTER TABLE ");
  AppendRangeVarToStringInfo(buf, stmt->relation);

  if (list_length(stmt->cmds) != 1) {
    elog(ERROR,
         "support for multiple commands in <ALTER TABLE> statement is not "
         "implemented");
  }

  AlterTableCmd* cmd = linitial_node(AlterTableCmd, stmt->cmds);

  switch (cmd->subtype) {
    case AT_DropColumn: {
      GetAlterTableCmdDropColumn(cmd, context);
      break;
    }

    case AT_DropConstraint: {
      GetAlterTableCmdDropConstraint(cmd, context);
      break;
    }

    // TODO: add support for the full ALTER TABLE syntax
    case AT_AddConstraint: {
      GetAlterTableCmdAddConstraint(cmd, context);
      break;
    }

    case AT_AddColumn: {
      GetAlterTableCmdAddColumn(cmd, context);
      break;
    }

    case AT_SetNotNull: {
      GetAlterTableCmdSetNotNull(cmd, context);
      break;
    }

    case AT_DropNotNull: {
      GetAlterTableCmdDropNotNull(cmd, context);
      break;
    }

    case AT_AlterColumnType: {
      GetAlterTableCmdAlterColumnType(cmd, context);
      break;
    }

    default: {
      elog(ERROR,
           "support for operation %u is not implemented in <ALTER TABLE> "
           "statement",
           cmd->subtype);
    }
  }
}

void GetInterleaveIn(InterleaveSpec* interleave_spec, deparse_context* context) {
  StringInfo buf = context->buf;

  switch(interleave_spec->interleavetype) {
    case INTERLEAVE_IN_PARENT: {
      appendStringInfoString(buf, " INTERLEAVE IN PARENT ");
      break;
    }
    case INTERLEAVE_IN: {
      appendStringInfoString(buf, " INTERLEAVE IN ");
      break;
    }
    default: {
      elog(ERROR, "Interleve type should be only INTERELAVE_IN and INTERLEAVE_IN_PARENT");
    }
  }
  appendStringInfoString(buf, quote_identifier(interleave_spec->parent->relname));
}

// Deparse CREATE TABLE statements
void GetCreateStmtDef(CreateStmt* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  if (stmt->if_not_exists) {
    elog(ERROR,
         "support for <IF NOT EXISTS> is not implemented in <CREATE TABLE> "
         "statement");
  }
  if (list_length(stmt->inhRelations) > 0) {
    elog(ERROR,
         "support for <INHERITS> clause is not implemented in <CREATE TABLE> "
         "statement");
  }
  if (stmt->ofTypename != NULL) {
    elog(ERROR,
         "support for <OF type_name> clause is not implemented in <CREATE "
         "TABLE> statement");
  }
  if (stmt->oncommit != ONCOMMIT_NOOP) {
    elog(ERROR,
         "support for <ON COMMIT> clause is not implemented in <CREATE TABLE> "
         "statement");
  }
  if (list_length(stmt->options)) {
    elog(ERROR,
         "support for <WITH> clause is not implemented in <CREATE TABLE> "
         "statement");
  }
  if (stmt->partbound != NULL) {
    elog(ERROR,
         "support for <FOR VALUES> clause is not implemented in <CREATE TABLE> "
         "statement");
  }
  if (stmt->partspec != NULL) {
    elog(ERROR,
         "support for <PARTITION BY> clause is not implemented in <CREATE "
         "TABLE> statement");
  }
  if (stmt->tablespacename != NULL) {
    elog(ERROR,
         "support for <TABLESPACE> clause is not implemented in <CREATE TABLE> "
         "statement");
  }

  appendStringInfoString(buf, "CREATE TABLE ");
  AppendRangeVarToStringInfo(buf, stmt->relation);
  appendStringInfoString(buf, " (");

  // <tableElts> contains a List that can include ColumnDef, Constraint or
  // T_TableLikeClause.
  bool add_comma = false;
  for (ListCell* elt_it = list_head(stmt->tableElts); elt_it;
       elt_it = lnext(stmt->tableElts, elt_it)) {
    Node* elt = (Node*) lfirst(elt_it);

	if (add_comma) {
      appendStringInfoString(buf, ", ");
    }
	add_comma = true;

    // If pretty-printing is enabled, provide line break and indentation for
    // each column/constraint definition
    appendContextKeyword(context, "", 0, 0, 2);
    switch (elt->type) {
      case T_ColumnDef: {
        GetColumnDefDef(castNode(ColumnDef, elt), context);
        break;
      }

      case T_Constraint: {
        GetConstraintDef(castNode(Constraint, elt), context);
        break;
      }

      case T_TableLikeClause: {
        elog(ERROR,
             "support for <LIKE> clause is not implemented in <CREATE TABLE> "
             "statement");
      }

      default: {
        elog(ERROR, "unkown clause in <CREATE TABLE> statement: %d", elt->type);
      }
    }

  }

  // TODO: constraints field is populated with all the CHECK
  // constraints by the analyzer. Figure out if we need to process it, and if so
  // - add a test running parse tree through analyzer before deparser.
  if (list_length(stmt->constraints) > 0) {
    elog(ERROR, "unexpected constraint node");
  }

  appendContextKeyword(context, ")", 0, 0, 0);
  if (stmt->interleavespec != NULL) {
    GetInterleaveIn(stmt->interleavespec, context);
  }
}

// Deparse CREATE SCHEMA statements
void GetCreateSchemaStmtDef(CreateSchemaStmt* stmt, deparse_context* context) {
  StringInfo buf = context->buf;

  if (stmt->if_not_exists) {
    elog(ERROR,
         "support for <IF NOT EXISTS> is not implemented in <CREATE SCHEMA> "
         "statement");
  }
  if (stmt->authrole != NULL) {
    elog(ERROR,
         "support for <TABLESPACE> clause is not implemented in <CREATE TABLE> "
         "statement");
  }
  if (list_length(stmt->schemaElts) > 0) {
    elog(ERROR,
         "support for <INHERITS> clause is not implemented in <CREATE TABLE> "
         "statement");
  }

  appendStringInfo(buf, "CREATE SCHEMA %s", stmt->schemaname);
}

// Shimmed version of the Postgres get_utility_query_def function
// Based on the utility statement type, delegates the deparsing to the
// corresponding handler.
// TODO: use ereport instead of elog for error reporting
void get_utility_query_def(Query* query, deparse_context* context) {
  Node* stmt = query->utilityStmt;
  if (!stmt) {
    elog(ERROR, "missing utility statement");
  }

  // TODO: we should support pretty-printing for large statements,
  // e.g. multiline, indents, etc.
  switch (stmt->type) {
    case T_NotifyStmt: {
      return GetNotifyStmtDef(castNode(NotifyStmt, stmt), context);
    }

    case T_CreatedbStmt: {
      return GetCreatedbStmtDef(castNode(CreatedbStmt, stmt), context);
    }

    case T_AlterDatabaseSetStmt: {
      return GetAlterDatabaseSetStmtDef(castNode(AlterDatabaseSetStmt, stmt),
                                        context);
    }

    case T_DropStmt: {
      return GetDropStmtDef(castNode(DropStmt, stmt), context);
    }

    case T_IndexStmt: {
      return GetIndexStmtDef(castNode(IndexStmt, stmt), context);
    }

    case T_AlterTableStmt: {
      return GetAlterTableStmtDef(castNode(AlterTableStmt, stmt), context);
    }

    case T_CreateStmt: {
      return GetCreateStmtDef(castNode(CreateStmt, stmt), context);
    }

    case T_CreateSchemaStmt: {
      return GetCreateSchemaStmtDef(castNode(CreateSchemaStmt, stmt), context);
    }

    default: {
      elog(ERROR, "unexpected utility statement type %s",
           CNodeTagToNodeString(query->utilityStmt->type));
    }
  }
}

// We made these functions non-static to call from here. Forward declare
// them to limit the scope.
void get_from_clause(Query *query, const char *prefix,
					 deparse_context *context);
void get_rule_expr(Node *node, deparse_context *context,
				   bool showimplicit);
void get_target_list(List *targetList, deparse_context *context,
					 TupleDesc resultDesc, bool colNamesVisible);
void get_update_query_targetlist_def(Query *query, List *targetList,
									 deparse_context *context,
									 RangeTblEntry *rte);
void get_with_clause(Query *query, deparse_context *context);


void get_delete_query_def(Query *query, deparse_context *context, bool colNamesVisible)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *rte;

	/* Insert the WITH clause if given */
	get_with_clause(query, context);

	/*
	 * Start the query with DELETE FROM relname
	 */
	rte = rt_fetch(query->resultRelation, query->rtable);
	Assert(rte->rtekind == RTE_RELATION);
	if (POSTGRES_PRETTY_INDENT(context))
	{
		appendStringInfoChar(buf, ' ');
		context->indentLevel += POSTGRES_PRETTYINDENT_STD;
	}
	appendStringInfo(buf, "DELETE FROM %s%s",
					 only_marker(rte),
					 generate_relation_name(rte->relid, NIL));
  if (rte->alias != NULL)
		appendStringInfo(buf, " %s",
						 quote_identifier(rte->alias->aliasname));

	// Mark the result relation as not inFromCl so that it will not be included
	// in the USING clause.
	bool original_inFromCl = rte->inFromCl;
	rte->inFromCl = false;

	/* Add the USING clause if given */
	get_from_clause(query, " USING ", context);

	// Change the rte inFromCl back to the original value.
	rte->inFromCl = original_inFromCl;

	/* Add a WHERE clause if given */
	if (query->jointree->quals != NULL)
	{
		appendContextKeyword(context, " WHERE ",
							 -POSTGRES_PRETTYINDENT_STD, POSTGRES_PRETTYINDENT_STD, 1);
		get_rule_expr(query->jointree->quals, context, false);
	}

	/* Add RETURNING if present */
	if (query->returningList)
	{
		appendContextKeyword(context, " RETURNING",
							 -POSTGRES_PRETTYINDENT_STD, POSTGRES_PRETTYINDENT_STD, 1);
		get_target_list(query->returningList, context, NULL, colNamesVisible);
	}
}

void get_update_query_def(Query *query, deparse_context *context,
                          bool colNamesVisible)
{
	StringInfo	buf = context->buf;
	RangeTblEntry *rte;

	/* Insert the WITH clause if given */
	get_with_clause(query, context);

	/*
	 * Start the query with UPDATE relname SET
	 */
	rte = rt_fetch(query->resultRelation, query->rtable);
	Assert(rte->rtekind == RTE_RELATION);
	if (POSTGRES_PRETTY_INDENT(context))
	{
		appendStringInfoChar(buf, ' ');
		context->indentLevel += POSTGRES_PRETTYINDENT_STD;
	}
	appendStringInfo(buf, "UPDATE %s%s",
					 only_marker(rte),
					 generate_relation_name(rte->relid, NIL));
	if (rte->alias != NULL)
		appendStringInfo(buf, " %s",
						 quote_identifier(rte->alias->aliasname));
	appendStringInfoString(buf, " SET ");

	/* Deparse targetlist */
	get_update_query_targetlist_def(query, query->targetList, context, rte);

	// Mark the result relation as not inFromCl so that it will not be included
	// in the FROM clause.
	bool original_inFromCl = rte->inFromCl;
	rte->inFromCl = false;

	/* Add the FROM clause if needed */
	get_from_clause(query, " FROM ", context);

	// Change the rte inFromCl back to the original value.
	rte->inFromCl = original_inFromCl;

	/* Add a WHERE clause if given */
	if (query->jointree->quals != NULL)
	{
		appendContextKeyword(context, " WHERE ",
							 -POSTGRES_PRETTYINDENT_STD, POSTGRES_PRETTYINDENT_STD, 1);
		get_rule_expr(query->jointree->quals, context, false);
	}

	/* Add RETURNING if present */
	if (query->returningList)
	{
		appendContextKeyword(context, " RETURNING",
							 -POSTGRES_PRETTYINDENT_STD, POSTGRES_PRETTYINDENT_STD, 1);
		get_target_list(query->returningList, context, NULL, colNamesVisible);
	}
}
