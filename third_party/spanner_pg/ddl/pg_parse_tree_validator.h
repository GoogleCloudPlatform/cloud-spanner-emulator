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

#ifndef DDL_PG_PARSE_TREE_VALIDATOR_H_
#define DDL_PG_PARSE_TREE_VALIDATOR_H_

#include "absl/status/status.h"
#include "third_party/spanner_pg/ddl/ddl_translator.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"

namespace postgres_translator {
namespace spangres {

absl::Status inline UnsupportedTranslationError(
    absl::string_view error_message) {
  return absl::FailedPreconditionError(error_message);
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

// Validations methods to check supported parse tree functionatiliy, one per PG
// node type. Every node type we process should have separate method here. Each
// ValidateParseTreeNode method should have:
// - AssertPGNodeConsistsOf construct listing all the fields in the same order
//   as defined in parsenodes.h
// - in the order fields are listed in AssertPGNodeConsistsOf, for each field,
//   one of:
//   - ZETASQL_RET_CHECK validation if presence of the field breaks the invariant
//   - Validate and return UnsupportedTranslationError if field is misused
//   - Comment explaining where and how we validate the field if more validation
//     is required.
absl::Status ValidateParseTreeNode(const CreatedbStmt& node);
absl::Status ValidateParseTreeNode(const AlterDatabaseSetStmt& node);
absl::Status ValidateParseTreeNode(const VariableSetStmt& node,
                                   absl::string_view parent_statement_type);
absl::Status ValidateParseTreeNode(const RangeVar& node,
                                   absl::string_view parent_statement_type);
absl::Status ValidateParseTreeNode(const InterleaveSpec& node,
                                   absl::string_view parent_statement_type);
absl::Status ValidateParseTreeNode(const Constraint& node, bool add_in_alter);
absl::Status ValidateParseTreeNode(const AlterTableStmt& node,
                                   const TranslationOptions& options);
absl::Status ValidateParseTreeNode(const AlterTableCmd& node,
                                   const ObjectType object_type,
                                   const TranslationOptions& options);
absl::Status ValidateParseTreeNode(const Ttl& node);
absl::Status ValidateParseTreeNode(const DropStmt& node,
                                   const TranslationOptions& options);
absl::Status ValidateParseTreeNode(const TypeName& node);
// ColumnDef is used in two distinct contexts: in CREATE TABLE and in ALTER
// TABLE ALTER COLUMN. The latter uses only a subset of ColumnDef fields, so
// validation rules there are different. <alter_column> indicates if
// validation is performed for ALTER TABLE ALTER COLUMN statement.
absl::Status ValidateParseTreeNode(const ColumnDef& node, bool alter_column);
absl::Status ValidateParseTreeNode(const CreateStmt& node,
                                   const TranslationOptions& options);
absl::Status ValidateParseTreeNode(const CreateSchemaStmt& node);
absl::Status ValidateParseTreeNode(const AlterSpangresStatsStmt& node);
absl::Status ValidateParseTreeNode(const VacuumStmt& node);
absl::Status ValidateParseTreeNode(const ViewStmt& node);
absl::Status ValidateParseTreeNode(const CreateChangeStreamStmt& node);
absl::Status ValidateParseTreeNode(const AlterChangeStreamStmt& node);
absl::Status ValidateParseTreeNode(const CreateRoleStmt& node);
absl::Status ValidateParseTreeNode(const DropRoleStmt& node);
absl::Status ValidateParseTreeNode(const GrantStmt& node);
absl::Status ValidateParseTreeNode(const AccessPriv& node, bool is_grant,
                                   bool is_privilege);
absl::Status ValidateParseTreeNode(const ObjectWithArgs& node, bool is_grant);
absl::Status ValidateParseTreeNode(const RoleSpec& node);
absl::Status ValidateParseTreeNode(const GrantRoleStmt& node);
absl::Status ValidateParseTreeNode(const IndexStmt& node,
                                   const TranslationOptions& options);
absl::Status ValidateParseTreeNode(const IndexElem& node);
absl::Status ValidateParseTreeNode(const BoolExpr& node);
absl::Status ValidateParseTreeNode(const NullTest& node);
absl::Status ValidateParseTreeNode(const CreateSeqStmt& node,
                                   const TranslationOptions& options);
absl::Status ValidateParseTreeNode(const AlterSeqStmt& node,
                                   const TranslationOptions& options);

}  // namespace spangres
}  // namespace postgres_translator

#endif  // DDL_PG_PARSE_TREE_VALIDATOR_H_
