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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator::test {
namespace {

// Matches against a PostgreSQL Node* instance (or compatible struct).
// Matches if that struct can correctly be round-trip'ed through the
// serializer:  If it is repeatedly serialized and deserialized,
// the result always has the same value.
// Note that the struct must implement equal() as well, so that we can
// compare its value.  (Most do.)
// Note that this matcher depends on the CurrentMemoryContext thread-local
// variable.  The 'ValidMemoryContext' fixture (below) initializes it correctly.
MATCHER(CanSerializeAndDeserialize, "") {
  // `arg` is intended to be a Node* or a "subclass" of Node*.  Cast it
  // to a Node* for clarity.
  // The Node struct hierarchy is written in C (not C++) so it implements
  // logical subclassing without compiler support.
  // So a reinterpret cast is required in C++ to cast it to the base class.
  Node* node = reinterpret_cast<Node*>(arg);

  // When adding support for a new node type, need to support serializing
  // via nodeToString()
  char* parse_tree_str = nullptr;
  {
    absl::StatusOr<char*> status = CheckedPgNodeToString(node);
    if (!status.ok()) {
      *result_listener << "Failed to parse: " << status.status().message();
      return false;
    }
    parse_tree_str = *status;
  }

  if (parse_tree_str == nullptr) {
    *result_listener << "Couldn't serialize node";
    return false;
  }

  Node* deserialized_node = nullptr;
  {
    // When adding support for a new node type, need to support deserializing
    // via stringToNode() (usually using the Checked* wrapper to handle errors)
    absl::StatusOr<void*> status = CheckedPgStringToNode(parse_tree_str);
    if (!status.ok()) {
      // Couldn't deserialize
      *result_listener << "Failed to deserialize: "
                       << status.status().message();
      return false;
    }
    // The return value of `CheckedPgStringToNode` is guaranteed to be safely
    // castable to `Node*`.  (It would be "a subclass of Node*", but C doesn't
    // support subclasses, so we have to cast a void*.)
    deserialized_node = reinterpret_cast<Node*>(*status);
  }

  // If you're implementing a new type, note that you might have to add
  // support in 'equal()' as well.
  // Double-check the 'equal()' call by doing another round of
  // serialization and string-comparing the result.
  char* reparse_tree_str = nullptr;
  {
    absl::StatusOr<char*> status = CheckedPgNodeToString(node);
    if (!status.ok()) {
      *result_listener << "Failed to re-parse deparsed output:  "
                       << status.status().message();
      return false;
    }
    reparse_tree_str = *status;
  }

  bool nodes_are_equal = false;
  {
    absl::StatusOr<bool> status = CheckedPgEqual(node, deserialized_node);
    if (!status.ok()) {
      *result_listener << "Failed to compare against round-tripped node: "
                       << status.status().message();
      return false;
    }
    nodes_are_equal = *status;
  }

  if (!nodes_are_equal || absl::string_view(parse_tree_str) !=
                              absl::string_view(reparse_tree_str)) {
    // In case of failure, print the serialized strings for debugging
    *result_listener
        << parse_tree_str
        << " after first serialization, deserializes and re-serializes as "
        << reparse_tree_str;
    return false;
  }

  // It worked!
  *result_listener << "Serialized successfully to " << parse_tree_str;
  return true;
}

// Return a Node.
// The value of this Node is unspecified.
// Intended for testing,
// where you need a placeholder Node value and you don't care what it is.
Node* PlaceHolderNode() {
  A_Const* a_const = makeNode(A_Const);
  a_const->val.sval = *makeString(pstrdup("PlaceHolder Test Node"));
  a_const->location = 1;

  // If C supported inheritance, Node would be a base class for A_Const,
  // so this wouldn't require a cast.
  // Unfortunately, it doesn't, so it does.
  return reinterpret_cast<Node*>(a_const);
}

using SerializationDeserializationTest = ValidMemoryContext;

TEST_F(SerializationDeserializationTest, RawStmt) {
  // 'makeRawStmt()' is declared static in PG so we can't call it here.
  // Do something equivalent ourselves.
  RawStmt* raw_stmt = makeNode(RawStmt);
  raw_stmt->stmt = reinterpret_cast<Node*>(makeNode(SelectStmt));
  // Locations and other constants are bogus.
  // Just make up something fun that the serializer will have to propagate.
  raw_stmt->stmt_location = 42;
  raw_stmt->stmt_len = 9000;
  raw_stmt->statementHints = list_make2(PlaceHolderNode(), PlaceHolderNode());
  EXPECT_THAT(raw_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ResTarget) {
  // ResTarget doesn't have a `makeResTarget()` constructor.
  // Do something equivalent ourselves.
  ResTarget* res_target = makeNode(ResTarget);
  res_target->name = pstrdup("Test ResTarget");
  res_target->indirection = list_make1(PlaceHolderNode());
  res_target->val = PlaceHolderNode();
  res_target->location = 384;
  EXPECT_THAT(res_target, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstNull) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->isnull = true;
  a_const->location = 4;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstInteger) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.ival = *makeInteger(1);
  a_const->location = 3;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstNegativeInteger) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.ival = *makeInteger(-1);
  a_const->location = 5;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstFloat) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.fval = *makeFloat(pstrdup("123.456"));
  a_const->location = 6;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstBitString) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.bsval = *makeBitString(pstrdup("b1010"));
  a_const->location = 7;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstString) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.sval = *makeString(pstrdup("Hello World"));
  a_const->location = 8;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstEmptyString) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.sval = *makeString(pstrdup(""));
  a_const->location = 8;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AConstStringWithSpecialChars) {
  // 'makeAConst()' and friends are declared static so we can't call them here.
  // Do something equivalent ourselves.
  A_Const* a_const = makeNode(A_Const);
  a_const->val.sval = *makeString(pstrdup("\n\\n🙂"));
  a_const->location = 8;
  EXPECT_THAT(a_const, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, TypeCast) {
  // 'makeTypeCast()' is declared static in PG so we can't call it here.
  // Do something equivalent ourselves.
  TypeCast* type_cast = makeNode(TypeCast);
  type_cast->arg = PlaceHolderNode();
  type_cast->typeName = makeTypeName(pstrdup("TestType"));
  type_cast->location = 17;
  EXPECT_THAT(type_cast, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, TypeName) {
  EXPECT_THAT(makeTypeName(pstrdup("TestType")), CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, FuncCall) {
  FuncCall* func_call = makeFuncCall(SystemFuncName(pstrdup("TestFunction")),
                                     list_make1(PlaceHolderNode()),
                                     COERCE_EXPLICIT_CALL, /*location=*/5);

  func_call->agg_order = list_make1(PlaceHolderNode());
  func_call->agg_filter = PlaceHolderNode();
  func_call->agg_within_group = true;
  func_call->agg_star = true;
  func_call->agg_distinct = true;
  func_call->func_variadic = true;
  // TODO:  Whenever we add support for WindowDef, test one here.
  func_call->over = nullptr;
  func_call->functionHints = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(func_call, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ColumnRef) {
  // 'makeColumnRef()' is declared static in PG so we can't call it here.
  // Do something equivalent ourselves.
  ColumnRef* column_ref = makeNode(ColumnRef);
  column_ref->fields = list_make1(PlaceHolderNode());
  column_ref->location = 12;
  EXPECT_THAT(column_ref, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, A_Star) {
  // A_Star has no fields, so there is no 'makeAStar()' to call.
  EXPECT_THAT(makeNode(A_Star), CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, A_ArrayExpr) {
  // 'makeAArrayExpr()' is declared static in PG so we can't call it here.
  // Do something equivalent ourselves.
  A_ArrayExpr* a_array_expr_node = makeNode(A_ArrayExpr);
  a_array_expr_node->elements = list_make2_int(16, 17);  // PlaceHolder values.
  a_array_expr_node->location = 18;                      // PlaceHolder value.

  EXPECT_THAT(a_array_expr_node, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, RangeSubselect) {
  // There is no 'makeRangeSubselect()' to call. Parser also builds it this way.
  RangeSubselect* range_subselect_node = makeNode(RangeSubselect);
  range_subselect_node->lateral = false;
  range_subselect_node->subquery = PlaceHolderNode();
  range_subselect_node->alias = makeAlias(
      "alias_name",
      list_make2(makeString(pstrdup("col0")), makeString(pstrdup("col1"))));

  EXPECT_THAT(range_subselect_node, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CreateStmt) {
  CreateStmt* create_stmt = makeNode(CreateStmt);
  create_stmt->relation = makeNode(RangeVar);
  create_stmt->tableElts = list_make1(PlaceHolderNode());
  create_stmt->inhRelations = list_make1(PlaceHolderNode());
  create_stmt->partbound = makeNode(PartitionBoundSpec);
  // TODO:  Whenever we support PartitionSpec, test one here
  create_stmt->partspec = nullptr;
  create_stmt->ofTypename = makeNode(TypeName);
  create_stmt->constraints = list_make1(PlaceHolderNode());
  create_stmt->options = list_make1(PlaceHolderNode());
  create_stmt->oncommit = ONCOMMIT_PRESERVE_ROWS;
  create_stmt->tablespacename = pstrdup("test_tablespace");
  create_stmt->if_not_exists = true;
  create_stmt->ttl = makeNode(Ttl);
  create_stmt->ttl->name = pstrdup("some_column");

  EXPECT_THAT(create_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ColumnDef) {
  ColumnDef* column_def = makeColumnDef("test_column", VARCHAROID, 10, 100);
  column_def->inhcount = 1;
  column_def->is_local = true;
  column_def->is_not_null = true;
  column_def->is_from_type = true;
  column_def->storage = 'a';  // make up a storage for this test
  column_def->raw_default = PlaceHolderNode();
  column_def->cooked_default = PlaceHolderNode();
  column_def->identity = 'a';  // make up an identity for this test
  column_def->identitySequence =
      makeRangeVar(pstrdup("test_schema"), pstrdup("test_sequence"), 2);
  // TODO:  Whenever we support CollateClause, test one here
  column_def->collClause = nullptr;
  column_def->constraints = list_make1(PlaceHolderNode());
  column_def->fdwoptions = list_make1(PlaceHolderNode());
  column_def->location = 3;

  EXPECT_THAT(column_def, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, Constraint) {
  Constraint* constraint = makeNode(Constraint);
  constraint->access_method = pstrdup("test_access_method");
  constraint->conname = pstrdup("conname");
  constraint->contype = CONSTR_PRIMARY;
  constraint->cooked_expr = pstrdup("test_cooked_expr");
  constraint->deferrable = true;
  constraint->exclusions = list_make1(PlaceHolderNode());
  constraint->fk_attrs = list_make1(PlaceHolderNode());
  constraint->fk_del_action = 'q';
  constraint->fk_matchtype = 'z';
  constraint->fk_upd_action = 'w';
  constraint->generated_when = 'y';
  constraint->indexname = pstrdup("test_index");
  constraint->indexspace = pstrdup("test_indexspace");
  constraint->initdeferred = true;
  constraint->initially_valid = true;
  constraint->is_no_inherit = true;
  constraint->keys = list_make1(PlaceHolderNode());
  constraint->location = 99;
  constraint->old_conpfeqop = list_make1(PlaceHolderNode());
  constraint->old_pktable_oid = 123;
  constraint->options = list_make1(PlaceHolderNode());
  constraint->pk_attrs = list_make1(PlaceHolderNode());
  constraint->pktable =
      makeRangeVar(pstrdup("test_schema"), pstrdup("test_table"), 98);
  constraint->raw_expr = PlaceHolderNode();
  constraint->skip_validation = true;
  constraint->stored_kind = GENERATED_COL_NON_STORED;
  constraint->vector_length = 321;
  constraint->where_clause = PlaceHolderNode();

  EXPECT_THAT(constraint, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CreatedbStmt) {
  CreatedbStmt* createdb_stmt = makeNode(CreatedbStmt);
  createdb_stmt->dbname = pstrdup("test_db");
  createdb_stmt->options = list_make1(PlaceHolderNode());

  EXPECT_THAT(createdb_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, DropStmt) {
  DropStmt* drop_stmt = makeNode(DropStmt);
  drop_stmt->behavior = DROP_CASCADE;
  drop_stmt->concurrent = true;
  drop_stmt->missing_ok = true;
  drop_stmt->objects = list_make1(PlaceHolderNode());
  drop_stmt->removeType = OBJECT_INDEX;

  EXPECT_THAT(drop_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, PartitionSpec) {
  PartitionSpec* partition_spec = makeNode(PartitionSpec);
  partition_spec->location = 99;
  partition_spec->partParams = list_make1(PlaceHolderNode());
  partition_spec->strategy = pstrdup("test_strategy");

  EXPECT_THAT(partition_spec, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, PartitionElem) {
  PartitionElem* partition_elem = makeNode(PartitionElem);
  partition_elem->collation = list_make1(PlaceHolderNode());
  partition_elem->expr = PlaceHolderNode();
  partition_elem->location = 99;
  partition_elem->name = pstrdup("test_partitionelem");
  partition_elem->opclass = list_make1(PlaceHolderNode());

  EXPECT_THAT(partition_elem, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, TableLikeClause) {
  TableLikeClause* table_like_clause = makeNode(TableLikeClause);
  table_like_clause->options = 123;
  table_like_clause->relation =
      makeRangeVar(pstrdup("test_schema"), pstrdup("test_table"), 99);

  EXPECT_THAT(table_like_clause, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CollateClause) {
  CollateClause* collate_clause = makeNode(CollateClause);
  collate_clause->collname = list_make1(PlaceHolderNode());
  collate_clause->arg = PlaceHolderNode();
  collate_clause->location = 99;

  EXPECT_THAT(collate_clause, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, DefElem) {
  DefElem* option = makeNode(DefElem);
  option->arg = PlaceHolderNode();
  option->defaction = DEFELEM_SET;
  option->defname = pstrdup("test_option");
  option->defnamespace = pstrdup("test_option_namespace");
  option->location = 99;

  EXPECT_THAT(option, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterDatabaseSetStmt) {
  AlterDatabaseSetStmt* alterdb_stmt = makeNode(AlterDatabaseSetStmt);

  alterdb_stmt->dbname = pstrdup("test_db");
  alterdb_stmt->setstmt = makeNode(VariableSetStmt);

  EXPECT_THAT(alterdb_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, VariableSetStmt) {
  VariableSetStmt* var_stmt = makeNode(VariableSetStmt);

  var_stmt->args = list_make1(PlaceHolderNode());
  var_stmt->is_local = true;
  var_stmt->kind = VAR_SET_VALUE;
  var_stmt->name = pstrdup("test_var");

  EXPECT_THAT(var_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, InsertStmt) {
  InsertStmt* insert_stmt = makeNode(InsertStmt);

  insert_stmt->relation = makeNode(RangeVar);
  insert_stmt->cols = list_make1(PlaceHolderNode());
  insert_stmt->selectStmt = PlaceHolderNode();
  insert_stmt->onConflictClause = makeNode(OnConflictClause);
  insert_stmt->returningList = list_make1(PlaceHolderNode());
  insert_stmt->withClause = makeNode(WithClause);
  insert_stmt->override = OVERRIDING_USER_VALUE;

  EXPECT_THAT(insert_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, OnConflictClause) {
  OnConflictClause* on_conflict_clause = makeNode(OnConflictClause);

  on_conflict_clause->action = ONCONFLICT_UPDATE;
  on_conflict_clause->infer = makeNode(InferClause);
  on_conflict_clause->targetList = list_make1(PlaceHolderNode());
  on_conflict_clause->whereClause = PlaceHolderNode();
  on_conflict_clause->location = 123;

  EXPECT_THAT(on_conflict_clause, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, InferClause) {
  InferClause* infer_clause = makeNode(InferClause);

  infer_clause->indexElems = list_make1(PlaceHolderNode());
  infer_clause->whereClause = PlaceHolderNode();
  infer_clause->conname = pstrdup("test_conname");
  infer_clause->location = 789;

  EXPECT_THAT(infer_clause, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, WithClause) {
  WithClause* with_clause = makeNode(WithClause);

  with_clause->ctes = list_make1(PlaceHolderNode());
  with_clause->recursive = true;
  with_clause->location = 456;

  EXPECT_THAT(with_clause, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, IndexElem) {
  IndexElem* index_elem = makeNode(IndexElem);

  index_elem->name = pstrdup("index_elem_name");
  index_elem->expr = PlaceHolderNode();
  index_elem->indexcolname = pstrdup("index_col_name");
  index_elem->collation = list_make1(PlaceHolderNode());
  index_elem->opclass = list_make1(PlaceHolderNode());
  index_elem->ordering = SORTBY_DEFAULT;
  index_elem->nulls_ordering = SORTBY_NULLS_DEFAULT;

  EXPECT_THAT(index_elem, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, UpdateStmt) {
  UpdateStmt* update_stmt = makeNode(UpdateStmt);

  update_stmt->relation = makeNode(RangeVar);
  update_stmt->targetList = list_make1(PlaceHolderNode());
  update_stmt->whereClause = PlaceHolderNode();
  update_stmt->fromClause = list_make1(PlaceHolderNode());
  update_stmt->returningList = list_make1(PlaceHolderNode());
  update_stmt->withClause = makeNode(WithClause);

  EXPECT_THAT(update_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, MultiAssignRef) {
  MultiAssignRef* multi_assign_ref = makeNode(MultiAssignRef);

  multi_assign_ref->source = PlaceHolderNode();
  multi_assign_ref->colno = 1;
  multi_assign_ref->ncolumns = 10;

  EXPECT_THAT(multi_assign_ref, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, DeleteStmt) {
  DeleteStmt* delete_stmt = makeNode(DeleteStmt);

  delete_stmt->relation = makeNode(RangeVar);
  delete_stmt->usingClause = list_make1(PlaceHolderNode());
  delete_stmt->whereClause = PlaceHolderNode();
  delete_stmt->returningList = list_make1(PlaceHolderNode());
  delete_stmt->withClause = makeNode(WithClause);

  EXPECT_THAT(delete_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, IndexStmt) {
  IndexStmt* index_stmt = makeNode(IndexStmt);

  index_stmt->idxname = pstrdup("index_name");
  index_stmt->relation = makeNode(RangeVar);
  index_stmt->accessMethod = pstrdup("btree");
  index_stmt->tableSpace = pstrdup("placeholder_tablespace");
  index_stmt->indexParams = list_make1(PlaceHolderNode());
  index_stmt->indexIncludingParams = list_make1(PlaceHolderNode());
  index_stmt->options = list_make1(PlaceHolderNode());
  index_stmt->whereClause = PlaceHolderNode();
  index_stmt->excludeOpNames = list_make1(PlaceHolderNode());
  index_stmt->idxcomment = pstrdup("placeholder comment");
  index_stmt->indexOid = 124;
  index_stmt->oldNode = 125;
  index_stmt->unique = true;
  index_stmt->nulls_not_distinct = true;
  index_stmt->primary = true;
  index_stmt->isconstraint = true;
  index_stmt->deferrable = true;
  index_stmt->initdeferred = true;
  index_stmt->transformed = true;
  index_stmt->concurrent = true;
  index_stmt->if_not_exists = true;

  EXPECT_THAT(index_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, RoleSpec) {
  RoleSpec* role_spec = makeNode(RoleSpec);

  role_spec->roletype = ROLESPEC_SESSION_USER;
  role_spec->rolename = pstrdup("role_name");
  role_spec->location = 123;

  EXPECT_THAT(role_spec, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterTableCmd) {
  AlterTableCmd* alter_table_cmd = makeNode(AlterTableCmd);

  alter_table_cmd->subtype = AT_DropColumn;
  alter_table_cmd->name = pstrdup("alter_table_cmd_name");
  alter_table_cmd->num = 123;
  alter_table_cmd->newowner = makeNode(RoleSpec);
  alter_table_cmd->def = PlaceHolderNode();
  alter_table_cmd->behavior = DROP_RESTRICT;
  alter_table_cmd->missing_ok = true;

  EXPECT_THAT(alter_table_cmd, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterTableCmdColumnDefault) {
  AlterTableCmd* alter_table_cmd = makeNode(AlterTableCmd);

  alter_table_cmd->subtype = AT_ColumnDefault;
  alter_table_cmd->name = pstrdup("alter_table_cmd_name");
  alter_table_cmd->def = PlaceHolderNode();
  alter_table_cmd->raw_expr_string = pstrdup("k > 0");

  EXPECT_THAT(alter_table_cmd, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterTableStmt) {
  AlterTableStmt* alter_table_stmt = makeNode(AlterTableStmt);

  alter_table_stmt->relation =
      makeRangeVar(pstrdup("test_schema"), pstrdup("test_sequence"), 123);
  alter_table_stmt->cmds = list_make1(PlaceHolderNode());
  alter_table_stmt->objtype = OBJECT_TABLE;
  alter_table_stmt->missing_ok = true;

  EXPECT_THAT(alter_table_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, WindowDef) {
  WindowDef* window_def = makeNode(WindowDef);

  window_def->name = pstrdup("window_name");
  window_def->refname = pstrdup("window_ref_name");
  window_def->partitionClause = list_make1(PlaceHolderNode());
  window_def->orderClause = list_make1(PlaceHolderNode());
  window_def->frameOptions = 123;
  window_def->startOffset = PlaceHolderNode();
  window_def->endOffset = PlaceHolderNode();
  window_def->location = 124;

  EXPECT_THAT(window_def, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, SortBy) {
  SortBy* sort_by = makeNode(SortBy);

  sort_by->node = PlaceHolderNode();
  sort_by->sortby_dir = SORTBY_ASC;
  sort_by->sortby_nulls = SORTBY_NULLS_FIRST;
  sort_by->useOp = list_make1(PlaceHolderNode());
  sort_by->location = 123;

  EXPECT_THAT(sort_by, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, PrepareStmt) {
  PrepareStmt* prepare_stmt = makeNode(PrepareStmt);

  prepare_stmt->name = pstrdup("prepare_stmt_name");
  prepare_stmt->argtypes = list_make1(PlaceHolderNode());
  prepare_stmt->query = PlaceHolderNode();

  EXPECT_THAT(prepare_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ExecuteStmt) {
  ExecuteStmt* execute_stmt = makeNode(ExecuteStmt);

  execute_stmt->name = pstrdup("execute_stmt_name");
  execute_stmt->params = list_make1(PlaceHolderNode());

  EXPECT_THAT(execute_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ParamRef) {
  ParamRef* param_ref = makeNode(ParamRef);

  param_ref->number = 1;
  param_ref->location = -1;

  EXPECT_THAT(param_ref, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AExpr) {
  // The full list of A_Expr_Kind copied from parsenodes.h
  std::vector<A_Expr_Kind> all_a_expr_kinds = {
      AEXPR_OP,               // normal operator
      AEXPR_OP_ANY,           // scalar op ANY (array)
      AEXPR_OP_ALL,           // scalar op ALL (array)
      AEXPR_DISTINCT,         // IS DISTINCT FROM - name must be "="
      AEXPR_NOT_DISTINCT,     // IS NOT DISTINCT FROM - name must be "="
      AEXPR_NULLIF,           // NULLIF - name must be "="
      AEXPR_IN,               // [NOT] IN - name must be "=" or "<>"
      AEXPR_LIKE,             // [NOT] LIKE - name must be "~~" or "!~~"
      AEXPR_ILIKE,            // [NOT] ILIKE - name must be "~~*" or "!~~*"
      AEXPR_SIMILAR,          // [NOT] SIMILAR - name must be "~" or "!~"
      AEXPR_BETWEEN,          // name must be "BETWEEN"
      AEXPR_NOT_BETWEEN,      // name must be "NOT BETWEEN"
      AEXPR_BETWEEN_SYM,      // name must be "BETWEEN SYMMETRIC"
      AEXPR_NOT_BETWEEN_SYM,  // name must be "NOT BETWEEN SYMMETRIC"
  };
  for (A_Expr_Kind kind : all_a_expr_kinds) {
    A_Expr* a_expr = makeNode(A_Expr);
    a_expr->kind = kind;
    a_expr->name = list_make1(PlaceHolderNode());
    a_expr->lexpr = PlaceHolderNode();
    a_expr->rexpr = PlaceHolderNode();
    a_expr->location = -1;

    EXPECT_THAT(a_expr, CanSerializeAndDeserialize());
  }
}

TEST_F(SerializationDeserializationTest, InterleaveSpec) {
  std::vector<InterleaveInType> all_interleave_kinds = {
      INTERLEAVE_IN,
      INTERLEAVE_IN_PARENT
  };
  for (InterleaveInType type : all_interleave_kinds) {
    InterleaveSpec* interleave_spec = makeNode(InterleaveSpec);
    interleave_spec->interleavetype = type;
    interleave_spec->parent =
        makeRangeVar(pstrdup("schemaname"), pstrdup("tablename"), -1);
    interleave_spec->location = -1;

    EXPECT_THAT(interleave_spec, CanSerializeAndDeserialize());
  }
}

TEST_F(SerializationDeserializationTest, ExplainStmt) {
  ExplainStmt* explain = makeNode(ExplainStmt);
  explain->query = PlaceHolderNode();
  explain->options = list_make1(
      makeDefElem(pstrdup("elem_name"), PlaceHolderNode(), /*location=*/1));

  EXPECT_THAT(explain, CanSerializeAndDeserialize());
}

// Queries that wrap Explain require special handling, so test them.
TEST_F(SerializationDeserializationTest, ExplainQuery) {
  Query* query = makeNode(Query);
  query->commandType = CMD_UTILITY;
  ExplainStmt* explain = makeNode(ExplainStmt);
  explain->query = PlaceHolderNode();
  explain->options = list_make1(PlaceHolderNode());
  query->utilityStmt = internal::PostgresCastToNode(explain);

  EXPECT_THAT(query, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, RangeVar) {
  RangeVar* range_var = makeRangeVar(pstrdup("schema_name"),
                                     pstrdup("table_name"), /*location=*/1);
  range_var->tableHints = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(range_var, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, VacuumRelation) {
  RangeVar* range_var = makeRangeVar(pstrdup("schema_name"),
                                     pstrdup("table_name"), /*location=*/1);
  VacuumRelation* vac_rel = makeVacuumRelation(range_var, /*oid=*/InvalidOid,
                                               list_make1(PlaceHolderNode()));

  EXPECT_THAT(vac_rel, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, VacuumStmt) {
  VacuumStmt* vacuum = makeNode(VacuumStmt);
  vacuum->options = list_make1(makeDefElem(pstrdup("analyze"), NULL, 1));
  vacuum->rels = list_make1(makeVacuumRelation(nullptr, InvalidOid, NIL));
  vacuum->is_vacuumcmd = true;

  EXPECT_THAT(vacuum, CanSerializeAndDeserialize());
}

// We added hints to the query struct, so test it.
TEST_F(SerializationDeserializationTest, Query) {
  Query* query = makeNode(Query);
  query->commandType = CMD_SELECT;
  query->statementHints = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(query, CanSerializeAndDeserialize());
}

// We added hints to SubLink, so test it.
TEST_F(SerializationDeserializationTest, SubLink) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto sublink,
      internal::makeSubLink(SubLinkType::ANY_SUBLINK, /*subLinkId=*/1,
                            PlaceHolderNode(), list_make1(PlaceHolderNode()),
                            PlaceHolderNode()));
  sublink->joinHints = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(sublink, CanSerializeAndDeserialize());
}

// We added hints to JoinExpr, so test it.
TEST_F(SerializationDeserializationTest, JoinExpr) {
  JoinExpr* join_expr = makeNode(JoinExpr);
  join_expr->jointype = JOIN_INNER;
  join_expr->joinHints = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(join_expr, CanSerializeAndDeserialize());
}

// We added hints to RangeTblEntry, so test it.
TEST_F(SerializationDeserializationTest, RangeTblEntry) {
  RangeTblEntry* rte = makeNode(RangeTblEntry);
  rte->rtekind = RTE_RELATION;
  rte->tableHints = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(rte, CanSerializeAndDeserialize());
}


TEST_F(SerializationDeserializationTest, TruncateStmt) {
  TruncateStmt* truncate = makeNode(TruncateStmt);
  truncate->relations = list_make2(PlaceHolderNode(), PlaceHolderNode());
  truncate->restart_seqs = true;
  truncate->behavior = DROP_RESTRICT;

  EXPECT_THAT(truncate, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, TransactionStmt) {
  TransactionStmt* transaction = makeNode(TransactionStmt);
  transaction->kind = TRANS_STMT_COMMIT;
  transaction->options = list_make2(PlaceHolderNode(), PlaceHolderNode());
  transaction->savepoint_name = pstrdup("savepoint name");
  transaction->gid = pstrdup("gid");
  transaction->chain = true;

  EXPECT_THAT(transaction, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AIndirection) {
  A_Indirection* a_indirection = makeNode(A_Indirection);
  a_indirection->arg = PlaceHolderNode();
  a_indirection->indirection = list_make2(PlaceHolderNode(), PlaceHolderNode());

  EXPECT_THAT(a_indirection, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AIndices) {
  A_Indices* a_indices = makeNode(A_Indices);
  a_indices->is_slice = true;
  a_indices->lidx = PlaceHolderNode();
  a_indices->uidx = PlaceHolderNode();

  EXPECT_THAT(a_indices, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, DropTTL) {
  AlterTableCmd *drop = makeNode(AlterTableCmd);
  drop->subtype = AT_DropTtl;

  EXPECT_THAT(drop, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AddTTL) {
  AlterTableCmd *add = makeNode(AlterTableCmd);
  add->subtype = AT_AddTtl;
  add->name = pstrdup("a_column");

  EXPECT_THAT(add, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterSetInterleaveIn) {
  std::vector<InterleaveInType> all_interleave_kinds = {
    INTERLEAVE_IN,
    INTERLEAVE_IN_PARENT
  };
  for (InterleaveInType type : all_interleave_kinds) {
    AlterTableCmd *add = makeNode(AlterTableCmd);
    add->subtype = AT_SetInterleaveIn;
    InterleaveSpec* interleave_spec = makeNode(InterleaveSpec);
    interleave_spec->interleavetype = type;
    interleave_spec->parent =
        makeRangeVar(pstrdup("schemaname"), pstrdup("tablename"), -1);
    interleave_spec->location = -1;
    add->def = (Node *) interleave_spec;

    EXPECT_THAT(add, CanSerializeAndDeserialize());
  }
}

TEST_F(SerializationDeserializationTest, RangeFunction) {
  RangeFunction* range_function = makeNode(RangeFunction);
  range_function->lateral = true;
  range_function->ordinality = true;
  range_function->is_rowsfrom = true;
  range_function->functions = list_make2(PlaceHolderNode(), PlaceHolderNode());
  range_function->alias = makeAlias("aliasname", list_make1(PlaceHolderNode()));
  range_function->coldeflist = list_make1(PlaceHolderNode());

  EXPECT_THAT(range_function, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CreateChangeStreamStmt) {
  RangeVar* range_var = makeRangeVar(
      pstrdup("schema_name"), pstrdup("change_stream_name"), /*location=*/1);
  CreateChangeStreamStmt* create_change_stream_stmt =
      makeNode(CreateChangeStreamStmt);
  create_change_stream_stmt->opt_options = list_make1(PlaceHolderNode());
  create_change_stream_stmt->opt_for_tables = list_make1(PlaceHolderNode());
  create_change_stream_stmt->change_stream_name = range_var;
  create_change_stream_stmt->for_all = false;
  EXPECT_THAT(create_change_stream_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterChangeStreamStmt) {
  RangeVar* range_var = makeRangeVar(
      pstrdup("schema_name"), pstrdup("change_stream_name"), /*location=*/1);
  AlterChangeStreamStmt* alter_change_stream_stmt =
      makeNode(AlterChangeStreamStmt);
  alter_change_stream_stmt->opt_options = list_make1(PlaceHolderNode());
  alter_change_stream_stmt->opt_for_tables = list_make1(PlaceHolderNode());
  alter_change_stream_stmt->opt_drop_for_tables = list_make1(PlaceHolderNode());
  alter_change_stream_stmt->change_stream_name = range_var;
  alter_change_stream_stmt->for_all = false;
  alter_change_stream_stmt->drop_for_all = false;
  EXPECT_THAT(alter_change_stream_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ChangeStreamTrackedTable) {
  RangeVar* range_var = makeRangeVar(pstrdup("schema_name"),
                                     pstrdup("table_name"), /*location=*/1);
  ChangeStreamTrackedTable* change_stream_tracked_table =
      makeNode(ChangeStreamTrackedTable);
  change_stream_tracked_table->table_name = range_var;
  change_stream_tracked_table->columns = list_make1(PlaceHolderNode());
  change_stream_tracked_table->for_all_columns = false;
  EXPECT_THAT(change_stream_tracked_table, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CreateRoleStmt) {
  CreateRoleStmt* create_role_stmt = makeNode(CreateRoleStmt);
  create_role_stmt->stmt_type = ROLESTMT_ROLE;
  create_role_stmt->role = pstrdup("role_name");
  create_role_stmt->options = list_make1(PlaceHolderNode());
  EXPECT_THAT(create_role_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, DropRoleStmt) {
  DropRoleStmt* drop_role_stmt = makeNode(DropRoleStmt);
  drop_role_stmt->roles = list_make1(PlaceHolderNode());
  drop_role_stmt->missing_ok = false;
  EXPECT_THAT(drop_role_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, GrantStmt) {
  GrantStmt* grant_stmt = makeNode(GrantStmt);
  grant_stmt->is_grant = true;
  grant_stmt->targtype = ACL_TARGET_OBJECT;
  grant_stmt->objtype = OBJECT_TABLE;
  grant_stmt->objects = list_make1(PlaceHolderNode());
  grant_stmt->privileges = list_make1(PlaceHolderNode());
  grant_stmt->grantees = list_make1(PlaceHolderNode());
  grant_stmt->grant_option = false;
  grant_stmt->behavior = DROP_RESTRICT;
  EXPECT_THAT(grant_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AccessPriv) {
  AccessPriv* access_priv = makeNode(AccessPriv);
  access_priv->priv_name = pstrdup("SELECT");
  access_priv->cols = list_make1(PlaceHolderNode());
  EXPECT_THAT(access_priv, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, ObjectWithArgs) {
  ObjectWithArgs* object_with_args = makeNode(ObjectWithArgs);
  object_with_args->objname = list_make1(PlaceHolderNode());
  object_with_args->objargs = list_make1(PlaceHolderNode());
  object_with_args->args_unspecified = false;
  EXPECT_THAT(object_with_args, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, GrantRoleStmt) {
  GrantRoleStmt* grant_role_stmt = makeNode(GrantRoleStmt);
  grant_role_stmt->granted_roles = list_make1(PlaceHolderNode());
  grant_role_stmt->grantee_roles = list_make1(PlaceHolderNode());
  grant_role_stmt->is_grant = true;
  grant_role_stmt->admin_opt = true;
  grant_role_stmt->grantor = makeNode(RoleSpec);
  grant_role_stmt->behavior = DROP_RESTRICT;
  EXPECT_THAT(grant_role_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CreateSeqStmt) {
  CreateSeqStmt* create_seq_stmt = makeNode(CreateSeqStmt);
  RangeVar* range_var = makeRangeVar(pstrdup("schema_name"),
                                     pstrdup("sequence_name"), /*location=*/1);
  create_seq_stmt->sequence = range_var;
  create_seq_stmt->options = list_make1(makeNode(DefElem));
  create_seq_stmt->ownerId = 123;
  create_seq_stmt->for_identity = false;
  create_seq_stmt->if_not_exists = true;
  EXPECT_THAT(create_seq_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterSeqStmt) {
  AlterSeqStmt* alter_seq_stmt = makeNode(AlterSeqStmt);
  RangeVar* range_var = makeRangeVar(pstrdup("schema_name"),
                                     pstrdup("sequence_name"), /*location=*/1);
  alter_seq_stmt->sequence = range_var;
  alter_seq_stmt->options = list_make1(makeNode(DefElem));
  alter_seq_stmt->for_identity = false;
  alter_seq_stmt->missing_ok = true;
  EXPECT_THAT(alter_seq_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, RenameStmt) {
  RenameStmt* rename_stmt = makeNode(RenameStmt);
  rename_stmt->renameType = OBJECT_TABLE;
  rename_stmt->relation = makeNode(RangeVar);
  rename_stmt->newname = pstrdup("new_name");
  rename_stmt->missing_ok = false;
  rename_stmt->addSynonym = true;
  EXPECT_THAT(rename_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, TableRenameOp) {
  TableRenameOp* rename_op = makeNode(TableRenameOp);
  rename_op->fromName = makeNode(RangeVar);
  rename_op->toName = pstrdup("new_name");
  EXPECT_THAT(rename_op, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, TableChainedRenameStmt) {
  TableChainedRenameStmt* rename_stmt = makeNode(TableChainedRenameStmt);
  rename_stmt->ops = list_make1(makeNode(TableRenameOp));
  EXPECT_THAT(rename_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, SynonymClause) {
  SynonymClause* synonym_clause = makeNode(SynonymClause);
  synonym_clause->name = pstrdup("test_synonym");
  EXPECT_THAT(synonym_clause, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, FunctionHints) {
  FuncExpr* function_expr = makeFuncExpr(F_CONCAT, TEXTOID,
                                         /*args=*/NIL,
                                         /*funccollid=*/InvalidOid,
                                         /*inputcollid=*/InvalidOid,
                                         COERCE_EXPLICIT_CALL);
  function_expr->functionHints =
      list_make2(PlaceHolderNode(), PlaceHolderNode());
  EXPECT_THAT(function_expr, CanSerializeAndDeserialize());

  ZETASQL_ASSERT_OK_AND_ASSIGN(Aggref* aggref,
                       internal::makeAggref(F_COUNT_ANY, INT8OID,
                                            /*aggcollid=*/InvalidOid,
                                            /*inputcollid=*/InvalidOid,
                                            /*aggtranstype=*/InvalidOid,
                                            /*arg_types=*/NIL,
                                            /*aggdirectargs=*/nullptr,
                                            /*args=*/NIL,
                                            /*aggorder=*/nullptr,
                                            /*aggdistinct=*/NIL,
                                            /*aggfilter=*/nullptr,
                                            /*aggstar=*/true));
  aggref->functionHints = list_make2(PlaceHolderNode(), PlaceHolderNode());
  EXPECT_THAT(aggref, CanSerializeAndDeserialize());

  WindowFunc* window_func = makeNode(WindowFunc);
  window_func->functionHints = list_make2(PlaceHolderNode(), PlaceHolderNode());
  EXPECT_THAT(window_func, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, CallStmt) {
  CallStmt* call_stmt = makeNode(CallStmt);
  call_stmt->funccall = makeNode(FuncCall);
  call_stmt->funcexpr = makeNode(FuncExpr);
  call_stmt->outargs = list_make1(PlaceHolderNode());
  EXPECT_THAT(call_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterOwnerStmt) {
  AlterOwnerStmt* alter_owner_stmt = makeNode(AlterOwnerStmt);
  alter_owner_stmt->objectType = OBJECT_STATISTIC_EXT;
  alter_owner_stmt->relation = makeNode(RangeVar);
  alter_owner_stmt->object = PlaceHolderNode();
  alter_owner_stmt->newowner = makeNode(RoleSpec);
  EXPECT_THAT(alter_owner_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterStatsStmt) {
  AlterStatsStmt* alter_stats_stmt = makeNode(AlterStatsStmt);
  alter_stats_stmt->defnames = list_make1(PlaceHolderNode());
  alter_stats_stmt->stxstattarget = 37;
  alter_stats_stmt->missing_ok = true;
  EXPECT_THAT(alter_stats_stmt, CanSerializeAndDeserialize());
}

TEST_F(SerializationDeserializationTest, AlterObjectSchemaStmt) {
  AlterObjectSchemaStmt* alter_object_schema_stmt =
      makeNode(AlterObjectSchemaStmt);
  alter_object_schema_stmt->objectType = OBJECT_STATISTIC_EXT;
  alter_object_schema_stmt->relation = makeNode(RangeVar);
  alter_object_schema_stmt->object = PlaceHolderNode();
  alter_object_schema_stmt->newschema = pstrdup("new_schema");
  alter_object_schema_stmt->missing_ok = true;
  EXPECT_THAT(alter_object_schema_stmt, CanSerializeAndDeserialize());
}

}  // namespace
}  // namespace postgres_translator::test
