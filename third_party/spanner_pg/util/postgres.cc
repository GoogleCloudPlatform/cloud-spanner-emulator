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

#include "third_party/spanner_pg/util/postgres.h"
#include <stdbool.h>

#include <string>

#include "zetasql/base/logging.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_namespace_d.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace internal {

absl::StatusOr<Const*> makeScalarConst(Oid consttype, Datum constvalue,
                                       bool constisnull) {
  return CheckedPgMakeConst(
      /*consttype=*/consttype,
      /*consttypmod=*/-1,          // No typmod for simple scalar values
      /*constcollid=*/InvalidOid,  // No collation ID for simple scalar values
      /*constlen=*/sizeof(Datum),  // Should really be sizeof(C type)
      /*constvalue=*/constvalue,
      /*constisnull=*/constisnull,
      /*constbyval=*/true  // Scalars are passed by value
  );
}

static absl::StatusOr<Const*> string_to_const(const char* str, Oid datatype,
                                              bool constisnull) {
  Datum conval = PointerGetDatum(nullptr);
  if (!constisnull) {
    ZETASQL_ASSIGN_OR_RETURN(conval, CheckedPgStringToDatum(str, datatype));
  }
  Oid collation;
  int constlen;

  switch (datatype) {
    case TEXTOID:
    case VARCHAROID:
    case BPCHAROID:
      collation = DEFAULT_COLLATION_OID;
      constlen = -1;
      break;

    case NAMEOID:
      collation = InvalidOid;
      constlen = NAMEDATALEN;
      break;

    case BYTEAOID:
      collation = InvalidOid;
      constlen = -1;
      break;

    default:
      ABSL_LOG(FATAL) << "unexpected datatype in string_to_const: " << datatype;
      return nullptr;
  }

  return CheckedPgMakeConst(
      /*consttype=*/datatype,
      /*consttypmod=*/-1,
      /*constcollid=*/collation,
      /*constlen=*/constlen,
      /*constvalue=*/conval,
      /*constisnull=*/constisnull,
      /*constbyval=*/false);
}

absl::StatusOr<Const*> makeStringConst(Oid consttype, const char* str,
                                       bool constisnull) {
  return string_to_const(str, consttype, constisnull);
}

absl::StatusOr<Const*> makeByteConst(const std::string& byte_string,
                                     bool constisnull) {
  Datum conval = PointerGetDatum(nullptr);
  if (!constisnull) {
    ZETASQL_ASSIGN_OR_RETURN(const text* pg_text,
                     CheckedPgCStringToTextWithLen(byte_string.data(),
                                                   byte_string.length()));
    conval = PointerGetDatum(pg_text);
  }
  return CheckedPgMakeConst(/*consttype=*/BYTEAOID,
                            /*consttypmod=*/-1,
                            /*constcollid=*/InvalidOid,
                            /*constlen=*/-1,
                            /*constvalue=*/conval,
                            /*constisnull=*/constisnull,
                            /*constbyval=*/false);
}

// Creates a Param object.
absl::StatusOr<Param*> makeParam(ParamKind paramkind, int paramid,
                                 Oid paramtype, int32_t paramtypmod,
                                 Oid paramcollid, int location) {
  Param* pg_param;
  ZETASQL_ASSIGN_OR_RETURN(pg_param, CheckedPgMakeNode(Param));
  pg_param->paramkind = paramkind;
  pg_param->paramid = paramid;
  pg_param->paramtype = paramtype;
  pg_param->paramtypmod = paramtypmod;
  pg_param->paramcollid = paramcollid;
  pg_param->location = location;
  return pg_param;
}

absl::StatusOr<NamedArgExpr*> makeNamedArgExpr(Expr* arg,
                                               const std::string& name,
                                               int arg_num) {
  NamedArgExpr* named_arg;
  ZETASQL_ASSIGN_OR_RETURN(char* name_str, CheckedPgPstrdup(name.c_str()));
  ZETASQL_ASSIGN_OR_RETURN(named_arg, CheckedPgMakeNode(NamedArgExpr));
  named_arg->arg = arg;
  named_arg->name = name_str;
  named_arg->argnumber = arg_num;
  return named_arg;
}

absl::StatusOr<FuncExpr*> makeFuncExpr(Oid funcid, Oid rettype, List* args,
                                       CoercionForm funcformat) {
  return CheckedPgMakeFuncExpr(funcid, rettype, args, /*funccollid=*/InvalidOid,
                               /*inputcollid=*/InvalidOid, funcformat);
}

absl::StatusOr<JoinExpr*> makeJoinExpr(Index rtindex, JoinType join_type,
                                       Node* left_arg, Node* right_arg,
                                       List* using_clause, Node* quals,
                                       List* join_hints) {
  JoinExpr* join_expr;
  ZETASQL_ASSIGN_OR_RETURN(join_expr, CheckedPgMakeNode(JoinExpr));

  join_expr->rtindex = rtindex;
  join_expr->jointype = join_type;
  // TODO : Natural joins will be supported later.
  join_expr->isNatural = false;
  join_expr->larg = left_arg;
  join_expr->rarg = right_arg;
  join_expr->usingClause = using_clause;
  join_expr->quals = quals;
  join_expr->joinHints = join_hints;
  return join_expr;
}

absl::StatusOr<OpExpr*> makeOpExpr(Oid opno, Oid funcid, Oid rettype,
                                   List* args) {
  OpExpr* pg_op;
  ZETASQL_ASSIGN_OR_RETURN(pg_op, CheckedPgMakeNode(OpExpr));
  pg_op->opno = opno;
  pg_op->opfuncid = funcid;
  pg_op->opresulttype = rettype;
  pg_op->opretset = false;
  pg_op->opcollid = InvalidOid;
  pg_op->inputcollid = InvalidOid;
  pg_op->args = args;
  pg_op->location = -1;
  return pg_op;
}

absl::StatusOr<SubLink*> makeSubLink(SubLinkType subLinkType, int subLinkId,
                                     Node* testexpr, List* operName,
                                     Node* subselect, int location) {
  SubLink* pg_sublink;
  ZETASQL_ASSIGN_OR_RETURN(pg_sublink, CheckedPgMakeNode(SubLink));
  pg_sublink->subLinkType = subLinkType;
  pg_sublink->subLinkId = subLinkId;
  pg_sublink->testexpr = testexpr;
  pg_sublink->operName = operName;
  pg_sublink->subselect = subselect;
  pg_sublink->location = location;
  return pg_sublink;
}

absl::StatusOr<CoerceViaIO*> makeCoerceViaIO(Expr* arg, Oid resulttype,
                                             Oid resultcollid,
                                             CoercionForm coerceformat,
                                             int location) {
  CoerceViaIO* pg_coerce_via_io;
  ZETASQL_ASSIGN_OR_RETURN(pg_coerce_via_io, CheckedPgMakeNode(CoerceViaIO));
  pg_coerce_via_io->arg = arg;
  pg_coerce_via_io->resulttype = resulttype;
  pg_coerce_via_io->resultcollid = resultcollid;
  pg_coerce_via_io->coerceformat = coerceformat;
  pg_coerce_via_io->location = location;
  return pg_coerce_via_io;
}

absl::StatusOr<SetToDefault*> makeSetToDefault(Oid typeId, int32_t typeMod,
                                               Oid collation, int location) {
  SetToDefault* set_to_default;
  ZETASQL_ASSIGN_OR_RETURN(set_to_default, CheckedPgMakeNode(SetToDefault));
  set_to_default->typeId = typeId;
  set_to_default->typeMod = typeMod;
  set_to_default->collation = collation;
  set_to_default->location = location;
  return set_to_default;
}

absl::StatusOr<Aggref*> makeAggref(
    Oid aggfuncid, Oid aggresulttype, Oid aggcollid, Oid inputcollid,
    Oid aggtranstype, List* aggargtypes, List* aggdirectargs, List* args,
    List* aggorder, List* aggdistinct, Expr* aggfilter, bool aggstar,
    bool aggvariadic, char aggkind, Index agglevelsup, AggSplit aggsplit,
    int location) {
  Aggref* aggref;
  ZETASQL_ASSIGN_OR_RETURN(aggref, CheckedPgMakeNode(Aggref));
  aggref->aggfnoid = aggfuncid;
  aggref->aggtype = aggresulttype;
  aggref->aggcollid = aggcollid;
  aggref->inputcollid = inputcollid;
  aggref->aggtranstype = aggtranstype;
  aggref->aggargtypes = aggargtypes;
  aggref->aggdirectargs = aggdirectargs;
  aggref->args = args;
  aggref->aggorder = aggorder;
  aggref->aggdistinct = aggdistinct;
  aggref->aggfilter = aggfilter;
  aggref->aggstar = aggstar;
  aggref->aggvariadic = aggvariadic;
  aggref->aggkind = aggkind;
  aggref->agglevelsup = agglevelsup;
  aggref->aggsplit = aggsplit;
  aggref->location = location;
  aggref->functionHints = NIL;
  return aggref;
}

absl::StatusOr<SortGroupClause*> makeSortGroupClause(Index targetlistreference,
                                                     Oid equals_operator,
                                                     Oid sort_operator,
                                                     bool nulls_first,
                                                     bool hashable) {
  SortGroupClause* sort_clause;
  ZETASQL_ASSIGN_OR_RETURN(sort_clause, CheckedPgMakeNode(SortGroupClause));
  sort_clause->tleSortGroupRef = targetlistreference;
  sort_clause->eqop = equals_operator;
  sort_clause->sortop = sort_operator;
  sort_clause->nulls_first = nulls_first;
  sort_clause->hashable = hashable;
  return sort_clause;
}

absl::StatusOr<CoalesceExpr*> makeCoalesceExpr(Oid coalesce_type, List* args,
                                               Oid coalesce_collid,
                                               int location) {
  CoalesceExpr* coalesce_expr;
  ZETASQL_ASSIGN_OR_RETURN(coalesce_expr, CheckedPgMakeNode(CoalesceExpr));
  coalesce_expr->coalescetype = coalesce_type;
  coalesce_expr->coalescecollid = coalesce_collid;
  coalesce_expr->args = args;
  coalesce_expr->location = location;
  return coalesce_expr;
}

absl::StatusOr<NullTest*> makeNullTest(Expr* arg, NullTestType null_test_type,
                                       bool arg_is_row, int location) {
  NullTest* null_test;
  ZETASQL_ASSIGN_OR_RETURN(null_test, CheckedPgMakeNode(NullTest));
  null_test->arg = arg;
  null_test->nulltesttype = null_test_type;
  null_test->argisrow = arg_is_row;
  null_test->location = location;
  return null_test;
}

absl::StatusOr<BooleanTest*> makeBooleanTest(Expr* arg,
                                             BoolTestType bool_test_type,
                                             int location) {
  BooleanTest* bool_test;
  ZETASQL_ASSIGN_OR_RETURN(bool_test, CheckedPgMakeNode(BooleanTest));
  bool_test->arg = arg;
  bool_test->booltesttype = bool_test_type;
  bool_test->location = location;
  return bool_test;
}

absl::StatusOr<CaseWhen*> makeCaseWhen(Expr* case_expr, Expr* then_expr,
                                       int location) {
  CaseWhen* case_when;
  ZETASQL_ASSIGN_OR_RETURN(case_when, CheckedPgMakeNode(CaseWhen));
  case_when->expr = case_expr;
  case_when->result = then_expr;
  case_when->location = location;
  return case_when;
}

absl::StatusOr<CaseExpr*> makeCaseExpr(Oid output_type, List* case_when_args,
                                       Expr* else_arg, Expr* test_expr,
                                       Oid casecollid, int location) {
  CaseExpr* case_expr;
  ZETASQL_ASSIGN_OR_RETURN(case_expr, CheckedPgMakeNode(CaseExpr));
  case_expr->casetype = output_type;
  case_expr->casecollid = casecollid;
  case_expr->arg = test_expr;
  case_expr->args = case_when_args;
  case_expr->defresult = else_arg;
  case_expr->location = location;
  return case_expr;
}

absl::StatusOr<CaseTestExpr*> makeCaseTestExpr(Oid type_id, int32_t type_mod,
                                               Oid collation) {
  CaseTestExpr* case_test;
  ZETASQL_ASSIGN_OR_RETURN(case_test, CheckedPgMakeNode(CaseTestExpr));
  case_test->typeId = type_id;
  case_test->typeMod = type_mod;
  case_test->collation = collation;
  return case_test;
}

absl::StatusOr<MinMaxExpr*> makeMinMaxExpr(Oid min_max_type, List* args,
                                           MinMaxOp min_max_op,
                                           Oid min_max_collid, Oid input_collid,
                                           int location) {
  MinMaxExpr* min_max_expr;
  ZETASQL_ASSIGN_OR_RETURN(min_max_expr, CheckedPgMakeNode(MinMaxExpr));
  min_max_expr->minmaxtype = min_max_type;
  min_max_expr->minmaxcollid = min_max_collid;
  min_max_expr->inputcollid = input_collid;
  min_max_expr->op = min_max_op;
  min_max_expr->args = args;
  min_max_expr->location = location;
  return min_max_expr;
}

absl::StatusOr<NullIfExpr*> makeNullIfExpr(Oid rettype, List* args, Oid opno,
                                           Oid opfuncid, bool opretset,
                                           Oid opcollid, Oid inputcollid,
                                           int location) {
  NullIfExpr* nullif_expr;
  ZETASQL_ASSIGN_OR_RETURN(nullif_expr, CheckedPgMakeNode(NullIfExpr));
  nullif_expr->opno = opno;
  nullif_expr->opfuncid = opfuncid;
  nullif_expr->opresulttype = rettype;
  nullif_expr->opretset = opretset;
  nullif_expr->opcollid = opcollid;
  nullif_expr->inputcollid = inputcollid;
  nullif_expr->args = args;
  nullif_expr->location = location;
  return nullif_expr;
}

absl::StatusOr<DistinctExpr*> makeDistinctExpr(Oid rettype, List* args,
                                               Oid opno, Oid opfuncid,
                                               bool opretset, Oid opcollid,
                                               Oid inputcollid, int location) {
  DistinctExpr* distinctexpr;
  ZETASQL_ASSIGN_OR_RETURN(distinctexpr, CheckedPgMakeNode(DistinctExpr));
  distinctexpr->opno = opno;
  distinctexpr->opfuncid = opfuncid;
  distinctexpr->opresulttype = rettype;
  distinctexpr->opretset = opretset;
  distinctexpr->opcollid = opcollid;
  distinctexpr->inputcollid = inputcollid;
  distinctexpr->args = args;
  distinctexpr->location = location;
  return distinctexpr;
}

absl::StatusOr<SetOperationStmt*> makeSetOperationStmt(
    SetOperation op, bool all, Node* larg, Node* rarg, List* colTypes,
    List* colTypmods, List* colCollations, List* groupClauses) {
  SetOperationStmt* set_op_stmt;
  ZETASQL_ASSIGN_OR_RETURN(set_op_stmt, CheckedPgMakeNode(SetOperationStmt));
  set_op_stmt->op = op;
  set_op_stmt->all = all;
  set_op_stmt->larg = larg;
  set_op_stmt->rarg = rarg;
  set_op_stmt->colTypes = colTypes;
  set_op_stmt->colTypmods = colTypmods;
  set_op_stmt->colCollations = colCollations;
  set_op_stmt->groupClauses = groupClauses;
  return set_op_stmt;
}

absl::StatusOr<ScalarArrayOpExpr*> makeScalarArrayOpExpr(List* args, Oid opno,
                                                         Oid opfuncid,
                                                         bool useOr,
                                                         Oid inputcollid,
                                                         int location) {
  ScalarArrayOpExpr* scalar_array;
  ZETASQL_ASSIGN_OR_RETURN(scalar_array, CheckedPgMakeNode(ScalarArrayOpExpr));
  scalar_array->opno = opno;
  scalar_array->opfuncid = opfuncid;
  scalar_array->useOr = useOr;
  scalar_array->inputcollid = inputcollid;
  scalar_array->location = location;
  scalar_array->args = args;
  return scalar_array;
}

absl::StatusOr<ArrayExpr*> makeArrayExpr(List* elements, Oid array_typeid,
                                         Oid element_typeid, bool multidims,
                                         Oid array_collid, int location) {
  ArrayExpr* array_expr;
  ZETASQL_ASSIGN_OR_RETURN(array_expr, CheckedPgMakeNode(ArrayExpr));
  array_expr->array_typeid = array_typeid;
  array_expr->array_collid = array_collid;
  array_expr->element_typeid = element_typeid;
  array_expr->elements = elements;
  array_expr->multidims = multidims;
  array_expr->location = location;
  return array_expr;
}

absl::StatusOr<SQLValueFunction*> makeSQLValueFunction(
    SQLValueFunctionOp function_op, Oid rettype, int32_t typmod, int location) {
  SQLValueFunction* function;
  ZETASQL_ASSIGN_OR_RETURN(function, CheckedPgMakeNode(SQLValueFunction));
  function->op = function_op;
  function->type = rettype;
  function->typmod = typmod;
  function->location = location;
  return function;
}

absl::StatusOr<SubscriptingRef*> makeSubscriptingRef(
    Oid container_type, Oid element_type, Oid result_type, int32_t typmod,
    Oid ref_collid, List* upper_index_exprs, List* lower_index_exprs,
    Expr* ref_expr, Expr* assign_expr) {
  SubscriptingRef* subscripting_ref;
  ZETASQL_ASSIGN_OR_RETURN(subscripting_ref, CheckedPgMakeNode(SubscriptingRef));
  subscripting_ref->refcontainertype = container_type;
  subscripting_ref->refelemtype = element_type;
  subscripting_ref->refrestype = result_type;
  subscripting_ref->reftypmod = typmod;
  subscripting_ref->refcollid = ref_collid;
  subscripting_ref->refupperindexpr = upper_index_exprs;
  subscripting_ref->reflowerindexpr = lower_index_exprs;
  subscripting_ref->refexpr = ref_expr;
  subscripting_ref->refassgnexpr = assign_expr;
  return subscripting_ref;
}

absl::StatusOr<RangeTblRef*> makeRangeTblRef(Index rtindex) {
  RangeTblRef* rtr;
  ZETASQL_ASSIGN_OR_RETURN(rtr, CheckedPgMakeNode(RangeTblRef));
  rtr->rtindex = rtindex;
  return rtr;
}

absl::StatusOr<RangeTblEntry*> makePartialRangeTblEntry(bool inFromCl,
                                                        AclMode acl_mode) {
  RangeTblEntry* rte;
  ZETASQL_ASSIGN_OR_RETURN(rte, CheckedPgMakeNode(RangeTblEntry));
  rte->lateral = false;

  rte->inh = true;  // Callers should set this, but true is a safe default.
  rte->inFromCl = inFromCl;
  rte->checkAsUser =
      InvalidOid;  // User access check is not relevant in this context
  rte->selectedCols = nullptr;
  rte->insertedCols = nullptr;
  rte->updatedCols = nullptr;
  rte->extraUpdatedCols = nullptr;
  rte->requiredPerms = acl_mode;

  return rte;
}

absl::StatusOr<RangeTblFunction*> makeRangeTblFunction(FuncExpr* func_expr,
                                                       bool ordinality) {
  RangeTblFunction* function;
  ZETASQL_ASSIGN_OR_RETURN(function, CheckedPgMakeNode(RangeTblFunction));
  function->funcexpr = internal::PostgresCastToNode(func_expr);
  // These fields are only used if the function has a column definition list
  // (see struct definition for details)
  function->funccolnames = NIL;
  function->funccoltypes = NIL;
  function->funccoltypmods = NIL;
  function->funccolcollations = NIL;
  // Unused by Spangres
  function->funcparams = nullptr;
  // Add an output column for WITH ORDINALITY.
  function->funccolcount = ordinality ? 2 : 1;
  return function;
}

absl::StatusOr<RowMarkClause*> makeRowMarkClause(Index rti,
                                                 LockClauseStrength strength,
                                                 LockWaitPolicy wait_policy,
                                                 bool pushed_down) {
  RowMarkClause* row_mark_clause;
  ZETASQL_ASSIGN_OR_RETURN(row_mark_clause, CheckedPgMakeNode(RowMarkClause));
  row_mark_clause->rti = rti;
  row_mark_clause->strength = strength;
  row_mark_clause->waitPolicy = wait_policy;
  row_mark_clause->pushedDown = pushed_down;
  return row_mark_clause;
}

bool IsExpr(const Node& input) {
  switch (nodeTag(&input)) {
    case T_Var:
    case T_Const:
    case T_Param:
    case T_Aggref:
    case T_GroupingFunc:
    case T_WindowFunc:
    case T_FuncExpr:
    case T_NamedArgExpr:
    case T_OpExpr:
    case T_DistinctExpr:
    case T_NullIfExpr:
    case T_ScalarArrayOpExpr:
    case T_BoolExpr:
    case T_SubLink:
    case T_SubPlan:
    case T_AlternativeSubPlan:
    case T_FieldSelect:
    case T_FieldStore:
    case T_RelabelType:
    case T_CoerceViaIO:
    case T_ArrayCoerceExpr:
    case T_ConvertRowtypeExpr:
    case T_CollateExpr:
    case T_CaseExpr:
    case T_CaseTestExpr:
    case T_ArrayExpr:
    case T_RowExpr:
    case T_RowCompareExpr:
    case T_CoalesceExpr:
    case T_MinMaxExpr:
    case T_SQLValueFunction:
    case T_XmlExpr:
    case T_NullTest:
    case T_BooleanTest:
    case T_CoerceToDomain:
    case T_CoerceToDomainValue:
    case T_SetToDefault:
    case T_CurrentOfExpr:
    case T_NextValueExpr:
    case T_InferenceElem:
    case T_PlaceHolderVar:
    case T_SubscriptingRef:
      return true;
    default:
      return false;
  }
}

bool IsString(const Node& input) { return nodeTag(&input) == T_String; }

Node* PostgresCastToNode(void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);
  // Node is the base struct for all other node types in Postgres.
  // So it's safe to cast to Node here.
  return reinterpret_cast<Node*>(pointer);
}

const Node* PostgresConstCastToNode(const void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);
  // Node is the base struct for all other node types in Postgres.
  // So it's safe to cast to Node here.
  return reinterpret_cast<const Node*>(pointer);
}

Expr* PostgresCastToExpr(void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);

  // Check that pointer is an actual Expr before casting it.
  ABSL_CHECK(IsExpr(*reinterpret_cast<Node*>(pointer)));
  return reinterpret_cast<Expr*>(pointer);
}

const Expr* PostgresConstCastToExpr(const void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);

  // Check that pointer is an actual Expr before casting it.
  ABSL_CHECK(IsExpr(*reinterpret_cast<const Node*>(pointer)));
  return reinterpret_cast<const Expr*>(pointer);
}

String* PostgresCastToString(void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);

  // Check that pointer is an actual Value before casting it.
  ABSL_CHECK(IsString(*PostgresCastToNode(pointer)));
  return reinterpret_cast<String*>(pointer);
}

std::string RTEKindToString(RTEKind rtekind) {
  switch (rtekind) {
    case RTE_RELATION:
      return "RTE_RELATION"; /* ordinary relation reference */
    case RTE_SUBQUERY:
      return "RTE_SUBQUERY"; /* subquery in FROM */
    case RTE_JOIN:           /* join */
      return "RTE_JOIN";
    case RTE_FUNCTION: /* function in FROM */
      return "RTE_FUNCTION";
    case RTE_TABLEFUNC: /* TableFunc(.., column list) */
      return "RTE_TABLEFUNC";
    case RTE_VALUES: /* VALUES (<exprlist>), (<exprlist>), ... */
      return "RTE_VALUES";
    case RTE_CTE: /* common table expr (WITH list element) */
      return "RTE_CTE";
    case RTE_NAMEDTUPLESTORE: /* tuplestore, e.g. for AFTER triggers */
      return "RTE_NAMEDTUPLESTORE";
    case RTE_RESULT:
      return "RTE_RESULT";
  }
  ABSL_LOG(ERROR) << "Unknown RTEKind: " << rtekind;
  return "unknown RangeTblEntry kind";
}

std::string ParamKindToString(ParamKind param_kind) {
  switch (param_kind) {
    case PARAM_EXTERN:
      return "PARAM_EXTERN";
    case PARAM_EXEC:
      return "PARAM_EXEC";
    case PARAM_SUBLINK:
      return "PARAM_SUBLINK";
    case PARAM_MULTIEXPR:
      return "PARAM_MULTIEXPR";
  }
  ABSL_LOG(ERROR) << "Unknown ParamKind: " << param_kind;
  return "unknown parameter kind";
}

std::string SubLinkTypeToString(SubLinkType sublink_type) {
  switch (sublink_type) {
    case EXISTS_SUBLINK:
      return "EXISTS_SUBLINK";
    case ALL_SUBLINK:
      return "ALL_SUBLINK";
    case ANY_SUBLINK:
      return "ANY_SUBLINK";
    case ROWCOMPARE_SUBLINK:
      return "ROWCOMPARE_SUBLINK";
    case EXPR_SUBLINK:
      return "EXPR_SUBLINK";
    case MULTIEXPR_SUBLINK:
      return "MULTIEXPR_SUBLINK";
    case ARRAY_SUBLINK:
      return "ARRAY_SUBLINK";
    case CTE_SUBLINK:
      return "CTE_SUBLINK";
  }
  ABSL_LOG(ERROR) << "Unknown SubLinkType: " << sublink_type;
  return absl::StrCat("unknown sublink type: ", sublink_type);
}

std::string PostgresJoinTypeToString(JoinType join_type) {
  switch (join_type) {
    case JOIN_INNER:
      return "JOIN_INNER";
    case JOIN_LEFT:
      return "JOIN_LEFT";
    case JOIN_FULL:
      return "JOIN_FULL";
    case JOIN_RIGHT:
      return "JOIN_RIGHT";
    case JOIN_SEMI:
      return "JOIN_SEMI";
    case JOIN_ANTI:
      return "JOIN_ANTI";
    case JOIN_UNIQUE_OUTER:
      return "JOIN_UNIQUE_OUTER";
    case JOIN_UNIQUE_INNER:
      return "JOIN_UNIQUE_INNER";
  }
  ABSL_LOG(ERROR) << "Unknown PostgreSQL join type: " << join_type;
  return "unknown PostgreSQL join type";
}

absl::StatusOr<Oid> GetArrayUnnestProcOid() {
  constexpr absl::string_view kUnnestName = "unnest";
  ZETASQL_ASSIGN_OR_RETURN(
      const char* pg_catalog_namespace_name,
      PgBootstrapCatalog::Default()->GetNamespaceName(PG_CATALOG_NAMESPACE));
  return PgBootstrapCatalog::Default()->GetProcOid(pg_catalog_namespace_name,
                                                   kUnnestName,
                                                   {ANYARRAYOID});
}

absl::StatusOr<bool> IsAnnFunction(FuncExpr* func_expr) {
  constexpr absl::string_view kAnnFunctionNames[] = {
      "approx_cosine_distance", "approx_euclidean_distance",
      "approx_dot_product"};
  std::string proc_name;
  ZETASQL_ASSIGN_OR_RETURN(proc_name,
                   PgBootstrapCatalog::Default()->GetProcName(
                       func_expr->funcid));
  for (const absl::string_view ann_function_name : kAnnFunctionNames) {
    if (proc_name == ann_function_name) {
      return true;
    }
  }
  return false;
}

}  // namespace internal
}  // namespace postgres_translator
