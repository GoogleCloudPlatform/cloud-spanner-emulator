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

#ifndef UTIL_POSTGRES_H_
#define UTIL_POSTGRES_H_

#include <string>

#include "zetasql/base/logging.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/postgres_includes/all.h"  // IWYU pragma: keep for spangres_src includes

// The function names in the file are intentionally designed to be
// consistent with similar Postgres C function names. Some might violate
// Google's C++ style. The purpose is to avoid mixing two naming styles
// when calling these functions together with Postgres functions.
// Some violations of Google style that are intentional:
//  - Function names start with lower case letters. (e.g. makeObjectType())
//  - The function arguments might be Postgres style. (e.g. constisnull)
//  - The allocated objects are passed via raw pointers. The justification
//    is that the objects are actually allocated via Postgres C code which
//    manages the allocated memory. We prefer not to modify this part of logic.
//
// Unlike regular Postgres functions, the functions here are exception-safe. Any
// that call into exception-throwing Postgres code do so via error_shim
// wrappers.

namespace postgres_translator {
namespace internal {

// Helper method to create a Const object for a simple scalar value.
// Value must be fixed-size and have no type metadata (typmod, etc)
// other than the base type oid.
absl::StatusOr<Const*> makeScalarConst(Oid consttype, Datum constvalue,
                                       bool constisnull);

// Helper method to create a Const object for string values.
absl::StatusOr<Const*> makeStringConst(Oid consttype, const char* str,
                                       bool constisnull);

// Makes a Const of type BYTEA. This must be handled separately from String
// objects because Byte strings may contain null values.
absl::StatusOr<Const*> makeByteConst(const std::string& byte_string,
                                     bool constisnull);

// Creates a Param object with default values.
absl::StatusOr<Param*> makeParam(ParamKind paramkind, int paramid,
                                 Oid paramtype, int32_t paramtypmod = -1,
                                 Oid paramcollid = InvalidOid,
                                 int location = -1);

absl::StatusOr<NamedArgExpr*> makeNamedArgExpr(Expr* arg,
                                               const std::string& name,
                                               int arg_num);

absl::StatusOr<FuncExpr*> makeFuncExpr(Oid funcid, Oid rettype, List* args,
                                       CoercionForm funcformat);

absl::StatusOr<JoinExpr*> makeJoinExpr(Index rtindex, JoinType join_type,
                                       Node* left_arg, Node* right_arg,
                                       List* using_clause, Node* quals,
                                       List* join_hints);

absl::StatusOr<OpExpr*> makeOpExpr(Oid opno, Oid funcid, Oid rettype,
                                   List* args);

absl::StatusOr<SubLink*> makeSubLink(SubLinkType subLinkType, int subLinkId,
                                     Node* testexpr, List* operName,
                                     Node* subselect, int location = -1);

absl::StatusOr<CoerceViaIO*> makeCoerceViaIO(Expr* arg, Oid resulttype,
                                             Oid resultcollid,
                                             CoercionForm coerceformat,
                                             int location = -1);

absl::StatusOr<SetToDefault*> makeSetToDefault(Oid typeId, int32_t typeMod = -1,
                                               Oid collation = InvalidOid,
                                               int location = -1);

absl::StatusOr<Aggref*> makeAggref(
    Oid aggfuncid, Oid aggresulttype, Oid aggcollid, Oid inputcollid,
    Oid aggtranstype, List* aggargtypes, List* aggdirectargs, List* args,
    List* aggorder = NULL, List* aggdistinct = NULL, Expr* aggfilter = NULL,
    bool aggstar = false, bool aggvariadic = false,
    char aggkind = AGGKIND_NORMAL, Index agglevelsup = 0,
    AggSplit aggsplit = AGGSPLIT_SIMPLE, int location = -1);

absl::StatusOr<SortGroupClause*> makeSortGroupClause(Index targetlistreference,
                                                     Oid equals_operator,
                                                     Oid sort_operator,
                                                     bool nulls_first,
                                                     bool hashable);

absl::StatusOr<CoalesceExpr*> makeCoalesceExpr(Oid coalesce_type, List* args,
                                               Oid coalesce_collid = InvalidOid,
                                               int location = -1);

absl::StatusOr<NullTest*> makeNullTest(Expr* arg,
                                       NullTestType null_test_type = IS_NULL,
                                       bool arg_is_row = false,
                                       int location = -1);

absl::StatusOr<BooleanTest*> makeBooleanTest(Expr* arg,
                                             BoolTestType bool_test_type,
                                             int location = -1);

absl::StatusOr<CaseWhen*> makeCaseWhen(Expr* case_expr, Expr* then_expr,
                                       int location = -1);

absl::StatusOr<CaseExpr*> makeCaseExpr(Oid output_type, List* case_when_args,
                                       Expr* else_arg,
                                       Expr* test_expr = nullptr,
                                       Oid casecollid = InvalidOid,
                                       int location = -1);

absl::StatusOr<CaseTestExpr*> makeCaseTestExpr(Oid type_id, int32_t type_mod = -1,
                                               Oid collation = 100);

absl::StatusOr<MinMaxExpr*> makeMinMaxExpr(Oid min_max_type, List* args,
                                           MinMaxOp min_max_op,
                                           Oid min_max_collid = InvalidOid,
                                           Oid input_collid = InvalidOid,
                                           int location = -1);

absl::StatusOr<NullIfExpr*> makeNullIfExpr(Oid rettype, List* args, Oid opno,
                                           Oid opfuncid, bool opretset = false,
                                           Oid opcollid = InvalidOid,
                                           Oid inputcollid = InvalidOid,
                                           int location = -1);

absl::StatusOr<SetOperationStmt*> makeSetOperationStmt(
    SetOperation op, bool all, Node* larg, Node* rarg, List* colTypes = NULL,
    List* colTypmods = NULL, List* colCollations = NULL,
    List* groupClauses = NULL);

// 'useOr' is true as long as only IN and NOT IN are used. It may have to become
// dynamic if ANY/SOME or ALL are added.
absl::StatusOr<ScalarArrayOpExpr*> makeScalarArrayOpExpr(
    List* args, Oid opno, Oid opfuncid, bool useOr = true,
    Oid inputcollid = InvalidOid, int location = -1);

absl::StatusOr<ArrayExpr*> makeArrayExpr(List* elements, Oid array_typeid,
                                         Oid element_typeid, bool multidims,
                                         Oid array_collid = InvalidOid,
                                         int location = -1);

absl::StatusOr<SQLValueFunction*> makeSQLValueFunction(
    SQLValueFunctionOp function_op, Oid rettype, int32_t typmod = -1,
    int location = -1);

absl::StatusOr<SubscriptingRef*> makeSubscriptingRef(
    Oid container_type, Oid element_type, Oid result_type, int32_t typmod,
    Oid ref_collid, List* upper_index_exprs, List* lower_index_exprs,
    Expr* ref_expr, Expr* assign_expr);

absl::StatusOr<RangeTblRef*> makeRangeTblRef(Index rtindex);

// Builds a RangeTblEntry and sets some common fields to default values. Caller
// must set remaining fields depending on the application.
absl::StatusOr<RangeTblEntry*> makePartialRangeTblEntry(bool inFromCl,
                                                        AclMode acl_mode);

// Builds the RangeTblFunction for this function call. RangeTblFunction is a
// helper struct addendum that holds special funciton-specific information in
// a RTE_FUNCTION RangeTblEntry.
// `ordinality` indicates whether this call includes WITH ORDINALITY.
absl::StatusOr<RangeTblFunction*> makeRangeTblFunction(FuncExpr* func_expr,
                                                       bool ordinality);

// Builds a RowMarkClause to be added to a query if the query specified a
// locking clause.
absl::StatusOr<RowMarkClause*> makeRowMarkClause(Index rti,
                                                 LockClauseStrength strength,
                                                 LockWaitPolicy wait_policy,
                                                 bool pushed_down);

// Returns true if the input Node is an expression.
bool IsExpr(const Node& input);

bool IsValue(const Node& input);

// Casts a Postgres object held by `pointer` to Node type, using C++ style
// casting. `pointer` should point to a valid PostgreSQL object.
Node* PostgresCastToNode(void* pointer);
const Node* PostgresConstCastToNode(const void* pointer);

// Casts a Postgres object held by `pointer` to Expr type, using C++ style
// casting. `pointer` should point to a valid PostgreSQL object that is a
// derived class of Expr.
Expr* PostgresCastToExpr(void* pointer);
const Expr* PostgresConstCastToExpr(const void* pointer);

// Casts a Postgres object held by `pointer` to String type, using C++ style
// casting. `pointer` should point to a valid PostgreSQL object that is a
// derived class of String.
String* PostgresCastToString(void* pointer);

// PostgreSQL safe cast function. Mimics the behavior of castNode(), but with
// C++ constructs.
// REQUIRES (and verifies): `ptr` is a `_type_`* and `_type_` is a PostgreSQL
// Node "subclass" (PostgreSQL uses NodeTag for this pseudo-inheritence).
// Think of this as the PostgreSQL-equivalent to dynamic_cast.
#define PostgresCastNode(_type_, ptr) \
  PostgresCastNodeTemplate<_type_>(T_##_type_, ptr)
template <typename NodeType>
NodeType* PostgresCastNodeTemplate(NodeTag tag, void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);
  ABSL_CHECK_EQ(reinterpret_cast<Node*>(pointer)->type, tag);
  return reinterpret_cast<NodeType*>(pointer);
}

#define PostgresConstCastNode(_type_, ptr) \
  PostgresConstCastNodeTemplate<_type_>(T_##_type_, ptr)
template <typename NodeType>
const NodeType* PostgresConstCastNodeTemplate(NodeTag tag,
                                              const void* pointer) {
  ABSL_CHECK_NE(pointer, nullptr);
  ABSL_CHECK_EQ(reinterpret_cast<const Node*>(pointer)->type, tag);
  return reinterpret_cast<const NodeType*>(pointer);
}

std::string RTEKindToString(RTEKind rtekind);

std::string ParamKindToString(ParamKind param_kind);

std::string SubLinkTypeToString(SubLinkType sublink_type);

std::string PostgresJoinTypeToString(JoinType join_type);

// Helper function to find and return the Array Unnest proc from bootstrap.
// We only support the Array 'unnest' proc: unnest(anyarray)->anyelement, not
// the other variants (for record or range).
absl::StatusOr<Oid> GetArrayUnnestProcOid();

// Helper function to check if the function call is a Vector Search ANN
// function.
absl::StatusOr<bool> IsAnnFunction(FuncExpr* func_expr);

// For converting unnamed function parameters to named parameters.
inline constexpr absl::string_view kUnnamedFunctionParameterPrefix = "$";

// Helper function to create a UDF input parameter name from the given
// parameter index.
// Note: the index is 1-based, not 0-based.
inline std::string GetUdfParameterName(int parameter_index) {
  return absl::StrCat(kUnnamedFunctionParameterPrefix, parameter_index);
}

}  // namespace internal
}  // namespace postgres_translator

#endif  // UTIL_POSTGRES_H_
