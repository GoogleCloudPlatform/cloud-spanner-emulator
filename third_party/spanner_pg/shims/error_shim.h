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

#ifndef SHIMS_ERROR_SHIM_H_
#define SHIMS_ERROR_SHIM_H_

#include <stddef.h>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/parser_shim.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

inline constexpr Datum NULL_DATUM = (Datum)0;

// Error-handling verions of the PostgreSQL parser and analyzer.
absl::StatusOr<interfaces::ParserOutput> CheckedPgRawParserFullOutput(
    const char* sql);
absl::StatusOr<List*> CheckedPgRawParser(const char* sql);
absl::StatusOr<Query*> CheckedPgParseAnalyze(RawStmt* raw_stmt, const char* sql,
                                             const Oid* param_types,
                                             int num_params,
                                             QueryEnvironment* query_env);
absl::StatusOr<Query*> CheckedPgParseAnalyzeVarparams(
    RawStmt* raw_stmt, const char* sql, Oid** param_types, int* num_params,
    QueryEnvironment* query_env = NULL);
absl::StatusOr<void*> CheckedPgStringToNode(const char* data);
absl::StatusOr<char*> CheckedPgNodeToString(const void* obj);
absl::StatusOr<char*> CheckedPgPrettyFormatNodeDump(const char* dump);
absl::StatusOr<char*> CheckedPgDeparseQuery(Query* query,
                                            bool prettyPrint = false);
absl::StatusOr<char*> CheckedPgDeparseExprInQuery(Node* expr, Query* query);
absl::StatusOr<bool> CheckedPgEqual(const void* a, const void* b);
absl::StatusOr<Datum> CheckedPgStringTypeDatum(Type tp, char* string,
                                             int32_t atttypmod);
absl::StatusOr<Datum> CheckedPgStringToDatum(const char* str, Oid datatype);
absl::StatusOr<text*> CheckedPgCStringToTextWithLen(const char* str, int len);
absl::StatusOr<bool> CheckedPgBmsIsMember(int x, const Bitmapset* set);
absl::StatusOr<Bitmapset*> CheckedPgBmsAddMember(Bitmapset* set, int x);
absl::StatusOr<void*> CheckedPgPalloc(size_t size);
absl::StatusOr<TargetEntry*> CheckedPgGetSortGroupTargetEntry(Index sortref,
                                                              List* targetList);
absl::StatusOr<TypeCacheEntry*> CheckedPgLookupTypeCache(Oid type_id,
                                                         int flags);
absl::StatusOr<Oid> CheckedPgExprType(const Node* expr);
absl::StatusOr<Datum> CheckedDirectFunctionCall1(PGFunction func, Datum arg1);
absl::StatusOr<Datum> CheckedOidFunctionCall1(Oid functionId, Datum arg1);
absl::StatusOr<Datum> CheckedNullableOidFunctionCall1(Oid functionId,
                                                      Datum arg1);
absl::StatusOr<Datum> CheckedOidFunctionCall2(Oid functionId, Datum arg1,
                                              Datum arg2);
absl::StatusOr<Datum> CheckedNullableOidFunctionCall2(Oid functionId,
                                                      Datum arg1, Datum arg2);
absl::StatusOr<Datum> CheckedOidFunctionCall3(Oid functionId, Datum arg1,
                                              Datum arg2, Datum arg3);
absl::StatusOr<Datum> CheckedNullableOidFunctionCall3(Oid functionId,
                                                      Datum arg1, Datum arg2,
                                                      Datum arg3);
absl::StatusOr<Datum> CheckedOidFunctionCall4(Oid functionId, Datum arg1,
                                              Datum arg2, Datum arg3,
                                              Datum arg4);
absl::StatusOr<Datum> CheckedOidFunctionCall7(Oid functionId, Datum arg1,
                                              Datum arg2, Datum arg3,
                                              Datum arg4, Datum arg5,
                                              Datum arg6, Datum arg7);
absl::StatusOr<List*> CheckedPgListMake1(void* datum);
absl::StatusOr<List*> CheckedPgListMake2(void* datum1, void* datum2);
absl::StatusOr<List*> CheckedPgLappend(List* list, void* datum);
absl::StatusOr<List*> CheckedPgLappendOid(List* list, Oid oid);
absl::StatusOr<List*> CheckedPgLappendInt(List* list, int datum);
absl::StatusOr<List*> CheckedPgListDeleteNthCell(List* list, int pos);
absl::StatusOr<List*> CheckedPgListInsertNth(List* list, int pos, void* datum);
absl::StatusOr<ListCell*> CheckedPgListHead(const List* list);
absl::StatusOr<List*> CheckedPgListConcat(List* list1, const List* list2);

template <typename NodeType>
absl::StatusOr<NodeType*> CheckedPgLinitialNode(List* list) {
  ListCell* lc;
  ZETASQL_ASSIGN_OR_RETURN(lc, CheckedPgListHead(list));
  ZETASQL_RET_CHECK(lc != nullptr);
  return reinterpret_cast<NodeType*>(lfirst(lc));
}

absl::StatusOr<char*> CheckedPgPstrdup(const char* input);
absl::Status CheckedPgGetSortGroupOperators(Oid argtype, bool needLT,
                                            bool needEQ, bool needGT,
                                            Oid* ltOpr, Oid* eqOpr, Oid* gtOpr,
                                            bool* isHashable);
absl::StatusOr<Var*> CheckedPgMakeVar(int varno, AttrNumber varattno,
                                      Oid vartype, int32_t vartypmod,
                                      Oid varcollid, Index varlevelsup);
absl::StatusOr<String*> CheckedPgMakeString(char* input);
absl::StatusOr<Alias*> CheckedPgMakeAlias(const char* aliasname,
                                          List* colnames);
absl::StatusOr<Const*> CheckedPgMakeConst(Oid consttype, int32_t consttypmod,
                                          Oid constcollid, int constlen,
                                          Datum constvalue, bool constisnull,
                                          bool constbyval);
absl::StatusOr<Node*> CheckedPgMakeBoolConst(bool value, bool isnull);
absl::StatusOr<TargetEntry*> CheckedPgMakeTargetEntry(Expr* expr,
                                                      AttrNumber resno,
                                                      char* resname,
                                                      bool resjunk);
absl::StatusOr<DefElem*> CheckedPgMakeDefElem(char* name, Node* arg,
                                              int location);
absl::StatusOr<FuncExpr*> CheckedPgMakeFuncExpr(Oid funcid, Oid rettype,
                                                List* args, Oid funccollid,
                                                Oid inputcollid,
                                                CoercionForm fformat);
absl::StatusOr<Node*> CheckedPgMakeNodeImpl(size_t size, NodeTag tag);
absl::StatusOr<MemoryContext> CheckedPgAllocSetContextCreateInternal(
    MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize);
absl::StatusOr<char*> CheckedPgTextDatumGetCString(Datum datum);
absl::StatusOr<char*> CheckedPgCStringDatumToCString(Datum cstr);
absl::StatusOr<char*> CheckedPgFormatTypeBe(Oid type_oid);
absl::StatusOr<ArrayIterator> CheckedPgArrayCreateIterator(
    ArrayType* arr, int slice_ndim, ArrayMetaState* mstate);
absl::StatusOr<bool> CheckedPgArrayIterate(ArrayIterator iterator, Datum* value,
                                           bool* isnull);
absl::StatusOr<ArrayType*> CheckedPgConstructMdArray(Datum* elems, bool* nulls,
                                                     int ndims, int* dims,
                                                     int* lbs, Oid elmtype,
                                                     int elmlen, bool elmbyval,
                                                     char elmalign);
absl::StatusOr<ArrayType*> CheckedPgConstructEmptyArray(Oid elmtype);
absl::Status CheckedPgGetTyplenbyvalalign(Oid typid, int16_t* typlen,
                                          bool* typbyval, char* typalign);
absl::StatusOr<ArrayType*> CheckedPgDatumGetArrayTypeP(Datum datum);
absl::StatusOr<Type> CheckedPgTypeidType(Oid id);

#define CheckedPgMakeNode(_type_) \
  CheckedPgMakeNodeTemplate<_type_>(sizeof(_type_), T_##_type_)

template <typename NodeType>
absl::StatusOr<NodeType*> CheckedPgMakeNodeTemplate(size_t size, NodeTag tag) {
  Node* node;
  ZETASQL_ASSIGN_OR_RETURN(node, CheckedPgMakeNodeImpl(size, tag));
  // This is the same as PostgresCastNode(), but we can't call it as postgres.h
  // needs to include error_shim.h. So if we use PostgresCastNode() here, we
  // need to include postgres.h, which introduces a circular dependency.
  ZETASQL_RET_CHECK_NE(node, nullptr);
  ZETASQL_RET_CHECK_EQ(node->type, tag);
  return reinterpret_cast<NodeType*>(node);
}

//
// Functions below are for test only:
//
// Error-handling functions that optionally generate PostgreSQL errors at
// various levels.
absl::StatusOr<int> TEST_CheckedError(bool throw_error, int error_code);
absl::StatusOr<int> TEST_CheckedFatal(bool throw_error, int error_code);
absl::StatusOr<int> TEST_CheckedPanic(bool throw_error, int error_code);
absl::StatusOr<int> TEST_CheckedElogError(bool throw_error);

// Error-handling versions of the myriad PostgreSQL allocators.
absl::StatusOr<void*> CheckedPgPalloc0(size_t size);
absl::StatusOr<void*> CheckedPgPalloc0fast(size_t size);
absl::StatusOr<void*> CheckedPgRepalloc(void* pointer, size_t size);
absl::StatusOr<void*> CheckedPgPallocExtended(size_t size, int flags);
absl::StatusOr<void*> CheckedPgMemoryContextAlloc(MemoryContext context,
                                                  size_t size);
absl::StatusOr<void*> CheckedPgMemoryContextAllocZero(MemoryContext context,
                                                      size_t size);
absl::StatusOr<void*> CheckedPgMemoryContextAllocZeroAligned(
    MemoryContext context, size_t size);
absl::StatusOr<void*> CheckedPgMemoryContextAllocHuge(MemoryContext context,
                                                      size_t size);
absl::Status CheckedPgMemoryContextDelete(MemoryContext context);
absl::Status CheckedPgAsetDeleteFreelists();
absl::StatusOr<ArrayType*> CheckedPgDatumGetArrayTypeP(Datum datum);

absl::StatusOr<pg_tz*> CheckedPgTZSet(const char *tzname);
absl::Status CheckedPgTimezoneInitialize(void);
absl::StatusOr<pg_tz*> CheckedPgTZOffsetSet(int32_t gmt_offset);

absl::StatusOr<int64_t> CheckedPgDefGetInt64(DefElem* def);

}  // namespace postgres_translator

#endif  // SHIMS_ERROR_SHIM_H_
