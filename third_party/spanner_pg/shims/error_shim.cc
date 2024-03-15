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

#include "third_party/spanner_pg/shims/error_shim.h"

#include <stddef.h>

#include <csetjmp>
#include <utility>

#include "zetasql/base/logging.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/errors/error_catalog.h"
#include "third_party/spanner_pg/errors/errors.h"
#include "third_party/spanner_pg/interface/parser_output.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/postgres_includes/deparser.h"
#include "third_party/spanner_pg/shims/catalog_shim.h"
#include "third_party/spanner_pg/shims/ereport_shim.h"
#include "third_party/spanner_pg/shims/parser_shim.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

// Defined in elog.c
extern "C" __thread jmp_buf* error_jump_buffer;
extern "C" __thread const char* error_string_buffer;
extern "C" __thread int error_code_buffer;

// Internal PG function for freeing memory inside PG's thread-local freelists
extern "C" void AsetDeleteFreelists(void);

namespace postgres_translator {

// Hardcodes the collation for all function calls
const Oid DEFAULT_COLLATION = C_COLLATION_OID;

namespace {

// Converts a PostgreSQL error to an absl::Status.
absl::Status ConvertPostgresError(
    const PostgresEreportException& exc) {
  const std::optional<absl::Status> error_status = exc.error_status();
  if (error_status.has_value()) {
    // This is a generated error from the error catalog.
    return error_status.value();
  }
  if (exc.error_data().message_id != nullptr) {
    // This is an error from the original PG code.
    std::function<absl::Status(
        const PostgresEreportException&)>
        error_func = spangres::LookupPGErrorFunc(exc.error_data().sqlerrcode,
                                                 exc.error_data().message_id);
    if (error_func != nullptr) {
      return error_func(exc);
    }
  }

  if (exc.error_data().message_id != nullptr) {
    ABSL_LOG(FATAL) << "PG error is not defined in the error catalog, error code: ["
                << exc.error_data().sqlerrcode << "] error message: ["
                << exc.error_data().message << "] message id: ["
                << exc.error_data().message_id << "].";
  }

  // TODO: We should throw a Spangres internal error instead of
  // having a generic conversion after the migration to the error catalog is
  // done.
  return absl::Status(
      spangres::error::CanonicalCode(exc.error_data().sqlerrcode), exc.what());
}

}  // namespace

// General PostgreSQL error-handling wrapper. Takes a PostgreSQL function and
// its args and calls it in a PostgreSQL error-handling context that converts
// any errors and returns an absl::StatusOr<> containing any converted
// PostgreSQL error if possible.
template <typename RetType, typename... ArgTypes>
absl::StatusOr<RetType> ErrorCheckedPgCall(RetType (*pg_func)(ArgTypes...),
                                           ArgTypes... args) {
  try {
    return pg_func(args...);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    return ConvertPostgresError(exc);
  }
}

template <typename RetType, typename... ArgTypes>
absl::Status ErrorCheckedPgCallNoReturnValue(RetType (*pg_func)(ArgTypes...),
                                             ArgTypes... args) {
  try {
    pg_func(args...);
    return absl::OkStatus();
  } catch (const postgres_translator::PostgresEreportException& exc) {
    return ConvertPostgresError(exc);
  }
}

// Error-handling versions of PostgreSQL parser and analyzer invocations. Calls
// the appropriate underlying PostgreSQL function and if an error is thrown,
// catches it and turns it into a util::Status error instead.
absl::StatusOr<interfaces::ParserOutput> CheckedPgRawParserFullOutput(
    const char* sql) {
  if (CurrentMemoryContext == nullptr) {
    return absl::Status(absl::StatusCode::kInternal,
                        "Thread-local MemoryContext required by parser. Use "
                        "MemoryContextManager::Init");
  }
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  SpangresTokenLocations locations;
  ZETASQL_ASSIGN_OR_RETURN(List * parse_tree,
                   ErrorCheckedPgCall(raw_parser_spangres, sql,
                                      RAW_PARSE_DEFAULT, &locations));
  return interfaces::ParserOutput(
      parse_tree, {.token_locations = std::move(locations.start_end_pairs),
                   .serialized_parse_tree_size = 0});
}

absl::StatusOr<List*> CheckedPgRawParser(const char* sql) {
  ZETASQL_ASSIGN_OR_RETURN(interfaces::ParserOutput output,
                   CheckedPgRawParserFullOutput(sql));
  return output.parse_tree();
}

absl::StatusOr<Query*> CheckedPgParseAnalyze(RawStmt* raw_stmt, const char* sql,
                                             Oid* param_types, int num_params,
                                             QueryEnvironment* query_env) {
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());

  return ErrorCheckedPgCall(parse_analyze, raw_stmt, sql, param_types,
                            num_params, query_env);
}

absl::StatusOr<Query*> CheckedPgParseAnalyzeVarparams(RawStmt* raw_stmt,
                                                      const char* sql,
                                                      Oid** param_types,
                                                      int* num_params) {
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());

  return ErrorCheckedPgCall(parse_analyze_varparams, raw_stmt, sql, param_types,
                            num_params);
}

// Add in a forward declaration of stringToNode because we don't want to
// add a dependency on all of postgres' headers if we can avoid it.
extern "C" {
extern void *stringToNode(const char *str);
}

absl::StatusOr<void*> CheckedPgStringToNode(const char* data) {
  if (CurrentMemoryContext == nullptr) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "Thread-local MemoryContext required by deserializer. Use "
        "MemoryContextManager::Init");
  }
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(stringToNode, data);
}

absl::StatusOr<char*> CheckedPgNodeToString(const void* obj) {
  if (CurrentMemoryContext == nullptr) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "Thread-local MemoryContext required by serializer. Use "
        "MemoryContextManager::Init");
  }
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(nodeToString, obj);
}

absl::StatusOr<char*> CheckedPgPrettyFormatNodeDump(const char* dump) {
  if (CurrentMemoryContext == nullptr) {
    return absl::Status(
        absl::StatusCode::kInternal,
        "Thread-local MemoryContext required by pretty format. Use "
        "MemoryContextManager::Init");
  }
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(pretty_format_node_dump, dump);
}

absl::StatusOr<char*> CheckedPgDeparseQuery(Query* query, bool prettyPrint) {
  if (CurrentMemoryContext == nullptr) {
    return absl::Status(absl::StatusCode::kInternal,
                        "Thread-local MemoryContext required by deparser. Use "
                        "MemoryContextManager::Init");
  }
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());

  return ErrorCheckedPgCall(deparse_query, query, prettyPrint);
}

absl::StatusOr<char*> CheckedPgDeparseExprInQuery(Node* expr, Query* query) {
  if (CurrentMemoryContext == nullptr) {
    return absl::Status(absl::StatusCode::kInternal,
                        "Thread-local MemoryContext required by deparser. Use "
                        "MemoryContextManager::Init");
  }
  // Set the stack base here so PostgreSQL stack depth is being checked properly
  // to avoid overflow.
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(deparse_expression_in_query, expr, query);
}

absl::StatusOr<bool> CheckedPgEqual(const void* a, const void* b) {
  return ErrorCheckedPgCall(equal, a, b);
}

absl::StatusOr<Datum> CheckedPgStringTypeDatum(Type tp, char* string,
                                             int32_t atttypmod) {
  return ErrorCheckedPgCall(stringTypeDatum, tp, string, atttypmod);
}

absl::StatusOr<Datum> CheckedPgStringToDatum(const char* str, Oid datatype) {
  if (datatype == NAMEOID) {
    return ErrorCheckedPgCall(DirectFunctionCall1Coll, /*func=*/namein,
                              /*collation=*/DEFAULT_COLLATION,
                              /*arg1=*/CStringGetDatum(str));
  } else if (datatype == BYTEAOID) {
    return ErrorCheckedPgCall(DirectFunctionCall1Coll, /*func=*/byteain,
                              /*collation=*/DEFAULT_COLLATION,
                              /*arg1=*/CStringGetDatum(str));
  } else {
    ZETASQL_ASSIGN_OR_RETURN(text * text_string,
                     ErrorCheckedPgCall(cstring_to_text, str));
    return PointerGetDatum(text_string);
  }
}

absl::StatusOr<char*> CheckedPgCStringDatumToCString(Datum cstr) {
  try {
    // DatumGetCString is a macro, so we can not use ErrorCheckedPgCall
    return DatumGetCString(cstr);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    return ConvertPostgresError(exc);
  }
}

absl::StatusOr<text*> CheckedPgCStringToTextWithLen(const char* str, int len) {
  return ErrorCheckedPgCall(cstring_to_text_with_len, str, len);
}

absl::StatusOr<bool> CheckedPgBmsIsMember(int x, const Bitmapset* set) {
  return ErrorCheckedPgCall(bms_is_member, x, set);
}

absl::StatusOr<Bitmapset*> CheckedPgBmsAddMember(Bitmapset* set, int x) {
  return ErrorCheckedPgCall(bms_add_member, set, x);
}

absl::StatusOr<void*> CheckedPgPalloc(size_t size) {
  return ErrorCheckedPgCall(palloc, size);
}

// Add in a forward declaration of get_sortgroupref_tle to avoid adding in an
// include of optimizer.h and all postgres' header dependencies.
extern "C" {
extern TargetEntry *get_sortgroupref_tle(Index sortref, List *targetList);
}

absl::StatusOr<void*> CheckedPgPalloc0(size_t size) {
  return ErrorCheckedPgCall(palloc0, size);
}

absl::StatusOr<void*> CheckedPgPalloc0fast(size_t size) {
  // palloc0fast is defined as a macro instead of a function so we can't use the
  // usual ErrorCheckedPgCall mechanism.
  try {
    return palloc0fast(size);
  } catch (const postgres_translator::PostgresEreportException& exc) {
    return ConvertPostgresError(exc);
  }
}

absl::StatusOr<void*> CheckedPgRepalloc(void* pointer, size_t size) {
  return ErrorCheckedPgCall(repalloc, pointer, size);
}

absl::StatusOr<void*> CheckedPgPallocExtended(size_t size, int flags) {
  return ErrorCheckedPgCall(palloc_extended, size, flags);
}

absl::StatusOr<void*> CheckedPgMemoryContextAlloc(MemoryContext context,
                                                  size_t size) {
  return ErrorCheckedPgCall(MemoryContextAlloc, context, size);
}

absl::StatusOr<void*> CheckedPgMemoryContextAllocZero(MemoryContext context,
                                                      size_t size) {
  return ErrorCheckedPgCall(MemoryContextAllocZero, context, size);
}

absl::StatusOr<void*> CheckedPgMemoryContextAllocZeroAligned(
    MemoryContext context, size_t size) {
  return ErrorCheckedPgCall(MemoryContextAllocZeroAligned, context, size);
}

absl::StatusOr<void*> CheckedPgMemoryContextAllocHuge(MemoryContext context,
                                                      size_t size) {
  return ErrorCheckedPgCall(MemoryContextAllocHuge, context, size);
}

absl::Status CheckedPgMemoryContextDelete(MemoryContext context) {
  return ErrorCheckedPgCallNoReturnValue(MemoryContextDelete, context);
}

absl::Status CheckedPgAsetDeleteFreelists() {
  return ErrorCheckedPgCallNoReturnValue(AsetDeleteFreelists);
}

absl::StatusOr<TargetEntry*> CheckedPgGetSortGroupTargetEntry(
    Index sortref, List* targetList) {
  return ErrorCheckedPgCall(get_sortgroupref_tle, sortref, targetList);
}

absl::StatusOr<TypeCacheEntry*> CheckedPgLookupTypeCache(Oid type_id,
                                                         int flags) {
  return ErrorCheckedPgCall(lookup_type_cache, type_id, flags);
}

absl::StatusOr<Oid> CheckedPgExprType(const Node* expr) {
  return ErrorCheckedPgCall(exprType, expr);
}

absl::StatusOr<Datum> CheckedDirectFunctionCall1(PGFunction func, Datum arg1) {
  return ErrorCheckedPgCall(DirectFunctionCall1Coll, func,
                            /*collation=*/DEFAULT_COLLATION, arg1);
}

absl::StatusOr<Datum> CheckedOidFunctionCall1(Oid functionId, Datum arg1) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(OidFunctionCall1Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1);
}

absl::StatusOr<Datum> CheckedNullableOidFunctionCall1(Oid functionId,
                                                      Datum arg1) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(NullableOidFunctionCall1Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1);
}

absl::StatusOr<Datum> CheckedOidFunctionCall2(Oid functionId, Datum arg1,
                                              Datum arg2) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(OidFunctionCall2Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1, arg2);
}

absl::StatusOr<Datum> CheckedNullableOidFunctionCall2(Oid functionId,
                                                      Datum arg1, Datum arg2) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(NullableOidFunctionCall2Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1, arg2);
}

absl::StatusOr<Datum> CheckedOidFunctionCall3(Oid functionId, Datum arg1,
                                              Datum arg2, Datum arg3) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(OidFunctionCall3Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1, arg2, arg3);
}

absl::StatusOr<Datum> CheckedNullableOidFunctionCall3(Oid functionId,
                                                      Datum arg1, Datum arg2,
                                                      Datum arg3) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(NullableOidFunctionCall3Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1, arg2, arg3);
}

absl::StatusOr<Datum> CheckedOidFunctionCall4(Oid functionId, Datum arg1,
                                              Datum arg2, Datum arg3,
                                              Datum arg4) {
  ZETASQL_RET_CHECK(ErrorCheckedPgCall(set_stack_base).ok());
  return ErrorCheckedPgCall(OidFunctionCall4Coll, functionId,
                            /*collation=*/DEFAULT_COLLATION, arg1, arg2, arg3,
                            arg4);
}

absl::StatusOr<List*> CheckedPgListMake1(void* datum) {
  // list_make1 is a macro. Decompose it for the error shim.
  return ErrorCheckedPgCall(list_make1_impl, T_List, list_make_ptr_cell(datum));
}

absl::StatusOr<List*> CheckedPgListMake2(void* datum1, void* datum2) {
  // list_make2 is a macro. Decompose it for the error shim.
  return ErrorCheckedPgCall(list_make2_impl, T_List, list_make_ptr_cell(datum1),
                            list_make_ptr_cell(datum2));
}

absl::StatusOr<List*> CheckedPgLappend(List* list, void* datum) {
  return ErrorCheckedPgCall(lappend, list, datum);
}

absl::StatusOr<List*> CheckedPgLappendOid(List* list, Oid oid) {
  return ErrorCheckedPgCall(lappend_oid, list, oid);
}

absl::StatusOr<List*> CheckedPgLappendInt(List* list, int datum) {
  return ErrorCheckedPgCall(lappend_int, list, datum);
}

absl::StatusOr<List*> CheckedPgListDeleteNthCell(List* list, int pos) {
  return ErrorCheckedPgCall(list_delete_nth_cell, list, pos);
}

absl::StatusOr<List*> CheckedPgListInsertNth(List* list, int pos, void* datum) {
  return ErrorCheckedPgCall(list_insert_nth, list, pos, datum);
}

absl::StatusOr<ListCell*> CheckedPgListHead(const List* list) {
  return ErrorCheckedPgCall(list_head, list);
}

absl::StatusOr<List*> CheckedPgListConcat(List* list1, const List* list2) {
  return ErrorCheckedPgCall(list_concat,  list1,  list2);
}

absl::StatusOr<char*> CheckedPgPstrdup(const char* input) {
  return ErrorCheckedPgCall(pstrdup, input);
}

absl::Status CheckedPgGetSortGroupOperators(Oid argtype, bool needLT,
                                            bool needEQ, bool needGT,
                                            Oid* ltOpr, Oid* eqOpr, Oid* gtOpr,
                                            bool* isHashable) {
  return ErrorCheckedPgCallNoReturnValue(get_sort_group_operators, argtype,
                                         needLT, needEQ, needGT, ltOpr, eqOpr,
                                         gtOpr, isHashable);
}

absl::StatusOr<Var*> CheckedPgMakeVar(Index varno, AttrNumber varattno,
                                      Oid vartype, int32_t vartypmod,
                                      Oid varcollid, Index varlevelsup) {
  return ErrorCheckedPgCall(makeVar, varno, varattno, vartype, vartypmod,
                            varcollid, varlevelsup);
}

absl::StatusOr<Value*> CheckedPgMakeString(char* input) {
  return ErrorCheckedPgCall(makeString, input);
}

absl::StatusOr<Alias*> CheckedPgMakeAlias(const char* aliasname,
                                          List* colnames) {
  return ErrorCheckedPgCall(makeAlias, aliasname, colnames);
}

absl::StatusOr<Const*> CheckedPgMakeConst(Oid consttype, int32_t consttypmod,
                                          Oid constcollid, int constlen,
                                          Datum constvalue, bool constisnull,
                                          bool constbyval) {
  return ErrorCheckedPgCall(makeConst, consttype, consttypmod, constcollid,
                            constlen, constvalue, constisnull, constbyval);
}

absl::StatusOr<Node*> CheckedPgMakeBoolConst(bool value, bool isnull) {
  return ErrorCheckedPgCall(makeBoolConst, value, isnull);
}

absl::StatusOr<TargetEntry*> CheckedPgMakeTargetEntry(Expr* expr,
                                                      AttrNumber resno,
                                                      char* resname,
                                                      bool resjunk) {
  return ErrorCheckedPgCall(makeTargetEntry, expr, resno, resname, resjunk);
}

absl::StatusOr<DefElem*> CheckedPgMakeDefElem(char* name, Node* arg,
                                              int location) {
  return ErrorCheckedPgCall(makeDefElem, name, arg, location);
}

absl::StatusOr<FuncExpr*> CheckedPgMakeFuncExpr(Oid funcid, Oid rettype,
                                                List* args, Oid funccollid,
                                                Oid inputcollid,
                                                CoercionForm fformat) {
  return ErrorCheckedPgCall(makeFuncExpr, funcid, rettype, args, funccollid,
                            inputcollid, fformat);
}

absl::StatusOr<Node*> CheckedPgMakeNodeImpl(size_t size, NodeTag tag) {
  try {
    return reinterpret_cast<Node*>(newNode(size, tag));
  } catch (const postgres_translator::PostgresEreportException& exc) {
    return ConvertPostgresError(exc);
  }
}

absl::StatusOr<MemoryContext> CheckedPgAllocSetContextCreateInternal(
    MemoryContext parent, const char* name, Size minContextSize,
    Size initBlockSize, Size maxBlockSize) {
  return ErrorCheckedPgCall(AllocSetContextCreateInternal, parent, name,
                            minContextSize, initBlockSize, maxBlockSize);
}

absl::StatusOr<char*> CheckedPgTextDatumGetCString(Datum datum) {
  // TextDatumGetCString is a macro, so we can't pass it directly to our error
  // wrapper. We pass the underlying real function instead, and add the
  // casts/lookup helpers that the macro uses.
  return ErrorCheckedPgCall(
      text_to_cstring, reinterpret_cast<const text*>(DatumGetPointer(datum)));
}

absl::StatusOr<char*> CheckedPgFormatTypeBe(Oid type_oid) {
  return ErrorCheckedPgCall(format_type_be, type_oid);
}

absl::StatusOr<ArrayIterator> CheckedPgArrayCreateIterator(
    ArrayType* arr, int slice_ndim, ArrayMetaState* mstate) {
  return ErrorCheckedPgCall(array_create_iterator, arr, slice_ndim, mstate);
}

absl::StatusOr<bool> CheckedPgArrayIterate(ArrayIterator iterator, Datum* value,
                                           bool* isnull) {
  return ErrorCheckedPgCall(array_iterate, iterator, value, isnull);
}

absl::StatusOr<ArrayType*> CheckedPgConstructMdArray(Datum* elems, bool* nulls,
                                                     int ndims, int* dims,
                                                     int* lbs, Oid elmtype,
                                                     int elmlen, bool elmbyval,
                                                     char elmalign) {
  return ErrorCheckedPgCall(construct_md_array, elems, nulls, ndims, dims, lbs,
                            elmtype, elmlen, elmbyval, elmalign);
}

absl::StatusOr<ArrayType*> CheckedPgConstructEmptyArray(Oid elmtype) {
  return ErrorCheckedPgCall(construct_empty_array, elmtype);
}

absl::Status CheckedPgGetTyplenbyvalalign(Oid typid, int16_t* typlen,
                                          bool* typbyval, char* typalign) {
  return ErrorCheckedPgCallNoReturnValue(get_typlenbyvalalign, typid, typlen,
                                         typbyval, typalign);
}

absl::StatusOr<ArrayType*> CheckedPgDatumGetArrayTypeP(Datum datum) {
  // DatumGetArrayTypeP is a macro to hide two c-style casts. Decompose it  into
  // casts and the real function call.
  struct varlena* datum_as_varlena =
      reinterpret_cast<struct varlena*>(DatumGetPointer(datum));
  ZETASQL_ASSIGN_OR_RETURN(void* p,
                   ErrorCheckedPgCall(pg_detoast_datum, datum_as_varlena));
  return reinterpret_cast<ArrayType*>(p);
}

absl::StatusOr<Type> CheckedPgTypeidType(Oid id) {
  return ErrorCheckedPgCall(typeidType, id);
}

// Simple test functions that generate a PostgreSQL error when called.
// These test functions are defined here because the template definition needs
// PostgreSQL headers, thus putting it in a header is unacceptable from a
// namespace cleanliness perspective.
// Optionally throw a PostgreSQL error at error_level" or return 0.

int TEST_PgErrorFunc(bool throw_error, int error_code, int error_level) {
  if (throw_error) {
    // Re-define ERROR locally to the value required by the ereport() macro,
    // rather than the value used by Google's logging infrastructure
    const int ERROR = PgErrorLevel::PG_ERROR;
    ereport(error_level, (errmsg("test error message"), errcode(error_code)));
  }
  return 0;
}
// Wrap the error_func in the error shim for desired error levels.
absl::StatusOr<int> TEST_CheckedError(bool throw_error, int error_code) {
  return ErrorCheckedPgCall(&TEST_PgErrorFunc, throw_error, error_code,
                            static_cast<int>(PgErrorLevel::PG_ERROR));
}
absl::StatusOr<int> TEST_CheckedFatal(bool throw_error, int error_code) {
  return ErrorCheckedPgCall(&TEST_PgErrorFunc, throw_error, error_code,
                            static_cast<int>(PgErrorLevel::PG_FATAL));
}
absl::StatusOr<int> TEST_CheckedPanic(bool throw_error, int error_code) {
  return ErrorCheckedPgCall(&TEST_PgErrorFunc, throw_error, error_code,
                            static_cast<int>(PgErrorLevel::PG_PANIC));
}
// Same as above but for the legacy-style 'elog' error mechanism. That mechanism
// doesn't understand sql error codes, so we don't supply one.
int TEST_PgElogErrorFunc(bool throw_error, int error_level) {
  if (throw_error) {
    // Re-define ERROR locally to the value required by the elog() macro,
    // rather than the value used by Google's logging infrastructure
    const int ERROR = PgErrorLevel::PG_ERROR;
    elog(error_level, "Test error message.");
  }
  return 0;
}
absl::StatusOr<int> TEST_CheckedElogError(bool throw_error) {
  return ErrorCheckedPgCall(&TEST_PgElogErrorFunc, throw_error,
                            static_cast<int>(PgErrorLevel::PG_ERROR));
}

absl::StatusOr<pg_tz*> CheckedPgTZSet(const char *tzname) {
  return ErrorCheckedPgCall(pg_tzset, tzname);
}

absl::Status CheckedPgTimezoneInitialize(void) {
  return ErrorCheckedPgCallNoReturnValue(pg_timezone_initialize);
}

absl::StatusOr<pg_tz*> CheckedPgTZOffsetSet(int32_t gmt_offset) {
  return ErrorCheckedPgCall(pg_tzset_offset, static_cast<long>(gmt_offset));
}

absl::StatusOr<int64_t> CheckedPgDefGetInt64(DefElem* def) {
  return ErrorCheckedPgCall(defGetInt64, def);
}

}  // namespace postgres_translator
