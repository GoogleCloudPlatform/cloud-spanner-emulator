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

#include <stdlib.h>

#include "third_party/spanner_pg/shims/catalog_shim.h"

// This file defines stubs of catalog-shim wrapper methods.
// They are intended to be linked against PostgreSQL when we know we don't want
// to call any methods that will use the catalog, but when untangling the
// method that we do want to call out of the ball of yarn that is PostgreSQL's
// linker dependencies would disrupt the code more than it's worth.

ParseNamespaceItem* addRangeTableEntry(ParseState* pstate,
                                       RangeVar* relation,
                                       Alias* alias, bool inh,
                                       bool inFromCl) {
  abort();
}

int scanRTEForColumn(ParseState* pstate, RangeTblEntry* rte, Alias* eref,
                     const char* colname, int location, int fuzzy_rte_penalty,
                     FuzzyAttrMatchState* fuzzystate) {
  abort();
}

bool get_rte_attribute_is_dropped(RangeTblEntry* rte,
                                  AttrNumber attnum) {
  abort();
}

Oid getBaseTypeAndTypmod(Oid typid, int32_t *typmod) {
  abort();
}

void setup_parser_errposition_callback(ParseCallbackState* pcbstate,
                                       ParseState* pstate,
                                       int location) {
  abort();
}

void cancel_parser_errposition_callback(ParseCallbackState* pcbstate) {
  abort();
}

CoercionPathType find_coercion_pathway(Oid targetTypeId,
                                       Oid sourceTypeId,
                                       CoercionContext ccontext,
                                       Oid* funcid) {
  abort();
}

bool can_coerce_type(int nargs, const Oid* input_typeids,
                     const Oid* target_typeids,
                     CoercionContext ccontext) {
  abort();
}

Oid get_role_oid(const char *rolname, bool missing_ok) {
  abort();
}

char* format_type_extended(Oid type_oid, int32_t typemod, bits16 flags)
{
  abort();
}

bool TypeIsVisible(Oid typid) {
  abort();
}

void ReleaseCatCache(HeapTuple tuple) {
  abort();
}

Type typeidType(Oid id) {
  abort();
}

Type LookupTypeNameExtended(ParseState* pstate,
                            const TypeName* typeName, int32_t* typmod_p,
                            bool temp_ok, bool missing_ok) {
  abort();
}

Node* build_coercion_expression(Node* node, CoercionPathType pathtype,
                                Oid funcId, Oid targetTypeId,
                                int32_t targetTypMod,
                                CoercionContext ccontext,
                                CoercionForm cformat, int location) {
  abort();
}

Oid get_typcollation(Oid typid) {
  abort();
}

char* get_attname(Oid relid, AttrNumber attnum, bool missing_ok) {
  abort();
}

FuncCandidateList FuncnameGetCandidates(List* names, int nargs, List* argnames,
                                        bool expand_variadic,
                                        bool expand_defaults,
                                        bool include_out_arguments,
                                        bool missing_ok) {
  abort();
}

void fmgr_info_cxt_security(Oid functionId, FmgrInfo* finfo,
                            MemoryContext mcxt, bool ignore_security) {
  abort();
}

void get_type_category_preferred(Oid typid, char* typcategory,
                                 bool* typispreferred) {
  abort();
}

void get_type_io_data(Oid typid,
					  IOFuncSelector which_func,
					  int16_t* typlen,
					  bool* typbyval,
					  char* typalign,
					  char* typdelim,
					  Oid* typioparam,
					  Oid* func) {
  abort();
}

void AcquireRewriteLocks(Query* parsetree, bool forExecute,
                         bool forUpdatePushedDown) {
  abort();
}

FuncCandidateList OpernameGetCandidates(List* names, char oprkind,
                                        bool missing_schema_ok) {
  abort();
}

Oid OpernameGetOprid(List* names, Oid oprleft, Oid oprright) {
  abort();
}

Operator oper(ParseState* pstate, List* opname, Oid ltypeId,
              Oid rtypeId, bool noError, int location) {
  abort();
}

Operator left_oper(ParseState* pstate, List* op, Oid arg, bool noError,
                   int location) {
  abort();
}

void get_oper_expr(OpExpr* expr, deparse_context* context) {
  abort();
}

Oid get_func_variadictype(Oid funcid) {
  abort();
}

bool get_func_retset(Oid funcid) {
  abort();
}

char* get_rel_name(Oid relid) {
  abort();
}

Oid typeidTypeRelid(Oid type_id)
{
  abort();
}

Oid typeOrDomainTypeRelid(Oid type_id) {
  abort();
}

char get_typtype(Oid typid) {
  abort();
}

void set_relation_column_names(deparse_namespace* dpns,
                                        RangeTblEntry* rte,
                                        deparse_columns* colinfo) {
  abort();
}

char* generate_relation_name(Oid relid, List* namespaces) {
  abort();
}

void getTypeOutputInfo(Oid typid, Oid* typOutput, bool* typIsVarlena) {
  abort();
}

Oid get_element_type(Oid typid) {
  abort();
}

void getTypeInputInfo(Oid type, Oid* typInput, Oid* typIOParam) {
  abort();
}

FuncDetailCode func_get_detail(List* funcname, List* fargs, List* fargnames,
                               int nargs, Oid* argtypes, bool expand_variadic,
                               bool expand_defaults, bool include_out_arguments,
                               Oid* funcid, Oid* rettype, bool* retset,
                               int* nvargs, Oid* vatype, Oid** true_typeids,
                               List** argdefaults) {
  abort();
}

Node* ParseFuncOrColumn(ParseState* pstate, List* funcname, List* fargs,
                        Node* last_srf, FuncCall* fn, bool proc_call, int location) {
  abort();
}

char* generate_operator_name(Oid operid, Oid arg1, Oid arg2) {
  abort();
}

char*
generate_function_name(Oid funcid, int nargs, List* argnames,
                       Oid* argtypes, bool has_variadic,
                       bool* use_variadic_p,
                       ParseExprKind special_exprkind) {
  abort();
}

int setTargetTable(ParseState* pstate, RangeVar* relation,
                   bool inh, bool alsoSource, AclMode requiredPerms) {
  abort();
}

void
transformOnConflictArbiter(ParseState* pstate,
                           OnConflictClause* onConflictClause,
                           List** arbiterExpr, Node** arbiterWhere,
                           Oid* constraint) {
  abort();
}

List* resolve_unique_index_expr(
    ParseState* pstate, InferClause* infer) {
  abort();
}

Node *transformFrameOffset(ParseState *pstate, int frameOptions,
                           Oid rangeopfamily, Oid rangeopcintype,
                           Oid *inRangeFunc, Node *clause) {
  abort();
}

void free_parsestate(ParseState* pstate) {
  abort();
}

List* transformUpdateTargetList(ParseState* pstate, List* origTlist) {
  abort();
}

OnConflictExpr* transformOnConflictClause(
    ParseState* pstate, OnConflictClause* onConflictClause) {
  abort();
}

Expr* transformAssignedExpr(ParseState* pstate, Expr* expr,
                            ParseExprKind exprKind, char* colname,
                            int attrno, List* indirection,
                            int location) {
  abort();
}

List* checkInsertTargets(ParseState* pstate, List* cols, List** attrnos) {
  abort();
}

TypeCacheEntry* lookup_type_cache(Oid type_id, int flags) {
  abort();
}

void assign_record_type_typmod(TupleDesc tupDesc) {
  abort();
}

Oid get_array_type(Oid typid) {
  abort();
}

void expandRelation(Oid relid, Alias* eref,
                    int rtindex, int sublevels_up,
                    int location, bool include_dropped,
                    List** colnames, List** colvars) {
  abort();
}

TypeFuncClass internal_get_result_type(Oid funcid, Node* call_expr,
                                       ReturnSetInfo* rsinfo,
                                       Oid* resultTypeId,
                                       TupleDesc* resultTupleDesc) {
  abort();
}

bool IsBinaryCoercible(Oid srctype, Oid targettype) {
  abort();
}

Oid GetDefaultOpClass(Oid type_id, Oid am_id) {
  abort();
}

List* get_op_btree_interpretation(Oid opno) {
  abort();
}

Oid get_opclass_family(Oid opclass) {
  abort();
}

Oid get_opclass_input_type(Oid opclass) {
  abort();
}

Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype,
                        int16_t strategy) {
  abort();
}

bool get_ordering_op_properties(Oid opno, Oid *opfamily, Oid *opcintype,
                                int16_t *strategy) {
  abort();
}

Oid get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16_t procnum)
{
  abort();
}

RegProcedure get_opcode(Oid opno) {
  abort();
}

LockAcquireResult LockAcquireExtended(const LOCKTAG* locktag,
                                      LOCKMODE lockmode,
                                      bool sessionLock, bool dontWait,
                                      bool reportMemoryError,
                                      LOCALLOCK** locallockp) {
  abort();
}

void InitializeGUCOptions(void) {
  abort();
}

Oid get_range_subtype(Oid rangeOid) {
  abort();
}

Oid get_base_element_type(Oid typid) {
  abort();
}

void get_utility_query_def(Query *query, deparse_context *context) {
  abort();
}

Const* make_const(ParseState* pstate, Value* value, int location) {
  abort();
}

Bitmapset* get_primary_key_attnos(Oid relid, bool deferrableOk,
                                  Oid* constraintOid) {
  abort();
}

Oid RelnameGetRelid(const char* relname) {
  abort();
}

Oid get_relname_relid(const char* relname, Oid namespace_oid) {
  abort();
}

Oid get_rel_type_id(Oid relid) {
  abort();
}

Oid get_namespace_oid(const char* nspname, bool missing_ok) {
  abort();
}

Oid get_database_oid(const char* dbname, bool missing_ok) {
  abort();
}

Oid get_commutator(Oid opno) {
  abort();
}

Oid get_negator(Oid opno) {
  abort();
}

CoercionPathType find_typmod_coercion_function(Oid typeId, Oid* funcid)
{
  abort();
}

void get_delete_query_def(Query* query, deparse_context* context,
                          bool colNamesVisible) {
  abort();
}

void get_update_query_def(Query* query, deparse_context* context,
                          bool colNamesVisible) {
  abort();
}

bool RecordJoinInputsSpangres(RangeTblEntry* join_rte, Oid left_input,
                              int left_rtindex, Oid right_input,
                              int right_rtindex) {
  abort();
}

Node* transformSpangresHint(ParseState* pstate, DefElem* elem) {
  abort();
}

List* expandNSItemVars(ParseState* pstate, ParseNamespaceItem* nsitem,
                       int sublevels_up, int location, List** colnames) {
  abort();
}

void get_hint_list_def(List* hints, deparse_context* context, bool statement) {
  abort();
}

void get_typlenbyvalalign(Oid typid, int16_t* typlen, bool* typbyval,
                          char* typalign) {
  abort();
}

void transformContainerType(Oid *containerType, int32_t *containerTypmod) {
  abort();
}

SubscriptingRef* transformContainerSubscripts(
    ParseState* pstate, Node* containerBase, Oid containerType,
    int32_t containerTypMod, List* indirection, bool isAssignment) {
  abort();
}

char* get_func_result_name(Oid functionId) {
  abort();
}

void TupleDescInitEntry(TupleDesc desc, AttrNumber attributeNumber,
                        const char* attributeName, Oid oidtypeid, int32_t typmod,
                        int attdim) {
  abort();
}

Oid LookupExplicitNamespace(const char *nspname, bool missing_ok) {
  abort();
}

Node* transformParamRef(ParseState *pstate, ParamRef *pref) {
  abort();
}

void get_setop_query(Node *setOp, Query *query, deparse_context *context,
                     TupleDesc resultDesc, bool colNamesVisible) {
  abort();
}

Oid lookup_collation(const char *collname, Oid collnamespace, int32_t encoding) {
  abort();
}

Oid get_collation_oid(List *name, bool missing_ok) {
  abort();
}

Datum date_in(PG_FUNCTION_ARGS) {
  abort();
}

Datum timestamptz_in(PG_FUNCTION_ARGS) {
  abort();
}

RegProcedure get_typsubscript(Oid typid, Oid *typelemp) {
  abort();
}

Oid get_multirange_range(Oid multirangeOid) {
  abort();
}

int
DecodeDateTime(char **field, int *ftype, int nf,
               int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp) {
  abort();
}

int
DecodeTimezoneAbbrev(int field, char *lowtoken,
					 int *offset, pg_tz **tz) {
  abort();
}
