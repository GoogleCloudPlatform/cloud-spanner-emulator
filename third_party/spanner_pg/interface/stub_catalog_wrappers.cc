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

#include "third_party/spanner_pg/interface/catalog_wrappers.h"
#include "third_party/spanner_pg/postgres_includes/all.h"

const FormData_pg_type* GetTypeFromBootstrapCatalog(Oid type_id) {
  abort();
}

const FormData_pg_cast* GetCastFromBootstrapCatalog(Oid source_type_id,
                                                    Oid target_type_id) {
  abort();
}

const FormData_pg_proc* GetProcByOid(Oid oid) {
  abort();
}

void GetAmprocsByFamilyFromBootstrapCatalog(
    Oid opfamily, Oid lefttype, const FormData_pg_amproc* const** outlist,
    size_t* outcount) {
  abort();
}

void GetOpclassesByAccessMethodFromBootstrapCatalog(Oid am_id,
                                                    const Oid** opclasses,
                                                    size_t* opclass_count) {
  abort();
}

const FormData_pg_opclass* GetOpclassFromBootstrapCatalog(Oid opclass_id) {
  abort();
}

Oid GetNamespaceByNameFromBootstrapCatalog(const char* name) {
  abort();
}

void GetTypesByNameFromBootstrapCatalog(const char* name,
                                        const FormData_pg_type* const** outlist,
                                        size_t* outcount) {
  abort();
}

void GetAttributeTypeC(Oid relid, AttrNumber attnum, Oid* vartype,
                       int32_t* vartypmod, Oid* varcollid) {
  abort();
}

int GetColumnAttrNumber(Oid relid, const char* column_name) {
  abort();
}

void GetColumnNamesC(Oid relid, char*** real_colnames, int* ncolumns) {
  abort();
}

bool IsAttributePseudoColumnC(Oid relid, AttrNumber attnum) {
  abort();
}

char* GetTableNameC(Oid relid) {
  abort();
}

const FormData_pg_aggregate* GetAggregateFromBootstrapCatalog(Oid agg_id) {
  abort();
}

bool ShouldCoerceUnknownLiterals() {
  abort();
}

int GetFunctionArgInfo(Oid proc_oid, Oid** p_argtypes, char*** p_argnames,
                       char** p_argmodes) {
  abort();
}

void ExpandRelationC(Oid relid, Alias* eref, int rtindex, int sublevels_up,
                     int location, List** colnames, List** colvars) {
  abort();
}

List* ExpandNSItemVarsForJoinC(const List* rtable, ParseNamespaceItem* nsitem,
                               int sublevels_up, int location, List** colnames,
                               bool* error) {
  abort();
}

const FormData_pg_amop* GetAmopByFamilyFromBootstrapCatalog(Oid opfamily,
                                                            Oid lefttype,
                                                            Oid righttype,
                                                            int16_t strategy) {
  abort();
}

void GetAmopsByAmopOpIdFromBootstrapCatalog(
    Oid opid, const FormData_pg_amop* const** outlist, size_t* outcount) {
  abort();
}

const FormData_pg_amproc* GetAmprocByFamilyFromBootstrapCatalog(Oid opfamily,
                                                                Oid lefttype,
                                                                Oid righttype,
                                                                int16_t index) {
  abort();
}

const FormData_pg_operator* GetOperatorFromBootstrapCatalog(Oid operator_id) {
  abort();
}

char* GetAttributeNameC(Oid relid, AttrNumber attnum, bool missing_ok) {
  abort();
}

Oid GetOrGenerateOidFromNamespaceOidAndRelationNameC(
    Oid namespace_oid, const char* unqualified_table_name) {
  abort();
}

Oid GetOrGenerateOidFromTableNameC(const char* unqualified_table_name) {
  abort();
}

Oid GetOidFromNamespaceNameC(const char* unqualified_namespace_name) {
  abort();
}

Oid GetOrGenerateOidFromNamespaceNameC(const char* unqualified_namespace_name) {
  abort();
}

void GetProcsByName(const char* name, const FormData_pg_proc*** outlist,
                    size_t* outcount) {
  abort();
}

void GetOperatorsByNameFromBootstrapCatalog(const char* name,
                                            const Oid** outlist,
                                            size_t* outcount) {
  abort();
}

Oid GetCollationOidByNameFromBootstrapCatalog(const char* name) {
  abort();
}

char* GetNamespaceNameByOidFromBootstrapCatalog(Oid namespace_oid) {
  abort();
}

char* GetNamespaceNameC(Oid relid) {
  abort();
}

RangeTblEntry* AddRangeTableEntryC(ParseState* pstate, RangeVar* relation,
                                   Alias* alias, bool inh, bool inFromCl) {
  abort();
}

void GetColumnTypesC(Oid relid, List** coltypes, List** coltypmods,
                     List** colcollations, int* ncolumns) {
  abort();
}

RangeTblEntry* AddRangeTableEntryByOidC(ParseState* pstate, Oid relation_oid,
                                        Alias* alias, bool inh, bool inFromCl) {
  abort();
}
