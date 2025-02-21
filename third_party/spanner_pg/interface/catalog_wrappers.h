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

#ifndef INTERFACE_CATALOG_WRAPPERS_H_
#define INTERFACE_CATALOG_WRAPPERS_H_

#include "third_party/spanner_pg/postgres_includes/all.h"

#ifdef __cplusplus
// This file is meant to be included from both C and C++ code.
// In C++, it has a number of direct accessors and other functionality
// that depends on C++-specific objects, classes, etc.
// In C, those direct accessors are not available.  Only helper/wrapper
// functions are available.
//
// These functions may throw PostgreSQL exceptions (ereport)! Callers from
// outside PostgreSQL code should handle the exceptions or choose an API that
// uses Google3 Status for error handling.

#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/table_name.h"
// This file was formerly transformation functions common to both the PostgreSQL
// analyzer and our transformer. Most functions have been moved into the
// transformer.
// TODO: Merge this file into catalog_wrappers.
namespace postgres_translator {

// Given a PostgreSQL RangeVar representing a Table object, gets a TableName
// representing the qualified name per PostgreSQL namespacing rules
// (<catalogname>.<schemaname>.<relname>).
absl::StatusOr<TableName> TableNameFromRangeVar(RangeVar& relation);

}  // namespace postgres_translator

extern "C" {
#endif  // ifdef __cplusplus

// Construct a RangeTblEntry for the specified relation,
// by looking up its content in the Engine User Catalog.
// If the relation does not already have an assigned Oid, the transformer will
// ask the Catalog Adapter to generate one.
// Arguments are intended to be passed through directly from the
// PostgreSQL function `addRangeTableEntry()`.
// This function throws exceptions on table-not-found failure. Logs the error
// and returns nullptr on all other errors.
RangeTblEntry* AddRangeTableEntryC(ParseState* pstate, RangeVar* relation,
                                   Alias* alias, bool inh, bool inFromCl);

// Construct a RangeTblEntry for the specified relation oid,
// by looking up its content in the Engine User Catalog.
// The relation must already have an assigned Oid from the Catalog Adapter.
// Arguments are intended to be passed through directly from the
// PostgreSQL function `addRangeTableEntryByOid()`.
// If `nullptr` is returned, then `*error_msg` will be set to a non-null
// error message value.  The value is palloc()'ed.  The caller is responsible
// for pfree()'ing it.
RangeTblEntry* AddRangeTableEntryByOidC(ParseState* pstate, Oid relation_oid,
                                        Alias* alias, bool inh, bool inFromCl);

// Given an oid, looks up a table name in the thread-local catalog adapter and
// then the table in the googlesql catalog, copying the column name string into
// Postgres' thread-local CurrentMemoryContext. This function exists to give
// get_attname access to the C++-only thread-local catalog adapter and
// googlesql catalog.
// RETURNS a palloc'd copy of the name string or nullptr if not found.
char* GetAttributeNameC(Oid relid, AttrNumber attnum, bool missing_ok);

int GetFunctionArgInfo(Oid proc_oid, Oid **p_argtypes, char ***p_argnames,
                       char **p_argmodes);

// Given an oid identifying a relation and an attribute number, gets the type
// information (type oid, typemod, and collation) for that column and returns
// it.
// REQUIRES this table is known to catalog adapter for Oid lookup.
// RETURNS (via outargs) InvalidOid/-1 if the table or column is not found.
void GetAttributeTypeC(Oid relid, AttrNumber attnum, Oid* vartype,
                       int32_t* vartypmod, Oid* varcollid);

// Given an oid identifying a relation and an attribute number, returns true if
// the column is a pseudo column. Returns an error if the table or column is not
// found.
bool IsAttributePseudoColumnC(Oid relid, AttrNumber attnum);

// Given an oid, looks up a table's unqualified name or namespace's name in the
// thread-local catalog adapter.
// RETURNS a palloc'd copy of the name string or nullptr if not found.
char* GetTableNameC(Oid relid);
// Given a *table* oid, looks up the table's namespace and returns the name.
// RETURNS a palloc'd copy of the namespace name or nullptr if not found.
char* GetNamespaceNameC(Oid relid);

// Given an oid, looks up a table and its columns info in the thread-local
// catalog adapter and ZetaSQL catalog. Updates ncolumns with the correct
// number of columns and real_colnames with palloc'd copy of the column name
// strings. THIS FUNCTION THROWS EXCEPTIONS if it fails to get the column names.
void GetColumnNamesC(Oid relid, char*** real_colnames, int* ncolumns);

// Given an oid, looks up a table and its columns info in the thread-local
// catalog adapter and ZetaSQL catalog. Updates ncolumns with the correct
// number of columns and coltypes/coltypmods/colcollations with the column type
// information. Uses InvalidOid if the column type is unsupported.
// THIS FUNCTION THROWS EXCEPTIONS if it fails to get the column types.
void GetColumnTypesC(Oid relid, List** coltypes, List** coltypmods,
                     List** colcollations, int* ncolumns);

// Given an oid and column name, looks up a table and its columns info in the
// thread-local catalog adapter and ZetaSQL catalog.
// Returns the column attr number of the column name or InvalidAttrNumber if the
// column or table don't exist.
int GetColumnAttrNumber(Oid relid, const char* column_name);

// Wrapper functions for Bootstrap Catalog to be called directly from PostgreSQL
// source or catalog shim C functions. If BootstrapCatalog returns an error,
// these return nullptr, which is equivalent to a HeapTuple lookup failure.
const FormData_pg_cast* GetCastFromBootstrapCatalog(Oid source_type_id,
                                                    Oid target_type_id);
const FormData_pg_type* GetTypeFromBootstrapCatalog(Oid type_id);
const FormData_pg_operator* GetOperatorFromBootstrapCatalog(Oid operator_id);
const FormData_pg_aggregate* GetAggregateFromBootstrapCatalog(Oid agg_id);
const FormData_pg_opclass* GetOpclassFromBootstrapCatalog(Oid opclass_id);
Oid GetNamespaceByNameFromBootstrapCatalog(const char* name);
Oid GetCollationOidByNameFromBootstrapCatalog(const char* name);
char* GetNamespaceNameByOidFromBootstrapCatalog(Oid namespace_oid);

// These functions get a list of results matching a name by returning a pointer
// to <result>* and a length to avoid copying the Span data into a new temporary
// data structure. This follows the error -> return nullptr/zero semantics of
// the other functions in this set.
void GetOperatorsByNameFromBootstrapCatalog(const char* name,
                                            const Oid** outlist,
                                            size_t* outcount);
void GetTypesByNameFromBootstrapCatalog(const char* name,
                                        const FormData_pg_type* const** outlist,
                                        size_t* outcount);
// Like above, but retrieve all opclasses matching our access method (`am_id`).
void GetOpclassesByAccessMethodFromBootstrapCatalog(Oid am_id,
                                                    const Oid** opclasses,
                                                    size_t* opclass_count);
// Gets the Access Method Operator (amop), keyed by family, argument types, and
// strategy.
const FormData_pg_amop* GetAmopByFamilyFromBootstrapCatalog(Oid opfamily,
                                                            Oid lefttype,
                                                            Oid righttype,
                                                            int16_t strategy);
// Gets a list of Access Method Operators (amop) matching an operator oid by
// returning a pointer to FormData_pg_amop* and a length to avoid copying the
// Span data into a new temporary data structure. This follows the error ->
// return nullptr/zero semantics of the other functions in this set.
void GetAmopsByAmopOpIdFromBootstrapCatalog(
    Oid opid, const FormData_pg_amop* const** outlist, size_t* outcount);
// Gets the Access Method Procedure (amproc), keyed by family, input data types,
// and support procedure index.
const FormData_pg_amproc* GetAmprocByFamilyFromBootstrapCatalog(Oid opfamily,
                                                                Oid lefttype,
                                                                Oid righttype,
                                                                int16_t index);
// Gets a list of Access Method Procedures (amproc), keyed by family and input
// data type.
void GetAmprocsByFamilyFromBootstrapCatalog(
    Oid opfamily, Oid lefttype, const FormData_pg_amproc* const** outlist,
    size_t* outcount);

// Replaces both of PostgreSQL's expandRelation and expandTupleDesc together for
// looking up column names for a user table.
// Excludes columns marked `IsPseudoColumn`.
// Gets the table from CatalogAdapter and converts its types into PostgreSQL
// type Oids.
//
// THIS FUNCTION THROWS EXCEPTIONS. To pass detailed error information, this
// function throws in error cases.
void ExpandRelationC(Oid relid, Alias* eref, int rtindex, int sublevels_up,
                     int location, List** colnames, List** colvars);

// C++ wrapper for expandNSItemVars for joins. Excludes pseudo columns.
List* ExpandNSItemVarsForJoinC(const List* rtable, ParseNamespaceItem* nsitem,
                               int sublevels_up, int location, List** colnames,
                               bool* error);

// C++ wrappers for various CatalogAdapter functions with corresponding names.
// This is used primarily for PostgreSQL error reporting.
Oid GetOrGenerateOidFromTableNameC(const char* unqualified_table_name);
Oid GetOidFromNamespaceNameC(const char* unqualified_namespace_name);
Oid GetOrGenerateOidFromNamespaceNameC(const char* unqualified_namespace_name);
Oid GetOrGenerateOidFromNamespaceOidAndRelationNameC(
    Oid namespace_oid, const char* unqualified_table_name);

// Looks up a set of functions by name from both bootstrap (for built in
// functions) and the user catalog (for user-defined functions).
void GetProcsByName(const char* name, const FormData_pg_proc*** outlist,
                    size_t* outcount);

// Complement to above, looks up a proc by Oid from both catalogs. For UDF procs
// the proc must have already been looked up (and thus generated) by name.
// Returns NULL on lookup failure.
const FormData_pg_proc* GetProcByOid(Oid oid);

// Flags accessed in shims
bool ShouldCoerceUnknownLiterals();

#ifdef __cplusplus
}
#endif  // ifdef __cplusplus

#endif  // INTERFACE_CATALOG_WRAPPERS_H_
