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

#include "third_party/spanner_pg/shims/catalog_shim_cc_wrappers.h"

#include <cstring>
#include <string>
#include <vector>

#include "zetasql/public/catalog.h"
#include "zetasql/public/function_signature.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/bootstrap_catalog/bootstrap_catalog.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"
#include "third_party/spanner_pg/catalog/udf_support.h"
#include "third_party/spanner_pg/datetime_parsing/datetime_exports.h"
#include "third_party/spanner_pg/errors/error_catalog.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim.h"
#include "third_party/spanner_pg/shims/catalog_shim_transforms.h"
#include "third_party/spanner_pg/transformer/transformer.h"
#include "third_party/spanner_pg/util/pg_list_iterators.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

// Examine the status to determine if the message is cloud safe (usually
// kUnimplemented errors) and can be passed to PostgreSQL and Cloud Spanner
// customers via the desired error code. If the message is not cloud safe,
// use ERRCODE_INTERNAL_ERROR so that the actual error is logged but a generic
// internal error is returned to the customer.
//
// TODO: Employ a better error-conversion strategy here.
// Note that desired_error_code should not be ERRCODE_FEATURE_NOT_SUPPORTED
// because those get turned into Http501 errors and we don't want that.
// TODO: Return the right error code here.
void ereport_helper(const absl::Status& status, int desired_error_code,
                    const std::string& error_message) {
  // Define the ERROR symbol locally as it is required by PostgreSQL's macro.
  constexpr int ERROR = PG_ERROR;
  if (status.code() == absl::StatusCode::kUnimplemented) {
    ereport(ERROR, (errcode(desired_error_code),
                    errmsg("%s: %s", error_message.c_str(), status.message())));
  } else {
    // Other error codes get converted to a generic internal error
    // (cloud unsafe).
    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                    errmsg("%s: %s", error_message.c_str(),
                           status.ToString().c_str())));
  }
}

// Construct a RangeTblEntry for the specified relation,
// by looking up its content in the Engine User Catalog.
// If the relation does not already have an assigned Oid, the transformer will
// ask the Catalog Adapter to generate one.
// Arguments are intended to be passed through directly from the
// PostgreSQL function `addRangeTableEntrySpangres()`.
absl::StatusOr<RangeTblEntry*> AddRangeTableEntryCpp(ParseState* pstate,
                                                     RangeVar* relation,
                                                     Alias* alias, bool inh,
                                                     bool inFromCl) {
  if (relation == nullptr) {
    return absl::InternalError("No RangeVar specified for catalog lookup.");
  }
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_RET_CHECK_NE(relation, nullptr);
  ZETASQL_ASSIGN_OR_RETURN(TableName table_name, TableNameFromRangeVar(*relation));
  // Round-tripping this through CatalogAdapter below will strip any catalogname
  // so check here and error if the user tried to supply one.
  if (relation->catalogname != nullptr) {
    return absl::InvalidArgumentError(
        absl::StrCat("cross-database references are not implemented: \"",
                     table_name.ToString(), "\""));
  }
  RangeTblEntry* rte;
  ZETASQL_ASSIGN_OR_RETURN(Oid oid,
                   catalog_adapter->GetOrGenerateOidFromTableName(table_name));
  ZETASQL_ASSIGN_OR_RETURN(
      rte, Transformer::BuildPgRangeTblEntry(*catalog_adapter, oid, alias,
                                             inFromCl, ACL_SELECT));
  // The Transformer defaults to inh==true (allow implicit inheritence)
  // because there's no ZetaSQL concept of inheritence to map on to it. If
  // someone actually specified it in SQL, preserve that choice here.
  rte->inh = inh;
  // Add completed RTE to pstate's range table list, so that we know its
  // index.  But we don't add it to the join list --- caller must do that if
  // appropriate.
  pstate->p_rtable = lappend(pstate->p_rtable, rte);

  return rte;
}

// Construct a RangeTblEntry for the specified relation oid,
// by looking up its content in the Engine User Catalog.
// The relation must already have an assigned Oid from the Catalog Adapter.
// Arguments are intended to be passed through directly from the
// PostgreSQL function `addRangeTableEntryByOid()`.
absl::StatusOr<RangeTblEntry*> AddRangeTableEntryByOidCpp(ParseState* pstate,
                                                          Oid relation_oid,
                                                          Alias* alias,
                                                          bool inh,
                                                          bool inFromCl) {
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  RangeTblEntry* rte;
  ZETASQL_ASSIGN_OR_RETURN(
      rte, Transformer::BuildPgRangeTblEntry(*catalog_adapter, relation_oid,
                                             alias, inFromCl, ACL_SELECT));
  // Additionally add the new RTE to ParseState's range table list.
  pstate->p_rtable = lappend(pstate->p_rtable, rte);
  return rte;
}

// Helper function for table lookup via catalog adapter and user catalog (both
// accessed from thread-local storage).
static absl::StatusOr<const zetasql::Table*> GetTableByOid(Oid table_oid) {
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSIGN_OR_RETURN(TableName table_name,
                   catalog_adapter->GetTableNameFromOid(table_oid));
  const zetasql::Table* table;
  ZETASQL_RET_CHECK_OK(catalog_adapter->GetEngineUserCatalog()->FindTable(
      table_name.AsSpan(), &table));
  return table;
}

// C++ helper to GetAttributeNameC and other similar functions in this file.
// Gets column->name from table and returns a pstrdup'd copy of the name.
static absl::StatusOr<char*> GetAttributeNameCpp(const zetasql::Table& table,
                                                 AttrNumber attnum) {
  const int attribute_offset = attnum - 1;  // attnums are 1-indexed.
  ZETASQL_RET_CHECK_GE(attribute_offset, 0);
  ZETASQL_RET_CHECK_LE(attribute_offset, table.NumColumns());
  const zetasql::Column* column = table.GetColumn(attribute_offset);
  return pstrdup(column->Name().c_str());
}

// Helper for `GetAttributeNameC` so that the two places an error can be
// generated get handled once in GetAttributeNameC.
absl::StatusOr<char*> GetAttnameHelper(Oid relid, AttrNumber attnum) {
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableByOid(relid));
  return GetAttributeNameCpp(*table, attnum);
}

// C++ helper to GetAttributeTypeC below. Gets column->type from table and
// populates outargs with translated type information.
static absl::Status GetAttributeTypeCpp(const zetasql::Table& table,
                                        AttrNumber attnum, Oid* vartype,
                                        int32_t* vartypmod, Oid* varcollid) {
  const int attribute_offset = attnum - 1;  // attnums are 1-indexed.
  ZETASQL_RET_CHECK_GE(attribute_offset, 0);
  ZETASQL_RET_CHECK_LT(attribute_offset, table.NumColumns());
  const zetasql::Column* column = table.GetColumn(attribute_offset);
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSIGN_OR_RETURN(*vartype, Transformer::BuildPgTypeOid(*catalog_adapter,
                                                         column->GetType()));
  ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_type* pg_type,
                   PgBootstrapCatalog::Default()->GetType(*vartype));
  *vartypmod = pg_type->typtypmod;
  *varcollid = pg_type->typcollation;
  return absl::OkStatus();
}

// C++ helper to IsAttributePseudoColumnC below. Returns true if the column is a
// pseudo column.
static absl::StatusOr<bool> IsAttributePseudoColumnCpp(Oid relid,
                                                       AttrNumber attnum) {
  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableByOid(relid));
  const int attribute_offset = attnum - 1;  // attnums are 1-indexed.
  ZETASQL_RET_CHECK_GE(attribute_offset, 0);
  ZETASQL_RET_CHECK_LT(attribute_offset, table->NumColumns());
  return table->GetColumn(attribute_offset)->IsPseudoColumn();
}

// C++ helper to GetTableNameC below. Looks up table name from Oid in catalog
// adapter and returns a palloc'd C-style string of it.
// TODO: This copies the name up to *3* times. We can do better.
static absl::StatusOr<char*> GetTableNameCpp(Oid table_oid) {
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSIGN_OR_RETURN(TableName table_name,
                   catalog_adapter->GetTableNameFromOid(table_oid));
  // Ensure we have a null terminator because pstrdup requires it.
  return pstrdup(std::string(table_name.UnqualifiedName()).c_str());
}

static absl::StatusOr<char*> GetNamespaceNameCpp(Oid table_oid) {
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  ZETASQL_ASSIGN_OR_RETURN(TableName table_name,
                   catalog_adapter->GetTableNameFromOid(table_oid));
  const std::string* namespace_name = table_name.NamespaceName();
  if (namespace_name == nullptr) return nullptr;
  // Ensure we have a null terminator because pstrdup requires it.
  return pstrdup(namespace_name->c_str());
}

// C++ helper to GetColumnNamesC below. Looks up table name from Oid in catalog
// adapter, finds it in the engine user catalog for its columns info and updates
// `real_colnames` and `ncolumns` accordingly.
//
// `real_colnames` contains palloc'd copies of the column name strings or
// nullptr if a table is not found.
// `ncolumns` contains the actual number of table columns or 0 if a table is not
// found.
static absl::Status GetColumnNamesCpp(Oid table_oid, char*** real_colnames,
                                      int* ncolumns) {
  // Initialize the output, so they hold empty values in case of error.
  *ncolumns = 0;
  *real_colnames = nullptr;

  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableByOid(table_oid));
  ZETASQL_RET_CHECK_NE(table, nullptr);
  *ncolumns = table->NumColumns();
  *real_colnames = reinterpret_cast<char**>(palloc(*ncolumns * sizeof(char*)));
  for (int i = 0; i < table->NumColumns(); ++i) {
    (*real_colnames)[i] = pstrdup(table->GetColumn(i)->Name().c_str());
  }
  return absl::OkStatus();
}

// C++ helper to GetColumnTypesC below. Looks up table name from Oid in catalog
// adapter, finds it in the engine user catalog for its columns info and updates
// `coltypes`, `coltypmods`, `colcollations`, and `ncolumns`
// accordingly.
//
// `coltypes` contains a list of the column type oids. If a column type is not
// supported, `coltypes` will have InvalidOid for that column.
// `coltypmods` contains a list of the column type mods.
// `colcollations` contains a list of the column type collations.
// `ncolumns` contains the actual number of table columns or 0 if a table is not
// found.
static absl::Status GetColumnTypesCpp(Oid table_oid, List** coltypes,
                                      List** coltypmods, List** colcollations,
                                      int* ncolumns) {
  // Initialize the output, so they hold empty values in case of error.
  *ncolumns = 0;
  *coltypes = nullptr;
  *coltypmods = nullptr;
  *colcollations = nullptr;

  ZETASQL_ASSIGN_OR_RETURN(const zetasql::Table* table, GetTableByOid(table_oid));
  ZETASQL_RET_CHECK_NE(table, nullptr);
  *ncolumns = table->NumColumns();

  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  for (int i = 0; i < table->NumColumns(); ++i) {
    absl::StatusOr<Oid> col_type_or = Transformer::BuildPgTypeOid(
        *catalog_adapter, table->GetColumn(i)->GetType());
    // TODO: return an error if there are columns with unsupported
    // types.
    if (col_type_or.ok()) {
      *coltypes = lappend_oid(*coltypes, *col_type_or);
      ZETASQL_ASSIGN_OR_RETURN(const FormData_pg_type* pg_type,
                       PgBootstrapCatalog::Default()->GetType(*col_type_or));
      *coltypmods = lappend_oid(*coltypmods, pg_type->typtypmod);
      *colcollations = lappend_oid(*colcollations, pg_type->typcollation);
    } else {
      *coltypes = lappend_oid(*coltypes, InvalidOid);
      *coltypmods = lappend_oid(*coltypmods, -1);
      *colcollations = lappend_oid(*colcollations, 1);
    }
  }
  return absl::OkStatus();
}

// C++ helper to ExpandRelationC below. Looks up table name from Oid in catalog
// adapter and populates out args with column information.
// Note that callers may pass nullptr for either outarg to indicate those values
// are not required.
static absl::Status ExpandRelationCpp(Oid relid, Alias* eref, int rtindex,
                                      int sublevels_up, int location,
                                      List** colnames, List** colvars) {
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  EngineUserCatalog* catalog = catalog_adapter->GetEngineUserCatalog();
  if (catalog == nullptr) {
    return absl::InternalError(
        "Googlesql catalog not initialized on this thread.");
  }
  ZETASQL_ASSIGN_OR_RETURN(TableName table_name,
                   catalog_adapter->GetTableNameFromOid(relid));
  const zetasql::Table* user_table = nullptr;
  ZETASQL_RETURN_IF_ERROR(catalog->FindTable(table_name.AsSpan(), &user_table));
  ZETASQL_RET_CHECK_NE(user_table, nullptr)
      << "User table " << table_name << " not found.";

  // Also iterate through eref if one is provided.
  ListCell* aliascell = list_head(eref->colnames);

  // Note: PostgreSQL attribute numbers start at 1. The PostgreSQL version of
  // this function calls these "attribute numbers" but treats them as 0-indexed
  // offsets. To be consistent with other PostgreSQL APIs we use here, we'll use
  // the 1-indexed convention PostgreSQL uses most places.
  for (int varattno = 1; varattno <= user_table->NumColumns(); ++varattno) {
    // Skip pseudo columns since they weren't explictly referenced.
    // Column offset is attribute number - 1.
    if (user_table->GetColumn(varattno - 1)->IsPseudoColumn()) {
      if (aliascell != nullptr) {
        aliascell = lnext(eref->colnames, aliascell);
      }
      continue;
    }

    if (colnames != nullptr) {
      const char* label;

      // NB: The list of aliased column names is a continuous subset of the
      // first K columns of the table. This is enforced at RTE creation time.
      if (aliascell != nullptr) {
        label = strVal(lfirst(aliascell));
        aliascell = lnext(eref->colnames, aliascell);
      } else {
        // If we run out of aliases, use the underlying name.
        ZETASQL_ASSIGN_OR_RETURN(label, GetAttributeNameCpp(*user_table, varattno));
      }
      *colnames = lappend(*colnames, makeString(pstrdup(label)));
    }

    if (colvars != nullptr) {
      Oid type_oid;
      int32_t typemod;
      Oid collation;
      ZETASQL_RETURN_IF_ERROR(GetAttributeTypeCpp(*user_table, varattno, &type_oid,
                                          &typemod, &collation));
      Var* varnode = makeVar(rtindex, varattno, type_oid, typemod, collation,
                             sublevels_up);
      varnode->location = location;

      *colvars = lappend(*colvars, varnode);
    }
  }
  return absl::OkStatus();
}

// C++ helper to NSItemVarsForJoinC below.
//
// Note that callers may pass nullptr for colnames to indicate those values
// are not required. This function is derived from PostgreSQL's
// expandNSItemVars.
//
// `colnames`, if provided, will be populated with T_String Value nodes.
static absl::StatusOr<List*> ExpandNSItemVarsForJoinCpp(
    const List* rtable, ParseNamespaceItem* nsitem, int sublevels_up,
    int location, List** colnames) {
  CatalogAdapter* catalog_adapter;
  ZETASQL_ASSIGN_OR_RETURN(catalog_adapter, GetCatalogAdapter());
  EngineUserCatalog* catalog = catalog_adapter->GetEngineUserCatalog();
  if (catalog == nullptr) {
    return absl::InternalError(
        "Googlesql catalog not initialized on this thread.");
  }

  // The columns in this join reference two real tables by varno. Store the
  // varno --> table  mapping so we don't look up the same table multiple times.
  absl::flat_hash_map<int, const zetasql::Table*> varno_to_table_map;

  List* result = NIL;
  int colindex = -1;
  for (Value* colnameval : StructList<Value*>(nsitem->p_rte->eref->colnames)) {
    ++colindex;
    const char* colname = strVal(colnameval);

    // During ordinary parsing, there will never be any
    // deleted columns in the join; but we have to check since
    // this routine is also used by the rewriter, and joins
    // found in stored rules might have join columns for
    // since-deleted columns.  This will be signaled by an empty
    // name in the colnames list.
    if (!colname[0]) {
      // SPANGRES: dropped column cases do not apply to Spangres.
      continue;
    }

    // Skip Pseudo Columns.
    // This only applies when the column is from a relation and the column on
    // the ZetaSQL table corresponding to that relation is marked
    // IsPseudoColumn.
    ParseNamespaceColumn* nscol = nsitem->p_nscolumns + colindex;
    const zetasql::Table* table = nullptr;
    int varno = nscol->p_varno;
    int varattno = nscol->p_varattno;
    auto it = varno_to_table_map.find(varno);
    if (it != varno_to_table_map.end()) {
      table = it->second;
    } else {
      RangeTblEntry* rte = rt_fetch(varno, rtable);
      if (rte->rtekind == RTE_RELATION) {
        ZETASQL_RETURN_IF_ERROR(catalog_adapter->GetEngineUserCatalog()->FindTable(
            catalog_adapter->GetTableNameFromOid(rte->relid)->AsSpan(),
            &table));
      }
      // Store the mapping whether the rte is a relation or not.
      varno_to_table_map[varno] = table;
    }
    if (table) {
      // ZetaSQL column indices start at 0, varattnos start at 1.
      const zetasql::Column* gsql_column = table->GetColumn(varattno - 1);
      ZETASQL_RET_CHECK_NE(gsql_column, nullptr);

      if (gsql_column->IsPseudoColumn()) {
        // It's a pseudo column. Don't include in the expansion.
        continue;
      }
    }

    // This column is not a pseudo column. Construct a var for it.
    Var* var = makeVar(varno, varattno, nscol->p_vartype, nscol->p_vartypmod,
                       nscol->p_varcollid, sublevels_up);
    /* makeVar doesn't offer parameters for these, so set by hand: */
    var->varnosyn = nscol->p_varnosyn;
    var->varattnosyn = nscol->p_varattnosyn;
    var->location = location;
    result = lappend(result, var);
    if (colnames) {
      *colnames = lappend(*colnames, colnameval);
    }
  }
  return result;
}

// Given a TVF, build a FormData_pg_proc representing the same (transformed)
// function for use by the PG Analyzer.
//
// For now, the following restrictions apply:
// - Only a non-templated signature with a fixed return type is supported.
// - Only concrete arguments are supported (no variadic, no templated)
//
// Memory is allocated from and owned by CurrentMemoryContext, but
// CatalogAdapter will retain a pointer to it for later retrieval.
static absl::StatusOr<std::vector<const FormData_pg_proc*>> BuildPgProcsFromTVF(
    const zetasql::TableValuedFunction* tvf, CatalogAdapter* adapter) {
  std::vector<const FormData_pg_proc*> result;

  // Each signature will be created as a separate pg_proc.
  // For now, we only need to support a single signature.
  ZETASQL_RET_CHECK_EQ(tvf->NumSignatures(), 1);
  const zetasql::FunctionSignature* signature = tvf->GetSignature(0);

  // Build a corresponding FormData_pg_proc for this signature, including space
  // for the variable-length args list. This lives in, is owned by, and has
  // lifetime of CurrentMemoryContext, which is typically the length of this
  // translation operation.
  FormData_pg_proc* proc = reinterpret_cast<FormData_pg_proc*>(palloc0(
      sizeof(FormData_pg_proc) + signature->arguments().size() * sizeof(Oid)));

  // This oid will be assigned by CatalogAdapter later. It checks for InvalidOid
  // as a sanity check that we aren't assigning a duplicate.
  proc->oid = InvalidOid;
  ZETASQL_ASSIGN_OR_RETURN(proc->oid, adapter->GenerateAndStoreUDFProcOid(proc, tvf));
  ZETASQL_RET_CHECK(tvf->Name().size() < NAMEDATALEN);
  memcpy(NameStr(proc->proname), tvf->Name().c_str(), tvf->Name().size() + 1);

  // Treat this function as being in the "spanner" namespace (because it's
  // spanner-specific) similar to pending_commit_timestamp. In Spanner's view,
  // the function will be in the top-level namespace.
  std::string tvf_namespace;
  ZETASQL_RET_CHECK(tvf->function_name_path().size() < 3);
  if (tvf->function_name_path().size() == 1) {
    tvf_namespace = "spanner";
  } else if (tvf->function_name_path().size() == 2) {
    tvf_namespace = tvf->function_name_path()[0];
  }
  ZETASQL_ASSIGN_OR_RETURN(proc->pronamespace,
                   adapter->GetOidFromNamespaceName(tvf_namespace));

  // These fields are currently unused by the caller, but we might as well fill
  // them in with PG defaults. These defaults come from pg_proc.h.
  proc->proowner = BOOTSTRAP_SUPERUSERID;
  proc->prolang = INTERNALlanguageId;
  proc->procost = 1;
  proc->prorows = 0;
  proc->provariadic = 0;
  proc->prosupport = 0;
  proc->prokind = PROKIND_FUNCTION;
  proc->prosecdef = PROKIND_FUNCTION;
  // proretset is true for TVFs generally, though Spanner's TVFs return 1 row.
  proc->proretset = true;
  proc->provolatile = PROVOLATILE_VOLATILE;
  proc->proparallel = PROPARALLEL_SAFE;
  proc->pronargs = signature->arguments().size();
  proc->pronargdefaults = 0;  // TODO: support default args
  proc->proargtypes.ndim = 1;
  proc->proargtypes.dataoffset = 0;
  proc->proargtypes.elemtype = OIDOID;
  proc->proargtypes.dim1 = signature->arguments().size();
  proc->proargtypes.lbound1 = 0;
  for (int i = 0; i < signature->arguments().size(); ++i) {
    ZETASQL_ASSIGN_OR_RETURN(Oid oid, Transformer::BuildPgTypeOid(
                                  *adapter, signature->argument(i).type()));
    proc->proargtypes.values[i] = oid;
  }
  // For change stream TVFs, the argument's result type is the full return type.
  // If in the future we want to support some of the more complex TVF behaviors
  // like variable, multi-column result schemas, we will need to call
  // TableValuedFunction::Resolve() instead to support that behavior.
  // We're unwrapping this TVFRelation return type to peek at the columns inside
  // by labeling this whole function as set-returning, which is the closest PG
  // equivalent.
  ZETASQL_RET_CHECK(signature->result_type().IsFixedRelation());
  const zetasql::TVFRelation& output_schema =
      signature->result_type().options().relation_input_schema();
  ZETASQL_RET_CHECK_EQ(output_schema.num_columns(), 1);
  ZETASQL_ASSIGN_OR_RETURN(
      proc->prorettype,
      Transformer::BuildPgTypeOid(*adapter, output_schema.column(0).type));

  result.push_back(proc);
  return result;
}

// Find functions in the EngineUserCatalog that match the given name and return
// them as an array of FormData_pg_proc representations for consumption by the
// PG analyzer.
//
// ONLY UDFs ARE SUPPORTED. This avoids duplicate entries for builtin functions
// that may appear in both catalogs. Builtin functions should be added to the
// bootstrap_catalog and EngineSystemCatalog.
//
// For now, only TVFs are supported because the only supported UDF is the change
// stream TVF.
//
// Conforming to PG's behavior, googlesql functions with multiple signatures
// are represented as multiple (overloaded) pg_procs.
//
// This is a complement to GetProcsByNameFromBootstrapCatalog.
static absl::Status GetProcsByNameFromUserCatalog(
    const char* name, const FormData_pg_proc*** outlist, size_t* outcount) {
  // Initialize outargs for early return.
  *outlist = nullptr;
  *outcount = 0;
  std::vector<const FormData_pg_proc*> found_procs;

  CatalogAdapter* adapter;
  ZETASQL_ASSIGN_OR_RETURN(adapter, GetCatalogAdapter());
  EngineUserCatalog* catalog = adapter->GetEngineUserCatalog();

  // Get each matching function and add it to our output.
  // For now, only look in the top-level catalog and only for TVFs.
  // TODO: Add support for nested catalogs.
  const zetasql::TableValuedFunction* tvf;
  absl::Status status = catalog->FindTableValuedFunction({name}, &tvf);
  // NotFound is fine. Just swallow it and return an empty list. Any other error
  // is unexpected.
  if (absl::IsNotFound(status)) {
    return absl::OkStatus();
  } else if (!status.ok()) {
    return status;
  } else if (tvf->Name() != name) {
    // Treat case mismatch as any other not found case (no error, 0 count).
    return absl::OkStatus();
  }

  ZETASQL_ASSIGN_OR_RETURN(std::vector<const FormData_pg_proc*> new_procs,
                   BuildPgProcsFromTVF(tvf, adapter));
  found_procs.insert(found_procs.end(), new_procs.begin(), new_procs.end());

  // Copy our results into the output.
  *outcount = found_procs.size();
  size_t outsize = sizeof(FormData_pg_proc*) * found_procs.size();
  *outlist = reinterpret_cast<const FormData_pg_proc**>(palloc(outsize));
  memcpy(*outlist, found_procs.data(), outsize);
  return absl::OkStatus();
}

}  // namespace postgres_translator

extern "C" RangeTblEntry* AddRangeTableEntryC(ParseState* pstate,
                                              RangeVar* relation, Alias* alias,
                                              bool inh, bool inFromCl) {
  absl::StatusOr<RangeTblEntry*> rte;
  rte = postgres_translator::AddRangeTableEntryCpp(pstate, relation, alias, inh,
                                                   inFromCl);
  if (!rte.ok()) {
    if (rte.status().code() == absl::StatusCode::kNotFound) {
      // ZetaSQL uses this status code to report table not found (replacing
      // the catalog's error with a different one). Replace this with the
      // equivalent PG error message:
      //   relation "[schema_name.]table_name" does not exist
      if (relation->schemaname) {
        ereportUndefinedTableRelationNotFound(
            absl::StrFormat("%s.%s", relation->schemaname, relation->relname)
                .c_str());
      } else {
        ereportUndefinedTableRelationNotFound(relation->relname);
      }
    }
    // TODO: Examine all potential returned error messages and see if
    // we can pass them to PostgreSQL, so that they can be returned to Cloud
    // Spanner customers.
    LOG(ERROR)  // DO_NOT_TRANSFORM
        << "Error occurred during RangeTblEntry construction: "
        << rte.status().message();
    return nullptr;
  }

  return *rte;
}

extern "C" RangeTblEntry* AddRangeTableEntryByOidC(ParseState* pstate,
                                                   Oid relation_oid,
                                                   Alias* alias, bool inh,
                                                   bool inFromCl) {
  absl::StatusOr<RangeTblEntry*> rte;
  rte = postgres_translator::AddRangeTableEntryByOidCpp(pstate, relation_oid,
                                                        alias, inh, inFromCl);
  if (!rte.ok()) {
    // TODO: Examine all potential returned error messages and see if
    // we can pass them to PostgreSQL, so that they can be returned to Cloud
    // Spanner customers.
    LOG(ERROR)  // DO_NOT_TRANSFORM
        << "Error occurred during RangeTblEntry construction by oid: "
        << rte.status().message();
    return nullptr;
  }

  return *rte;
}

extern "C" char* GetAttributeNameC(Oid relid, AttrNumber attnum,
                                   bool missing_ok) {
  absl::StatusOr<char*> attname =
      postgres_translator::GetAttnameHelper(relid, attnum);
  if (attname.ok()) {
    return *attname;
  } else {
    ABSL_LOG(ERROR) << attname.status();
    if (missing_ok) {
      return nullptr;
    } else {
      // Convert error message to std::string so we can
      // get a null-terminated C string for it
      std::string error_message = std::string(attname.status().message());

      // Alias the value of ERROR so that the elog() macro, which uses it
      // both as an argument and internally, works properly.
      constexpr int ERROR = PG_ERROR;
      elog(ERROR, "cache lookup failed for attribute %d of relation %u: %s",
           attnum, relid, error_message.c_str());
    }
  }
}

extern "C" void GetAttributeTypeC(Oid relid, AttrNumber attnum, Oid* vartype,
                                  int32_t* vartypmod, Oid* varcollid) {
  absl::StatusOr<const zetasql::Table*> table =
      postgres_translator::GetTableByOid(relid);
  if (!table.ok()) {
    ABSL_LOG(ERROR) << table.status();
  } else if (*table == nullptr) {
    LOG(ERROR)  // DO_NOT_TRANSFORM
        << "Table with Oid " << relid << " not found.";
  } else {
    absl::Status attribute_status = postgres_translator::GetAttributeTypeCpp(
        **table, attnum, vartype, vartypmod, varcollid);
    if (!attribute_status.ok()) {
      ABSL_LOG(ERROR) << attribute_status;
      // If this failed partway, we might have set some of the outargs. Clear
      // them.
      *vartype = InvalidOid;
      *vartypmod = -1;
      *varcollid = InvalidOid;
    }
  }
}

extern "C" bool IsAttributePseudoColumnC(Oid relid, AttrNumber attnum) {
  absl::StatusOr<bool> is_pseudo =
      postgres_translator::IsAttributePseudoColumnCpp(relid, attnum);
  if (is_pseudo.ok()) {
    return *is_pseudo;
  } else {
    postgres_translator::ereport_helper(is_pseudo.status(),
                                        ERRCODE_INVALID_TABLE_DEFINITION,
                                        "Unable to get column data for table");
    return false;
  }
}

extern "C" char* GetTableNameC(Oid relid) {
  return postgres_translator::GetTableNameCpp(relid).value_or(nullptr);
}

extern "C" char* GetNamespaceNameC(Oid relid) {
  return postgres_translator::GetNamespaceNameCpp(relid).value_or(nullptr);
}

extern "C" void GetColumnNamesC(Oid relid, char*** real_colnames,
                                int* ncolumns) {
  auto status =
      postgres_translator::GetColumnNamesCpp(relid, real_colnames, ncolumns);
  if (!status.ok()) {
    postgres_translator::ereport_helper(status,
                                        ERRCODE_INVALID_TABLE_DEFINITION,
                                        "Unable to get column names for table");
  }
}

extern "C" void GetColumnTypesC(Oid relid, List** coltypes, List** coltypmods,
                                List** colcollations, int* ncolumns) {
  auto status = postgres_translator::GetColumnTypesCpp(
      relid, coltypes, coltypmods, colcollations, ncolumns);
  if (!status.ok()) {
    postgres_translator::ereport_helper(status,
                                        ERRCODE_INVALID_TABLE_DEFINITION,
                                        "Unable to get column types for table");
  }
}

extern "C" int GetColumnAttrNumber(Oid relid, const char* column_name) {
  char** real_colnames = nullptr;
  int ncolumns = 0;
  if (postgres_translator::GetColumnNamesCpp(relid, &real_colnames, &ncolumns)
          .ok()) {
    for (int i = 0; i < ncolumns; ++i) {
      const char* colname = real_colnames[i];
      if (strcmp(colname, column_name) == 0) {
        return i + 1;  // Attribute numbers start at 1.
      }
    }
  }
  // If the table or column could not be found, return InvalidAttrNumber.
  return InvalidAttrNumber;
}

extern "C" const FormData_pg_cast* GetCastFromBootstrapCatalog(
    Oid source_type_id, Oid target_type_id) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetCast(source_type_id, target_type_id)
      .value_or(nullptr);
}

extern "C" const FormData_pg_type* GetTypeFromBootstrapCatalog(Oid type_id) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetType(type_id)
      .value_or(nullptr);
}

extern "C" const FormData_pg_operator* GetOperatorFromBootstrapCatalog(
    Oid operator_id) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetOperator(operator_id)
      .value_or(nullptr);
}

const FormData_pg_aggregate* GetAggregateFromBootstrapCatalog(Oid agg_id) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetAggregate(agg_id)
      .value_or(nullptr);
}

const FormData_pg_opclass* GetOpclassFromBootstrapCatalog(Oid opclass_id) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetOpclass(opclass_id)
      .value_or(nullptr);
}

extern "C" Oid GetNamespaceByNameFromBootstrapCatalog(const char* name) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetNamespaceOid(name)
      .value_or(InvalidOid);
}

extern "C" char* GetNamespaceNameByOidFromBootstrapCatalog(Oid namespace_oid) {
  absl::StatusOr<const char*> namespace_name =
      postgres_translator::PgBootstrapCatalog::Default()->GetNamespaceName(
          namespace_oid);
  if (namespace_name.ok()) {
    return pstrdup(*namespace_name);
  } else {
    return nullptr;
  }
}

extern "C" Oid GetCollationOidByNameFromBootstrapCatalog(const char* name) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetCollationOid(name)
      .value_or(InvalidOid);
}

extern "C" void GetOperatorsByNameFromBootstrapCatalog(const char* name,
                                                       const Oid** outlist,
                                                       size_t* outcount) {
  absl::StatusOr<absl::Span<const Oid>> oid_list =
      postgres_translator::PgBootstrapCatalog::Default()->GetOperatorOids(name);
  if (oid_list.ok()) {
    *outlist = oid_list->data();
    *outcount = oid_list->size();
  } else {
    *outlist = nullptr;
    *outcount = 0;
  }
}

// This function is static because users should call GetProcsByName below to get
// both builtin procs from bootstrap and UDF procs from the user catalog.
static void GetProcsByNameFromBootstrapCatalog(
    const char* name, const FormData_pg_proc* const** outlist,
    size_t* outcount) {
  absl::StatusOr<absl::Span<const FormData_pg_proc* const>> proc_list =
      postgres_translator::PgBootstrapCatalog::Default()->GetProcsByName(name);
  if (proc_list.ok()) {
    *outlist = proc_list->data();
    *outcount = proc_list->size();
  } else {
    *outlist = nullptr;
    *outcount = 0;
  }
}

extern "C" void GetTypesByNameFromBootstrapCatalog(
    const char* name, const FormData_pg_type* const** outlist,
    size_t* outcount) {
  absl::StatusOr<absl::Span<const FormData_pg_type* const>> type_list =
      postgres_translator::PgBootstrapCatalog::Default()->GetTypesByName(name);
  if (type_list.ok()) {
    *outlist = type_list->data();
    *outcount = type_list->size();
  } else {
    *outlist = nullptr;
    *outcount = 0;
  }
}

extern "C" void GetOpclassesByAccessMethodFromBootstrapCatalog(
    Oid am_id, const Oid** opclasses, size_t* opclass_count) {
  absl::StatusOr<absl::Span<const Oid>> opclass_list =
      postgres_translator::PgBootstrapCatalog::Default()->GetOpclassesByAm(
          am_id);
  if (opclass_list.ok()) {
    *opclasses = opclass_list->data();
    *opclass_count = opclass_list->size();
  } else {
    *opclasses = nullptr;
    *opclass_count = 0;
  }
}

extern "C" void GetAmopsByAmopOpIdFromBootstrapCatalog(
    const Oid opid, const FormData_pg_amop* const** outlist, size_t* outcount) {
  absl::StatusOr<absl::Span<const FormData_pg_amop* const>> amop_list =
      postgres_translator::PgBootstrapCatalog::Default()->GetAmopsByAmopOpId(
          opid);
  if (amop_list.ok()) {
    *outlist = amop_list->data();
    *outcount = amop_list->size();
  } else {
    *outlist = nullptr;
    *outcount = 0;
  }
}

extern "C" const FormData_pg_amop* GetAmopByFamilyFromBootstrapCatalog(
    Oid opfamily, Oid lefttype, Oid righttype, int16_t strategy) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetAmopByFamily(opfamily, lefttype, righttype, strategy)
      .value_or(nullptr);
}

extern "C" const FormData_pg_amproc* GetAmprocByFamilyFromBootstrapCatalog(
    Oid opfamily, Oid lefttype, Oid righttype, int16_t index) {
  return postgres_translator::PgBootstrapCatalog::Default()
      ->GetAmprocByFamily(opfamily, lefttype, righttype, index)
      .value_or(nullptr);
}

extern "C" void GetAmprocsByFamilyFromBootstrapCatalog(
    Oid opfamily, Oid lefttype, const FormData_pg_amproc* const** outlist,
    size_t* outcount) {
  absl::StatusOr<absl::Span<const FormData_pg_amproc* const>> amproc_list =
      postgres_translator::PgBootstrapCatalog::Default()->GetAmprocsByFamily(
          opfamily, lefttype);
  if (amproc_list.ok()) {
    *outlist = amproc_list->data();
    *outcount = amproc_list->size();
  } else {
    *outlist = nullptr;
    *outcount = 0;
  }
}

// This function replaces both of expandRelation and expandTupleDesc. It's a
// combined replacement because expandRelation is essentially just a wrapper
// around expandTupleDesc after looking up the TupleDesc in the catalog.
// Combining the shim means we don't have support building a TupleDesc for user
// tables.
// We're using a C++ wrapper here so that catalog shim can utilize
// ZetaSQL's Table class and transformer catalog functions for type
// conversion.
extern "C" void ExpandRelationC(Oid relid, Alias* eref, int rtindex,
                                int sublevels_up, int location, List** colnames,
                                List** colvars) {
  absl::Status status = postgres_translator::ExpandRelationCpp(
      relid, eref, rtindex, sublevels_up, location, colnames, colvars);
  if (!status.ok()) {
    if (colnames) {
      *colnames = NIL;
    }
    if (colvars) {
      *colvars = NIL;
    }
    postgres_translator::ereport_helper(
        status, ERRCODE_UNDEFINED_COLUMN,
        "Error occurred when expanding relation columns");
  }
}

List* ExpandNSItemVarsForJoinC(const List* rtable, ParseNamespaceItem* nsitem,
                               int sublevels_up, int location, List** colnames,
                               bool* error) {
  if (error == nullptr) {
    LOG(ERROR)  // DO_NOT_TRANSFORM
        << "ExpandNSItemVarsForJoinC: input argument `error` is null";
    return nullptr;
  } else if (nsitem == nullptr) {
    *error = true;
    LOG(ERROR)  // DO_NOT_TRANSFORM
        << "ExpandNSItemVarsForJoinC: input argument `nsitem` is null";
    return nullptr;
  }
  absl::StatusOr<List*> result_or =
      postgres_translator::ExpandNSItemVarsForJoinCpp(
          rtable, nsitem, sublevels_up, location, colnames);
  *error = false;
  if (result_or.ok()) {
    return *result_or;
  }

  // Handle the error
  *error = true;
  // TODO: Examine all potential returned error messages and see if
  // we can pass them to PostgreSQL, so that they can be returned to Cloud
  // Spanner customers.
  LOG(ERROR)  // DO_NOT_TRANSFORM
      << "Error occurred when expanding a ParseNamespaceItem: "
      << result_or.status();
  return nullptr;
}

extern "C" Oid GetOrGenerateOidFromTableNameC(
    const char* unqualified_table_name) {
  postgres_translator::CatalogAdapter* adapter;
  ZETASQL_ASSIGN_OR_RETURN(
      adapter, postgres_translator::GetCatalogAdapter(),
      _.LogError().With([](const absl::Status& unused) { return InvalidOid; }));

  postgres_translator::TableName table_name({unqualified_table_name});
  return adapter->GetOrGenerateOidFromTableName(table_name)
      .value_or(InvalidOid);
}

extern "C" Oid GetOidFromNamespaceNameC(
    const char* unqualified_namespace_name) {
  postgres_translator::CatalogAdapter* adapter;
  ZETASQL_ASSIGN_OR_RETURN(
      adapter, postgres_translator::GetCatalogAdapter(),
      _.LogError().With([](const absl::Status& unused) { return InvalidOid; }));

  return adapter->GetOidFromNamespaceName(unqualified_namespace_name)
      .value_or(InvalidOid);
}

extern "C" Oid GetOrGenerateOidFromNamespaceNameC(
    const char* unqualified_namespace_name) {
  postgres_translator::CatalogAdapter* adapter;
  ZETASQL_ASSIGN_OR_RETURN(
      adapter, postgres_translator::GetCatalogAdapter(),
      _.LogError().With([](const absl::Status& unused) { return InvalidOid; }));

  return adapter->GetOrGenerateOidFromNamespaceName(unqualified_namespace_name)
      .value_or(InvalidOid);
}

extern "C" Oid GetOrGenerateOidFromNamespaceOidAndRelationNameC(
    Oid namespace_oid, const char* unqualified_table_name) {
  postgres_translator::CatalogAdapter* adapter;
  ZETASQL_ASSIGN_OR_RETURN(
      adapter, postgres_translator::GetCatalogAdapter(),
      _.LogError().With([](const absl::Status& unused) { return InvalidOid; }));

  return adapter
      ->GetOrGenerateOidFromNamespaceOidAndRelationName(
          namespace_oid, std::string(unqualified_table_name))
      .value_or(InvalidOid);
}

extern "C" void GetProcsByName(const char* name,
                               const FormData_pg_proc*** outlist,
                               size_t* outcount) {
  size_t bootstrap_count;
  const FormData_pg_proc* const* bootstrap_procs;
  GetProcsByNameFromBootstrapCatalog(name, &bootstrap_procs, &bootstrap_count);
  size_t udf_count;
  const FormData_pg_proc** udf_procs;
  absl::Status udf_status = postgres_translator::GetProcsByNameFromUserCatalog(
      name, &udf_procs, &udf_count);
  if (!udf_status.ok()) {
    *outcount = 0;
    *outlist = nullptr;
    postgres_translator::ereport_helper(
        udf_status, ERRCODE_INTERNAL_ERROR,
        "Failed to look up UDFs from user catalog");
  }

  // Merge the two lists and let the caller choose which proc they want. PG
  // uses signatures and namespaces to decide which function is the best match.
  // See FuncnameGetCandidates for details.
  *outcount = bootstrap_count + udf_count;
  *outlist = reinterpret_cast<const FormData_pg_proc**>(
      palloc(*outcount * sizeof **outlist));
  memcpy(*outlist, bootstrap_procs, bootstrap_count * sizeof *bootstrap_procs);
  memcpy(*outlist + bootstrap_count, udf_procs, udf_count * sizeof udf_procs);
}

// This wrapper just pulls catalog adapter from the thread-local, maybe converts
// an unexpected error into an ereport, or converts an expected NotFound into a
// NULL.
extern "C" const FormData_pg_proc* GetProcByOid(Oid oid) {
  const auto adapter = postgres_translator::GetCatalogAdapter();
  if (!adapter.ok()) {
    postgres_translator::ereport_helper(adapter.status(),
                                        ERRCODE_INTERNAL_ERROR,
                                        "Failed to get catalog adapter");
  }
  auto proc = postgres_translator::GetProcByOid(*adapter, oid);
  if (proc.ok()) {
    return proc.value();
  } else if (!absl::IsNotFound(proc.status())) {
    // Something other than NotFound is unexpected. Error out.
    postgres_translator::ereport_helper(proc.status(), ERRCODE_INTERNAL_ERROR,
                                        "Proc lookup by oid failed");
    return nullptr;  // Unreachable.
  } else {
    // Not found, return NULL.
    return nullptr;
  }
}

extern "C" DateADT date_in_c(char* input_string) {
  const auto adapter = postgres_translator::GetCatalogAdapter();
  if (!adapter.ok()) {
    postgres_translator::ereport_helper(adapter.status(),
                                        ERRCODE_INTERNAL_ERROR,
                                        "Failed to get catalog adapter");
  }
  auto result = date_in(
      input_string, (*adapter)->analyzer_options().default_time_zone().name());
  if (!result.ok()) {
    constexpr int ERROR = PG_ERROR;
    int sqlerrcode = absl::IsInvalidArgument(result.status())
                         ? ERRCODE_INVALID_DATETIME_FORMAT
                         : ERRCODE_INTERNAL_ERROR;
    ereport(ERROR, errcode(sqlerrcode),
            errmsg("%s", result.status().ToString()));
            // clang-format on
  } else {
    return *result;
  }
}

extern "C" TimestampTz timestamptz_in_c(char* input_string) {
  const auto adapter = postgres_translator::GetCatalogAdapter();
  if (!adapter.ok()) {
    postgres_translator::ereport_helper(adapter.status(),
                                        ERRCODE_INTERNAL_ERROR,
                                        "Failed to get catalog adapter");
  }
  auto result = timestamptz_in(
      input_string, (*adapter)->analyzer_options().default_time_zone().name());
  if (!result.ok()) {
    constexpr int ERROR = PG_ERROR;
    int sqlerrcode = absl::IsInvalidArgument(result.status())
                         ? ERRCODE_INVALID_DATETIME_FORMAT
                         : ERRCODE_INTERNAL_ERROR;
    ereport(ERROR, errcode(sqlerrcode),
            errmsg("%s", result.status().ToString()));
            // clang-format on
  } else {
    return *result;
  }
}
