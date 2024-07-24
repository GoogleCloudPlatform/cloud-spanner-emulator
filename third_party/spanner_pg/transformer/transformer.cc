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

#include "third_party/spanner_pg/transformer/transformer.h"

#include <algorithm>
#include <climits>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "zetasql/public/analyzer_options.h"
#include "zetasql/public/catalog.h"
#include "zetasql/public/language_options.h"
#include "zetasql/public/parse_location.h"
#include "zetasql/public/types/type.h"
#include "zetasql/resolved_ast/resolved_ast.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/transformer/transformer_helper.h"
#include "third_party/spanner_pg/util/postgres.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

using ::postgres_translator::internal::PostgresCastToString;

// Returns true if all alphabetic characters in `qualified_table_name` are
// lower-case.
static bool IsLowerCaseReference(const TableName& qualified_table_name) {
  return std::all_of(
      qualified_table_name.AsSpan().begin(),
      qualified_table_name.AsSpan().end(), [](const std::string& str) {
        return !std::any_of(str.begin(), str.end(), absl::ascii_isupper);
      });
}

// Build eref from alias, falling back to column names from the ZetaSQL
//  Table object where aliases are not available.
//
// NB: Alias' column list (if supplied) is rebuilt by this process to handle
// cases where the RTE references dropped columns. If we were sure we'd never
// handle a relation with dropped columns we could simplify that bit, but it
// seems safer to maintain the flexibility.
//
// Argument `lower_gsql_name` specifies that all upper-case name references from
// `gsql_table` need to be converted to lower-case.
static absl::StatusOr<Alias*> BuildRelationEref(
    const zetasql::Table& gsql_table, const Alias* alias,
    bool lower_gsql_name) {
  const int num_columns = gsql_table.NumColumns();
  const ListCell* alias_list_cell = nullptr;
  int num_aliases = 0;

  Alias* eref;
  ZETASQL_ASSIGN_OR_RETURN(
      eref,
      CheckedPgMakeAlias(alias != nullptr ? alias->aliasname
                         : lower_gsql_name
                             ? absl::AsciiStrToLower(gsql_table.Name()).c_str()
                             : gsql_table.Name().c_str(),
                         /*colnames=*/nullptr));

  if (alias) {
    alias_list_cell = list_head(alias->colnames);
    num_aliases = list_length(alias->colnames);
  }

  // Note that the alias list may be a subset of the actual table columns. We'll
  // fill in real column names when we run out of user-supplied names, and we'll
  // check below for the invalid case of more user-supplied names than actual
  // columns.
  for (int colnum = 0; colnum < num_columns; ++colnum) {
    const zetasql::Column* column = gsql_table.GetColumn(colnum);
    String* attribute_name;

    if (alias_list_cell != nullptr) {
      attribute_name = PostgresCastToString(lfirst(alias_list_cell));
      alias_list_cell = lnext(alias->colnames, alias_list_cell);
    } else {
      ZETASQL_ASSIGN_OR_RETURN(
          char* allocated_name,
          CheckedPgPstrdup(lower_gsql_name
                               ? absl::AsciiStrToLower(column->Name()).c_str()
                               : column->Name().c_str()));
      ZETASQL_ASSIGN_OR_RETURN(attribute_name, CheckedPgMakeString(allocated_name));
    }

    ZETASQL_ASSIGN_OR_RETURN(eref->colnames,
                     CheckedPgLappend(eref->colnames, attribute_name));
  }

  // Too many user-supplied aliases?
  if (alias_list_cell != nullptr) {
    return absl::InvalidArgumentError(
        absl::StrFormat("Table \"%s\" has %d columns available but %d columns "
                        "specified by Alias",
                        eref->aliasname, num_columns, num_aliases));
  }
  return eref;
}

absl::StatusOr<RangeTblEntry*> Transformer::BuildPgRangeTblEntry(
    CatalogAdapter& adapter, Oid oid,
    Alias* alias, bool inFromCl, AclMode acl_mode) {
  RangeTblEntry* rte;
  ZETASQL_ASSIGN_OR_RETURN(rte, internal::makePartialRangeTblEntry(inFromCl, acl_mode));
  rte->rtekind = RTE_RELATION;
  // Inheritence doesn't exist in ZetaSQL so use the defaults for
  // PostgreSQL. True is what you get when just writing "FROM <table_name>".
  // This is only false if the caller explicitly requests it: "FROM ONLY
  // <table_name>". INSERT statements are special: this is always false.
  rte->inh = acl_mode == ACL_INSERT ? false : true;
  rte->relid = oid;
  rte->relkind = RELKIND_RELATION;
  ZETASQL_ASSIGN_OR_RETURN(
      TableName qualified_table_name, adapter.GetTableNameFromOid(oid));

  const zetasql::Table* table;
  ZETASQL_RETURN_IF_ERROR(adapter.GetEngineUserCatalog()->FindTable(
      qualified_table_name.AsSpan(), &table));
  ZETASQL_RET_CHECK_NE(table, nullptr);

  // Checks whether table/view is part of a system catalog that is incluced in
  // the supplied schema list. Catalogs included in the list have all object
  // names in upper-case. If we keep such names as is, the only way for users to
  // access these objects from Spangres will be through quoted identifiers: e.g.
  // SELECT *FROM "SPANNER_SYS"."QUERY_STATS_TOP_MINUTE". To allow users to
  // access such objects without quotes, we will convert their names to
  // lower-case. Note: to match PostgreSQL semantic for unquoted identifiers we
  // disallow access to these objects using upper-case quoted identifiers, like
  // in the example above.

  ZETASQL_ASSIGN_OR_RETURN(
      const bool in_uppercase_catalog_path,
      adapter.GetEngineUserCatalog()->InUppercaseCatalogPath(table));

  // ZetaSQL's catalogs are not case-sensitive, but PostgreSQL's analyzer
  // is. Verify we adhere to the stricter rules because PostgreSQL may do
  // case-sensitive comparisons against the RTE's fields during analysis.
  // For system tables that use upper-case for their names, we will translate
  // these names to lower-case and thus have to check that references to these
  // names are in lower-case (note: PG automatically lower-cases all unquoted
  // identifiers).
  const bool case_mismatch_error =
      in_uppercase_catalog_path
          ? !IsLowerCaseReference(qualified_table_name)
          : qualified_table_name.UnqualifiedName() != table->Name();
  if (case_mismatch_error) {
    return zetasql_base::NotFoundErrorBuilder()
           << "Table not found: `" << qualified_table_name
           << "` not found in catalog "
           << adapter.GetEngineUserCatalog()->FullName() << " (case mismatch?)";
  }

  // Attach provided alias and use it to populate the effective column names
  // (actual names or user-supplied aliases as applicable).
  rte->alias = alias;

  ZETASQL_ASSIGN_OR_RETURN(rte->eref,
                   BuildRelationEref(*table, alias, in_uppercase_catalog_path));

  return rte;
}

absl::StatusOr<RangeTblEntry*> Transformer::BuildPgRangeTblEntry(
    CatalogAdapter& adapter, const zetasql::Table& resolved_table,
    Alias* alias, bool inFromCl, AclMode acl_mode) {
  ZETASQL_ASSIGN_OR_RETURN(
      TableName qualified_table_name,
      adapter.GetEngineUserCatalog()->GetTableNameForGsqlTable(resolved_table));
  ZETASQL_ASSIGN_OR_RETURN(auto oid,
                   adapter.GetOrGenerateOidFromTableName(qualified_table_name));
  return BuildPgRangeTblEntry(adapter, oid, alias, inFromCl, acl_mode);
}

absl::StatusOr<Oid> Transformer::BuildPgTypeOid(
    CatalogAdapter& catalog_adapter, const zetasql::Type* gsql_type) {
  const PostgresTypeMapping* pg_type_mapping =
      catalog_adapter.GetEngineSystemCatalog()->GetTypeFromReverseMapping(
          gsql_type);
  if (pg_type_mapping == nullptr) {
    return absl::UnimplementedError(absl::StrCat(
        "Unsupported ZetaSQL Type: ",
        gsql_type->TypeName(
            catalog_adapter.analyzer_options().language().product_mode())));
  }

  return pg_type_mapping->PostgresTypeOid();
}

absl::StatusOr<Oid*> Transformer::BuildPgParameterTypeList(
    CatalogAdapter& catalog_adapter,
    const std::map<std::string, const zetasql::Type*>& gsql_param_types,
    int* max_param) {
  // Allocate the memory for the Oid*.
  auto current_size = gsql_param_types.size();
  ZETASQL_ASSIGN_OR_RETURN(void* allocated_array,
                   CheckedPgPalloc(sizeof(Oid) * current_size));
  Oid* pg_param_types = (Oid*)allocated_array;
  /* Zero out all slots */
  MemSet(pg_param_types, 0, current_size * sizeof(Oid));

  // Look up each parameter type using the defined parameter prefix and the
  // parameter index, starting at 1. Given N parameters, the parameters should
  // be named p1, p2,...,pN, where N = PQ_QUERY_PARAM_MAX_LIMIT = USHRT_MAX as
  // defined in the PG backend. Transform each ZetaSQL type to a PG type.
  for (auto const& it : gsql_param_types) {
    // Check that it is a valid Spangres parameter (i.e. 'p1', 'p2', ...).
    std::string parameter_name = it.first;
    int i;
    if (parameter_name.length() < 2 ||
        !absl::StartsWith(parameter_name, kParameterPrefix) ||
        !absl::SimpleAtoi(parameter_name.substr(1), &i) || i < 1 ||
                          i > USHRT_MAX) {
      return absl::InvalidArgumentError(
          absl::StrFormat("Invalid parameter name: %s. Expected one of 'p1', "
                          "'p2', ..., 'p%d'",  parameter_name, USHRT_MAX));
    }
    // Increase array size if the index is larger than the current size. This is
    // possible if the query contains parameter types for some but not all
    // parameters (e.g. types have been specified for p1 and p3, but not for p2)
    if (i > current_size) {
      // Reallocate new memory for the Oid*.
      ZETASQL_ASSIGN_OR_RETURN(allocated_array,
                       CheckedPgRepalloc(allocated_array, sizeof(Oid) * i));
      pg_param_types = (Oid*)allocated_array;
      /* Zero out the previously-unreferenced slots */
      MemSet(pg_param_types + current_size, 0,
             (i - current_size) * sizeof(Oid));

      current_size = i;
    }

    // Transform the type from ZetaSQL to PostgreSQL.
    ZETASQL_ASSIGN_OR_RETURN(Oid pg_type,
                     Transformer::BuildPgTypeOid(catalog_adapter, it.second));
    pg_param_types[i - 1] = pg_type;
  }
  *max_param = current_size;
  return pg_param_types;
}

}  // namespace postgres_translator
