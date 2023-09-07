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

// Portions derived from source copyright Postgres Global Development Group in
// accordance with the following notice:
//------------------------------------------------------------------------------
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
// Portions Copyright 2022 Google LLC
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

#include "third_party/spanner_pg/shims/catalog_shim.h"

#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim_cc_wrappers.h"

Type typeidType(Oid id) { return PgTypeFormHeapTuple(id); }

// We made this function non-static to call from here. Forward declare it to
// limit the scope.
int32_t typenameTypeMod(ParseState *pstate, const TypeName *typeName,
					  Type typ);

Type LookupTypeNameExtended(ParseState* pstate,
							const TypeName* typeName, int32_t* typmod_p,
							bool temp_ok, bool missing_ok) {
	Oid typoid = InvalidOid;

	if (typeName->names == NIL)
	{
		/* We have the OID already if it's an internally generated TypeName */
		typoid = typeName->typeOid;
	}
	else if (typeName->pct_type)
	{
		ereport(ERROR, (errmsg("%TYPE references are not supported"),
										errcode(ERRCODE_FEATURE_NOT_SUPPORTED)));
	}
	else
	{
		/* Normal reference to a type name */
		char	   *schemaname;
		char	   *typname;
  	Oid			 namespaceId;

		/* deconstruct the name list */
		DeconstructQualifiedName(typeName->names, &schemaname, &typname);

    if (schemaname) {
      namespaceId = GetNamespaceByNameFromBootstrapCatalog(schemaname);
      if (!OidIsValid(namespaceId))
        return NULL;
    } else {
      /*
       * Spangres does not support search path. If a schemaname is not
       * provided, use pg_catalog by default.
       * TODO: Add support for search path.
       */
      namespaceId = GetNamespaceByNameFromBootstrapCatalog("pg_catalog");
    }

    /* Search bootstrap catalog by type name only */
    const FormData_pg_type* const* type_list;
    size_t type_count;
    GetTypesByNameFromBootstrapCatalog(typname, &type_list, &type_count);

    for (int type_index = 0; type_index < type_count; ++type_index) {
      const FormData_pg_type* typeform = type_list[type_index];
      /*
       * Here PostgreSQL would check search path if a namespace was not
       * provided. Spangres assumes that a namespace is provided or defaults
       * to pg_catalog.
       * TODO: Add support for search path.
       */
      if (typeform->typnamespace != namespaceId)
        continue;

      typoid = typeform->oid;
    }

		/* If an array reference, return the array type instead */
		if (typeName->arrayBounds != NIL) {
			typoid = get_array_type(typoid);
		}
	}

	if (!OidIsValid(typoid))
	{
		if (typmod_p) {
			// -1 represents "this type does not use a typmod" and is a widely-used
			// safe default for PostgreSQL.
			*typmod_p = -1;
		}
		return NULL;	// Let callers decide whether this is an error.
	}

	Type type_tuple = PgTypeFormHeapTuple(typoid);
	if (typmod_p) {
		*typmod_p = typenameTypeMod(pstate, typeName, type_tuple);
	}
	return type_tuple;
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type.
 *
 * Spangres note: this usually return InvalidOid because Spangres does not
 * currently support complex types outside of system table row types themselves.
 */
Oid typeidTypeRelid(Oid type_id) {
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(type_id);
	if (type_data == NULL) {
		elog(ERROR, "bootstrap catalog lookup failed for type %u", type_id);
	} else {
		return type_data->typrelid;
	}
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type or a domain over one.
 * This is the same as typeidTypeRelid(getBaseType(type_id)), but faster.
 */
Oid typeOrDomainTypeRelid(Oid type_id) {
	/*
	 * This Spangres shim is not actually faster for now because getBaseType()
	 * under Spanges is currently trivial so can't be optimized much.
	 * (It calls getBaseTypeAndTypmod(), below, which currently just
	 * immediately returns its own first argument.)
	 * If getBaseTypeAndTypmod() gets extended to contain more upstream
	 * logic, this function may require optimization.
	 */
	return typeidTypeRelid(getBaseType(type_id));
}
