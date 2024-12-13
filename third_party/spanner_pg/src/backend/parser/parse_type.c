/*-------------------------------------------------------------------------
 *
 * parse_type.c
 *		handle type operations for parser
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_type.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


#include "third_party/spanner_pg/shims/catalog_shim.h"
#include "third_party/spanner_pg/shims/catalog_shim_cc_wrappers.h"

/* SPANGRES BEGIN */
// We've made this function non-static to call it from catalog_shim.
int32 typenameTypeMod(ParseState *pstate, const TypeName *typeName,
					  Type typ);
/* SPANGRES END */


/*
 * LookupTypeName
 *		Wrapper for typical case.
 */
Type
LookupTypeName(ParseState *pstate, const TypeName *typeName,
			   int32 *typmod_p, bool missing_ok)
{
	return LookupTypeNameExtended(pstate,
								  typeName, typmod_p, true, missing_ok);
}

/*
 * LookupTypeNameExtended
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns NULL if no such type can be found.  If the type is found,
 *		the typmod value represented in the TypeName struct is computed and
 *		stored into *typmod_p.
 *
 * NB: on success, the caller must ReleaseSysCache the type tuple when done
 * with it.
 *
 * NB: direct callers of this function MUST check typisdefined before assuming
 * that the type is fully valid.  Most code should go through typenameType
 * or typenameTypeId instead.
 *
 * typmod_p can be passed as NULL if the caller does not care to know the
 * typmod value, but the typmod decoration (if any) will be validated anyway,
 * except in the case where the type is not found.  Note that if the type is
 * found but is a shell, and there is typmod decoration, an error will be
 * thrown --- this is intentional.
 *
 * If temp_ok is false, ignore types in the temporary namespace.  Pass false
 * when the caller will decide, using goodness of fit criteria, whether the
 * typeName is actually a type or something else.  If typeName always denotes
 * a type (or denotes nothing), pass true.
 *
 * pstate is only used for error location info, and may be NULL.
 *
 * SPANGRES: Create a Type (HeapTuple) out of our bootstrap_catalog instead of
 * the real heap.
 *
 */
Type
LookupTypeNameExtended(ParseState *pstate,
					   const TypeName *typeName, int32 *typmod_p,
					   bool temp_ok, bool missing_ok)
{
	// SPANGRES BEGIN
	Oid typoid = InvalidOid;
	// SPANGRES END

	if (typeName->names == NIL)
	{
		/* We have the OID already if it's an internally generated TypeName */
		typoid = typeName->typeOid;
	}
	else if (typeName->pct_type)
	{
		// SPANGRES BEGIN
		ereport(ERROR, (errmsg("%TYPE references are not supported"),
										errcode(ERRCODE_FEATURE_NOT_SUPPORTED)));
		// SPANGRES END
	}
	else
	// SPANGRES BEGIN
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
	// SPANGRES END

	// SPANGRES BEGIN
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
	// SPANGRES END
}

/*
 * LookupTypeNameOid
 *		Given a TypeName object, lookup the pg_type syscache entry of the type.
 *		Returns InvalidOid if no such type can be found.  If the type is found,
 *		return its Oid.
 *
 * NB: direct callers of this function need to be aware that the type OID
 * returned may correspond to a shell type.  Most code should go through
 * typenameTypeId instead.
 *
 * pstate is only used for error location info, and may be NULL.
 */
Oid
LookupTypeNameOid(ParseState *pstate, const TypeName *typeName, bool missing_ok)
{
	Oid			typoid;
	Type		tup;

	tup = LookupTypeName(pstate, typeName, NULL, missing_ok);
	if (tup == NULL)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist",
							TypeNameToString(typeName)),
					 parser_errposition(pstate, typeName->location)));

		return InvalidOid;
	}

	typoid = ((Form_pg_type) GETSTRUCT(tup))->oid;
	ReleaseSysCache(tup);

	return typoid;
}

/*
 * typenameType - given a TypeName, return a Type structure and typmod
 *
 * This is equivalent to LookupTypeName, except that this will report
 * a suitable error message if the type cannot be found or is not defined.
 * Callers of this can therefore assume the result is a fully valid type.
 */
Type
typenameType(ParseState *pstate, const TypeName *typeName, int32 *typmod_p)
{
	Type		tup;

	tup = LookupTypeName(pstate, typeName, typmod_p, false);
	if (tup == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));
	if (!((Form_pg_type) GETSTRUCT(tup))->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" is only a shell",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));
	return tup;
}

/*
 * typenameTypeId - given a TypeName, return the type's OID
 *
 * This is similar to typenameType, but we only hand back the type OID
 * not the syscache entry.
 */
Oid
typenameTypeId(ParseState *pstate, const TypeName *typeName)
{
	Oid			typoid;
	Type		tup;

	tup = typenameType(pstate, typeName, NULL);
	typoid = ((Form_pg_type) GETSTRUCT(tup))->oid;
	ReleaseSysCache(tup);

	return typoid;
}

/*
 * typenameTypeIdAndMod - given a TypeName, return the type's OID and typmod
 *
 * This is equivalent to typenameType, but we only hand back the type OID
 * and typmod, not the syscache entry.
 */
void
typenameTypeIdAndMod(ParseState *pstate, const TypeName *typeName,
					 Oid *typeid_p, int32 *typmod_p)
{
	Type		tup;

	tup = typenameType(pstate, typeName, typmod_p);
	*typeid_p = ((Form_pg_type) GETSTRUCT(tup))->oid;
	ReleaseSysCache(tup);
}

/*
 * typenameTypeMod - given a TypeName, return the internal typmod value
 *
 * This will throw an error if the TypeName includes type modifiers that are
 * illegal for the data type.
 *
 * The actual type OID represented by the TypeName must already have been
 * looked up, and is passed as "typ".
 *
 * pstate is only used for error location info, and may be NULL.
 */
/* SPANGRES BEGIN */
// We've made this function non-static to call it from catalog_shim.
int32
typenameTypeMod(ParseState *pstate, const TypeName *typeName, Type typ)
/* SPANGRES END */
{
	int32		result;
	Oid			typmodin;
	Datum	   *datums;
	int			n;
	ListCell   *l;
	ArrayType  *arrtypmod;
	ParseCallbackState pcbstate;

	/* Return prespecified typmod if no typmod expressions */
	if (typeName->typmods == NIL)
		return typeName->typemod;

	/*
	 * Else, type had better accept typmods.  We give a special error message
	 * for the shell-type case, since a shell couldn't possibly have a
	 * typmodin function.
	 */
	if (!((Form_pg_type) GETSTRUCT(typ))->typisdefined)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier cannot be specified for shell type \"%s\"",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));

	typmodin = ((Form_pg_type) GETSTRUCT(typ))->typmodin;

	if (typmodin == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("type modifier is not allowed for type \"%s\"",
						TypeNameToString(typeName)),
				 parser_errposition(pstate, typeName->location)));

	/*
	 * Convert the list of raw-grammar-output expressions to a cstring array.
	 * Currently, we allow simple numeric constants, string literals, and
	 * identifiers; possibly this list could be extended.
	 */
	datums = (Datum *) palloc(list_length(typeName->typmods) * sizeof(Datum));
	n = 0;
	foreach(l, typeName->typmods)
	{
		Node	   *tm = (Node *) lfirst(l);
		char	   *cstr = NULL;

		if (IsA(tm, A_Const))
		{
			A_Const    *ac = (A_Const *) tm;

			if (IsA(&ac->val, Integer))
			{
				cstr = psprintf("%ld", (long) intVal(&ac->val));
			}
			else if (IsA(&ac->val, Float))
			{
				/* we can just use the string representation directly. */
				cstr = ac->val.fval.fval;
			}
			else if (IsA(&ac->val, String))
			{
				/* we can just use the string representation directly. */
				cstr = strVal(&ac->val);
			}
		}
		else if (IsA(tm, ColumnRef))
		{
			ColumnRef  *cr = (ColumnRef *) tm;

			if (list_length(cr->fields) == 1 &&
				IsA(linitial(cr->fields), String))
				cstr = strVal(linitial(cr->fields));
		}
		if (!cstr)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("type modifiers must be simple constants or identifiers"),
					 parser_errposition(pstate, typeName->location)));
		datums[n++] = CStringGetDatum(cstr);
	}

	/* hardwired knowledge about cstring's representation details here */
	arrtypmod = construct_array(datums, n, CSTRINGOID,
								-2, false, TYPALIGN_CHAR);

	/* arrange to report location if type's typmodin function fails */
	setup_parser_errposition_callback(&pcbstate, pstate,
									  typeName->location);

	result = DatumGetInt32(OidFunctionCall1(typmodin,
											PointerGetDatum(arrtypmod)));

	cancel_parser_errposition_callback(&pcbstate);

	pfree(datums);
	pfree(arrtypmod);

	return result;
}

/*
 * appendTypeNameToBuffer
 *		Append a string representing the name of a TypeName to a StringInfo.
 *		This is the shared guts of TypeNameToString and TypeNameListToString.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
static void
appendTypeNameToBuffer(const TypeName *typeName, StringInfo string)
{
	if (typeName->names != NIL)
	{
		/* Emit possibly-qualified name as-is */
		ListCell   *l;

		foreach(l, typeName->names)
		{
			if (l != list_head(typeName->names))
				appendStringInfoChar(string, '.');
			appendStringInfoString(string, strVal(lfirst(l)));
		}
	}
	else
	{
		/* Look up internally-specified type */
		appendStringInfoString(string, format_type_be(typeName->typeOid));
	}

	/*
	 * Add decoration as needed, but only for fields considered by
	 * LookupTypeName
	 */
	if (typeName->pct_type)
		appendStringInfoString(string, "%TYPE");

	if (typeName->arrayBounds != NIL)
		appendStringInfoString(string, "[]");
}

/*
 * TypeNameToString
 *		Produce a string representing the name of a TypeName.
 *
 * NB: this must work on TypeNames that do not describe any actual type;
 * it is mostly used for reporting lookup errors.
 */
char *
TypeNameToString(const TypeName *typeName)
{
	StringInfoData string;

	initStringInfo(&string);
	appendTypeNameToBuffer(typeName, &string);
	return string.data;
}

/*
 * TypeNameListToString
 *		Produce a string representing the name(s) of a List of TypeNames
 */
char *
TypeNameListToString(List *typenames)
{
	StringInfoData string;
	ListCell   *l;

	initStringInfo(&string);
	foreach(l, typenames)
	{
		TypeName   *typeName = lfirst_node(TypeName, l);

		if (l != list_head(typenames))
			appendStringInfoChar(&string, ',');
		appendTypeNameToBuffer(typeName, &string);
	}
	return string.data;
}

/*
 * LookupCollation
 *
 * Look up collation by name, return OID, with support for error location.
 */
Oid
LookupCollation(ParseState *pstate, List *collnames, int location)
{
	Oid			colloid;
	ParseCallbackState pcbstate;

	if (pstate)
		setup_parser_errposition_callback(&pcbstate, pstate, location);

	colloid = get_collation_oid(collnames, false);

	if (pstate)
		cancel_parser_errposition_callback(&pcbstate);

	return colloid;
}

/*
 * GetColumnDefCollation
 *
 * Get the collation to be used for a column being defined, given the
 * ColumnDef node and the previously-determined column type OID.
 *
 * pstate is only used for error location purposes, and can be NULL.
 */
Oid
GetColumnDefCollation(ParseState *pstate, ColumnDef *coldef, Oid typeOid)
{
	Oid			result;
	Oid			typcollation = get_typcollation(typeOid);
	int			location = coldef->location;

	if (coldef->collClause)
	{
		/* We have a raw COLLATE clause, so look up the collation */
		location = coldef->collClause->location;
		result = LookupCollation(pstate, coldef->collClause->collname,
								 location);
	}
	else if (OidIsValid(coldef->collOid))
	{
		/* Precooked collation spec, use that */
		result = coldef->collOid;
	}
	else
	{
		/* Use the type's default collation if any */
		result = typcollation;
	}

	/* Complain if COLLATE is applied to an uncollatable type */
	if (OidIsValid(result) && !OidIsValid(typcollation))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("collations are not supported by type %s",
						format_type_be(typeOid)),
				 parser_errposition(pstate, location)));

	return result;
}

/* return a Type structure, given a type id */
/* NB: caller must ReleaseSysCache the type tuple when done with it
 *
 * SPANGRES: Create a Type (HeapTuple) out of our bootstrap_catalog instead of
 * the real heap.
 */
Type
typeidType(Oid id)
{
	// SPANGRES BEGIN
	return PgTypeFormHeapTuple(id);
	// SPANGRES END
}

/* given type (as type struct), return the type OID */
Oid
typeTypeId(Type tp)
{
	if (tp == NULL)				/* probably useless */
		elog(ERROR, "typeTypeId() called with NULL type struct");
	return ((Form_pg_type) GETSTRUCT(tp))->oid;
}

/* given type (as type struct), return the length of type */
int16
typeLen(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typlen;
}

/* given type (as type struct), return its 'byval' attribute */
bool
typeByVal(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	return typ->typbyval;
}

/* given type (as type struct), return the type's name */
char *
typeTypeName(Type t)
{
	Form_pg_type typ;

	typ = (Form_pg_type) GETSTRUCT(t);
	/* pstrdup here because result may need to outlive the syscache entry */
	return pstrdup(NameStr(typ->typname));
}

/* given type (as type struct), return its 'typrelid' attribute */
Oid
typeTypeRelid(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);
	return typtup->typrelid;
}

/* given type (as type struct), return its 'typcollation' attribute */
Oid
typeTypeCollation(Type typ)
{
	Form_pg_type typtup;

	typtup = (Form_pg_type) GETSTRUCT(typ);
	return typtup->typcollation;
}

/*
 * Given a type structure and a string, returns the internal representation
 * of that string.  The "string" can be NULL to perform conversion of a NULL
 * (which might result in failure, if the input function rejects NULLs).
 */
Datum
stringTypeDatum(Type tp, char *string, int32 atttypmod)
{
	Form_pg_type typform = (Form_pg_type) GETSTRUCT(tp);
	Oid			typinput = typform->typinput;
	Oid			typioparam = getTypeIOParam(tp);

	return OidInputFunctionCall(typinput, string, typioparam, atttypmod);
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type.
 *
 * SPANGRES: This usually return InvalidOid because Spangres does not
 * currently support complex types outside of system table row types themselves.
 */
Oid
typeidTypeRelid(Oid type_id)
{
	// SPANGRES BEGIN
	const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(type_id);
	if (type_data == NULL) {
		elog(ERROR, "bootstrap catalog lookup failed for type %u", type_id);
	} else {
		return type_data->typrelid;
	}
	// SPANGRES END
}

/*
 * Given a typeid, return the type's typrelid (associated relation), if any.
 * Returns InvalidOid if type is not a composite type or a domain over one.
 * This is the same as typeidTypeRelid(getBaseType(type_id)), but faster.
 */
Oid
typeOrDomainTypeRelid(Oid type_id)
{
	// SPANGRES BEGIN
	/*
	 * This Spangres code is not actually faster for now because getBaseType()
	 * under Spanges is currently trivial so can't be optimized much.
	 * (It calls getBaseTypeAndTypmod(), below, which currently just
	 * immediately returns its own first argument.)
	 * If getBaseTypeAndTypmod() gets extended to contain more upstream
	 * logic, this function may require optimization.
	 */
	return typeidTypeRelid(getBaseType(type_id));
	// SPANGRES END
}

/*
 * error context callback for parse failure during parseTypeString()
 */
static void
pts_error_callback(void *arg)
{
	const char *str = (const char *) arg;

	errcontext("invalid type name \"%s\"", str);
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and return the result as a TypeName.
 * If the string cannot be parsed as a type, an error is raised.
 */
TypeName *
typeStringToTypeName(const char *str)
{
	List	   *raw_parsetree_list;
	TypeName   *typeName;
	ErrorContextCallback ptserrcontext;

	/* make sure we give useful error for empty input */
	if (strspn(str, " \t\n\r\f") == strlen(str))
		goto fail;

	/*
	 * Setup error traceback support in case of ereport() during parse
	 */
	ptserrcontext.callback = pts_error_callback;
	ptserrcontext.arg = unconstify(char *, str);
	ptserrcontext.previous = error_context_stack;
	error_context_stack = &ptserrcontext;

	raw_parsetree_list = raw_parser(str, RAW_PARSE_TYPE_NAME);

	error_context_stack = ptserrcontext.previous;

	/* We should get back exactly one TypeName node. */
	Assert(list_length(raw_parsetree_list) == 1);
	typeName = linitial_node(TypeName, raw_parsetree_list);

	/* The grammar allows SETOF in TypeName, but we don't want that here. */
	if (typeName->setof)
		goto fail;

	return typeName;

fail:
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("invalid type name \"%s\"", str)));
	return NULL;				/* keep compiler quiet */
}

/*
 * Given a string that is supposed to be a SQL-compatible type declaration,
 * such as "int4" or "integer" or "character varying(32)", parse
 * the string and convert it to a type OID and type modifier.
 * If missing_ok is true, InvalidOid is returned rather than raising an error
 * when the type name is not found.
 */
void
parseTypeString(const char *str, Oid *typeid_p, int32 *typmod_p, bool missing_ok)
{
	TypeName   *typeName;
	Type		tup;

	typeName = typeStringToTypeName(str);

	tup = LookupTypeName(NULL, typeName, typmod_p, missing_ok);
	if (tup == NULL)
	{
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" does not exist",
							TypeNameToString(typeName)),
					 parser_errposition(NULL, typeName->location)));
		*typeid_p = InvalidOid;
	}
	else
	{
		Form_pg_type typ = (Form_pg_type) GETSTRUCT(tup);

		if (!typ->typisdefined)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type \"%s\" is only a shell",
							TypeNameToString(typeName)),
					 parser_errposition(NULL, typeName->location)));
		*typeid_p = typ->oid;
		ReleaseSysCache(tup);
	}
}
