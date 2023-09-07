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

/*
 * Lookup a coercion function in bootstrap_catalog. If found, set the context
 * and cast function and return the cast type. If not found, do nothing and
 * return (which is interpreted as an unsupported cast request).
 * This shim function is a copy of find_coercion_pathway except that heap lookup
 * is replaced with bootstrap_catalog lookup.
 */
CoercionPathType find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
									   CoercionContext ccontext,
									   Oid* funcid) {
	CoercionPathType result = COERCION_PATH_NONE;

	*funcid = InvalidOid;

	/*
	 * Perhaps the types are domains; if so, look at their base types
	 * NB: These are no-ops in the current shim implementation (see
	 * getBaseTypeAndTypmod), but we preserve them here so that adding
	 * Domain support doesn't require changing this function.
	 */
	if (OidIsValid(sourceTypeId))
		sourceTypeId = getBaseType(sourceTypeId);
	if (OidIsValid(targetTypeId))
		targetTypeId = getBaseType(targetTypeId);

	/* Domains are always coercible to and from their base type */
	if (sourceTypeId == targetTypeId)
		return COERCION_PATH_RELABELTYPE;

	/*
	 * Here we bypass HeapTuple creation and get the FormData struct directly from
	 * bootstrap_catalog.
	 */
	const FormData_pg_cast* castForm =
			GetCastFromBootstrapCatalog(sourceTypeId, targetTypeId);
	if (castForm != NULL) {
		CoercionContext castcontext;

		/* convert char value for castcontext to CoercionContext enum */
		switch (castForm->castcontext)
		{
			case COERCION_CODE_IMPLICIT:
				castcontext = COERCION_IMPLICIT;
				break;
			case COERCION_CODE_ASSIGNMENT:
				castcontext = COERCION_ASSIGNMENT;
				break;
			case COERCION_CODE_EXPLICIT:
				castcontext = COERCION_EXPLICIT;
				break;
			default:
				elog(ERROR, "unrecognized castcontext: %d",
					 (int) castForm->castcontext);
				castcontext = 0;	/* keep compiler quiet */
				break;
		}

		/* Rely on ordering of enum for correct behavior here */
		if (ccontext >= castcontext)
		{
			switch (castForm->castmethod)
			{
				case COERCION_METHOD_FUNCTION:
					result = COERCION_PATH_FUNC;
					*funcid = castForm->castfunc;
					break;
				case COERCION_METHOD_INOUT:
					result = COERCION_PATH_COERCEVIAIO;
					break;
				case COERCION_METHOD_BINARY:
					result = COERCION_PATH_RELABELTYPE;
					break;
				default:
					elog(ERROR, "unrecognized castmethod: %d",
						 (int) castForm->castmethod);
					break;
			}
		}
	} else {
		/*
		 * If there's no pg_cast entry, perhaps we are dealing with a pair of
		 * array types.  If so, and if their element types have a conversion
		 * pathway, report that we can coerce with an ArrayCoerceExpr.
		 *
		 * Hack: disallow coercions to oidvector and int2vector, which
		 * otherwise tend to capture coercions that should go to "real" array
		 * types.  We want those types to be considered "real" arrays for many
		 * purposes, but not this one.  (Also, ArrayCoerceExpr isn't
		 * guaranteed to produce an output that meets the restrictions of
		 * these datatypes, such as being 1-dimensional.)
		 */
		if (targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID)
		{
			Oid			targetElem;
			Oid			sourceElem;

			if ((targetElem = get_element_type(targetTypeId)) != InvalidOid &&
				(sourceElem = get_element_type(sourceTypeId)) != InvalidOid)
			{
				CoercionPathType elempathtype;
				Oid			elemfuncid;

				elempathtype = find_coercion_pathway(targetElem,
													 sourceElem,
													 ccontext,
													 &elemfuncid);
				if (elempathtype != COERCION_PATH_NONE)
				{
					result = COERCION_PATH_ARRAYCOERCE;
				}
			}
		}

		/*
		 * If we still haven't found a possibility, consider automatic casting
		 * using I/O functions.  We allow assignment casts to string types and
		 * explicit casts from string types to be handled this way. (The
		 * CoerceViaIO mechanism is a lot more general than that, but this is
		 * all we want to allow in the absence of a pg_cast entry.) It would
		 * probably be better to insist on explicit casts in both directions,
		 * but this is a compromise to preserve something of the pre-8.3
		 * behavior that many types had implicit (yipes!) casts to text.
		 */
		if (result == COERCION_PATH_NONE)
		{
			if (ccontext >= COERCION_ASSIGNMENT &&
				TypeCategory(targetTypeId) == TYPCATEGORY_STRING)
				result = COERCION_PATH_COERCEVIAIO;
			else if (ccontext >= COERCION_EXPLICIT &&
					 TypeCategory(sourceTypeId) == TYPCATEGORY_STRING)
				result = COERCION_PATH_COERCEVIAIO;
		}
	}

	return result;
}

// Loop through the arrays of input and target types, and determine whether a
// coercion path exists for each. For complex types and generics, just return
// false for now.
bool can_coerce_type(int nargs, const Oid* input_typeids,
					 const Oid* target_typeids,
					 CoercionContext ccontext)
{
	bool have_generics = false;
	int i;

	/* run through argument list... */
	for (i = 0; i < nargs; i++)
	{
		Oid			inputTypeId = input_typeids[i];
		Oid			targetTypeId = target_typeids[i];
		CoercionPathType pathtype;
		Oid			funcId;

		/* no problem if same type */
		if (inputTypeId == targetTypeId)
			continue;

		/* accept if target is ANY */
		if (targetTypeId == ANYOID)
			continue;

		/*
		 * accept if target is polymorphic, for now
		 * NB: We will reject this later to keep control flow more closely-matched
		 * to the PostgreSQL source.
		 */
		if (IsPolymorphicType(targetTypeId))
		{
			have_generics = true;	/* do more checking later */
			continue;
		}

		/*
		 * If input is an untyped string constant, assume we can convert it to
		 * anything.
		 */
		if (inputTypeId == UNKNOWNOID)
			continue;

		/*
		 * If pg_cast shows that we can coerce, accept.  This test now covers
		 * both binary-compatible and coercion-function cases.
		 */
		pathtype = find_coercion_pathway(targetTypeId, inputTypeId,
										 ccontext, &funcId);
		if (pathtype != COERCION_PATH_NONE)
			continue;

		/*
		 * Here we skip checking complex types including records (and record
		 * arrays), enums, and relations.
		 * TODO: Add support for more types.
		 *
		 * Else, cannot coerce at this argument position
		 */
		return false;
	}

	/* If we found any generic argument types, cross-check them */
	if (have_generics)
	{
		if (!check_generic_type_consistency(input_typeids, target_typeids, nargs))
			return false;
	}

	return true;
}

// Construct an expression tree for applying a pg_cast entry.
// This is used for both type-coercion and length-coercion operations,
// since there is no difference in terms of the calling convention.
//
// For the Postgres Translator, we've copied this function with only a few
// changes to support running in our environment:
//   Code style is auto-formatted to Google3 standard.
//   Comment style is changed to // style to conform to local standards.
//   The FormData_pg_proc strict is retrieved directly from bootstrap_catalog
//     instead of getting and unwrapping a HeapTuple from the PostgreSQL system
//     cache.
// The rest of this function is left unchanged for easier diffing.
Node* build_coercion_expression(Node* node, CoercionPathType pathtype,
								Oid funcId, Oid targetTypeId,
								int32_t targetTypMod,
								CoercionContext ccontext,
								CoercionForm cformat, int location)
{
	int nargs = 0;

	if (OidIsValid(funcId))
	{
		const FormData_pg_proc* procstruct;

		/*
		 * Here we get the pg_proc struct directly from our catalogs instead of a
		 * HeapTuple from the catalog cache.
		 */
		procstruct = GetProcByOid(funcId);

		/*
		 * These Asserts essentially check that function is a legal coercion
		 * function.  We can't make the seemingly obvious tests on prorettype
		 * and proargtypes[0], even in the COERCION_PATH_FUNC case, because of
		 * various binary-compatibility cases.
		 * Spangres note: these asserts are here in comment form as documentation
		 * and to ensure they are not (incorrectly) added in the future.
		 */
		/* Assert(targetTypeId == procstruct->prorettype); */
		Assert(!procstruct->proretset);
		Assert(procstruct->prokind == PROKIND_FUNCTION);
		nargs = procstruct->pronargs;
		Assert(nargs >= 1 && nargs <= 3);
		/* Assert(procstruct->proargtypes.values[0] == exprType(node)); */
		Assert(nargs < 2 || procstruct->proargtypes.values[1] == INT4OID);
		Assert(nargs < 3 || procstruct->proargtypes.values[2] == BOOLOID);

		/* Here, postgres translator removed the cache release call. */
	}

	if (pathtype == COERCION_PATH_FUNC) {
		/* We build an ordinary FuncExpr with special arguments. */
		FuncExpr* fexpr;
		List* args;
		Const* cons;

		Assert(OidIsValid(funcId));

		args = list_make1(node);

		if (nargs >= 2)
		{
			/* Pass target typmod as an int4 constant */
			cons = makeConst(INT4OID,
							 -1,
							 InvalidOid,
							 sizeof(int32_t),
							 Int32GetDatum(targetTypMod),
							 false,
							 true);

			args = lappend(args, cons);
		}

		if (nargs == 3)
		{
			/* Pass it a boolean isExplicit parameter, too */
			cons = makeConst(BOOLOID,
							 -1,
							 InvalidOid,
							 sizeof(bool),
							 BoolGetDatum(ccontext == COERCION_EXPLICIT),
							 false,
							 true);

			args = lappend(args, cons);
		}

		fexpr = makeFuncExpr(funcId, targetTypeId, args,
							 InvalidOid, InvalidOid, cformat);
		fexpr->location = location;
		return (Node *) fexpr;
	}
	else if (pathtype == COERCION_PATH_ARRAYCOERCE)
	{
		/* We need to build an ArrayCoerceExpr */
		ArrayCoerceExpr *acoerce = makeNode(ArrayCoerceExpr);
		CaseTestExpr *ctest = makeNode(CaseTestExpr);
		Oid			sourceBaseTypeId;
		int32_t		sourceBaseTypeMod;
		Oid			targetElementType;
		Node	   *elemexpr;

		/*
		 * Look through any domain over the source array type.  Note we don't
		 * expect that the target type is a domain; it must be a plain array.
		 * (To get to a domain target type, we'll do coerce_to_domain later.)
		 */
		sourceBaseTypeMod = exprTypmod(node);
		sourceBaseTypeId = getBaseTypeAndTypmod(exprType(node),
												&sourceBaseTypeMod);

		/*
		 * Set up a CaseTestExpr representing one element of the source array.
		 * This is an abuse of CaseTestExpr, but it's OK as long as there
		 * can't be any CaseExpr or ArrayCoerceExpr within the completed
		 * elemexpr.
		 */
		ctest->typeId = get_element_type(sourceBaseTypeId);
		Assert(OidIsValid(ctest->typeId));
		ctest->typeMod = sourceBaseTypeMod;
		ctest->collation = InvalidOid;	/* Assume coercions don't care */

		/* And coerce it to the target element type */
		targetElementType = get_element_type(targetTypeId);
		Assert(OidIsValid(targetElementType));

		elemexpr = coerce_to_target_type(NULL,
										 (Node *) ctest,
										 ctest->typeId,
										 targetElementType,
										 targetTypMod,
										 ccontext,
										 cformat,
										 location);
		if (elemexpr == NULL)	/* shouldn't happen */
			elog(ERROR, "failed to coerce array element type as expected");

		acoerce->arg = (Expr *) node;
		acoerce->elemexpr = (Expr *) elemexpr;
		acoerce->resulttype = targetTypeId;

		/*
		 * Label the output as having a particular element typmod only if we
		 * ended up with a per-element expression that is labeled that way.
		 */
		acoerce->resulttypmod = exprTypmod(elemexpr);
		/* resultcollid will be set by parse_collate.c */
		acoerce->coerceformat = cformat;
		acoerce->location = location;

		return (Node *) acoerce;
	}
	else if (pathtype == COERCION_PATH_COERCEVIAIO)
	{
		/* We need to build a CoerceViaIO node */
		CoerceViaIO *iocoerce = makeNode(CoerceViaIO);

		Assert(!OidIsValid(funcId));

		iocoerce->arg = (Expr *) node;
		iocoerce->resulttype = targetTypeId;
		/* resultcollid will be set by parse_collate.c */
		iocoerce->coerceformat = cformat;
		iocoerce->location = location;

		return (Node *) iocoerce;
	}
	else
	{
		elog(ERROR, "unsupported pathtype %d in build_coercion_expression",
			 (int) pathtype);
		return NULL;			/* keep compiler quiet */
	}
}

// This function was static in parse_coerce.c. Made non-static to call here.
bool is_complex_array(Oid typid);

/* IsBinaryCoercible()
 *		Check if srctype is binary-coercible to targettype.
 *
 * This notion allows us to cheat and directly exchange values without
 * going through the trouble of calling a conversion function.  Note that
 * in general, this should only be an implementation shortcut.  Before 7.4,
 * this was also used as a heuristic for resolving overloaded functions and
 * operators, but that's basically a bad idea.
 *
 * As of 7.3, binary coercibility isn't hardwired into the code anymore.
 * We consider two types binary-coercible if there is an implicitly
 * invokable, no-function-needed pg_cast entry.  Also, a domain is always
 * binary-coercible to its base type, though *not* vice versa (in the other
 * direction, one must apply domain constraint checks before accepting the
 * value as legitimate).  We also need to special-case various polymorphic
 * types.
 *
 * This function replaces IsBinaryCompatible(), which was an inherently
 * symmetric test.  Since the pg_cast entries aren't necessarily symmetric,
 * the order of the operands is now significant.
 */
bool
IsBinaryCoercible(Oid srctype, Oid targettype)
{
	const FormData_pg_cast* castForm;
	bool		result;

	/* Fast path if same type */
	if (srctype == targettype)
		return true;

	/* Anything is coercible to ANY or ANYELEMENT or ANYCOMPATIBLE */
	if (targettype == ANYOID || targettype == ANYELEMENTOID ||
		targettype == ANYCOMPATIBLEOID)
		return true;

	/* If srctype is a domain, reduce to its base type */
	if (OidIsValid(srctype))
		srctype = getBaseType(srctype);

	/* Somewhat-fast path for domain -> base type case */
	if (srctype == targettype)
		return true;

	/* Also accept any array type as coercible to ANY[COMPATIBLE]ARRAY */
	if (targettype == ANYARRAYOID || targettype == ANYCOMPATIBLEARRAYOID)
		if (type_is_array(srctype))
			return true;

	/* Also accept any non-array type as coercible to ANY[COMPATIBLE]NONARRAY */
	if (targettype == ANYNONARRAYOID || targettype == ANYCOMPATIBLENONARRAYOID)
		if (!type_is_array(srctype))
			return true;

	/* Also accept any enum type as coercible to ANYENUM */
	if (targettype == ANYENUMOID)
		if (type_is_enum(srctype))
			return true;

	/* Also accept any range type as coercible to ANY[COMPATIBLE]RANGE */
	if (targettype == ANYRANGEOID || targettype == ANYCOMPATIBLERANGEOID)
		if (type_is_range(srctype))
			return true;

	/* Also accept any composite type as coercible to RECORD */
	if (targettype == RECORDOID)
		if (ISCOMPLEX(srctype))
			return true;

	/* Also accept any composite array type as coercible to RECORD[] */
	if (targettype == RECORDARRAYOID)
		if (is_complex_array(srctype))
			return true;

	/* Else look in pg_cast */
	// SPANGRES: Use bootstrap instead of syscache.
	castForm = GetCastFromBootstrapCatalog(srctype, targettype);
	if (castForm == NULL)
		return false;			/* no cast */

	result = (castForm->castmethod == COERCION_METHOD_BINARY &&
			  castForm->castcontext == COERCION_CODE_IMPLICIT);

	return result;
}

/*
 * find_typmod_coercion_function -- does the given type need length coercion?
 *
 * If the target type possesses a pg_cast function from itself to itself,
 * it must need length coercion.
 *
 * "bpchar" (ie, char(N)) and "numeric" are examples of such types.
 *
 * If the given type is a varlena array type, we do not look for a coercion
 * function associated directly with the array type, but instead look for
 * one associated with the element type.  An ArrayCoerceExpr node must be
 * used to apply such a function.  (Note: currently, it's pointless to
 * return the funcid in this case, because it'll just get looked up again
 * in the recursive construction of the ArrayCoerceExpr's elemexpr.)
 *
 * We use the same result enum as find_coercion_pathway, but the only possible
 * result codes are:
 *	COERCION_PATH_NONE: no length coercion needed
 *	COERCION_PATH_FUNC: apply the function returned in *funcid
 *	COERCION_PATH_ARRAYCOERCE: apply the function using ArrayCoerceExpr
 *
 * Use the bootstrap catalog to get the Form_pg_cast instead of the syscache.
 */
CoercionPathType
find_typmod_coercion_function(Oid typeId, Oid *funcid)
{
	CoercionPathType result;
	const FormData_pg_type* typeForm;

	*funcid = InvalidOid;
	result = COERCION_PATH_FUNC;

	// Here we bypass HeapTuple creation and get the FormData struct directly
	// from bootstrap_catalog.
	typeForm = GetTypeFromBootstrapCatalog(typeId);

	/* Check for a varlena array type */
	if (typeForm->typelem != InvalidOid && typeForm->typlen == -1)
	{
		/* Yes, switch our attention to the element type */
		typeId = typeForm->typelem;
		result = COERCION_PATH_ARRAYCOERCE;
	}

	// Here we bypass HeapTuple creation and get the FormData struct directly
	// from bootstrap_catalog.
	const FormData_pg_cast* castForm =
		GetCastFromBootstrapCatalog(typeId, typeId);
	if (castForm != NULL) {
		*funcid = castForm->castfunc;
	}

	if (!OidIsValid(*funcid))
		result = COERCION_PATH_NONE;

	return result;
}
