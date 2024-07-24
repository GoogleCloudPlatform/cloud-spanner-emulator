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

// Do nothing and return. We don't bother with global state at the cost of more
// specific parser internal errors.
void setup_parser_errposition_callback(ParseCallbackState* pcbstate,
									   ParseState* pstate,
									   int location) {}

void cancel_parser_errposition_callback(ParseCallbackState* pcbstate) {}

void free_parsestate(ParseState* pstate) {
	/*
	 * Check that we did not produce too many resnos; at the very least we
	 * cannot allow more than 2^16, since that would exceed the range of a
	 * AttrNumber. It seems safest to use MaxTupleAttributeNumber.
	 */
	if (pstate->p_next_resno - 1 > MaxTupleAttributeNumber) {
		ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("target lists can have at most %d entries",
										MaxTupleAttributeNumber)));
	}

	pfree(pstate);
}

/*
 * make_const
 *
 *	Convert a A_Const value (as returned by the grammar) to a Const node
 *	of the "natural" type for the constant.  Note that this routine is
 *	only used when there is no explicit cast for the constant, so we
 *	have to guess what type is wanted.
 *
 *	For string literals we produce a constant of type UNKNOWN ---- whose
 *	representation is the same as cstring, but it indicates to later type
 *	resolution that we're not sure yet what type it should be considered.
 *	Explicit "NULL" constants are also typed as UNKNOWN.
 *
 *	For integers and floats we produce int4, int8_t, or numeric depending
 *	on the value of the number.  XXX We should produce int2 as well,
 *	but additional cleanup is needed before we can do that; there are
 *	too many examples that fail if we try.
 *
 * SPANGRES: choose int8_t (instead of int4) for integers and int8_t or numeric
 * (never int4) for floats. 
 */
Const *
make_const(ParseState* pstate, A_Const* aconst)
{
	Const	   *con;
	Datum		val;
	int64_t		val64;
	char		*endptr;
	Oid			typeid;
	int			typelen;
	bool		typebyval;
	ParseCallbackState pcbstate;

	if (aconst->isnull)
	{
		/* return a null const */
		con = makeConst(UNKNOWNOID,
						-1,
						InvalidOid,
						-2,
						(Datum) 0,
						true,
						false);
		con->location = aconst->location;
		return con;
	}

	switch (nodeTag(&aconst->val))
	{
		case T_Integer:
			val = Int64GetDatum(intVal(&aconst->val));

			typeid = INT8OID;
			typelen = sizeof(int64_t);
			typebyval = true;
			break;

		case T_Float:
			/* could be an oversize integer as well as a float ... */
			errno = 0;
			val64 = strtoi64(aconst->val.fval.fval, &endptr, 10);
			if (errno == 0 && *endptr == '\0')
			{
				/* SPANGRES: skip checking if the value fits in an int32_t. */
				val = Int64GetDatum(val64);

				typeid = INT8OID;
				typelen = sizeof(int64_t);
				typebyval = FLOAT8PASSBYVAL;	/* int8_t and float8 alike */
			}
			else
			{
				/* arrange to report location if numeric_in() fails */
				setup_parser_errposition_callback(&pcbstate, pstate, aconst->location);
				val = DirectFunctionCall3(numeric_in,
										  CStringGetDatum(aconst->val.fval.fval),
										  ObjectIdGetDatum(InvalidOid),
										  Int32GetDatum(-1));
				cancel_parser_errposition_callback(&pcbstate);

				typeid = NUMERICOID;
				typelen = -1;	/* variable len */
				typebyval = false;
			}
			break;

		case T_Boolean:
			val = BoolGetDatum(boolVal(&aconst->val));

			typeid = BOOLOID;
			typelen = 1;
			typebyval = true;
			break;

		case T_String:

			/*
			 * We assume here that UNKNOWN's internal representation is the
			 * same as CSTRING
			 */
			val = CStringGetDatum(strVal(&aconst->val));

			typeid = UNKNOWNOID;	/* will be coerced later */
			typelen = -2;		/* cstring-style varwidth type */
			typebyval = false;
			break;

		case T_BitString:
			/* arrange to report location if bit_in() fails */
			setup_parser_errposition_callback(&pcbstate, pstate, aconst->location);
			val = DirectFunctionCall3(bit_in,
									  CStringGetDatum(aconst->val.bsval.bsval),
									  ObjectIdGetDatum(InvalidOid),
									  Int32GetDatum(-1));
			cancel_parser_errposition_callback(&pcbstate);
			typeid = BITOID;
			typelen = -1;
			typebyval = false;
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(&aconst->val));
			return NULL;		/* keep compiler quiet */
	}

	con = makeConst(typeid,
					-1,			/* typmod -1 is OK for all cases */
					InvalidOid, /* all cases are uncollatable types */
					typelen,
					val,
					false,
					typebyval);
	con->location = aconst->location;

	return con;
}

/*
 * transformContainerType()
 *		Identify the types involved in a subscripting operation for container
 *
 *
 * On entry, containerType/containerTypmod identify the type of the input value
 * to be subscripted (which could be a domain type).  These are modified if
 * necessary to identify the actual container type and typmod, and the
 * container's element type is returned.  An error is thrown if the input isn't
 * an array type.
 *
 * Spangres version uses bootstrap instead of syscache.
 */
void
transformContainerType(Oid *containerType, int32_t *containerTypmod)
{
	/*
	 * If the input is a domain, smash to base type, and extract the actual
	 * typmod to be applied to the base type. Subscripting a domain is an
	 * operation that necessarily works on the base container type, not the
	 * domain itself. (Note that we provide no method whereby the creator of a
	 * domain over a container type could hide its ability to be subscripted.)
	 */
	*containerType = getBaseTypeAndTypmod(*containerType, containerTypmod);

	/*
	 * Here is an array specific code. We treat int2vector and oidvector as
	 * though they were domains over int2[] and oid[].  This is needed because
	 * array slicing could create an array that doesn't satisfy the
	 * dimensionality constraints of the xxxvector type; so we want the result
	 * of a slice operation to be considered to be of the more general type.
	 */
	if (*containerType == INT2VECTOROID)
		*containerType = INT2ARRAYOID;
	else if (*containerType == OIDVECTOROID)
		*containerType = OIDARRAYOID;
}

/*
 * transformContainerSubscripts()
 *		Transform container (array, etc) subscripting.  This is used for both
 *		container fetch and container assignment.
 *
 * In a container fetch, we are given a source container value and we produce
 * an expression that represents the result of extracting a single container
 * element or a container slice.
 *
 * Container assignments are treated basically the same as container fetches
 * here.  The caller will modify the result node to insert the source value
 * that is to be assigned to the element or slice that a fetch would have
 * retrieved.  The execution result will be a new container value with
 * the source value inserted into the right part of the container.
 *
 * For both cases, if the source is of a domain-over-container type, the
 * result is the same as if it had been of the container type; essentially,
 * we must fold a domain to its base type before applying subscripting.
 * (Note that int2vector and oidvector are treated as domains here.)
 *
 * pstate			Parse state
 * containerBase	Already-transformed expression for the container as a whole
 * containerType	OID of container's datatype (should match type of
 *					containerBase, or be the base type of containerBase's
 *					domain type)
 * containerTypMod	typmod for the container
 * indirection		Untransformed list of subscripts (must not be NIL)
 * isAssignment		True if this will become a container assignment.
 */
SubscriptingRef *
transformContainerSubscripts(ParseState *pstate,
							 Node *containerBase,
							 Oid containerType,
							 int32_t containerTypMod,
							 List *indirection,
							 bool isAssignment)
{
	SubscriptingRef *sbsref;
	const struct SubscriptRoutines *sbsroutines;
	Oid			elementType;
	bool		isSlice = false;
	ListCell   *idx;

	/*
	 * Determine the actual container type, smashing any domain.  In the
	 * assignment case the caller already did this, since it also needs to
	 * know the actual container type.
	 */
	if (!isAssignment)
		transformContainerType(&containerType, &containerTypMod);

	/*
	 * Verify that the container type is subscriptable, and get its support
	 * functions and typelem.
	 */
	sbsroutines = getSubscriptingRoutines(containerType, &elementType);
	if (!sbsroutines)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot subscript type %s because it does not support subscripting",
						format_type_be(containerType)),
				 parser_errposition(pstate, exprLocation(containerBase))));

	/*
	 * Detect whether any of the indirection items are slice specifiers.
	 *
	 * A list containing only simple subscripts refers to a single container
	 * element.  If any of the items are slice specifiers (lower:upper), then
	 * the subscript expression means a container slice operation.
	 */
	foreach(idx, indirection)
	{
		A_Indices  *ai = lfirst_node(A_Indices, idx);

		if (ai->is_slice)
		{
			isSlice = true;
			break;
		}
	}

	/*
	 * Ready to build the SubscriptingRef node.
	 */
	sbsref = makeNode(SubscriptingRef);

	sbsref->refcontainertype = containerType;
	sbsref->refelemtype = elementType;
	/* refrestype is to be set by container-specific logic */
	sbsref->reftypmod = containerTypMod;
	/* refcollid will be set by parse_collate.c */
	/* refupperindexpr, reflowerindexpr are to be set by container logic */
	sbsref->refexpr = (Expr *) containerBase;
	sbsref->refassgnexpr = NULL;	/* caller will fill if it's an assignment */

	/*
	 * Call the container-type-specific logic to transform the subscripts and
	 * determine the subscripting result type.
	 */
	sbsroutines->transform(sbsref, indirection, pstate,
						   isSlice, isAssignment);

	/*
	 * Verify we got a valid type (this defends, for example, against someone
	 * using array_subscript_handler as typsubscript without setting typelem).
	 */
	if (!OidIsValid(sbsref->refrestype))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot subscript type %s because it does not support subscripting",
						format_type_be(containerType))));

	return sbsref;
}
