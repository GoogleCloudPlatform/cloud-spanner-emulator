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
 *	Convert a Value node (as returned by the grammar) to a Const node
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
make_const(ParseState *pstate, Value *value, int location)
{
	Const	   *con;
	Datum		val;
	int64_t		val64;
	Oid			typeid;
	int			typelen;
	bool		typebyval;
	ParseCallbackState pcbstate;

	switch (nodeTag(value))
	{
		case T_Integer:
			val = Int64GetDatum(intVal(value));

			typeid = INT8OID;
			typelen = sizeof(int64_t);
			typebyval = true;
			break;

		case T_Float:
			/* could be an oversize integer as well as a float ... */
			if (scanint8(strVal(value), true, &val64))
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
				setup_parser_errposition_callback(&pcbstate, pstate, location);
				val = DirectFunctionCall3(numeric_in,
										  CStringGetDatum(strVal(value)),
										  ObjectIdGetDatum(InvalidOid),
										  Int32GetDatum(-1));
				cancel_parser_errposition_callback(&pcbstate);

				typeid = NUMERICOID;
				typelen = -1;	/* variable len */
				typebyval = false;
			}
			break;

		case T_String:

			/*
			 * We assume here that UNKNOWN's internal representation is the
			 * same as CSTRING
			 */
			val = CStringGetDatum(strVal(value));

			typeid = UNKNOWNOID;	/* will be coerced later */
			typelen = -2;		/* cstring-style varwidth type */
			typebyval = false;
			break;

		case T_BitString:
			/* arrange to report location if bit_in() fails */
			setup_parser_errposition_callback(&pcbstate, pstate, location);
			val = DirectFunctionCall3(bit_in,
									  CStringGetDatum(strVal(value)),
									  ObjectIdGetDatum(InvalidOid),
									  Int32GetDatum(-1));
			cancel_parser_errposition_callback(&pcbstate);
			typeid = BITOID;
			typelen = -1;
			typebyval = false;
			break;

		case T_Null:
			/* return a null const */
			con = makeConst(UNKNOWNOID,
							-1,
							InvalidOid,
							-2,
							(Datum) 0,
							true,
							false);
			con->location = location;
			return con;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(value));
			return NULL;		/* keep compiler quiet */
	}

	con = makeConst(typeid,
					-1,			/* typmod -1 is OK for all cases */
					InvalidOid, /* all cases are uncollatable types */
					typelen,
					val,
					false,
					typebyval);
	con->location = location;

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
Oid
transformContainerType(Oid *containerType, int32_t *containerTypmod)
{
	Oid			origContainerType = *containerType;
	Oid			elementType;
	const FormData_pg_type* type_struct_container;

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

	/* Get the type tuple for the container */
  type_struct_container =  GetTypeFromBootstrapCatalog(*containerType);
	if (type_struct_container == NULL)
		elog(ERROR, "catalog lookup failed for type %u", *containerType);

	/* needn't check typisdefined since this will fail anyway */

	elementType = type_struct_container->typelem;
	if (elementType == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot subscript type %s because it is not an array",
						format_type_be(origContainerType))));

	return elementType;
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
 * In a container assignment, we are given a destination container value plus a
 * source value that is to be assigned to a single element or a slice of that
 * container. We produce an expression that represents the new container value
 * with the source data inserted into the right part of the container.
 *
 * For both cases, if the source container is of a domain-over-array type,
 * the result is of the base array type or its element type; essentially,
 * we must fold a domain to its base type before applying subscripting.
 * (Note that int2vector and oidvector are treated as domains here.)
 *
 * pstate			Parse state
 * containerBase	Already-transformed expression for the container as a whole
 * containerType	OID of container's datatype (should match type of
 *					containerBase, or be the base type of containerBase's
 *					domain type)
 * elementType		OID of container's element type (fetch with
 *					transformContainerType, or pass InvalidOid to do it here)
 * containerTypMod	typmod for the container (which is also typmod for the
 *					elements)
 * indirection		Untransformed list of subscripts (must not be NIL)
 * assignFrom		NULL for container fetch, else transformed expression for
 *					source.
 */
SubscriptingRef *
transformContainerSubscripts(ParseState *pstate,
							 Node *containerBase,
							 Oid containerType,
							 Oid elementType,
							 int32_t containerTypMod,
							 List *indirection,
							 Node *assignFrom)
{
	bool		isSlice = false;
	List	   *upperIndexpr = NIL;
	List	   *lowerIndexpr = NIL;
	ListCell   *idx;
	SubscriptingRef *sbsref;

	/*
	 * Caller may or may not have bothered to determine elementType.  Note
	 * that if the caller did do so, containerType/containerTypMod must be as
	 * modified by transformContainerType, ie, smash domain to base type.
	 */
	if (!OidIsValid(elementType))
		elementType = transformContainerType(&containerType, &containerTypMod);

	/*
	 * A list containing only simple subscripts refers to a single container
	 * element.  If any of the items are slice specifiers (lower:upper), then
	 * the subscript expression means a container slice operation.  In this
	 * case, we convert any non-slice items to slices by treating the single
	 * subscript as the upper bound and supplying an assumed lower bound of 1.
	 * We have to prescan the list to see if there are any slice items.
	 */
	foreach(idx, indirection)
	{
		A_Indices  *ai = (A_Indices *) lfirst(idx);

		if (ai->is_slice)
		{
			isSlice = true;
			break;
		}
	}

	/*
	 * Transform the subscript expressions.
	 */
	foreach(idx, indirection)
	{
		A_Indices  *ai = lfirst_node(A_Indices, idx);
		Node	   *subexpr;

		if (isSlice)
		{
			if (ai->lidx)
			{
				subexpr = transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
				/* If it's not int4 already, try to coerce */
				/* SPANGRES: use INT8 instead of INT4 */
				subexpr = coerce_to_target_type(pstate,
												subexpr, exprType(subexpr),
												INT8OID, -1,
												COERCION_ASSIGNMENT,
												COERCE_IMPLICIT_CAST,
												-1);
				if (subexpr == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("array subscript must have type integer"),
							 parser_errposition(pstate, exprLocation(ai->lidx))));
			}
			else if (!ai->is_slice)
			{
				/* Make a constant 1 */
				/* SPANGRES: use INT8 instead of INT4 */
				subexpr = (Node *) makeConst(INT8OID,
											 -1,
											 InvalidOid,
											 sizeof(int64_t),
											 Int64GetDatum(1),
											 false,
											 true); /* pass by value */
			}
			else
			{
				/* Slice with omitted lower bound, put NULL into the list */
				subexpr = NULL;
			}
			lowerIndexpr = lappend(lowerIndexpr, subexpr);
		}
		else
			Assert(ai->lidx == NULL && !ai->is_slice);

		if (ai->uidx)
		{
			subexpr = transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
			/* If it's not int4 already, try to coerce */
			/* SPANGRES: use INT8 instead of INT4 */
			subexpr = coerce_to_target_type(pstate,
											subexpr, exprType(subexpr),
											INT8OID, -1,
											COERCION_ASSIGNMENT,
											COERCE_IMPLICIT_CAST,
											-1);
			if (subexpr == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("array subscript must have type integer"),
						 parser_errposition(pstate, exprLocation(ai->uidx))));
		}
		else
		{
			/* Slice with omitted upper bound, put NULL into the list */
			Assert(isSlice && ai->is_slice);
			subexpr = NULL;
		}
		upperIndexpr = lappend(upperIndexpr, subexpr);
	}

	/*
	 * If doing an array store, coerce the source value to the right type.
	 * (This should agree with the coercion done by transformAssignedExpr.)
	 */
	if (assignFrom != NULL)
	{
		Oid			typesource = exprType(assignFrom);
		Oid			typeneeded = isSlice ? containerType : elementType;
		Node	   *newFrom;

		newFrom = coerce_to_target_type(pstate,
										assignFrom, typesource,
										typeneeded, containerTypMod,
										COERCION_ASSIGNMENT,
										COERCE_IMPLICIT_CAST,
										-1);
		if (newFrom == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("array assignment requires type %s"
							" but expression is of type %s",
							format_type_be(typeneeded),
							format_type_be(typesource)),
					 errhint("You will need to rewrite or cast the expression."),
					 parser_errposition(pstate, exprLocation(assignFrom))));
		assignFrom = newFrom;
	}

	/*
	 * Ready to build the SubscriptingRef node.
	 */
	sbsref = (SubscriptingRef *) makeNode(SubscriptingRef);
	if (assignFrom != NULL)
		sbsref->refassgnexpr = (Expr *) assignFrom;

	sbsref->refcontainertype = containerType;
	sbsref->refelemtype = elementType;
	sbsref->reftypmod = containerTypMod;
	/* refcollid will be set by parse_collate.c */
	sbsref->refupperindexpr = upperIndexpr;
	sbsref->reflowerindexpr = lowerIndexpr;
	sbsref->refexpr = (Expr *) containerBase;
	sbsref->refassgnexpr = (Expr *) assignFrom;

	return sbsref;
}
