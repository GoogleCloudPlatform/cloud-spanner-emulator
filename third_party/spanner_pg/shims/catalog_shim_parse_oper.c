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

// We made these functions non-static to call from here. Forward declare them to
// limit the scope.
Oid binary_oper_exact(List* opname, Oid arg1, Oid arg2);
void op_error(ParseState* pstate, List* op, char oprkind, Oid arg1, Oid arg2,
              FuncDetailCode fdresult, int location);
FuncDetailCode oper_select_candidate(int nargs, Oid* input_typeids,
                                     FuncCandidateList candidates,
                                     Oid* operOid);

/* oper() -- search for a binary operator
 * Given operator name, types of arg1 and arg2, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatypes.  Do not use this if
 * you need an exact- or binary-compatible match; see compatible_oper.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * The Spangres version of this function drops oper_cache support and gets the
 * Operator HeapTuple by creating one from a bootstrap_catalog entry (via a
 * copy). That's two significant pieces where we lose caching performance gains
 * compared to PostgreSQL.
 */
Operator oper(ParseState* pstate, List* opname, Oid ltypeId,
			  Oid rtypeId, bool noError, int location) {
	Oid			operOid;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/* Skip initial cache lookup */

	/*
	 * First try for an "exact" match.
	 */
	operOid = binary_oper_exact(opname, ltypeId, rtypeId);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get binary operators of given name */
		clist = OpernameGetCandidates(opname, 'b', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * Unspecified type for one of the arguments? then use the other
			 * (XXX this is probably dead code?)
			 */
			Oid inputOids[2];

			if (rtypeId == InvalidOid)
				rtypeId = ltypeId;
			else if (ltypeId == InvalidOid)
				ltypeId = rtypeId;
			inputOids[0] = ltypeId;
			inputOids[1] = rtypeId;
			fdresult = oper_select_candidate(2, inputOids, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = PgOperatorFormHeapTuple(operOid);

	if (HeapTupleIsValid(tup))
	{
		/*
		 * Operator cache is unsupported. Skip caching this tuple.
		 * TODO: Consider caching operator tuples.
		 */
	}
	else if (!noError)
		op_error(pstate, opname, 'b', ltypeId, rtypeId, fdresult, location);

	return (Operator)tup;
}

/* left_oper() -- search for a unary left operator (prefix operator)
 * Given operator name and type of arg, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatype.  Do not use this if
 * you need an exact- or binary-compatible match.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * The Spangres version of this function drops oper_cache support and gets the
 * Operator HeapTuple by creating one from a bootstrap_catalog entry (via a
 * copy). That's two significant pieces where we lose caching performance gains
 * compared to PostgreSQL.
 */
Operator left_oper(ParseState* pstate, List* op, Oid arg, bool noError,
														int location) {
	Oid			operOid;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/* Skip the initial cache lookup */

	/*
	 * First try for an "exact" match.
	 */
	operOid = OpernameGetOprid(op, InvalidOid, arg);
	if (!OidIsValid(operOid))
	{
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get prefix operators of given name */
		clist = OpernameGetCandidates(op, 'l', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * The returned list has args in the form (0, oprright). Move the
			 * useful data into args[0] to keep oper_select_candidate simple.
			 * XXX we are assuming here that we may scribble on the list!
			 */
			for (FuncCandidateList clisti = clist; clisti != NULL;
					 clisti = clisti->next) {
				clisti->args[0] = clisti->args[1];
			}

			/*
			 * We must run oper_select_candidate even if only one candidate,
			 * otherwise we may falsely return a non-type-compatible operator.
			 */
			fdresult = oper_select_candidate(1, &arg, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = PgOperatorFormHeapTuple(operOid);

	if (HeapTupleIsValid(tup))
	{
		/*
		 * Operator cache is unsupported. Skip caching this tuple.
		 * TODO: Consider caching operator tuples.
		 */
	}
	else if (!noError)
		op_error(pstate, op, 'l', InvalidOid, arg, fdresult, location);

	return (Operator)tup;
}

/* right_oper() -- search for a unary right operator (postfix operator)
 * Given operator name and type of arg, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatype.  Do not use this if
 * you need an exact- or binary-compatible match.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * The Spangres version of this function drops oper_cache support and gets the
 * Operator HeapTuple by creating one from a bootstrap_catalog entry (via a
 * copy). That's two significant pieces where we lose caching performance gains
 * compared to PostgreSQL.
 */
Operator right_oper(ParseState* pstate, List* op, Oid arg,
					bool noError, int location) {
	Oid			operOid;
	FuncDetailCode fdresult = FUNCDETAIL_NOTFOUND;
	HeapTuple	tup = NULL;

	/* Skip the initial cache lookup */

	/*
	 * First try for an "exact" match.
	 */
	operOid = OpernameGetOprid(op, arg, InvalidOid);
	if (!OidIsValid(operOid)) {
		/*
		 * Otherwise, search for the most suitable candidate.
		 */
		FuncCandidateList clist;

		/* Get postfix operators of given name */
		clist = OpernameGetCandidates(op, 'r', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			/*
			 * We must run oper_select_candidate even if only one candidate,
			 * otherwise we may falsely return a non-type-compatible operator.
			 */
			fdresult = oper_select_candidate(1, &arg, clist, &operOid);
		}
	}

	if (OidIsValid(operOid))
		tup = PgOperatorFormHeapTuple(operOid);

	if (HeapTupleIsValid(tup))
	{
		/*
		 * Operator cache is unsupported. Skip caching this tuple.
		 * TODO: Consider caching operator tuples.
		 */
	}
	else if (!noError)
		op_error(pstate, op, 'r', arg, InvalidOid, fdresult, location);

	return (Operator)tup;
}

