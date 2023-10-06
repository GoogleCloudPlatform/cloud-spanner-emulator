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

// This function (defined in parse_func.c) was made non-static to call from
// here. Forward declare it to limit the scope.
Oid FuncNameAsType(List *funcname);

/*
 * Find the named function in the system catalogs.
 *
 * Attempt to find the named function in the system catalogs with
 * arguments exactly as specified, so that the normal case (exact match)
 * is as quick as possible.
 *
 * If an exact match isn't found:
 *	1) check for possible interpretation as a type coercion request
 *	2) apply the ambiguous-function resolution rules
 *
 * Return values *funcid through *true_typeids receive info about the function.
 * If argdefaults isn't NULL, *argdefaults receives a list of any default
 * argument expressions that need to be added to the given arguments.
 * NOTE: Spangres does not support default arguments.
 *
 * When processing a named- or mixed-notation call (ie, fargnames isn't NIL),
 * the returned true_typeids and argdefaults are ordered according to the
 * call's argument ordering: first any positional arguments, then the named
 * arguments, then defaulted arguments (if needed and allowed by
 * expand_defaults).  Some care is needed if this information is to be compared
 * to the function's pg_proc entry, but in practice the caller can usually
 * just work with the call's argument ordering.
 * NOTE: Spangres does not support named arguments.
 *
 * We rely primarily on fargnames/nargs/argtypes as the argument description.
 * The actual expression node list is passed in fargs so that we can check
 * for type coercion of a constant.  Some callers pass fargs == NIL indicating
 * they don't need that check made.  Note also that when fargnames isn't NIL,
 * the fargs list must be passed if the caller wants actual argument position
 * information to be returned into the NamedArgExpr nodes.
 */
FuncDetailCode func_get_detail(List* funcname,
				List *fargs,
				List *fargnames,
				int nargs,
				Oid *argtypes,
				bool expand_variadic,
				bool expand_defaults,
				bool include_out_arguments,
				Oid *funcid,	/* return value */
				Oid *rettype,	/* return value */
				bool *retset,	/* return value */
				int *nvargs,	/* return value */
				Oid *vatype,	/* return value */
				Oid **true_typeids, /* return value */
				List **argdefaults) /* optional return value */
{
	FuncCandidateList raw_candidates;
	FuncCandidateList best_candidate;

	/* Initialize output arguments. (PostgreSQL claims this is "to silence
	 * compiler warnings" but that's a lie. Some of these are passed in
	 * uninitialized and then read by the caller.
	 */
	*funcid = InvalidOid;
	*rettype = InvalidOid;
	*retset = false;
	*nvargs = 0;
	*vatype = InvalidOid;
	*true_typeids = NULL;
	if (argdefaults)
		*argdefaults = NIL;

	/* Get list of possible candidates from namespace search */
	raw_candidates = FuncnameGetCandidates(funcname, nargs, fargnames,
										   expand_variadic, expand_defaults,
										   include_out_arguments, false);

	/*
	 * Quickly check if there is an exact match to the input datatypes (there
	 * can be only one)
	 */
	for (best_candidate = raw_candidates;
		 best_candidate != NULL;
		 best_candidate = best_candidate->next)
	{
		/* if nargs==0, argtypes can be null; don't pass that to memcmp */
		if (nargs == 0 ||
			memcmp(argtypes, best_candidate->args, nargs * sizeof(Oid)) == 0)
			break;
	}

	if (best_candidate == NULL)
	{
		/*
		 * If we didn't find an exact match, next consider the possibility
		 * that this is really a type-coercion request: a single-argument
		 * function call where the function name is a type name.  If so, and
		 * if the coercion path is RELABELTYPE or COERCEVIAIO, then go ahead
		 * and treat the "function call" as a coercion.
		 *
		 * This interpretation needs to be given higher priority than
		 * interpretations involving a type coercion followed by a function
		 * call, otherwise we can produce surprising results. For example, we
		 * want "text(varchar)" to be interpreted as a simple coercion, not as
		 * "text(name(varchar))" which the code below this point is entirely
		 * capable of selecting.
		 *
		 * We also treat a coercion of a previously-unknown-type literal
		 * constant to a specific type this way.
		 *
		 * The reason we reject COERCION_PATH_FUNC here is that we expect the
		 * cast implementation function to be named after the target type.
		 * Thus the function will be found by normal lookup if appropriate.
		 *
		 * The reason we reject COERCION_PATH_ARRAYCOERCE is mainly that you
		 * can't write "foo[] (something)" as a function call.  In theory
		 * someone might want to invoke it as "_foo (something)" but we have
		 * never supported that historically, so we can insist that people
		 * write it as a normal cast instead.
		 *
		 * We also reject the specific case of COERCEVIAIO for a composite
		 * source type and a string-category target type.  This is a case that
		 * find_coercion_pathway() allows by default, but experience has shown
		 * that it's too commonly invoked by mistake.  So, again, insist that
		 * people use cast syntax if they want to do that.
		 *
		 * NB: it's important that this code does not exceed what coerce_type
		 * can do, because the caller will try to apply coerce_type if we
		 * return FUNCDETAIL_COERCION.  If we return that result for something
		 * coerce_type can't handle, we'll cause infinite recursion between
		 * this module and coerce_type!
		 */
		if (nargs == 1 && fargs != NIL && fargnames == NIL)
		{
			Oid			targetType = FuncNameAsType(funcname);

			if (OidIsValid(targetType))
			{
				Oid			sourceType = argtypes[0];
				Node	   *arg1 = linitial(fargs);
				bool		iscoercion;

				if (sourceType == UNKNOWNOID && IsA(arg1, Const))
				{
					/* always treat typename('literal') as coercion */
					iscoercion = true;
				}
				else
				{
					CoercionPathType cpathtype;
					Oid			cfuncid;

					cpathtype = find_coercion_pathway(targetType, sourceType,
													  COERCION_EXPLICIT,
													  &cfuncid);
					switch (cpathtype)
					{
						case COERCION_PATH_RELABELTYPE:
							iscoercion = true;
							break;
						case COERCION_PATH_COERCEVIAIO:
							if ((sourceType == RECORDOID ||
								 ISCOMPLEX(sourceType)) &&
								TypeCategory(targetType) == TYPCATEGORY_STRING)
								iscoercion = false;
							else
								iscoercion = true;
							break;
						default:
							iscoercion = false;
							break;
					}
				}

				if (iscoercion)
				{
					/* Treat it as a type coercion */
					*funcid = InvalidOid;
					*rettype = targetType;
					*retset = false;
					*nvargs = 0;
					*vatype = InvalidOid;
					*true_typeids = argtypes;
					return FUNCDETAIL_COERCION;
				}
			}
		}

		/*
		 * didn't find an exact match, so now try to match up candidates...
		 */
		if (raw_candidates != NULL)
		{
			FuncCandidateList current_candidates;
			int			ncandidates;

			ncandidates = func_match_argtypes(nargs,
											  argtypes,
											  raw_candidates,
											  &current_candidates);

			/* one match only? then run with it... */
			if (ncandidates == 1)
				best_candidate = current_candidates;

			/*
			 * multiple candidates? then better decide or throw an error...
			 */
			else if (ncandidates > 1)
			{
				best_candidate = func_select_candidate(nargs,
													   argtypes,
													   current_candidates);

				/*
				 * If we were able to choose a best candidate, we're done.
				 * Otherwise, ambiguous function call.
				 */
				if (!best_candidate)
					return FUNCDETAIL_MULTIPLE;
			}
		}
	}

	if (best_candidate)
	{
		/*
		 * If processing named args or expanding variadics or defaults, the
		 * "best candidate" might represent multiple equivalently good
		 * functions; treat this case as ambiguous.
		 */
		if (!OidIsValid(best_candidate->oid))
			return FUNCDETAIL_MULTIPLE;

		/*
		 * We disallow VARIADIC with named arguments unless the last argument
		 * (the one with VARIADIC attached) actually matched the variadic
		 * parameter.  This is mere pedantry, really, but some folks insisted.
		 */
		if (fargnames != NIL && !expand_variadic && nargs > 0 &&
			best_candidate->argnumbers[nargs - 1] != nargs - 1)
			return FUNCDETAIL_NOTFOUND;

		*funcid = best_candidate->oid;
		*nvargs = best_candidate->nvargs;
		*true_typeids = best_candidate->args;

		/*
		 * If processing named args, return actual argument positions into
		 * NamedArgExpr nodes in the fargs list.  This is a bit ugly but not
		 * worth the extra notation needed to do it differently.
		 */
		if (best_candidate->argnumbers != NULL)
		{
			int			i = 0;
			ListCell   *lc;

			foreach(lc, fargs)
			{
				NamedArgExpr *na = (NamedArgExpr *) lfirst(lc);

				if (IsA(na, NamedArgExpr))
					na->argnumber = best_candidate->argnumbers[i];
				i++;
			}
		}

		const FormData_pg_proc* pform =
				GetProcByOid(best_candidate->oid);
		if (pform == NULL) {
			// should not happen.
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
											errmsg("catalog lookup failed for function %u",
														 best_candidate->oid)));
		}
		*rettype = pform->prorettype;
		*retset = pform->proretset;
		*vatype = pform->provariadic;

		/*
		 * Here we would fetch default args if caller wants them. But Spangres does
		 * not support default args (bootstrap catalog does not have them).
		 * TODO: Add support for default args.
		 */
		if (argdefaults && best_candidate->ndargs > 0) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
											errmsg("Default arguments are not supported.")));
		}

		FuncDetailCode result;
		switch (pform->prokind)
		{
			case PROKIND_AGGREGATE:
				result = FUNCDETAIL_AGGREGATE;
				break;
			case PROKIND_FUNCTION:
				result = FUNCDETAIL_NORMAL;
				break;
			case PROKIND_PROCEDURE:
				result = FUNCDETAIL_PROCEDURE;
				break;
			case PROKIND_WINDOW:
				result = FUNCDETAIL_WINDOWFUNC;
				break;
			default:
				elog(ERROR, "unrecognized prokind: %c", pform->prokind);
				result = FUNCDETAIL_NORMAL; /* keep compiler quiet */
				break;
		}

		return result;
	}

	return FUNCDETAIL_NOTFOUND;
}

// These functions are originally defined in parse_func.c. We made them
// non-static to call from here. Forward declare them to limit the scope.
void unify_hypothetical_args(ParseState* pstate, List* fargs,
                             int numAggregatedArgs, Oid* actual_arg_types,
                             Oid* declared_arg_types);
Node* ParseComplexProjection(ParseState* pstate, const char* funcname,
                             Node* first_arg, int location);


/*
 *	Parse a function call
 *
 *  SPANGRES: column projections are not supported.
 *  TODO: Add back support for column projections.
 *
 *	For historical reasons, Postgres tries to treat the notations tab.col
 *	and col(tab) as equivalent: if a single-argument function call has an
 *	argument of complex type and the (unqualified) function name matches
 *	any attribute of the type, we can interpret it as a column projection.
 *	Conversely a function of a single complex-type argument can be written
 *	like a column reference, allowing functions to act like computed columns.
 *
 *	If both interpretations are possible, we prefer the one matching the
 *	syntactic form, but otherwise the form does not matter.
 *
 *	Hence, both cases come through here.  If fn is null, we're dealing with
 *	column syntax not function syntax.  In the function-syntax case,
 *	the FuncCall struct is needed to carry various decoration that applies
 *	to aggregate and window functions.
 *
 *	Also, when fn is null, we return NULL on failure rather than
 *	reporting a no-such-function error.
 *
 *	The argument expressions (in fargs) must have been transformed
 *	already.  However, nothing in *fn has been transformed.
 *
 *	last_srf should be a copy of pstate->p_last_srf from just before we
 *	started transforming fargs.  If the caller knows that fargs couldn't
 *	contain any SRF calls, last_srf can just be pstate->p_last_srf.
 *
 *	proc_call is true if we are considering a CALL statement, so that the
 *	name must resolve to a procedure name, not anything else.
 */
Node* ParseFuncOrColumn(ParseState* pstate, List* funcname,
						List* fargs, Node* last_srf, FuncCall* fn,
						bool proc_call, int location) {
	bool		is_column = (fn == NULL);
	List	   *agg_order = (fn ? fn->agg_order : NIL);
	Expr	   *agg_filter = NULL;
	bool		agg_within_group = (fn ? fn->agg_within_group : false);
	bool		agg_star = (fn ? fn->agg_star : false);
	bool		agg_distinct = (fn ? fn->agg_distinct : false);
	bool		func_variadic = (fn ? fn->func_variadic : false);
	WindowDef  *over = (fn ? fn->over : NULL);
	Oid			rettype;
	Oid			funcid;
	ListCell	 *l;
	Node		 *first_arg = NULL;
	int			nargs;
	int			nargsplusdefs;
	Oid			actual_arg_types[FUNC_MAX_ARGS];
	Oid			 *declared_arg_types;
	List		 *argnames;
	List		 *argdefaults;
	Node		 *retval;
	bool		retset;
	int			nvargs;
	Oid			vatype;
	FuncDetailCode fdresult;
	char		aggkind = 0;
	ParseCallbackState pcbstate;

	/*
	 * If there's an aggregate filter, transform it using transformWhereClause
	 */
	if (fn && fn->agg_filter != NULL)
		agg_filter = (Expr *) transformWhereClause(pstate, fn->agg_filter,
												   EXPR_KIND_FILTER,
												   "FILTER");

	/*
	 * Most of the rest of the parser just assumes that functions do not have
	 * more than FUNC_MAX_ARGS parameters.  We have to test here to protect
	 * against array overruns, etc.  Of course, this may not be a function,
	 * but the test doesn't hurt.
	 */
	if (list_length(fargs) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg_plural("cannot pass more than %d argument to a function",
							   "cannot pass more than %d arguments to a function",
							   FUNC_MAX_ARGS,
							   FUNC_MAX_ARGS),
				 parser_errposition(pstate, location)));

	/*
	 * Extract arg type info in preparation for function lookup.
	 *
	 * If any arguments are Param markers of type VOID, we discard them from
	 * the parameter list. This is a hack to allow the JDBC driver to not have
	 * to distinguish "input" and "output" parameter symbols while parsing
	 * function-call constructs.  Don't do this if dealing with column syntax,
	 * nor if we had WITHIN GROUP (because in that case it's critical to keep
	 * the argument count unchanged).
	 */
	nargs = 0;
	foreach(l, fargs)
	{
		Node	   *arg = lfirst(l);
		Oid			argtype = exprType(arg);

		if (argtype == VOIDOID && IsA(arg, Param) &&
			!is_column && !agg_within_group)
		{
			fargs = foreach_delete_current(fargs, l);
			continue;
		}

		actual_arg_types[nargs++] = argtype;
	}

	/*
	 * Check for named arguments; if there are any, build a list of names.
	 *
	 * We allow mixed notation (some named and some not), but only with all
	 * the named parameters after all the unnamed ones.  So the name list
	 * corresponds to the last N actual parameters and we don't need any extra
	 * bookkeeping to match things up.
	 */
	argnames = NIL;
	foreach(l, fargs)
	{
		Node	   *arg = lfirst(l);

		if (IsA(arg, NamedArgExpr))
		{
			NamedArgExpr *na = (NamedArgExpr *) arg;
			ListCell   *lc;

			/* Reject duplicate arg names */
			foreach(lc, argnames)
			{
				if (strcmp(na->name, (char *) lfirst(lc)) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("argument name \"%s\" used more than once",
									na->name),
							 parser_errposition(pstate, na->location)));
			}
			argnames = lappend(argnames, na->name);
		}
		else
		{
			if (argnames != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("positional argument cannot follow named argument"),
						 parser_errposition(pstate, exprLocation(arg))));
		}
	}

	if (fargs)
	{
		first_arg = linitial(fargs);
		Assert(first_arg != NULL);
	}

	/*
	 * SPANGRES: skip considering if the construct could be a column projection.
	 * TODO: Add back support for column projections.
	 *
	 */

	/*
	 * func_get_detail looks up the function in the catalogs, does
	 * disambiguation for polymorphic functions, handles inheritance, and
	 * returns the funcid and type and set or singleton status of the
	 * function's return value.	It also returns the true argument types to
	 * the function.
	 *
	 * Note: for a named-notation or variadic function call, the reported
	 * "true" types aren't really what is in pg_proc: the types are reordered
	 * to match the given argument order of named arguments, and a variadic
	 * argument is replaced by a suitable number of copies of its element
	 * type.	We'll fix up the variadic case below.	We may also have to deal
	 * with default arguments.
	 */
	
	setup_parser_errposition_callback(&pcbstate, pstate, location);

	fdresult = func_get_detail(funcname, fargs, argnames, nargs,
							   actual_arg_types,
							   !func_variadic, true, proc_call,
							   &funcid, &rettype, &retset,
							   &nvargs, &vatype,
							   &declared_arg_types, &argdefaults);

	cancel_parser_errposition_callback(&pcbstate);

	/*
	 * Check for various wrong-kind-of-routine cases.
	 */

	/* If this is a CALL, reject things that aren't procedures */
	if (proc_call &&
		(fdresult == FUNCDETAIL_NORMAL ||
		 fdresult == FUNCDETAIL_AGGREGATE ||
		 fdresult == FUNCDETAIL_WINDOWFUNC ||
		 fdresult == FUNCDETAIL_COERCION))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is not a procedure",
						func_signature_string(funcname, nargs,
											  argnames,
											  actual_arg_types)),
				 errhint("To call a function, use SELECT."),
				 parser_errposition(pstate, location)));
	/* Conversely, if not a CALL, reject procedures */
	if (fdresult == FUNCDETAIL_PROCEDURE && !proc_call)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s is a procedure",
						func_signature_string(funcname, nargs,
											  argnames,
											  actual_arg_types)),
				 errhint("To call a procedure, use CALL."),
				 parser_errposition(pstate, location)));

	if (fdresult == FUNCDETAIL_NORMAL ||
		fdresult == FUNCDETAIL_PROCEDURE ||
		fdresult == FUNCDETAIL_COERCION)
	{
		/*
		 * In these cases, complain if there was anything indicating it must
		 * be an aggregate or window function.
		 */
		if (agg_star)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) specified, but %s is not an aggregate function",
							NameListToString(funcname),
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_distinct)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("DISTINCT specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("WITHIN GROUP specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_order != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("ORDER BY specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (agg_filter)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("FILTER specified, but %s is not an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		if (over)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("OVER specified, but %s is not a window function nor an aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
	}

	/*
	 * So far so good, so do some fdresult-type-specific processing.
	 */
	if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE)
	{
		/* Nothing special to do for these cases. */
	}
	else if (fdresult == FUNCDETAIL_AGGREGATE)
	{
		/*
		 * It's an aggregate; fetch needed info from the pg_aggregate entry.
		 */
		const FormData_pg_aggregate* classForm =
				GetAggregateFromBootstrapCatalog(funcid);
		if (classForm == NULL) { // should not happen.
			elog(ERROR, "bootstrap catalog lookup failed for aggregate %u", funcid);
		}
		aggkind = classForm->aggkind;
		int catDirectArgs = classForm->aggnumdirectargs;

		/* Now check various disallowed cases. */
		if (AGGKIND_IS_ORDERED_SET(aggkind))
		{
			int			numAggregatedArgs;
			int			numDirectArgs;

			if (!agg_within_group)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("WITHIN GROUP is required for ordered-set aggregate %s",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
			if (over)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("OVER is not supported for ordered-set aggregate %s",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
			/* gram.y rejects DISTINCT + WITHIN GROUP */
			Assert(!agg_distinct);
			/* gram.y rejects VARIADIC + WITHIN GROUP */
			Assert(!func_variadic);

			/*
			 * Since func_get_detail was working with an undifferentiated list
			 * of arguments, it might have selected an aggregate that doesn't
			 * really match because it requires a different division of direct
			 * and aggregated arguments.  Check that the number of direct
			 * arguments is actually OK; if not, throw an "undefined function"
			 * error, similarly to the case where a misplaced ORDER BY is used
			 * in a regular aggregate call.
			 */
			numAggregatedArgs = list_length(agg_order);
			numDirectArgs = nargs - numAggregatedArgs;
			Assert(numDirectArgs >= 0);

			if (!OidIsValid(vatype))
			{
				/* Test is simple if aggregate isn't variadic */
				if (numDirectArgs != catDirectArgs)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_FUNCTION),
							 errmsg("function %s does not exist",
									func_signature_string(funcname, nargs,
														  argnames,
														  actual_arg_types)),
							 errhint("There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
									 NameListToString(funcname),
									 catDirectArgs, numDirectArgs),
							 parser_errposition(pstate, location)));
			}
			else
			{
				/*
				 * If it's variadic, we have two cases depending on whether
				 * the agg was "... ORDER BY VARIADIC" or "..., VARIADIC ORDER
				 * BY VARIADIC".  It's the latter if catDirectArgs equals
				 * pronargs; to save a catalog lookup, we reverse-engineer
				 * pronargs from the info we got from func_get_detail.
				 */
				int			pronargs;

				pronargs = nargs;
				if (nvargs > 1)
					pronargs -= nvargs - 1;
				if (catDirectArgs < pronargs)
				{
					/* VARIADIC isn't part of direct args, so still easy */
					if (numDirectArgs != catDirectArgs)
						ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_FUNCTION),
								 errmsg("function %s does not exist",
										func_signature_string(funcname, nargs,
															  argnames,
															  actual_arg_types)),
								 errhint("There is an ordered-set aggregate %s, but it requires %d direct arguments, not %d.",
										 NameListToString(funcname),
										 catDirectArgs, numDirectArgs),
								 parser_errposition(pstate, location)));
				}
				else
				{
					/*
					 * Both direct and aggregated args were declared variadic.
					 * For a standard ordered-set aggregate, it's okay as long
					 * as there aren't too few direct args.  For a
					 * hypothetical-set aggregate, we assume that the
					 * hypothetical arguments are those that matched the
					 * variadic parameter; there must be just as many of them
					 * as there are aggregated arguments.
					 */
					if (aggkind == AGGKIND_HYPOTHETICAL)
					{
						if (nvargs != 2 * numAggregatedArgs)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_FUNCTION),
									 errmsg("function %s does not exist",
											func_signature_string(funcname, nargs,
																  argnames,
																  actual_arg_types)),
									 errhint("To use the hypothetical-set aggregate %s, the number of hypothetical direct arguments (here %d) must match the number of ordering columns (here %d).",
											 NameListToString(funcname),
											 nvargs - numAggregatedArgs, numAggregatedArgs),
									 parser_errposition(pstate, location)));
					}
					else
					{
						if (nvargs <= numAggregatedArgs)
							ereport(ERROR,
									(errcode(ERRCODE_UNDEFINED_FUNCTION),
									 errmsg("function %s does not exist",
											func_signature_string(funcname, nargs,
																  argnames,
																  actual_arg_types)),
									 errhint("There is an ordered-set aggregate %s, but it requires at least %d direct arguments.",
											 NameListToString(funcname),
											 catDirectArgs),
									 parser_errposition(pstate, location)));
					}
				}
			}

			/* Check type matching of hypothetical arguments */
			if (aggkind == AGGKIND_HYPOTHETICAL)
				unify_hypothetical_args(pstate, fargs, numAggregatedArgs,
										actual_arg_types, declared_arg_types);
		}
		else
		{
			/* Normal aggregate, so it can't have WITHIN GROUP */
			if (agg_within_group)
				ereport(ERROR,
						(errcode(ERRCODE_WRONG_OBJECT_TYPE),
						 errmsg("%s is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
								NameListToString(funcname)),
						 parser_errposition(pstate, location)));
		}
	}
	else if (fdresult == FUNCDETAIL_WINDOWFUNC)
	{
		/*
		 * True window functions must be called with a window definition.
		 */
		if (!over)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("window function %s requires an OVER clause",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
		/* And, per spec, WITHIN GROUP isn't allowed */
		if (agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("window function %s cannot have WITHIN GROUP",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));
	}
	else if (fdresult == FUNCDETAIL_COERCION)
	{
		/*
		 * We interpreted it as a type coercion. coerce_type can handle these
		 * cases, so why duplicate code...
		 */
		return coerce_type(pstate, linitial(fargs),
						   actual_arg_types[0], rettype, -1,
						   COERCION_EXPLICIT, COERCE_EXPLICIT_CALL, location);
	}
	else if (fdresult == FUNCDETAIL_MULTIPLE)
	{
		/*
		 * We found multiple possible functional matches.  If we are dealing
		 * with attribute notation, return failure, letting the caller report
		 * "no such column" (we already determined there wasn't one).  If
		 * dealing with function notation, report "ambiguous function",
		 * regardless of whether there's also a column by this name.
		 */
		if (is_column)
			return NULL;

		if (proc_call)
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
					 errmsg("procedure %s is not unique",
							func_signature_string(funcname, nargs, argnames,
													actual_arg_types)),
					 errhint("Could not choose a best candidate procedure. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
					 errmsg("function %s is not unique",
							func_signature_string(funcname, nargs, argnames,
													actual_arg_types)),
					 errhint("Could not choose a best candidate function. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
	}
	else
	{
		/*
		 * Not found as a function.  If we are dealing with attribute
		 * notation, return failure, letting the caller report "no such
		 * column" (we already determined there wasn't one).
		 */
		if (is_column)
			return NULL;

		/* TODO: Add back support for column projections */

		/*
		 * No function, and no column either.  Since we're dealing with
		 * function notation, report "function does not exist".
		 */
		if (list_length(agg_order) > 1 && !agg_within_group)
		{
			/* It's agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY */
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No aggregate function matches the given name and argument types. "
							 "Perhaps you misplaced ORDER BY; ORDER BY must appear "
							 "after all regular arguments of the aggregate."),
					 parser_errposition(pstate, location)));
		}
		else if (proc_call)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("procedure %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No procedure matches the given name and argument types. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function %s does not exist",
							func_signature_string(funcname, nargs, argnames,
												  actual_arg_types)),
					 errhint("No function matches the given name and argument types. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, location)));
	}

	/*
	 * If there are default arguments, we have to include their types in
	 * actual_arg_types for the purpose of checking generic type consistency.
	 * However, we do NOT put them into the generated parse node, because
	 * their actual values might change before the query gets run.  The
	 * planner has to insert the up-to-date values at plan time.
	 */
	nargsplusdefs = nargs;
	foreach(l, argdefaults)
	{
		Node	   *expr = (Node *) lfirst(l);

		/* probably shouldn't happen ... */
		if (nargsplusdefs >= FUNC_MAX_ARGS)
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
					 errmsg_plural("cannot pass more than %d argument to a function",
								   "cannot pass more than %d arguments to a function",
								   FUNC_MAX_ARGS,
								   FUNC_MAX_ARGS),
					 parser_errposition(pstate, location)));

		actual_arg_types[nargsplusdefs++] = exprType(expr);
	}

	/*
	 * enforce consistency with polymorphic argument and return types,
	 * possibly adjusting return type or declared_arg_types (which will be
	 * used as the cast destination by make_fn_arguments)
	 */
	rettype = enforce_generic_type_consistency(actual_arg_types,
											   declared_arg_types,
											   nargsplusdefs,
											   rettype,
											   false);

	/* perform the necessary typecasting of arguments */
	make_fn_arguments(pstate, fargs, actual_arg_types, declared_arg_types);

	/*
	 * If the function isn't actually variadic, forget any VARIADIC decoration
	 * on the call.  (Perhaps we should throw an error instead, but
	 * historically we've allowed people to write that.)
	 */
	if (!OidIsValid(vatype))
	{
		Assert(nvargs == 0);
		func_variadic = false;
	}

	/*
	 * If it's a variadic function call, transform the last nvargs arguments
	 * into an array --- unless it's an "any" variadic.
	 */
	if (nvargs > 0 && vatype != ANYOID)
	{
		ArrayExpr  *newa = makeNode(ArrayExpr);
		int			non_var_args = nargs - nvargs;
		List	   *vargs;

		Assert(non_var_args >= 0);
		vargs = list_copy_tail(fargs, non_var_args);
		fargs = list_truncate(fargs, non_var_args);

		newa->elements = vargs;
		/* assume all the variadic arguments were coerced to the same type */
		newa->element_typeid = exprType((Node *) linitial(vargs));
		newa->array_typeid = get_array_type(newa->element_typeid);
		if (!OidIsValid(newa->array_typeid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("could not find array type for data type %s",
							format_type_be(newa->element_typeid)),
					 parser_errposition(pstate, exprLocation((Node *) vargs))));
		/* array_collid will be set by parse_collate.c */
		newa->multidims = false;
		newa->location = exprLocation((Node *) vargs);

		fargs = lappend(fargs, newa);

		/* We could not have had VARIADIC marking before ... */
		Assert(!func_variadic);
		/* ... but now, it's a VARIADIC call */
		func_variadic = true;
	}

	/*
	 * If an "any" variadic is called with explicit VARIADIC marking, insist
	 * that the variadic parameter be of some array type.
	 */
	if (nargs > 0 && vatype == ANYOID && func_variadic)
	{
		Oid			va_arr_typid = actual_arg_types[nargs - 1];

		if (!OidIsValid(get_base_element_type(va_arr_typid)))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("VARIADIC argument must be an array"),
					 parser_errposition(pstate,
										exprLocation((Node *) llast(fargs)))));
	}

	/* if it returns a set, check that's OK */
	if (retset)
		check_srf_call_placement(pstate, last_srf, location);

	/* build the appropriate output structure */
	if (fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE)
	{
		FuncExpr   *funcexpr = makeNode(FuncExpr);

		funcexpr->funcid = funcid;
		funcexpr->funcresulttype = rettype;
		funcexpr->funcretset = retset;
		funcexpr->funcvariadic = func_variadic;
		funcexpr->funcformat = COERCE_EXPLICIT_CALL;
		/* funccollid and inputcollid will be set by parse_collate.c */
		funcexpr->args = fargs;
		funcexpr->location = location;

		retval = (Node *) funcexpr;
	}
	else if (fdresult == FUNCDETAIL_AGGREGATE && !over)
	{
		/* aggregate function */
		Aggref	   *aggref = makeNode(Aggref);

		aggref->aggfnoid = funcid;
		aggref->aggtype = rettype;
		/* aggcollid and inputcollid will be set by parse_collate.c */
		aggref->aggtranstype = InvalidOid;	/* will be set by planner */
		/* aggargtypes will be set by transformAggregateCall */
		/* aggdirectargs and args will be set by transformAggregateCall */
		/* aggorder and aggdistinct will be set by transformAggregateCall */
		aggref->aggfilter = agg_filter;
		aggref->aggstar = agg_star;
		aggref->aggvariadic = func_variadic;
		aggref->aggkind = aggkind;
		/* agglevelsup will be set by transformAggregateCall */
		aggref->aggsplit = AGGSPLIT_SIMPLE; /* planner might change this */
		aggref->location = location;

		/*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.  This is mere pedantry but some folks insisted ...
		 */
		if (fargs == NIL && !agg_star && !agg_within_group)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));

		if (retset)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("aggregates cannot return sets"),
					 parser_errposition(pstate, location)));

		/*
		 * We might want to support named arguments later, but disallow it for
		 * now.  We'd need to figure out the parsed representation (should the
		 * NamedArgExprs go above or below the TargetEntry nodes?) and then
		 * teach the planner to reorder the list properly.  Or maybe we could
		 * make transformAggregateCall do that?  However, if you'd also like
		 * to allow default arguments for aggregates, we'd need to do it in
		 * planning to avoid semantic problems.
		 */
		if (argnames != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregates cannot use named arguments"),
					 parser_errposition(pstate, location)));

		/* parse_agg.c does additional aggregate-specific processing */
		transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);

		retval = (Node *) aggref;
	}
	else
	{
		/* window function */
		WindowFunc *wfunc = makeNode(WindowFunc);

		Assert(over);			/* lack of this was checked above */
		Assert(!agg_within_group);	/* also checked above */

		wfunc->winfnoid = funcid;
		wfunc->wintype = rettype;
		/* wincollid and inputcollid will be set by parse_collate.c */
		wfunc->args = fargs;
		/* winref will be set by transformWindowFuncCall */
		wfunc->winstar = agg_star;
		wfunc->winagg = (fdresult == FUNCDETAIL_AGGREGATE);
		wfunc->aggfilter = agg_filter;
		wfunc->location = location;

		/*
		 * agg_star is allowed for aggregate functions but distinct isn't
		 */
		if (agg_distinct)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DISTINCT is not implemented for window functions"),
					 parser_errposition(pstate, location)));

		/*
		 * Reject attempt to call a parameterless aggregate without (*)
		 * syntax.  This is mere pedantry but some folks insisted ...
		 */
		if (wfunc->winagg && fargs == NIL && !agg_star)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("%s(*) must be used to call a parameterless aggregate function",
							NameListToString(funcname)),
					 parser_errposition(pstate, location)));

		/*
		 * ordered aggs not allowed in windows yet
		 */
		if (agg_order != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("aggregate ORDER BY is not implemented for window functions"),
					 parser_errposition(pstate, location)));

		/*
		 * FILTER is not yet supported with true window functions
		 */
		if (!wfunc->winagg && agg_filter)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("FILTER is not implemented for non-aggregate window functions"),
					 parser_errposition(pstate, location)));

		/*
		 * Window functions can't either take or return sets
		 */
		if (pstate->p_last_srf != last_srf)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("window function calls cannot contain set-returning function calls"),
					 errhint("You might be able to move the set-returning function into a LATERAL FROM item."),
					 parser_errposition(pstate,
										exprLocation(pstate->p_last_srf))));

		if (retset)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
					 errmsg("window functions cannot return sets"),
					 parser_errposition(pstate, location)));

		/* parse_agg.c does additional window-func-specific processing */
		transformWindowFuncCall(pstate, wfunc, over);

		retval = (Node *) wfunc;
	}

	/* if it returns a set, remember it for error checks at higher levels */
	if (retset)
		pstate->p_last_srf = retval;

	return retval;
}

