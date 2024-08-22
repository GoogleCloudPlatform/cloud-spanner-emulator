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

// If the type is in the bootstrap catalog, it's visible. We may later want to
// also permit user-defined types by checking the engine's catalog.
bool TypeIsVisible(Oid typid) {
	return GetTypeFromBootstrapCatalog(typid) != NULL;
}

/*
 * FuncnameGetCandidates
 *		Given a possibly-qualified function name and argument count,
 *		retrieve a list of the possible matches.
 *
 * If nargs is -1, we return all functions matching the given name,
 * regardless of argument count.  (argnames must be NIL, and expand_variadic
 * and expand_defaults must be false, in this case.)
 *
 * If argnames isn't NIL, we are considering a named- or mixed-notation call,
 * and only functions having all the listed argument names will be returned.
 * (We assume that length(argnames) <= nargs and all the passed-in names are
 * distinct.)  The returned structs will include an argnumbers array showing
 * the actual argument index for each logical argument position.
 * NOTE: Spangres does not support named args.
 *
 * If expand_variadic is true, then variadic functions having the same number
 * or fewer arguments will be retrieved, with the variadic argument and any
 * additional argument positions filled with the variadic element type.
 * nvargs in the returned struct is set to the number of such arguments.
 * If expand_variadic is false, variadic arguments are not treated specially,
 * and the returned nvargs will always be zero.
 *
 * If expand_defaults is true, functions that could match after insertion of
 * default argument values will also be retrieved.  In this case the returned
 * structs could have nargs > passed-in nargs, and ndargs is set to the number
 * of additional args (which can be retrieved from the function's
 * proargdefaults entry).
 *
 * If include_out_arguments is true, then OUT-mode arguments are considered to
 * be included in the argument list.  Their types are included in the returned
 * arrays, and argnumbers are indexes in proallargtypes not proargtypes.
 * We also set nominalnargs to be the length of proallargtypes not proargtypes.
 * Otherwise OUT-mode arguments are ignored.
 *
 * It is not possible for nvargs and ndargs to both be nonzero in the same
 * list entry, since default insertion allows matches to functions with more
 * than nargs arguments while the variadic transformation requires the same
 * number or less.
 *
 * When argnames isn't NIL, the returned args[] type arrays are not ordered
 * according to the functions' declarations, but rather according to the call:
 * first any positional arguments, then the named arguments, then defaulted
 * arguments (if needed and allowed by expand_defaults).  The argnumbers[]
 * array can be used to map this back to the catalog information.
 * argnumbers[k] is set to the proargtypes or proallargtypes index of the
 * k'th call argument.
 *
 * We search a single namespace if the function name is qualified, else
 * all namespaces in the search path.  In the multiple-namespace case,
 * we arrange for entries in earlier namespaces to mask identical entries in
 * later namespaces.
 *
 * When expanding variadics, we arrange for non-variadic functions to mask
 * variadic ones if the expanded argument list is the same.  It is still
 * possible for there to be conflicts between different variadic functions,
 * however.
 *
 * It is guaranteed that the return list will never contain multiple entries
 * with identical argument lists.  When expand_defaults is true, the entries
 * could have more than nargs positions, but we still guarantee that they are
 * distinct in the first nargs positions.  However, if argnames isn't NIL or
 * either expand_variadic or expand_defaults is true, there might be multiple
 * candidate functions that expand to identical argument lists.  Rather than
 * throw error here, we report such situations by returning a single entry
 * with oid = 0 that represents a set of such conflicting candidates.
 * The caller might end up discarding such an entry anyway, but if it selects
 * such an entry it should react as though the call were ambiguous.
 *
 * If missing_ok is true, an empty list (NULL) is returned if the name was
 * schema-qualified with a schema that does not exist.  Likewise if no
 * candidate is found for other reasons.
 */
FuncCandidateList FuncnameGetCandidates(List* names, int nargs,
										List* argnames,
										bool expand_variadic,
										bool expand_defaults,
										bool include_out_arguments,
										bool missing_ok) {
	FuncCandidateList resultList = NULL;
	bool		any_special = false;
	char	   *schemaname = NULL;
	char	   *funcname = NULL;
	Oid			namespaceId = InvalidOid;

	/* Check for caller error */
	Assert(nargs >= 0 || !(expand_variadic | expand_defaults));

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, &funcname);

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

	/* Search bootstrap and user catalogs by proc name only */
	const FormData_pg_proc** proc_list;
	size_t proc_count;
	GetProcsByName(funcname, &proc_list, &proc_count);

	for (int proc_index = 0; proc_index < proc_count; ++proc_index) {
		const FormData_pg_proc* procform = proc_list[proc_index];

		int pronargs = procform->pronargs;
		bool variadic = false;
		bool use_defaults = false;
		Oid va_elem_type = InvalidOid;
		int	*argnumbers = NULL;

		/* 
		 * Here PostgreSQL would check search path if a namespace was not
		 * provided. Spangres assumes that a namespace is provided or defaults
		 * to pg_catalog.
		 * TODO: Add support for search path.
		 */
		if (procform->pronamespace != namespaceId)
			continue;

		/*
		 * If we are asked to match to OUT arguments, then use the
		 * proallargtypes array (which includes those); otherwise use
		 * proargtypes (which doesn't).  Of course, if proallargtypes is null,
		 * we always use proargtypes.
		 */
		if (include_out_arguments)
		{
			/*
			 * SPANGRES does not have support for OUT arguments.
			 * TODO: b/329162323 - add back support for OUT arguments
			 */
			if (procform->prorettype != VOIDOID) {
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Procedures with output arguments are not supported")));
			}
		}

		if (argnames != NIL) {
			/*
			 * Call uses named or mixed notation
			 *
			 * Named or mixed notation can match a variadic function only if
			 * expand_variadic is off; otherwise there is no way to match the
			 * presumed-nameless parameters expanded from the variadic array.
			 */
			if (OidIsValid(procform->provariadic) && expand_variadic)
				continue;
			va_elem_type = InvalidOid;
			variadic = false;

			/*
			 * Check argument count.
			 */
			Assert(nargs >= 0); /* -1 not supported with argnames */

			if (pronargs > nargs && expand_defaults)
			{
				/* Ignore if not enough default expressions */
				if (nargs + procform->pronargdefaults < pronargs)
					continue;
				use_defaults = true;
			}
			else
				use_defaults = false;

			/* Ignore if it doesn't match requested argument count */
			if (pronargs != nargs && !use_defaults)
				continue;

			/* Check for argument name match, generate positional mapping */
			/* SPANGRES BEGIN */
			// Replace MatchNamedCall with NamedCallMatchFound which is a shim with a
			// different signature to avoid using HeapTuple.
			if (!NamedCallMatchFound(procform->oid, nargs, argnames,
															 include_out_arguments, pronargs, &argnumbers))
				continue;
			/* SPANGRES END */

			/* Named argument matching is always "special" */
			any_special = true;
		} else {
			/*
			 * Call uses positional notation
			 *
			 * Check if function is variadic, and get variadic element type if
			 * so.  If expand_variadic is false, we should just ignore
			 * variadic-ness.
			 */
			if (pronargs <= nargs && expand_variadic)
			{
				va_elem_type = procform->provariadic;
				variadic = OidIsValid(va_elem_type);
				any_special |= variadic;
			}
			else
			{
				va_elem_type = InvalidOid;
				variadic = false;
			}

			/*
			 * Check if function can match by using parameter defaults.
			 */
			if (pronargs > nargs && expand_defaults)
			{
				// Ignore if not enough default expressions.
				if (nargs + procform->pronargdefaults < pronargs)
					continue;
				use_defaults = true;
				any_special = true;
			}
			else
				use_defaults = false;

			/* Ignore if it doesn't match requested argument count */
			if (nargs >= 0 && pronargs != nargs && !variadic && !use_defaults)
				continue;
		}

		/*
		 * We must compute the effective argument list so that we can easily
		 * compare it to earlier results.  We waste a palloc cycle if it gets
		 * masked by an earlier result, but really that's a pretty infrequent
		 * case so it's not worth worrying about.
		 */
		int effective_nargs = Max(pronargs, nargs);
		FuncCandidateList newResult = (FuncCandidateList)
			palloc(offsetof(struct _FuncCandidateList, args) +
				   effective_nargs * sizeof(Oid));
		newResult->pathpos = 0;	// Search path is not supported.
		newResult->oid = procform->oid;
		newResult->nominalnargs = pronargs;
		newResult->nargs = effective_nargs;
		newResult->argnumbers = argnumbers;
		if (argnumbers)
		{
			/* Re-order the argument types into call's logical order */
			int			i;

			for (i = 0; i < pronargs; i++)
				newResult->args[i] = procform->proargtypes.values[argnumbers[i]];
		}
		else
		{
			memcpy(newResult->args, procform->proargtypes.values,
						pronargs * sizeof(Oid));
		}

		if (variadic)
		{
			newResult->nvargs = effective_nargs - pronargs + 1;
			/* Expand variadic argument into N copies of element type */
			for (int arg_index = pronargs - 1; arg_index < effective_nargs;
					 ++arg_index) {
				newResult->args[arg_index] = va_elem_type;
			}
		}
		else
			newResult->nvargs = 0;
		newResult->ndargs = use_defaults ? pronargs - nargs : 0;

		/*
		 * Does it have the same arguments as something we already accepted?
		 * If so, decide what to do to avoid returning duplicate argument
		 * lists.  We can skip this check for the single-namespace case if no
		 * special (named, variadic or defaults) match has been made, since
		 * then the unique index on pg_proc guarantees all the matches have
		 * different argument lists.
		 */
		if (resultList != NULL
			&& (any_special || !OidIsValid(namespaceId)))
		{
			/*
			 * If we have an ordered list from SearchSysCacheList (the normal
			 * case), then any conflicting proc must immediately adjoin this
			 * one in the list, so we only need to look at the newest result
			 * item.  If we have an unordered list, we have to scan the whole
			 * result list.  Also, if either the current candidate or any
			 * previous candidate is a special match, we can't assume that
			 * conflicts are adjacent.
			 * NOTE: Spangres does not have this ordering guarantee, so we must always
			 * scan the list.
			 *
			 * We ignore defaulted arguments in deciding what is a match.
			 */
			FuncCandidateList prevResult;

			int cmp_nargs = newResult->nargs - newResult->ndargs;

			for (prevResult = resultList; prevResult; prevResult = prevResult->next) {
				if (cmp_nargs == prevResult->nargs - prevResult->ndargs &&
						memcmp(newResult->args, prevResult->args,
									 cmp_nargs * sizeof(Oid)) == 0) {
					break;
				}
			}

			if (prevResult)
			{
				/*
				 * We have a match with a previous result.  Decide which one
				 * to keep, or mark it ambiguous if we can't decide.  The
				 * logic here is preference > 0 means prefer the old result,
				 * preference < 0 means prefer the new, preference = 0 means
				 * ambiguous.
				 */
				int preference;

				/*
				 * Spangres note: here PostgreSQL considers search path order first,
				 * which we don't support.
				 * TODO: Add support for qualified names and search path.
				 */
				if (variadic && prevResult->nvargs == 0)
				{
					/*
					 * With variadic functions we could have, for example,
					 * both foo(numeric) and foo(variadic numeric[]) in the
					 * same namespace; if so we prefer the non-variadic match
					 * on efficiency grounds.
					 */
					preference = 1;
				}
				else if (!variadic && prevResult->nvargs > 0)
				{
					preference = -1;
				}
				else
				{
					/*----------
					 * We can't decide.  This can happen with, for example,
					 * both foo(numeric, variadic numeric[]) and
					 * foo(variadic numeric[]) in the same namespace, or
					 * both foo(int) and foo (int, int default something)
					 * in the same namespace, or both foo(a int, b text)
					 * and foo(b text, a int) in the same namespace.
					 *----------
					 */
					preference = 0;
				}

				if (preference > 0)
				{
					/* keep previous result */
					pfree(newResult);
					continue;
				}
				else if (preference < 0)
				{
					/* remove previous result from the list */
					if (prevResult == resultList)
						resultList = prevResult->next;
					else
					{
						FuncCandidateList prevPrevResult;

						for (prevPrevResult = resultList; prevPrevResult;
								 prevPrevResult = prevPrevResult->next) {
							if (prevResult == prevPrevResult->next) {
								prevPrevResult->next = prevResult->next;
								break;
							}
						}
						Assert(prevPrevResult);	/* assert we found it */
					}
					pfree(prevResult);
					/* fall through to add newResult to list */
				}
				else
				{
					/* mark old result as ambiguous, discard new */
					prevResult->oid = InvalidOid;
					pfree(newResult);
					continue;
				}
			}
		}

		/*
		 * Okay to add it to result list
		 */
		newResult->next = resultList;
		resultList = newResult;
	}

	return resultList;
}

/*
 * NamedCallMatchFound (modified version of MatchNamedCall)
 *		Given a pg_proc oid and a call's list of argument names,
 *		check whether the function could match the call.
 *
 * The call could match if all supplied argument names are accepted by
 * the function, in positions after the last positional argument, and there
 * are defaults for all unsupplied arguments.
 *
 * If include_out_arguments is true, we are treating OUT arguments as
 * included in the argument list.  pronargs is the number of arguments
 * we're considering (the length of either proargtypes or proallargtypes).
 *
 * The number of positional arguments is nargs - list_length(argnames).
 * Note caller has already done basic checks on argument count.
 *
 * On match, return true and fill *argnumbers with a palloc'd array showing
 * the mapping from call argument positions to actual function argument
 * numbers.  Defaulted arguments are included in this map, at positions
 * after the last supplied argument.
 */
bool
NamedCallMatchFound(Oid proc_oid, int nargs, List *argnames,
			   						bool include_out_arguments, int pronargs,
			   						int **argnumbers)
{
	const FormData_pg_proc *procform = GetProcByOid(proc_oid);
	int			numposargs = nargs - list_length(argnames);
	int			pronallargs;
	Oid		   *p_argtypes = NULL;
	char	  **p_argnames = NULL;
	char	   *p_argmodes = NULL;
	bool		arggiven[FUNC_MAX_ARGS];
	int			ap;				/* call args position */
	int			pp;				/* proargs position */
	ListCell   *lc;

	Assert(argnames != NIL);
	Assert(numposargs >= 0);
	Assert(nargs <= pronargs);

	/* OK, let's extract the argument names and types */
	pronallargs = GetFunctionArgInfo(
		proc_oid, &p_argtypes, &p_argnames, &p_argmodes);

	/* Ignore this function if its proargnames is null */
	if (p_argnames == NULL)
		return false;

	Assert(p_argnames != NULL);

	Assert(include_out_arguments ? (pronargs == pronallargs) : (pronargs <= pronallargs));

	/* initialize state for matching */
	*argnumbers = (int *) palloc(pronargs * sizeof(int));
	memset(arggiven, false, pronargs * sizeof(bool));

	/* there are numposargs positional args before the named args */
	for (ap = 0; ap < numposargs; ap++)
	{
		(*argnumbers)[ap] = ap;
		arggiven[ap] = true;
	}

	/* now examine the named args */
	foreach(lc, argnames)
	{
		char	   *argname = (char *) lfirst(lc);
		bool		found;
		int			i;

		pp = 0;
		found = false;
		for (i = 0; i < pronallargs; i++)
		{
			/* consider only input params, except with include_out_arguments */
			if (!include_out_arguments &&
				p_argmodes &&
				(p_argmodes[i] != FUNC_PARAM_IN &&
				 p_argmodes[i] != FUNC_PARAM_INOUT &&
				 p_argmodes[i] != FUNC_PARAM_VARIADIC))
				continue;
			if (p_argnames[i] && strcmp(p_argnames[i], argname) == 0)
			{
				/* fail if argname matches a positional argument */
				if (arggiven[pp])
					return false;
				arggiven[pp] = true;
				(*argnumbers)[ap] = pp;
				found = true;
				break;
			}
			/* increase pp only for considered parameters */
			pp++;
		}
		/* if name isn't in proargnames, fail */
		if (!found)
			return false;
		ap++;
	}

	Assert(ap == nargs);		/* processed all actual parameters */

	/* Check for default arguments */
	if (nargs < pronargs)
	{
		int			first_arg_with_default = pronargs - procform->pronargdefaults;

		for (pp = numposargs; pp < pronargs; pp++)
		{
			if (arggiven[pp])
				continue;
			/* fail if arg not given and no default available */
			if (pp < first_arg_with_default)
				return false;
			(*argnumbers)[ap++] = pp;
		}
	}

	Assert(ap == pronargs);		/* processed all function parameters */

	return true;
}

/*
 * OpernameGetCandidates
 *		Given a possibly-qualified operator name and operator kind,
 *		retrieve a list of the possible matches.
 *
 * If oprkind is '\0', we return all operators matching the given name,
 * regardless of arguments.
 *
 * We search a single namespace if the operator name is qualified, else
 * all namespaces in the search path.  The return list will never contain
 * multiple entries with identical argument lists --- in the multiple-
 * namespace case, we arrange for entries in earlier namespaces to mask
 * identical entries in later namespaces.
 *
 * The returned items always have two args[] entries --- one or the other
 * will be InvalidOid for a prefix or postfix oprkind.  nargs is 2, too.
 */
FuncCandidateList OpernameGetCandidates(List* names, char oprkind,
										bool missing_schema_ok) {
	FuncCandidateList resultList = NULL;
	char	   *resultSpace = NULL;
	int			nextResult = 0;
	char	   *schemaname;
	char	   *opername;
  Oid			 namespaceId;

	/* Deconstruct the name list to extract schema and operator names. */
	DeconstructQualifiedName(names, &schemaname, &opername);

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

	/* Get the Oids of Operators with this name */
	const Oid* oid_list = NULL;
	size_t oid_list_size = 0;
	GetOperatorsByNameFromBootstrapCatalog(opername, &oid_list, &oid_list_size);

	/*
	 * In typical scenarios, most if not all of the operators found by the
	 * catcache search will end up getting returned; and there can be quite a
	 * few, for common operator names such as '=' or '+'.  To reduce the time
	 * spent in palloc, we allocate the result space as an array large enough
	 * to hold all the operators.  The original coding of this routine did a
	 * separate palloc for each operator, but profiling revealed that the
	 * pallocs used an unreasonably large fraction of parsing time.
	 */
#define SPACE_PER_OP MAXALIGN(offsetof(struct _FuncCandidateList, args) + \
							  2 * sizeof(Oid))

	if (oid_list_size > 0) {
		resultSpace = palloc(oid_list_size * SPACE_PER_OP);
	}

	for (int i = 0; i < oid_list_size; i++) {
		const FormData_pg_operator* operform =
				GetOperatorFromBootstrapCatalog(oid_list[i]);
		int pathpos = 0;
		FuncCandidateList newResult;

		/* Ignore operators of wrong kind, if specific kind requested */
		if (oprkind && operform->oprkind != oprkind) continue;

    /*
		 * Ignore operators with a mismatched namespace. Here Spangres ignores
		 * search path in considering which operators to return.
		 * TODO: Add support for search path.
		 */
    if (operform->oprnamespace != namespaceId) continue;

		/*
		 * Here Spangres ignores a check for operators with the same signatures. We
		 * will need a check here if we later support user-defined that can overload
		 * system operators.
		 */

		/*
		 * Okay to add it to result list
		 */
		newResult = (FuncCandidateList)(resultSpace + nextResult);
		nextResult += SPACE_PER_OP;

		newResult->pathpos = pathpos;
		newResult->oid = oid_list[i];
		newResult->nargs = 2;
		newResult->nvargs = 0;
		newResult->ndargs = 0;
		newResult->argnumbers = NULL;
		newResult->args[0] = operform->oprleft;
		newResult->args[1] = operform->oprright;
		newResult->next = resultList;
		resultList = newResult;
	}

	return resultList;
}

// Given a possibly-qualified operator name and exact input
// datatypes, look up the operator.  Returns InvalidOid if not found.
//
// Pass oprleft = InvalidOid for a prefix operator, oprright = InvalidOid for
// a postfix operator.
Oid OpernameGetOprid(List* names, Oid oprleft, Oid oprright) {
	char	   *schemaname;
	char	   *opername;
  Oid			 namespaceId;

	/* deconstruct the name list */
	DeconstructQualifiedName(names, &schemaname, &opername);

  if (schemaname) {
    namespaceId = GetNamespaceByNameFromBootstrapCatalog(schemaname);
    if (!OidIsValid(namespaceId))
      return InvalidOid;
  } else {
    /*
     * Spangres does not support search path. If a schemaname is not
     * provided, use pg_catalog by default.
     * TODO: Add support for search path.
     */
    namespaceId = GetNamespaceByNameFromBootstrapCatalog("pg_catalog");
  }

	/*
	 * Get the list of Operator Oids with this name from bootstrap catalog and
	 * iterate through it to find one with exact operator matches.
	 */
	const Oid* oid_list = NULL;
	size_t oid_list_size = 0;
	GetOperatorsByNameFromBootstrapCatalog(opername, &oid_list, &oid_list_size);

	if (oid_list_size == 0) {
		/* no hope, fall out early */
		return InvalidOid;
	}

	/* Look through the list and see if any operator matches our args exactly */
	for (int i = 0; i < oid_list_size; i++) {
		const FormData_pg_operator* operform =
				GetOperatorFromBootstrapCatalog(oid_list[i]);

    /*
		 * Ignore operators with a mismatched namespace. Here Spangres ignores
		 * search path in considering which operators to return. This would be
		 * relevant if search path determined which of multiple matching operators
		 * to return.
		 * TODO: Add support for search path.
		 */
    if (operform->oprnamespace != namespaceId) continue;

		if (operform->oprleft == oprleft && operform->oprright == oprright) {
			return oid_list[i];
		}
	}

	return InvalidOid;
}

Oid RelnameGetRelid(const char *relname) {
	return GetOrGenerateOidFromTableNameC(relname);
}

Oid get_namespace_oid(const char* nspname, bool missing_ok) {
	Oid oid = GetOidFromNamespaceNameC(nspname);
	if (oid != InvalidOid) return oid;
	if (missing_ok) {
		return InvalidOid;
	} else {
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", nspname)));
	}
}

/*
 * LookupExplicitNamespace
 *		Process an explicitly-specified schema name: look up the schema
 *		and verify we have USAGE (lookup) rights in it.
 *
 * Spangres version skips ACL checks and only looks up in catalog adapter.
 * Spangres version does not support special pg_temp namespace alias.
 *
 * Returns the namespace OID
 */
Oid
LookupExplicitNamespace(const char *nspname, bool missing_ok)
{
	Oid			namespaceId;

	/* check for pg_temp alias */
	if (strcmp(nspname, "pg_temp") == 0)
	{
		ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pg_temp namespace is not supported"));
	}

	namespaceId = get_namespace_oid(nspname, missing_ok);
	if (missing_ok && !OidIsValid(namespaceId))
		return InvalidOid;

	return namespaceId;
}

Oid
lookup_collation(const char *collname, Oid collnamespace, int32_t encoding) {
	if(collnamespace != PG_CATALOG_NAMESPACE) {
		ereport(ERROR, errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("Collations outside of pg_catalog are not supported"));
	}
	return GetCollationOidByNameFromBootstrapCatalog(collname);
}

Oid
get_collation_oid(List *name, bool missing_ok)
{
	char *schemaname;
	char *collation_name;
	int32_t dbencoding = GetDatabaseEncoding();
	Oid namespaceId;
	Oid colloid;

	/* deconstruct the name list */
	DeconstructQualifiedName(name, &schemaname, &collation_name);

	if (schemaname) {
		/* use exact schema given */
		namespaceId = LookupExplicitNamespace(schemaname, missing_ok);
		if (missing_ok && !OidIsValid(namespaceId))
			return InvalidOid;

		colloid = lookup_collation(collation_name, namespaceId, dbencoding);
		if (OidIsValid(colloid))
			return colloid;
	} else {
		namespaceId = LookupExplicitNamespace("pg_catalog", missing_ok);
		colloid = lookup_collation(collation_name, namespaceId, dbencoding);
		if (OidIsValid(colloid))
			return colloid;
	 }
	
	/* Not found in path */
	if (!missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("Collations are not supported")));
	return InvalidOid;
}
