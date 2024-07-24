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


// Methods are called non-statically.
void updateFuzzyAttrMatchState(int fuzzy_rte_penalty,
						  FuzzyAttrMatchState *fuzzystate, RangeTblEntry *rte,
						  const char *actual, const char *match, int attnum);

int scanRTEForColumn(ParseState *pstate, RangeTblEntry *rte, Alias *eref,
				 const char *colname, int location,
				 int fuzzy_rte_penalty,
				 FuzzyAttrMatchState *fuzzystate)
{
	int			result = InvalidAttrNumber;
	int			attnum = 0;
	ListCell   *c;

	/*
	 * Scan the user column names (or aliases) for a match. Complain if
	 * multiple matches.
	 *
	 * Note: eref->colnames may include entries for dropped columns, but those
	 * will be empty strings that cannot match any legal SQL identifier, so we
	 * don't bother to test for that case here.
	 *
	 * Should this somehow go wrong and we try to access a dropped column,
	 * we'll still catch it by virtue of the check in scanNSItemForColumn().
	 * Callers interested in finding match with shortest distance need to
	 * defend against this directly, though.
	 */
	foreach(c, eref->colnames)
	{
		const char *attcolname = strVal(lfirst(c));

		attnum++;
		if (strcmp(attcolname, colname) == 0)
		{
			if (result)
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_COLUMN),
						 errmsg("column reference \"%s\" is ambiguous",
								colname),
						 parser_errposition(pstate, location)));
			result = attnum;
		}

		/* Update fuzzy match state, if provided. */
		if (fuzzystate != NULL)
			updateFuzzyAttrMatchState(fuzzy_rte_penalty, fuzzystate,
									  rte, attcolname, colname, attnum);
	}

	/*
	 * If we have a unique match, return it.  Note that this allows a user
	 * alias to override a system column name (such as OID) without error.
	 */
	if (result)
		return result;

  // SPANGRES: System column look-ups are currently unsupported.
  // TODO: Add support for system column look-ups.
	return InvalidAttrNumber;
}

bool
get_rte_attribute_is_dropped(RangeTblEntry *rte, AttrNumber attnum)
{
	// It's not possible to get a table from googlesql with dropped columns.
	// Unless we someday support PostgreSQL-style rules that can store table
	// definitions which may become stale, this is always false in spangres.
	return false;
}


// Look up function by Oid in catalog adapter and return its column information.
// This requires that the catalog adapter has already seen this table when the
// query's FROM clause was first processed by the analyzer.
// To utilize ZetaSQL's Table class, we do this in C++ with a wrapper.
void expandRelation(Oid relid, Alias *eref,
					int rtindex, int sublevels_up,
					int location, bool include_dropped,
					List **colnames, List **colvars) {
  ExpandRelationC(relid, eref, rtindex, sublevels_up, location,
                  colnames, colvars);
}

List *
expandNSItemVars(ParseState *pstate, ParseNamespaceItem *nsitem,
				 int sublevels_up, int location,
				 List **colnames)
{
	List	   *result = NIL;
	int			colindex;
	ListCell   *lc;

	if (colnames)
		*colnames = NIL;

	// SPANGRES: Use expandRelation for tables, which will exclude
	// pseudo columns. This is not safe for expanding tables before they are
	// joined together.
	if (nsitem->p_rte->rtekind == RTE_RELATION) {
		RangeTblEntry *rte = nsitem->p_rte;
		expandRelation(
			rte->relid, rte->eref, nsitem->p_rtindex, sublevels_up, location,
			/*include_dropped=*/false, colnames, &result);
		return result;
	}

	// SPANGRES: Use expandNSItemVarsForJoinC which will exclude pseudo columns
	// from the post-join results.
	if (nsitem->p_rte->rtekind == RTE_JOIN) {
		bool error;
		result = ExpandNSItemVarsForJoinC(pstate->p_rtable, nsitem, sublevels_up,
										  location, colnames, &error);
		if (error) {
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Failed to expand join vars")));
		}
		return result;
	}

	colindex = 0;
	foreach(lc, nsitem->p_rte->eref->colnames)
	{
		void	   *colnameval = (void *) lfirst(lc);
		const char *colname = strVal(colnameval);
		ParseNamespaceColumn *nscol = nsitem->p_nscolumns + colindex;

		if (colname[0])
		{
			Var		   *var;

			Assert(nscol->p_varno > 0);

			var = makeVar(nscol->p_varno,
						  nscol->p_varattno,
						  nscol->p_vartype,
						  nscol->p_vartypmod,
						  nscol->p_varcollid,
						  sublevels_up);
			/* makeVar doesn't offer parameters for these, so set by hand: */
			var->varnosyn = nscol->p_varnosyn;
			var->varattnosyn = nscol->p_varattnosyn;
			var->location = location;
			result = lappend(result, var);
			if (colnames)
				*colnames = lappend(*colnames, colnameval);
		}
		else
		{
			/* dropped column, ignore */
			Assert(nscol->p_varno == 0);
		}
		colindex++;
	}
	return result;
}
