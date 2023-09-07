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

List* transformUpdateTargetList(ParseState* pstate, List* origTlist) {
	List	   *tlist = NIL;
	RangeTblEntry *target_rte;
	ListCell   *orig_tl;
	ListCell   *tl;

	tlist = transformTargetList(pstate, origTlist,
								EXPR_KIND_UPDATE_SOURCE);

	/*
	 * SPANGRES: Get the number of columns and the column data from the ZetaSQL
	 * catalog.
	 */
	int ncolumns = 0;
	char** real_colnames = NULL;
	GetColumnNamesC(pstate->p_target_relation_oid, &real_colnames, &ncolumns);

	/* Prepare to assign non-conflicting resnos to resjunk attributes */
	if (pstate->p_next_resno <= ncolumns) {
		pstate->p_next_resno = ncolumns + 1;
	}

	/* Prepare non-junk columns for assignment to target table */
	target_rte = pstate->p_target_nsitem->p_rte;
	orig_tl = list_head(origTlist);

	foreach(tl, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(tl);
		ResTarget  *origTarget;
		int			attrno;

		if (tle->resjunk)
		{
			/*
			 * Resjunk nodes need no additional processing, but be sure they
			 * have resnos that do not match any target columns; else rewriter
			 * or planner might get confused.  They don't need a resname
			 * either.
			 */
			tle->resno = (AttrNumber) pstate->p_next_resno++;
			tle->resname = NULL;
			continue;
		}
		if (orig_tl == NULL)
			elog(ERROR, "UPDATE target count mismatch --- internal error");
		origTarget = lfirst_node(ResTarget, orig_tl);
		
		/* SPANGRES: Lookup the column id in the ZetaSQL catalog */
		attrno = GetColumnAttrNumber(pstate->p_target_relation_oid,
									 origTarget->name);
		if (attrno == InvalidAttrNumber) {
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
									origTarget->name,
									GetTableNameC(pstate->p_target_relation_oid)),
					 parser_errposition(pstate, origTarget->location)));
		}

		updateTargetListEntry(pstate, tle, origTarget->name,
							  attrno,
							  origTarget->indirection,
							  origTarget->location);

		/* Mark the target column as requiring update permissions */
		target_rte->updatedCols = bms_add_member(target_rte->updatedCols,
												 attrno - FirstLowInvalidHeapAttributeNumber);

		orig_tl = lnext(origTlist, orig_tl);
	}
	if (orig_tl != NULL)
		elog(ERROR, "UPDATE target count mismatch --- internal error");

	return tlist;
}

OnConflictExpr* transformOnConflictClause(
    ParseState* pstate, OnConflictClause* onConflictClause) {
	List	   *arbiterElems;
	Node	   *arbiterWhere;
	Oid			arbiterConstraint;
	List	   *onConflictSet = NIL;
	Node	   *onConflictWhere = NULL;
	int			exclRelIndex = 0;
	OnConflictExpr *result;

	/* Process the arbiter clause, ON CONFLICT ON (...) */
	transformOnConflictArbiter(pstate, onConflictClause, &arbiterElems,
							   &arbiterWhere, &arbiterConstraint);
	
	/* Process DO UPDATE */
	if (onConflictClause->action == ONCONFLICT_UPDATE)
	{
		ParseNamespaceItem *exclNSItem;
		RangeTblEntry *exclRte;

		/*
		 * All INSERT expressions have been parsed, get ready for potentially
		 * existing SET statements that need to be processed like an UPDATE.
		 */
		pstate->p_is_insert = false;

		/*
		 * Add range table entry for the EXCLUDED pseudo relation.  relkind is
		 * set to composite to signal that we're not dealing with an actual
		 * relation, and no permission checks are required on it.  (We'll
		 * check the actual target relation, instead.)
		 */
		exclNSItem = addRangeTableEntryByOid(pstate,
											 pstate->p_target_relation_oid,
											 makeAlias("excluded", NIL),
											 false, false);
		exclRte = exclNSItem->p_rte;
		exclRelIndex = exclNSItem->p_rtindex;

		exclRte->relkind = RELKIND_COMPOSITE_TYPE;
		exclRte->requiredPerms = 0;
		/* other permissions fields in exclRte are already empty */

		// TODO : Add back call to BuildOnConflictExcludedTargetlist,
		// which creates the EXCLUDED rel's targetlist for use by EXPLAIN.

		/*
		 * Add EXCLUDED and the target RTE to the namespace, so that they can
		 * be used in the UPDATE subexpressions.
		 */
		addNSItemToQuery(pstate, exclNSItem, false, true, true);
		addNSItemToQuery(pstate, pstate->p_target_nsitem,
						 false, true, true);

		/*
		 * Now transform the UPDATE subexpressions.
		 */
		onConflictSet =
			transformUpdateTargetList(pstate, onConflictClause->targetList);

		onConflictWhere = transformWhereClause(pstate,
											   onConflictClause->whereClause,
											   EXPR_KIND_WHERE, "WHERE");
	}

	/* Finally, build ON CONFLICT DO [NOTHING | UPDATE] expression */
	result = makeNode(OnConflictExpr);

	result->action = onConflictClause->action;
	result->arbiterElems = arbiterElems;
	result->arbiterWhere = arbiterWhere;
	result->constraint = arbiterConstraint;
	result->onConflictSet = onConflictSet;
	result->onConflictWhere = onConflictWhere;
	result->exclRelIndex = exclRelIndex;
	// TODO : Add back assignment of exclRelTlist, which is the
	// EXCLUDED rel's targetlist for use by EXPLAIN.
	// result->exclRelTlist = exclRelTlist;

	return result;
}
