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
void checkExprIsVarFree(ParseState *pstate, Node *n, const char *constructName);

// Methods are called non-statically.
void checkExprIsVarFree(ParseState *pstate, Node *n, const char *constructName);

int setTargetTable(ParseState *pstate, RangeVar *relation,
				   bool inh, bool alsoSource, AclMode requiredPerms) {
	/*
	 * ENRs hide tables of the same name, so we need to check for them first.
	 * In contrast, CTEs don't hide tables (for this purpose).
	 */
	if (relation->schemaname == NULL &&
		scanNameSpaceForENR(pstate, relation->relname)) {
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("relation \"%s\" cannot be the target of a modifying statement",
						relation->relname)));
	}

	/*
	 * Now build an RTE and a ParseNamespaceItem.
	 *
	 * SPANGRES: use the RangeVar relation to construct the RangeTableEntry
	 * instead of the Relation pstate->p_target_relation.
	 */
	ParseNamespaceItem* nsitem =
		addRangeTableEntry(pstate, relation, relation->alias, inh, false);

	/* remember the RTE/nsitem as being the query target */
	pstate->p_target_nsitem = nsitem;

	/* SPANGRES: set the new ParseState p_target_relation_oid field */
	pstate->p_target_relation_oid = nsitem->p_rte->relid;

	/*
	 * Override addRangeTableEntry's default ACL_SELECT permissions check, and
	 * instead mark target table as requiring exactly the specified
	 * permissions.
	 *
	 * If we find an explicit reference to the rel later during parse
	 * analysis, we will add the ACL_SELECT bit back again; see
	 * markVarForSelectPriv and its callers.
	 */
	nsitem->p_rte->requiredPerms = requiredPerms;

	/*
	 * If UPDATE/DELETE, add table to joinlist and namespace.
	 */
	if (alsoSource)
		addNSItemToQuery(pstate, nsitem, true, true, true);

	return nsitem->p_rtindex;
}

void
transformOnConflictArbiter(ParseState* pstate,
						   OnConflictClause* onConflictClause,
						   List** arbiterExpr, Node** arbiterWhere,
						   Oid* constraint) {
	InferClause* infer = onConflictClause->infer;

	*arbiterExpr = NIL;
	*arbiterWhere = NULL;
	*constraint = InvalidOid;

	if (onConflictClause->action == ONCONFLICT_UPDATE && !infer)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("ON CONFLICT DO UPDATE requires inference specification or constraint name"),
				 errhint("For example, ON CONFLICT (column_name)."),
				 parser_errposition(pstate,
									exprLocation((Node *) onConflictClause))));

	/*
	 * Spangres does not support system catalog tables. Remove check and ereport
	 * if INSERT/UPDATE/DELETE are attempted on a system catalog table.
	 * TODO : Add check for INSERT/UPDATE/DELETE on a system catalog
	 * table.
	 */

	/* ON CONFLICT DO NOTHING does not require an inference clause */
	if (infer)
	{
		if (infer->indexElems)
			*arbiterExpr = resolve_unique_index_expr(pstate, infer);

		/*
		 * Handling inference WHERE clause (for partial unique index
		 * inference)
		 */
		if (infer->whereClause)
			*arbiterWhere = transformExpr(pstate, infer->whereClause,
										  EXPR_KIND_INDEX_PREDICATE);

		/*
		 * If the arbiter is specified by constraint name, get the constraint
		 * OID and mark the constrained columns as requiring SELECT privilege,
		 * in the same way as would have happened if the arbiter had been
		 * specified by explicit reference to the constraint's index columns.
		 */
		if (infer->conname)
		{
			Oid			relid = pstate->p_target_relation_oid;
			RangeTblEntry *rte = pstate->p_target_nsitem->p_rte;
			Bitmapset  *conattnos;

			conattnos = get_relation_constraint_attnos(relid, infer->conname,
													   false, constraint);

			/* Make sure the rel as a whole is marked for SELECT access */
			rte->requiredPerms |= ACL_SELECT;
			/* Mark the constrained columns as requiring SELECT access */
			rte->selectedCols = bms_add_members(rte->selectedCols, conattnos);
		}
	}

	/*
	 * It's convenient to form a list of expressions based on the
	 * representation used by CREATE INDEX, since the same restrictions are
	 * appropriate (e.g. on subqueries).  However, from here on, a dedicated
	 * primnode representation is used for inference elements, and so
	 * assign_query_collations() can be trusted to do the right thing with the
	 * post parse analysis query tree inference clause representation.
	 */
}

List* resolve_unique_index_expr(ParseState* pstate, InferClause* infer) {
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, infer->indexElems)
	{
		IndexElem  *ielem = (IndexElem *) lfirst(l);
		InferenceElem *pInfer = makeNode(InferenceElem);
		Node	   *parse;

		/*
		 * Raw grammar re-uses CREATE INDEX infrastructure for unique index
		 * inference clause, and so will accept opclasses by name and so on.
		 *
		 * Make no attempt to match ASC or DESC ordering or NULLS FIRST/NULLS
		 * LAST ordering, since those are not significant for inference
		 * purposes (any unique index matching the inference specification in
		 * other regards is accepted indifferently).  Actively reject this as
		 * wrong-headed.
		 */
		if (ielem->ordering != SORTBY_DEFAULT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("ASC/DESC is not allowed in ON CONFLICT clause"),
					 parser_errposition(pstate,
										exprLocation((Node *) infer))));
		if (ielem->nulls_ordering != SORTBY_NULLS_DEFAULT)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("NULLS FIRST/LAST is not allowed in ON CONFLICT clause"),
					 parser_errposition(pstate,
										exprLocation((Node *) infer))));

		if (!ielem->expr)
		{
			/* Simple index attribute */
			ColumnRef  *n;

			/*
			 * Grammar won't have built raw expression for us in event of
			 * plain column reference.  Create one directly, and perform
			 * expression transformation.  Planner expects this, and performs
			 * its own normalization for the purposes of matching against
			 * pg_index.
			 */
			n = makeNode(ColumnRef);
			n->fields = list_make1(makeString(ielem->name));
			/* Location is approximately that of inference specification */
			n->location = infer->location;
			parse = (Node *) n;
		}
		else
		{
			/* Do parse transformation of the raw expression */
			parse = (Node *) ielem->expr;
		}

		/*
		 * transformExpr() will reject subqueries, aggregates, window
		 * functions, and SRFs, based on being passed
		 * EXPR_KIND_INDEX_EXPRESSION.  So we needn't worry about those
		 * further ... not that they would match any available index
		 * expression anyway.
		 */
		pInfer->expr = transformExpr(pstate, parse, EXPR_KIND_INDEX_EXPRESSION);

		/* Perform lookup of collation and operator class as required */
		if (!ielem->collation)
			pInfer->infercollid = InvalidOid;
		else
			pInfer->infercollid = LookupCollation(pstate, ielem->collation,
												  exprLocation(pInfer->expr));

		if (!ielem->opclass)
			pInfer->inferopclass = InvalidOid;
		else
			pInfer->inferopclass = get_opclass_oid(BTREE_AM_OID,
												   ielem->opclass, false);

		result = lappend(result, pInfer);
	}

	return result;
}

/*
 * transformFrameOffset
 *		Process a window frame offset expression
 *
 * In RANGE mode, rangeopfamily is the sort opfamily for the input ORDER BY
 * column, and rangeopcintype is the input data type the sort operator is
 * registered with.  We expect the in_range function to be registered with
 * that same type.  (In binary-compatible cases, it might be different from
 * the input column's actual type, so we can't use that for the lookups.)
 * We'll return the OID of the in_range function to *inRangeFunc.
 */
Node* transformFrameOffset(ParseState *pstate, int frameOptions,
													 Oid rangeopfamily, Oid rangeopcintype,
													 Oid *inRangeFunc, Node *clause) {
	const char *constructName = NULL;
	Node	   *node;

	*inRangeFunc = InvalidOid;	/* default result */

	/* Quick exit if no offset expression */
	if (clause == NULL)
		return NULL;

	if (frameOptions & FRAMEOPTION_ROWS)
	{
		/* Transform the raw expression tree */
		node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_ROWS);

		/*
		 * Like LIMIT clause, simply coerce to int8_t
		 */
		constructName = "ROWS";
		node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
	}
	else if (frameOptions & FRAMEOPTION_RANGE)
	{
		/*
		 * We must look up the in_range support function that's to be used,
		 * possibly choosing one of several, and coerce the "offset" value to
		 * the appropriate input type.
		 */
		Oid			nodeType;
		Oid			preferredType;
		int			nfuncs = 0;
		int			nmatches = 0;
		Oid			selectedType = InvalidOid;
		Oid			selectedFunc = InvalidOid;

		/* Transform the raw expression tree */
		node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_RANGE);
		nodeType = exprType(node);

		/*
		 * If there are multiple candidates, we'll prefer the one that exactly
		 * matches nodeType; or if nodeType is as yet unknown, prefer the one
		 * that exactly matches the sort column type.  (The second rule is
		 * like what we do for "known_type operator unknown".)
		 */
		preferredType = (nodeType != UNKNOWNOID) ? nodeType : rangeopcintype;

		/* Find the in_range support functions applicable to this case */
		const FormData_pg_amproc* const* amproc_list;
		size_t amproc_count;
		GetAmprocsByFamilyFromBootstrapCatalog(rangeopfamily, rangeopcintype,
																					 &amproc_list, &amproc_count);
		for (int amproc_index = 0; amproc_index < amproc_count; ++amproc_index)
		{
			const FormData_pg_amproc* procform = amproc_list[amproc_index];

			/* The search will find all support proc types; ignore others */
			if (procform->amprocnum != BTINRANGE_PROC)
				continue;
			nfuncs++;

			/* Ignore function if given value can't be coerced to that type */
			if (!can_coerce_type(1, &nodeType, &procform->amprocrighttype,
								 COERCION_IMPLICIT))
				continue;
			nmatches++;

			/* Remember preferred match, or any match if didn't find that */
			if (selectedType != preferredType)
			{
				selectedType = procform->amprocrighttype;
				selectedFunc = procform->amproc;
			}
		}

		/*
		 * Throw error if needed.  It seems worth taking the trouble to
		 * distinguish "no support at all" from "you didn't match any
		 * available offset type".
		 */
		if (nfuncs == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("RANGE with offset PRECEDING/FOLLOWING is not supported for column type %s",
							format_type_be(rangeopcintype)),
					 parser_errposition(pstate, exprLocation(node))));
		if (nmatches == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("RANGE with offset PRECEDING/FOLLOWING is not supported for column type %s and offset type %s",
							format_type_be(rangeopcintype),
							format_type_be(nodeType)),
					 errhint("Cast the offset value to an appropriate type."),
					 parser_errposition(pstate, exprLocation(node))));
		if (nmatches != 1 && selectedType != preferredType)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("RANGE with offset PRECEDING/FOLLOWING has multiple interpretations for column type %s and offset type %s",
							format_type_be(rangeopcintype),
							format_type_be(nodeType)),
					 errhint("Cast the offset value to the exact intended type."),
					 parser_errposition(pstate, exprLocation(node))));

		/* OK, coerce the offset to the right type */
		constructName = "RANGE";
		node = coerce_to_specific_type(pstate, node,
									   selectedType, constructName);
		*inRangeFunc = selectedFunc;
	}
	else if (frameOptions & FRAMEOPTION_GROUPS)
	{
		/* Transform the raw expression tree */
		node = transformExpr(pstate, clause, EXPR_KIND_WINDOW_FRAME_GROUPS);

		/*
		 * Like LIMIT clause, simply coerce to int8_t
		 */
		constructName = "GROUPS";
		node = coerce_to_specific_type(pstate, node, INT8OID, constructName);
	}
	else
	{
		Assert(false);
		node = NULL;
	}

	/* Disallow variables in frame offsets */
	checkExprIsVarFree(pstate, node, constructName);

	return node;
}
