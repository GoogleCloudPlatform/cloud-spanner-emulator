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

// We made this function non-static to call from here. Forward declare it to
// limit the scope.
Node* transformAssignmentIndirection(ParseState* pstate,
									 Node* basenode,
									 const char* targetName,
									 bool targetIsSubscripting,
									 Oid targetTypeId,
									 int32_t targetTypMod,
									 Oid targetCollation,
									 List* indirection,
									 ListCell* indirection_cell,
									 Node* rhs,
									 CoercionContext ccontext,
									 int location);

Expr* transformAssignedExpr(ParseState* pstate, Expr* expr,
							ParseExprKind exprKind, char* colname,
							int attrno, List* indirection,
							int location) {
	Oid			type_id;		/* type of value provided */
	Oid			attrtype;		/* type of target column */
	int32_t		attrtypmod;
	Oid			attrcollation;	/* collation of target column */
	ParseExprKind sv_expr_kind;

	/*
	 * Save and restore identity of expression type we're parsing.  We must
	 * set p_expr_kind here because we can parse subscripts without going
	 * through transformExpr().
	 */
	Assert(exprKind != EXPR_KIND_NONE);
	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = exprKind;

	Assert(pstate->p_target_relation_oid != InvalidOid);
	if (attrno <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot assign to system column \"%s\"",
						colname),
				 parser_errposition(pstate, location)));

	/*
	 * SPANGRES: Use the Engine System Catalog and Bootstrap Catalog to get the
	 * type info.
	 */
	GetAttributeTypeC(pstate->p_target_relation_oid, attrno, &attrtype,
										&attrtypmod, &attrcollation);
	if (attrtype == InvalidOid) {
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_COLUMN),
			 errmsg("Unable to get type of column \"%s\" of relation \"%s\". "
							"(Is the column type supported?)",
							colname, pstate->p_target_nsitem->p_rte->eref->aliasname)));
	}

	/*
	 * If the expression is a DEFAULT placeholder, insert the attribute's
	 * type/typmod/collation into it so that exprType etc will report the
	 * right things.  (We expect that the eventually substituted default
	 * expression will in fact have this type and typmod.  The collation
	 * likely doesn't matter, but let's set it correctly anyway.)  Also,
	 * reject trying to update a subfield or array element with DEFAULT, since
	 * there can't be any default for portions of a column.
	 */
	if (expr && IsA(expr, SetToDefault))
	{
		SetToDefault *def = (SetToDefault *) expr;

		def->typeId = attrtype;
		def->typeMod = attrtypmod;
		def->collation = attrcollation;
		if (indirection)
		{
			if (IsA(linitial(indirection), A_Indices))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot set an array element to DEFAULT"),
						 parser_errposition(pstate, location)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot set a subfield to DEFAULT"),
						 parser_errposition(pstate, location)));
		}
	}

	/* Now we can use exprType() safely. */
	type_id = exprType((Node *) expr);

	/*
	 * If there is indirection on the target column, prepare an array or
	 * subfield assignment expression.  This will generate a new column value
	 * that the source value has been inserted into, which can then be placed
	 * in the new tuple constructed by INSERT or UPDATE.
	 */
	if (indirection)
	{
		Node	   *colVar;

		if (pstate->p_is_insert)
		{
			/*
			 * The command is INSERT INTO table (col.something) ... so there
			 * is not really a source value to work with. Insert a NULL
			 * constant as the source value.
			 */
			colVar = (Node *) makeNullConst(attrtype, attrtypmod,
											attrcollation);
		}
		else
		{
			/*
			 * Build a Var for the column to be updated.
			 */
			Var		   *var;
			var = makeVar(pstate->p_target_nsitem->p_rtindex, attrno,
							   attrtype, attrtypmod, attrcollation, 0);
			var->location = location;

			colVar = (Node *) var;
		}

		expr = (Expr *)
			transformAssignmentIndirection(pstate,
										   colVar,
										   colname,
										   false,
										   attrtype,
										   attrtypmod,
										   attrcollation,
										   indirection,
										   list_head(indirection),
										   (Node *) expr,
											 COERCION_ASSIGNMENT,
										   location);
	}
	else
	{
		/*
		 * For normal non-qualified target column, do type checking and
		 * coercion.
		 */
		Node	   *orig_expr = (Node *) expr;

		expr = (Expr *)
			coerce_to_target_type(pstate,
								  orig_expr, type_id,
								  attrtype, attrtypmod,
								  COERCION_ASSIGNMENT,
								  COERCE_IMPLICIT_CAST,
								  -1);
		if (expr == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("column \"%s\" is of type %s"
							" but expression is of type %s",
							colname,
							format_type_be(attrtype),
							format_type_be(type_id)),
					 errhint("You will need to rewrite or cast the expression."),
					 parser_errposition(pstate, exprLocation(orig_expr))));
	}

	pstate->p_expr_kind = sv_expr_kind;

	return expr;
}

List* checkInsertTargets(ParseState* pstate, List* cols, List** attrnos) {
	*attrnos = NIL;

	if (cols == NIL)
	{
		/*
		 * Generate default column list for INSERT.
		 *
		 * SPANGRES: use p_target_relation_oid and the Engine User Catalog instead
		 * of p_target_relation.
		 */
		char** real_colnames;
		int numcol;
		GetColumnNamesC(pstate->p_target_relation_oid, &real_colnames, &numcol);

		for (int i = 0; i < numcol; ++i)
		{
			if (IsAttributePseudoColumnC(pstate->p_target_relation_oid, i + 1)) {
				continue;
			}
			ResTarget* col = makeNode(ResTarget);
			col->name = real_colnames[i];
			col->indirection = NIL;
			col->val = NULL;
			col->location = -1;
			cols = lappend(cols, col);
			*attrnos = lappend_int(*attrnos, i + 1);
		}
	}
	else
	{
		/*
		 * Do initial validation of user-supplied INSERT column list.
		 */
		Bitmapset* wholecols = NULL;
		Bitmapset* partialcols = NULL;
		ListCell* tl;

		foreach(tl, cols)
		{
			ResTarget* col = (ResTarget*) lfirst(tl);
			char* name = col->name;
			int attrno;

			/* Lookup column id using Engine User Catalog, ereport on failure */
			attrno = GetColumnAttrNumber(pstate->p_target_relation_oid, name);
			if (attrno == InvalidAttrNumber)
				ereport(ERROR,
								(errcode(ERRCODE_UNDEFINED_COLUMN),
								 errmsg("column \"%s\" of relation \"%s\" does not exist",
										name,
										GetTableNameC(pstate->p_target_relation_oid)),
								 parser_errposition(pstate, col->location)));

			/*
			 * Check for duplicates, but only of whole columns --- we allow
			 * INSERT INTO foo (col.subcol1, col.subcol2)
			 */
			if (col->indirection == NIL)
			{
				/* whole column; must not have any other assignment */
				if (bms_is_member(attrno, wholecols) ||
					bms_is_member(attrno, partialcols))
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("column \"%s\" specified more than once",
									name),
							 parser_errposition(pstate, col->location)));
				wholecols = bms_add_member(wholecols, attrno);
			}
			else
			{
				/* partial column; must not have any whole assignment */
				if (bms_is_member(attrno, wholecols))
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("column \"%s\" specified more than once",
									name),
							 parser_errposition(pstate, col->location)));
				partialcols = bms_add_member(partialcols, attrno);
			}

			*attrnos = lappend_int(*attrnos, attrno);
		}
	}

	return cols;
}
