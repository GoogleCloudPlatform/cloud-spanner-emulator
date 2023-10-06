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
void get_rule_expr_paren(Node* node, deparse_context* context,
						 bool showimplicit, Node* parentNode);

void get_oper_expr(OpExpr* expr, deparse_context* context) {
	StringInfo	buf = context->buf;
	Oid			opno = expr->opno;
	List	   *args = expr->args;

	if (!POSTGRES_PRETTY_PAREN(context))
		appendStringInfoChar(buf, '(');
	if (list_length(args) == 2)
	{
		/* binary operator */
		Node		 *arg1 = (Node *) linitial(args);
		Node		 *arg2 = (Node *) lsecond(args);

		get_rule_expr_paren(arg1, context, true, (Node *) expr);
		appendStringInfo(buf, " %s ",
						 generate_operator_name(opno,
												exprType(arg1),
												exprType(arg2)));
		get_rule_expr_paren(arg2, context, true, (Node *) expr);
	}
	else
	{
		/* unary operator --- but which side? */
		Node		 *arg = (Node *) linitial(args);

		/* SPANGRES: look up operator from the Bootstrap Catalog */
		const FormData_pg_operator* optup = GetOperatorFromBootstrapCatalog(opno);
		if (optup == NULL) {
			elog(ERROR, "catalog lookup failed for operator %u", opno);
		}
		switch (optup->oprkind)
		{
			case 'l':
				appendStringInfo(buf, "%s ",
								 generate_operator_name(opno,
														InvalidOid,
														exprType(arg)));
				get_rule_expr_paren(arg, context, true, (Node *) expr);
				break;
			case 'r':
				get_rule_expr_paren(arg, context, true, (Node *) expr);
				appendStringInfo(buf, " %s",
								 generate_operator_name(opno,
														exprType(arg),
														InvalidOid));
				break;
			default:
				elog(ERROR, "bogus oprkind: %d", optup->oprkind);
		}
	}
	if (!POSTGRES_PRETTY_PAREN(context))
		appendStringInfoChar(buf, ')');
}

// Gets a table name given its oid. This shimmed function is different from its
// PostgreSQL version in the following ways:
// - It doesn't manually add a namespace prefix to the table name, since Spanner
// does not support inferring namespace from search path. If the table is in
// fact in a namespace it will use that name.
// - The table name is looked up in the catalog adapter instead of the
// PostgreSQL catalog.
// - It doesn't use the query's cteList (in WITH clause) to check against
// the need to qualify the table name.
// TODO : revisit this function when we support WITH clauses.
char* generate_relation_name(Oid relid, List* namespaces)
{
	char* table_name = GetTableNameC(relid);
	char* namespace_name = GetNamespaceNameC(relid);
	// Temporary fix: drop "public" since it is assumed in the search path.
	// TODO: Replace this with a more PG-faithful version when
	// adding search path.
	if (strcmp(namespace_name, "public") == 0) {
		namespace_name = NULL;
	}
	if (table_name == NULL) {
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("There is no table name with oid %s in the catalog adapter",
								relid)));
	}
	// TODO : `quote_qualified_identifier` uses
	// `quote_all_identifiers`, a PostgreSQL configuration parameter. We need to
	// properly handle its uses.
	return quote_qualified_identifier(namespace_name, table_name);
}

char* generate_operator_name(Oid operid, Oid arg1, Oid arg2)
{
	StringInfoData buf;
	const	   FormData_pg_operator* operform;
	char	   *oprname;
	char	   *nspname;
	Operator	p_result;

	initStringInfo(&buf);

	operform = GetOperatorFromBootstrapCatalog(operid);
	if (operform == NULL) {
		// Should not happen.
		elog(ERROR, "bootstrap catalog lookup failed for operator %u",
			 operid);
	}
	// Make a non-const copy of oprname.
	oprname = pstrdup(NameStr(operform->oprname));

	/*
	 * The idea here is to schema-qualify only if the parser would fail to
	 * resolve the correct operator given the unqualified op name with the
	 * specified argtypes.
	 */
	switch (operform->oprkind)
	{
		case 'b':
			p_result = oper(NULL, list_make1(makeString(oprname)), arg1, arg2,
							true, -1);
			break;
		case 'l':
			p_result = left_oper(NULL, list_make1(makeString(oprname)), arg2,
								 true, -1);
			break;
		default:
			elog(ERROR, "unrecognized oprkind: %d", operform->oprkind);
			p_result = NULL;	/* keep compiler quiet */
			break;
	}


	if (p_result != NULL && oprid(p_result) == operid)
		nspname = NULL;
	else {
		// TODO : Support namespaces.
		elog(ERROR, "namespaces and schemas are not supported in Spangres.");
	}

	appendStringInfoString(&buf, oprname);

	if (nspname)
		appendStringInfoChar(&buf, ')');

	return buf.data;
}

char* generate_function_name(Oid funcid, int nargs, List* argnames, Oid* argtypes,
							 bool has_variadic, bool* use_variadic_p,
							 ParseExprKind special_exprkind)
{
	char	   *result;
	char	   *proname;
	bool		use_variadic;
	char	   *nspname;
	FuncDetailCode p_result;
	Oid			p_funcid;
	Oid			p_rettype;
	bool		p_retset;
	int			p_nvargs;
	Oid			p_vatype;
	Oid		   *p_true_typeids;
	bool		force_qualify = false;

	const FormData_pg_proc* procform = GetProcByOid(funcid);
	if (procform == NULL) {
		elog(ERROR, "bootstrap catalog lookup failed for function %u", funcid);
	}
	/* Make a non-const copy of proname */
	proname = pstrdup(NameStr(procform->proname));

	/*
	 * Due to parser hacks to avoid needing to reserve CUBE, we need to force
	 * qualification in some special cases.
	 */
	if (special_exprkind == EXPR_KIND_GROUP_BY)
	{
		if (strcmp(proname, "cube") == 0 || strcmp(proname, "rollup") == 0)
			force_qualify = true;
	}

	/*
	 * Determine whether VARIADIC should be printed.  We must do this first
	 * since it affects the lookup rules in func_get_detail().
	 *
	 * We always print VARIADIC if the function has a merged variadic-array
	 * argument.  Note that this is always the case for functions taking a
	 * VARIADIC argument type other than VARIADIC ANY.  If we omitted VARIADIC
	 * and printed the array elements as separate arguments, the call could
	 * match a newer non-VARIADIC function.
	 */
	if (use_variadic_p) {
		/* Parser should not have set funcvariadic unless fn is variadic */
		Assert(!has_variadic || OidIsValid(procform->provariadic));
		use_variadic = has_variadic;
		*use_variadic_p = use_variadic;
	} else {
		Assert(!has_variadic);
		use_variadic = false;
	}

	/*
	 * The idea here is to schema-qualify only if the parser would fail to
	 * resolve the correct function given the unqualified func name with the
	 * specified argtypes and VARIADIC flag.  But if we already decided to
	 * force qualification, then we can skip the lookup and pretend we didn't
	 * find it.
	 */
	if (!force_qualify)
		p_result = func_get_detail(list_make1(makeString(proname)),
											NIL, argnames, nargs, argtypes,
											!use_variadic, true, false,
											&p_funcid, &p_rettype,
											&p_retset, &p_nvargs, &p_vatype,
											&p_true_typeids, NULL);
	else
	{
		p_result = FUNCDETAIL_NOTFOUND;
		p_funcid = InvalidOid;
	}

	if ((p_result == FUNCDETAIL_NORMAL ||
		 p_result == FUNCDETAIL_AGGREGATE ||
		 p_result == FUNCDETAIL_WINDOWFUNC) &&
		p_funcid == funcid)
		nspname = NULL;
	else
		nspname = GetNamespaceNameByOidFromBootstrapCatalog(procform->pronamespace);

	result = quote_qualified_identifier(nspname, proname);
	return result;
}

// We made these functions non-static to call from here. Forward declare them to
// limit the scope.
char *make_colname_unique(char* colname, deparse_namespace* dpns,
                          deparse_columns* colinfo);
void expand_colnames_array_to(deparse_columns* colinfo, int n);

// Sets column names of the table specififed in the RangeTblEntry object.
// Functionally the same as the PostgreSQL version, but uses a ZetaSQL catalog
// to look up table and columns information.
void set_relation_column_names(deparse_namespace *dpns,
							   RangeTblEntry *rte,
							   deparse_columns *colinfo) {
	int ncolumns = 0;
	char** real_colnames = NULL;

	// Extract the RTE's "real" column names.
	if (rte->rtekind == RTE_RELATION) {
		GetColumnNamesC(rte->relid, &real_colnames, &ncolumns);
	} else {
		// Otherwise use the column names from eref.
		ncolumns = list_length(rte->eref->colnames);
		real_colnames = (char**) palloc(ncolumns * sizeof(char *));

		int column_index = 0;
		ListCell* lc;
		foreach(lc, rte->eref->colnames) {
			// Column names in eref should not be empty:
			char* cname = strVal(lfirst(lc));
			if (cname == NULL || cname[0] == '\0') {
				ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
								errmsg("Column name in RangeTblEntry should not be empty")));
			}
			real_colnames[column_index] = cname;
			++column_index;
		}
	}

	// Ensure colinfo->colnames has a slot for each column.	(It could be long
	// enough already, if we pushed down a name for the last column.)	Note:
	// it's possible that there are now more columns than there were when the
	// query was parsed, ie colnames could be longer than rte->eref->colnames.
	// We must assign unique aliases to the new columns too, else there could
	// be unresolved conflicts when the view/rule is reloaded.
	expand_colnames_array_to(colinfo, ncolumns);
	Assert(colinfo->num_cols == ncolumns);

	// Make sufficiently large new_colnames and is_new_col arrays, too.
	// Note: because we leave colinfo->num_new_cols zero until after the loop,
	// colname_is_unique will not consult that array, which is fine because it
	// would only be duplicate effort.
	colinfo->new_colnames = (char**) palloc(ncolumns * sizeof(char*));
	colinfo->is_new_col = (bool*) palloc(ncolumns * sizeof(bool));

	// Scan the columns, select a unique alias for each one, and store it in
	// colinfo->colnames and colinfo->new_colnames. Also mark new_colnames
	// entries as to whether they are new since parse time; this is the case for
	// entries beyond the length of rte->eref->colnames.
	int noldcolumns = list_length(rte->eref->colnames);
	bool changed_any = false;
	int new_column_index = 0;

	for (int i = 0; i < ncolumns; ++i) {
		char* real_colname = real_colnames[i];
		char* colname = colinfo->colnames[i];

		// If alias already assigned, that's what to use
		if (colname == NULL) {
			// If user wrote an alias, prefer that over real column name
			if (rte->alias && i < list_length(rte->alias->colnames)) {
				colname = strVal(list_nth(rte->alias->colnames, i));
			} else {
				colname = real_colname;
			}
			// Unique-ify and insert into colinfo
			colname = make_colname_unique(colname, dpns, colinfo);

			colinfo->colnames[i] = colname;
		}

		// Put names in new_colnames[] too
		colinfo->new_colnames[new_column_index] = colname;
		// And mark them as new or not
		colinfo->is_new_col[new_column_index] = (i >= noldcolumns);
		++new_column_index;

		// Remember if any assigned aliases differ from "real" name
		if (!changed_any && strcmp(colname, real_colname) != 0) {
			changed_any = true;
		}
	}

	// Set correct length for new_colnames[] array.	(Note: if columns have
	// been added, colinfo->num_cols includes them, which is not really quite
	// right but is harmless, since any new columns must be at the end where
	// they won't affect varattnos of pre-existing columns.)
	colinfo->num_new_cols = new_column_index;

	// For a relation RTE, we need only print the alias column names if any
	// are different from the underlying "real" names.	For a function RTE,
	// always emit a complete column alias list; this is to protect against
	// possible instability of the default column names (eg, from altering
	// parameter names).	For tablefunc RTEs, we never print aliases, because
	// the column names are part of the clause itself.	For other RTE types,
	// print if we changed anything OR if there were user-written column
	// aliases (since the latter would be part of the underlying "reality").
	if (rte->rtekind == RTE_FUNCTION) {
		colinfo->printaliases = true;
	} else if (rte->rtekind == RTE_TABLEFUNC) {
		colinfo->printaliases = false;
	} else if (rte->alias && rte->alias->colnames != NIL) {
		colinfo->printaliases = true;
	} else {
		colinfo->printaliases = changed_any;
	}
}

// Parse back one query parsetree If resultDesc is not NULL, then it is the
// output tuple descriptor for the view represented by a SELECT query. This 
// function is forward declared to make it accessible by get_setop_query.
void get_query_def(Query* query, StringInfo buf, List* parentnamespace,
                   TupleDesc resultDesc, bool colNamesVisible, int prettyFlags,
                   int wrapColumn, int startIndent);

// Append a keyword to the buffer. If prettyPrint is enabled, perform a line
// break and adjust indentation. Otherwise, just append the keyword. This 
// function is forward declared to make it accessible by get_setop_query. 
void appendContextKeyword(deparse_context* context, const char* str,
                          int indentBefore, int indentAfter, int indentPlus);

void get_setop_query(Node *setOp, Query *query, deparse_context *context,
				TupleDesc resultDesc, bool colNamesVisible)
{
	StringInfo	buf = context->buf;
	bool		need_paren;

	/* Guard against excessively long or deeply-nested queries */
	CHECK_FOR_INTERRUPTS();
	check_stack_depth();

	if (IsA(setOp, RangeTblRef))
	{
		RangeTblRef *rtr = (RangeTblRef *) setOp;
		RangeTblEntry *rte = rt_fetch(rtr->rtindex, query->rtable);
		Query	   *subquery = rte->subquery;

		Assert(subquery != NULL);
		/* SPANGRES BEGIN */
		// Spangres removes an assert which checks that the subquery does not
		// include set operations. The assert caused subqueries containing set
		// operations and limit/offset/order by to fail in the deparser. The assert
		// can be safely removed without causing the deparser to incorrectly bind
		// a WITH, ORDER BY, FOR UPDATE, or LIMIT/OFFSET clause to an incorrect
		// subquery level.
		/* SPANGRES END */

		/* Need parens if WITH, ORDER BY, FOR UPDATE, or LIMIT; see gram.y */
		need_paren = (subquery->cteList ||
					  subquery->sortClause ||
					  subquery->rowMarks ||
					  subquery->limitOffset ||
					  subquery->limitCount);
		if (need_paren)
			appendStringInfoChar(buf, '(');
		get_query_def(subquery, buf, context->namespaces, resultDesc,
									colNamesVisible,
									context->prettyFlags, context->wrapColumn,
									context->indentLevel);
		if (need_paren)
			appendStringInfoChar(buf, ')');
	}
	else if (IsA(setOp, SetOperationStmt))
	{
		SetOperationStmt *op = (SetOperationStmt *) setOp;
		int			subindent;

		/*
		 * We force parens when nesting two SetOperationStmts, except when the
		 * lefthand input is another setop of the same kind.  Syntactically,
		 * we could omit parens in rather more cases, but it seems best to use
		 * parens to flag cases where the setop operator changes.  If we use
		 * parens, we also increase the indentation level for the child query.
		 *
		 * There are some cases in which parens are needed around a leaf query
		 * too, but those are more easily handled at the next level down (see
		 * code above).
		 */
		if (IsA(op->larg, SetOperationStmt))
		{
			SetOperationStmt *lop = (SetOperationStmt *) op->larg;

			if (op->op == lop->op && op->all == lop->all)
				need_paren = false;
			else
				need_paren = true;
		}
		else
			need_paren = false;

		if (need_paren)
		{
			appendStringInfoChar(buf, '(');
			subindent = POSTGRES_PRETTYINDENT_STD;
			appendContextKeyword(context, "", subindent, 0, 0);
		}
		else
			subindent = 0;

		get_setop_query(op->larg, query, context, resultDesc, colNamesVisible);

		if (need_paren)
			appendContextKeyword(context, ") ", -subindent, 0, 0);
		else if (POSTGRES_PRETTY_INDENT(context))
			appendContextKeyword(context, "", -subindent, 0, 0);
		else
			appendStringInfoChar(buf, ' ');

		switch (op->op)
		{
			case SETOP_UNION:
				appendStringInfoString(buf, "UNION ");
				break;
			case SETOP_INTERSECT:
				appendStringInfoString(buf, "INTERSECT ");
				break;
			case SETOP_EXCEPT:
				appendStringInfoString(buf, "EXCEPT ");
				break;
			default:
				elog(ERROR, "unrecognized set op: %d",
					 (int) op->op);
		}
		if (op->all)
			appendStringInfoString(buf, "ALL ");

		/* Always parenthesize if RHS is another setop */
		need_paren = IsA(op->rarg, SetOperationStmt);

		/*
		 * The indentation code here is deliberately a bit different from that
		 * for the lefthand input, because we want the line breaks in
		 * different places.
		 */
		if (need_paren)
		{
			appendStringInfoChar(buf, '(');
			subindent = POSTGRES_PRETTYINDENT_STD;
		}
		else
			subindent = 0;
		appendContextKeyword(context, "", subindent, 0, 0);

		get_setop_query(op->rarg, query, context, resultDesc, false);

		if (POSTGRES_PRETTY_INDENT(context))
			context->indentLevel -= subindent;
		if (need_paren)
			appendContextKeyword(context, ")", 0, 0, 0);
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(setOp));
	}
}

