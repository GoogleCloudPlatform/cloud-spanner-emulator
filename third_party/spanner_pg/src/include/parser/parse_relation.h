/*-------------------------------------------------------------------------
 *
 * parse_relation.h
 *	  prototypes for parse_relation.c.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_relation.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_RELATION_H
#define PARSE_RELATION_H

#include "parser/parse_node.h"
// SPANGRES BEGIN
// Includes required for new Spangres functions.
#include "parser/parse_type.h"
#include "parser/parse_oper.h"
#include "utils/ruleutils.h"
// SPANGRES END


extern ParseNamespaceItem *refnameNamespaceItem(ParseState *pstate,
												const char *schemaname,
												const char *refname,
												int location,
												int *sublevels_up);
extern CommonTableExpr *scanNameSpaceForCTE(ParseState *pstate,
											const char *refname,
											Index *ctelevelsup);
extern bool scanNameSpaceForENR(ParseState *pstate, const char *refname);
extern void checkNameSpaceConflicts(ParseState *pstate, List *namespace1,
									List *namespace2);
extern ParseNamespaceItem *GetNSItemByRangeTablePosn(ParseState *pstate,
													 int varno,
													 int sublevels_up);
extern RangeTblEntry *GetRTEByRangeTablePosn(ParseState *pstate,
											 int varno,
											 int sublevels_up);
extern CommonTableExpr *GetCTEForRTE(ParseState *pstate, RangeTblEntry *rte,
									 int rtelevelsup);
extern Node *scanNSItemForColumn(ParseState *pstate, ParseNamespaceItem *nsitem,
								 int sublevels_up, const char *colname,
								 int location);
extern Node *colNameToVar(ParseState *pstate, const char *colname, bool localonly,
						  int location);
extern void markVarForSelectPriv(ParseState *pstate, Var *var);
extern Relation parserOpenTable(ParseState *pstate, const RangeVar *relation,
								int lockmode);
// SPANGRES BEGIN
// Performs a lookup in googlesql's Catalog and translates a looked-up
// zetasql::Table into an RTE.
ParseNamespaceItem* addRangeTableEntry(struct ParseState* pstate,
                                       RangeVar* relation, Alias* alias,
                                       bool inh, bool inFromCl);

// Like addRangeTableEntry, except looks up the table name in the catalog
// adapter instead of reading it off of the RangeVar*.
extern ParseNamespaceItem* addRangeTableEntryByOid(ParseState* pstate,
                                            Oid relation_oid,
                                            Alias* alias, bool inh,
                                            bool inFromCl);

// Gets a constructed HeapTuple for a given type id. This can substitute for
// calls like SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_id)).
// Returns NULL when type_id is unknown.
Type PgTypeFormHeapTuple(Oid type_id);

// Gets a constructed HeapTuple for a given operator id. This can substitute for
// calls like SearchSysCache1(OPEROID, ObjectIdGetDatum(opno)).
// Returns NULL when operator_id is unknown.
Operator PgOperatorFormHeapTuple(Oid operator_id);

// Analyzer transform for Spangres Hints (List of DefElem nodes), similar to the
// transformXXX() set of functions in parse_expr.c.
//
// Spangres needs a separate transform function here because hints have the
// special property that identifiers are converted to string constants (no
// catalog lookups) and becasue the hint value can be an arbitrary expression,
// not just Value/TypeName. This throws on error.
//
// `pstate`: information on current state of parse used for error reporting.
// `elem`: Spangres hint to be transformed.
//
// Note: Like many transformXXX functions in PostgreSQL transformation is done
// in-place.
Node* transformSpangresHint(ParseState* pstate, DefElem* elem);

// Additional deparser helper function for lists of hints. Intended to mimic
// other deparser helper functions. If adding the `statement` hint, add a
// trailing space. Otherwise add a leading space. This is to conform to existing
// deparser behavior.
void get_hint_list_def(List* hints, deparse_context* context, bool statement);
// SPANGRES END

extern ParseNamespaceItem *addRangeTableEntryForRelation(ParseState *pstate,
														 Relation rel,
														 int lockmode,
														 Alias *alias,
														 bool inh,
														 bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForSubquery(ParseState *pstate,
														 Query *subquery,
														 Alias *alias,
														 bool lateral,
														 bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForFunction(ParseState *pstate,
														 List *funcnames,
														 List *funcexprs,
														 List *coldeflists,
														 RangeFunction *rangefunc,
														 bool lateral,
														 bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForValues(ParseState *pstate,
													   List *exprs,
													   List *coltypes,
													   List *coltypmods,
													   List *colcollations,
													   Alias *alias,
													   bool lateral,
													   bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForTableFunc(ParseState *pstate,
														  TableFunc *tf,
														  Alias *alias,
														  bool lateral,
														  bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForJoin(ParseState *pstate,
													 List *colnames,
													 ParseNamespaceColumn *nscolumns,
													 JoinType jointype,
													 int nummergedcols,
													 List *aliasvars,
													 List *leftcols,
													 List *rightcols,
													 Alias *joinalias,
													 Alias *alias,
													 bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForCTE(ParseState *pstate,
													CommonTableExpr *cte,
													Index levelsup,
													RangeVar *rv,
													bool inFromCl);
extern ParseNamespaceItem *addRangeTableEntryForENR(ParseState *pstate,
													RangeVar *rv,
													bool inFromCl);
extern bool isLockedRefname(ParseState *pstate, const char *refname);
extern void addNSItemToQuery(ParseState *pstate, ParseNamespaceItem *nsitem,
							 bool addToJoinList,
							 bool addToRelNameSpace, bool addToVarNameSpace);
extern void errorMissingRTE(ParseState *pstate, RangeVar *relation) pg_attribute_noreturn();
extern void errorMissingColumn(ParseState *pstate,
							   const char *relname, const char *colname, int location) pg_attribute_noreturn();
extern void expandRTE(RangeTblEntry *rte, int rtindex, int sublevels_up,
					  int location, bool include_dropped,
					  List **colnames, List **colvars);
// SPANGRES BEGIN
extern List *expandNSItemVars(ParseState* pstate, ParseNamespaceItem *nsitem,
							  int sublevels_up, int location,
							  List **colnames);
// SPANGRES END
extern List *expandNSItemAttrs(ParseState *pstate, ParseNamespaceItem *nsitem,
							   int sublevels_up, bool require_col_privs,
							   int location);
extern int	attnameAttNum(Relation rd, const char *attname, bool sysColOK);
extern const NameData *attnumAttName(Relation rd, int attid);
extern Oid	attnumTypeId(Relation rd, int attid);
extern Oid	attnumCollationId(Relation rd, int attid);
extern bool isQueryUsingTempRelation(Query *query);

// SPANGRES BEGIN
// Visible for testing
void expandRelation(Oid relid, Alias *eref,
						   int rtindex, int sublevels_up,
						   int location, bool include_dropped,
						   List **colnames, List **colvars);
// SPANGRES END

#endif							/* PARSE_RELATION_H */
