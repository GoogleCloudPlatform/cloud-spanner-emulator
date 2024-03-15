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

#ifndef SHIMS_CATALOG_SHIM_H_
#define SHIMS_CATALOG_SHIM_H_

#include "third_party/spanner_pg/postgres_includes/all.h"

// Define the interceptor interface for PostgreSQL to use in the analyzer.
#ifdef __cplusplus
extern "C" {
#endif

// Redefines the same macros as in PostgreSQL's utils/adt/ruleutils.c but
// prefixes the macros with POSTGRES_ to prevent collisions in Google3.
#define POSTGRES_PRETTYINDENT_STD 8
#define POSTGRES_PRETTYINDENT_JOIN 4
#define POSTGRES_PRETTYINDENT_VAR 4
#define POSTGRES_PRETTYINDENT_LIMIT 40

#define POSTGRES_PRETTYFLAG_PAREN 0x0001
#define POSTGRES_PRETTYFLAG_INDENT 0x0002
#define POSTGRES_PRETTYFLAG_SCHEMA 0x0004
#define POSTGRES_PRETTY_PAREN(context) \
  ((context)->prettyFlags & POSTGRES_PRETTYFLAG_PAREN)
#define POSTGRES_PRETTY_INDENT(context) \
  ((context)->prettyFlags & POSTGRES_PRETTYFLAG_INDENT)
#define POSTGRES_PRETTY_SCHEMA(context) \
  ((context)->prettyFlags & POSTGRES_PRETTYFLAG_SCHEMA)

// Redefines the same macro as in PostgreSQL's utils/adt/ruleutils.c.
#define only_marker(rte) ((rte)->inh ? "" : "ONLY ")

// Redefines the same struct as in PostgreSQL's utils/adt/ruleutils.c
/*
 * Each level of query context around a subtree needs a level of Var namespace.
 * A Var having varlevelsup=N refers to the N'th item (counting from 0) in
 * the current context's namespaces list.
 *
 * rtable is the list of actual RTEs from the Query or PlannedStmt.
 * rtable_names holds the alias name to be used for each RTE (either a C
 * string, or NULL for nameless RTEs such as unnamed joins).
 * rtable_columns holds the column alias names to be used for each RTE.
 *
 * subplans is a list of Plan trees for SubPlans and CTEs (it's only used
 * in the PlannedStmt case).
 * ctes is a list of CommonTableExpr nodes (only used in the Query case).
 * appendrels, if not null (it's only used in the PlannedStmt case), is an
 * array of AppendRelInfo nodes, indexed by child relid.  We use that to map
 * child-table Vars to their inheritance parents.
 *
 * In some cases we need to make names of merged JOIN USING columns unique
 * across the whole query, not only per-RTE.  If so, unique_using is true
 * and using_names is a list of C strings representing names already assigned
 * to USING columns.
 *
 * When deparsing plan trees, there is always just a single item in the
 * deparse_namespace list (since a plan tree never contains Vars with
 * varlevelsup > 0).  We store the Plan node that is the immediate
 * parent of the expression to be deparsed, as well as a list of that
 * Plan's ancestors.  In addition, we store its outer and inner subplan nodes,
 * as well as their targetlists, and the index tlist if the current plan node
 * might contain INDEX_VAR Vars.  (These fields could be derived on-the-fly
 * from the current Plan node, but it seems notationally clearer to set them
 * up as separate fields.)
 */
typedef struct {
  List* rtable;               /* List of RangeTblEntry nodes */
  List* rtable_names;         /* Parallel list of names for RTEs */
  List* rtable_columns;       /* Parallel list of deparse_columns structs */
  List* subplans;             /* List of Plan trees for SubPlans */
  List* ctes;                 /* List of CommonTableExpr nodes */
  AppendRelInfo** appendrels; /* Array of AppendRelInfo nodes, or NULL */
  /* Workspace for column alias assignment: */
  bool unique_using; /* Are we making USING names globally unique */
  List* using_names; /* List of assigned names for USING columns */
  /* Remaining fields are used only when deparsing a Plan tree: */
  Plan* plan;        /* immediate parent of current expression */
  List* ancestors;   /* ancestors of plan */
  Plan* outer_plan;  /* outer subnode, or NULL if none */
  Plan* inner_plan;  /* inner subnode, or NULL if none */
  List* outer_tlist; /* referent for OUTER_VAR Vars */
  List* inner_tlist; /* referent for INNER_VAR Vars */
  List* index_tlist; /* referent for INDEX_VAR Vars */
  /* Special namespace representing a function signature: */
  char* funcname;
  int numargs;
  char** argnames;
} deparse_namespace;

// Redefines the same struct as in PostgreSQL's utils/adt/ruleutils.c
/*
 * Per-relation data about column alias names.
 *
 * Selecting aliases is unreasonably complicated because of the need to dump
 * rules/views whose underlying tables may have had columns added, deleted, or
 * renamed since the query was parsed.  We must nonetheless print the rule/view
 * in a form that can be reloaded and will produce the same results as before.
 *
 * For each RTE used in the query, we must assign column aliases that are
 * unique within that RTE.  SQL does not require this of the original query,
 * but due to factors such as *-expansion we need to be able to uniquely
 * reference every column in a decompiled query.  As long as we qualify all
 * column references, per-RTE uniqueness is sufficient for that.
 *
 * However, we can't ensure per-column name uniqueness for unnamed join RTEs,
 * since they just inherit column names from their input RTEs, and we can't
 * rename the columns at the join level.  Most of the time this isn't an issue
 * because we don't need to reference the join's output columns as such; we
 * can reference the input columns instead.  That approach can fail for merged
 * JOIN USING columns, however, so when we have one of those in an unnamed
 * join, we have to make that column's alias globally unique across the whole
 * query to ensure it can be referenced unambiguously.
 *
 * Another problem is that a JOIN USING clause requires the columns to be
 * merged to have the same aliases in both input RTEs, and that no other
 * columns in those RTEs or their children conflict with the USING names.
 * To handle that, we do USING-column alias assignment in a recursive
 * traversal of the query's jointree.  When descending through a JOIN with
 * USING, we preassign the USING column names to the child columns, overriding
 * other rules for column alias assignment.  We also mark each RTE with a list
 * of all USING column names selected for joins containing that RTE, so that
 * when we assign other columns' aliases later, we can avoid conflicts.
 *
 * Another problem is that if a JOIN's input tables have had columns added or
 * deleted since the query was parsed, we must generate a column alias list
 * for the join that matches the current set of input columns --- otherwise, a
 * change in the number of columns in the left input would throw off matching
 * of aliases to columns of the right input.  Thus, positions in the printable
 * column alias list are not necessarily one-for-one with varattnos of the
 * JOIN, so we need a separate new_colnames[] array for printing purposes.
 */
typedef struct {
  /*
   * colnames is an array containing column aliases to use for columns that
   * existed when the query was parsed.  Dropped columns have NULL entries.
   * This array can be directly indexed by varattno to get a Var's name.
   *
   * Non-NULL entries are guaranteed unique within the RTE, *except* when
   * this is for an unnamed JOIN RTE.  In that case we merely copy up names
   * from the two input RTEs.
   *
   * During the recursive descent in set_using_names(), forcible assignment
   * of a child RTE's column name is represented by pre-setting that element
   * of the child's colnames array.  So at that stage, NULL entries in this
   * array just mean that no name has been preassigned, not necessarily that
   * the column is dropped.
   */
  int num_cols;    /* length of colnames[] array */
  char** colnames; /* array of C strings and NULLs */

  /*
   * new_colnames is an array containing column aliases to use for columns
   * that would exist if the query was re-parsed against the current
   * definitions of its base tables.  This is what to print as the column
   * alias list for the RTE.  This array does not include dropped columns,
   * but it will include columns added since original parsing.  Indexes in
   * it therefore have little to do with current varattno values.  As above,
   * entries are unique unless this is for an unnamed JOIN RTE.  (In such an
   * RTE, we never actually print this array, but we must compute it anyway
   * for possible use in computing column names of upper joins.) The
   * parallel array is_new_col marks which of these columns are new since
   * original parsing.  Entries with is_new_col false must match the
   * non-NULL colnames entries one-for-one.
   */
  int num_new_cols;    /* length of new_colnames[] array */
  char** new_colnames; /* array of C strings */
  bool* is_new_col;    /* array of bool flags */

  /* This flag tells whether we should actually print a column alias list */
  bool printaliases;

  /* This list has all names used as USING names in joins above this RTE */
  List* parentUsing; /* names assigned to parent merged columns */

  /*
   * If this struct is for a JOIN RTE, we fill these fields during the
   * set_using_names() pass to describe its relationship to its child RTEs.
   *
   * leftattnos and rightattnos are arrays with one entry per existing
   * output column of the join (hence, indexable by join varattno).  For a
   * simple reference to a column of the left child, leftattnos[i] is the
   * child RTE's attno and rightattnos[i] is zero; and conversely for a
   * column of the right child.  But for merged columns produced by JOIN
   * USING/NATURAL JOIN, both leftattnos[i] and rightattnos[i] are nonzero.
   * Note that a simple reference might be to a child RTE column that's been
   * dropped; but that's OK since the column could not be used in the query.
   *
   * If it's a JOIN USING, usingNames holds the alias names selected for the
   * merged columns (these might be different from the original USING list,
   * if we had to modify names to achieve uniqueness).
   */
  int leftrti;      /* rangetable index of left child */
  int rightrti;     /* rangetable index of right child */
  int* leftattnos;  /* left-child varattnos of join cols, or 0 */
  int* rightattnos; /* right-child varattnos of join cols, or 0 */
  List* usingNames; /* names assigned to merged columns */
} deparse_columns;

// Redefines the ParseState struct from PostgreSQL's parser/parse_node.h
// Replaces p_target_relation with p_target_relation_oid, which is an Oid for
// the target table of INSERT, UPDATE, or DELETE.
/*
 * State information used during parse analysis
 *
 * parentParseState: NULL in a top-level ParseState.  When parsing a subquery,
 * links to current parse state of outer query.
 *
 * p_sourcetext: source string that generated the raw parsetree being
 * analyzed, or NULL if not available.  (The string is used only to
 * generate cursor positions in error messages: we need it to convert
 * byte-wise locations in parse structures to character-wise cursor
 * positions.)
 *
 * p_rtable: list of RTEs that will become the rangetable of the query.
 * Note that neither relname nor refname of these entries are necessarily
 * unique; searching the rtable by name is a bad idea.
 *
 * p_joinexprs: list of JoinExpr nodes associated with p_rtable entries.
 * This is one-for-one with p_rtable, but contains NULLs for non-join
 * RTEs, and may be shorter than p_rtable if the last RTE(s) aren't joins.
 *
 * p_joinlist: list of join items (RangeTblRef and JoinExpr nodes) that
 * will become the fromlist of the query's top-level FromExpr node.
 *
 * p_namespace: list of ParseNamespaceItems that represents the current
 * namespace for table and column lookup.  (The RTEs listed here may be just
 * a subset of the whole rtable.  See ParseNamespaceItem comments below.)
 *
 * p_lateral_active: true if we are currently parsing a LATERAL subexpression
 * of this parse level.  This makes p_lateral_only namespace items visible,
 * whereas they are not visible when p_lateral_active is FALSE.
 *
 * p_ctenamespace: list of CommonTableExprs (WITH items) that are visible
 * at the moment.  This is entirely different from p_namespace because a CTE
 * is not an RTE, rather "visibility" means you could make an RTE from it.
 *
 * p_future_ctes: list of CommonTableExprs (WITH items) that are not yet
 * visible due to scope rules.  This is used to help improve error messages.
 *
 * p_parent_cte: CommonTableExpr that immediately contains the current query,
 * if any.
 *
 * p_target_relation_oid: Oid of the target relation, if query is INSERT,
 * UPDATE, or DELETE.
 *
 * p_target_nsitem: target relation's ParseNamespaceItem.
 *
 * p_is_insert: true to process assignment expressions like INSERT, false
 * to process them like UPDATE.  (Note this can change intra-statement, for
 * cases like INSERT ON CONFLICT UPDATE.)
 *
 * p_windowdefs: list of WindowDefs representing WINDOW and OVER clauses.
 * We collect these while transforming expressions and then transform them
 * afterwards (so that any resjunk tlist items needed for the sort/group
 * clauses end up at the end of the query tlist).  A WindowDef's location in
 * this list, counting from 1, is the winref number to use to reference it.
 *
 * p_expr_kind: kind of expression we're currently parsing, as per enum above;
 * EXPR_KIND_NONE when not in an expression.
 *
 * p_next_resno: next TargetEntry.resno to assign, starting from 1.
 *
 * p_multiassign_exprs: partially-processed MultiAssignRef source expressions.
 *
 * p_locking_clause: query's FOR UPDATE/FOR SHARE clause, if any.
 *
 * p_locked_from_parent: true if parent query level applies FOR UPDATE/SHARE
 * to this subquery as a whole.
 *
 * p_resolve_unknowns: resolve unknown-type SELECT output columns as type TEXT
 * (this is true by default).
 *
 * p_hasAggs, p_hasWindowFuncs, etc: true if we've found any of the indicated
 * constructs in the query.
 *
 * p_last_srf: the set-returning FuncExpr or OpExpr most recently found in
 * the query, or NULL if none.
 *
 * p_pre_columnref_hook, etc: optional parser hook functions for modifying the
 * interpretation of ColumnRefs and ParamRefs.
 *
 * p_ref_hook_state: passthrough state for the parser hook functions.
 */
struct ParseState {
  ParseState* parentParseState;  /* stack link */
  const char* p_sourcetext;      /* source text, or NULL if not available */
  List* p_rtable;                /* range table so far */
  List* p_joinexprs;             /* JoinExprs for RTE_JOIN p_rtable entries */
  List* p_joinlist;              /* join items so far (will become FromExpr
                                  * node's fromlist) */
  List* p_namespace;             /* currently-referenceable RTEs (List of
                                  * ParseNamespaceItem) */
  bool p_lateral_active;         /* p_lateral_only items visible? */
  List* p_ctenamespace;          /* current namespace for common table exprs */
  List* p_future_ctes;           /* common table exprs not yet in namespace */
  CommonTableExpr* p_parent_cte; /* this query's containing CTE */
  /* SPANGRES BEGIN */
  // Add new field to replace p_target_relation.
  Oid p_target_relation_oid;  // INSERT/UPDATE/DELETE target rel oid
  /* SPANGRES END */
  ParseNamespaceItem* p_target_nsitem; /* target rel's NSItem, or NULL */
  bool p_is_insert;          /* process assignment like INSERT not UPDATE */
  List* p_windowdefs;        /* raw representations of window clauses */
  ParseExprKind p_expr_kind; /* what kind of expression we're parsing */
  int p_next_resno;          /* next targetlist resno to assign */
  List* p_multiassign_exprs; /* junk tlist entries for multiassign */
  List* p_locking_clause;    /* raw FOR UPDATE/FOR SHARE info */
  bool p_locked_from_parent; /* parent has marked this subquery
                              * with FOR UPDATE/FOR SHARE */
  bool p_resolve_unknowns;   /* resolve unknown-type SELECT outputs as
                              * type text */

  QueryEnvironment* p_queryEnv; /* curr env, incl refs to enclosing env */

  /* Flags telling about things found in the query: */
  bool p_hasAggs;
  bool p_hasWindowFuncs;
  bool p_hasTargetSRFs;
  bool p_hasSubLinks;
  bool p_hasModifyingCTE;

  Node* p_last_srf; /* most recent set-returning func/op found */

  /*
   * Optional hook functions for parser callbacks.  These are null unless
   * set up by the caller of make_parsestate.
   */
  PreParseColumnRefHook p_pre_columnref_hook;
  PostParseColumnRefHook p_post_columnref_hook;
  ParseParamRefHook p_paramref_hook;
  CoerceParamHook p_coerce_param_hook;
  void* p_ref_hook_state; /* common passthrough link for above */
};

// Redefines the same struct as in PostgreSQL's util/adt/ruleutils.c
/* Context info needed for invoking a recursive querytree display routine */
typedef struct {
  StringInfo buf;                 /* output buffer to append to */
  List* namespaces;               /* List of deparse_namespace nodes */
  List* windowClause;             /* Current query level's WINDOW clause */
  List* windowTList;              /* targetlist for resolving WINDOW clause */
  int prettyFlags;                /* enabling of pretty-print functions */
  int wrapColumn;                 /* max line length, or -1 for no limit */
  int indentLevel;                /* current indent level for pretty-print */
  bool varprefix;                 /* true to print prefixes on Vars */
  ParseExprKind special_exprkind; /* set only for exprkinds needing special
                                   * handling */
  Bitmapset* appendparents;       /* if not null, map child Vars of these relids
                                   * back to the parent rel */
} deparse_context;

/*
 * Support for fuzzily matching columns.
 *
 * This is for building diagnostic messages, where non-exact matching
 * attributes are suggested to the user.  The struct's fields may be facets of
 * a particular RTE, or of an entire range table, depending on context.
 */
typedef struct
{
	int			distance;		/* Weighted distance (lowest so far) */
	RangeTblEntry *rfirst;		/* RTE of first */
	AttrNumber	first;			/* Closest attribute so far */
	RangeTblEntry *rsecond;		/* RTE of second */
	AttrNumber	second;			/* Second closest attribute so far */
} FuzzyAttrMatchState;

/*
 * SPANGRES: Hardcodes the set of supported timezone abbreviations rather than
 * loading them from a file
 *
 * Note that this table must be strictly alphabetically ordered to allow an
 * O(ln(N)) search algorithm to be used.
 *
 * The token field must be NUL-terminated; we truncate entries to TOKMAXLEN
 * characters to fit.
 *
 */
static const datetkn SpangresTimezoneAbbrevTable[] = {
    {"z", TZ, 0}
};

static const int SpangresTimezoneAbbrevTableSize =
    sizeof SpangresTimezoneAbbrevTable / sizeof SpangresTimezoneAbbrevTable[0];

/*---------------------------------------------------------------------------*/
/* Shimmed functions from catalog/namespace.c*/

// Instead of checking namespaces and search path, just returns whether
// bootstrap_catalog knows about the type.
bool TypeIsVisible(Oid typid);

// Given a possibly-qualified function name and argument count, retrieve a list
// of the possible matches.
// Currently identical to FuncnameGetCandidates except for two blocks of
// code marked SPANGRES BEGIN/END which are commented out due to dependencies
// that only exist in namespace.c
FuncCandidateList FuncnameGetCandidates(List* names, int nargs, List* argnames,
                                        bool expand_variadic,
                                        bool expand_defaults,
                                        bool include_out_arguments,
                                        bool missing_ok);

// Given an Operator name, returns a list of candidate functions to be selected
// from based on desired operand types.
FuncCandidateList OpernameGetCandidates(List* names, char oprkind,
                                        bool missing_schema_ok);

// Given an operator name and input datatypes, lookup the operator (or return
// InvalidOid if not found). Spangres version of this function uses
// bootstrap_catalog to look up the operator and ignores namespacing.
Oid OpernameGetOprid(List* names, Oid oprleft, Oid oprright);

// Attempts to resolve an unqualified relation name. PostgreSQL uses the active
// search path for this. We can supply some of that information by checking
// CatalogAdapter to see if it knows this table. This is often used in error
// reporting.
Oid RelnameGetRelid(const char* relname);

// Gets the Oid associated with a namespace.
// If not found, returns InvalidOid if `missing_ok` or throws an unsupported
// exception otherwise.
// REQUIRES: CatalogAdapter initialized on this thread.
Oid get_namespace_oid(const char* nspname, bool missing_ok);

// Gets the Oid associated with a collation.
// In the case that no schema name is provided, the spangres
// version will assume that the schema is pg_catalog.
Oid get_collation_oid(List* name, bool missing_ok);

// If there's a collation with the given name/namespace, and it works with the
// encoding, return its OID. The spangres version enforces that the collation
// namespace is pg_catalog and throws an error if it is not. If the collation
// namespace is pg_catalog but the collation itself is not found spangres will
// return InvalidOid.
Oid lookup_collation(const char* collname, Oid collnamespace, int32_t encoding);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from catalog/pg_constraint.c*/

// get_primary_key_attnos
// Identifies the columns in a relation's primary key, if any.
// SPANGRES: We don't support this feature and always return as if there are no
// primary keys. This means certain queries that depend on primary keys for
// validity will be rejected such as:
//
// SELECT value FROM keyvalue GROUP BY key;
//
// Here "key" being a primary key would functionally determine the whole row and
// save us from having to group on "value" as well. Someday we could add primary
// key lookup to support this.
Bitmapset* get_primary_key_attnos(Oid relid, bool deferrableOk,
                                  Oid* constraintOid);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from commands/dbcommands.c*/

// Gets the Oid associated with a database.
// Spangres does not support Oids for databases, so this returns InvalidOid if
// missing_ok or throws an unsupported exception otherwise.
// NOTE: though PostgreSQL treats "databases" as special in this context,
// Spangres does not--they are simply an additional name qualifier.
Oid get_database_oid(const char* dbname, bool missing_ok);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from commands/indexcmds.c*/

// Given the OIDs of a datatype and an access method, find the default operator
// class, if any. Returns InvalidOid if there is none.
// Spangres version uses bootstrap catalog instead of a heap scan.
Oid GetDefaultOpClass(Oid type_id, Oid am_id);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/analyze.c*/

// Handle SET clause in UPDATE/INSERT ... ON CONFLICT UPDATE.
// The shimmed version uses the CatalogAdapter to get the table name from an oid
// and the Engine User Catalog to get number of columns in the table and the
// column attr number for each column name.
List* transformUpdateTargetList(ParseState* pstate, List* origTlist);

// Transform an OnConflictClause in an INSERT.
// The shimmed version uses the p_target_relation_oid to create a RangeTblEntry
// instead of the p_target_relation. It temporarily removes support for
// exclRelTlist, which is used by EXPLAIN to describe the targetlist of the
// excluded relation.
OnConflictExpr* transformOnConflictClause(ParseState* pstate,
                                          OnConflictClause* onConflictClause);

Query *transformCallStmt(ParseState *pstate, CallStmt *stmt);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_clause.c*/

// Add the target relation of INSERT/UPDATE/DELETE to the range table,
// and make the special links to it in the ParseState.
// The original setTargetTable function linked the target relation to the
// ParseState p_target_relation field. This modified version links the
// target relation oid to the ParseState p_target_relation_oid field.
//
// If alsoSource is true, add the target to the query's joinlist and
// namespace.  For INSERT, we don't want the target to be joined to;
// it's a destination of tuples, not a source.   For UPDATE/DELETE,
// we do need to scan or join the target.  (NOTE: we do not bother
// to check for namespace conflict; we assume that the namespace was
// initially empty in these cases.)
//
// Finally, we mark the relation as requiring the permissions specified
// by requiredPerms.
//
// Returns the rangetable index of the target relation.
int setTargetTable(ParseState* pstate, RangeVar* relation, bool inh,
                   bool alsoSource, AclMode requiredPerms);

// Transform arbiter expressions in an ON CONFLICT clause.
//
// Transformed expressions used to infer one unique index relation to serve as
// an ON CONFLICT arbiter.  Partial unique indexes may be inferred using WHERE
// clause from inference specification clause.
//
// Same as transformOnConflictArbiter except checks for system catalog insertion
// are removed. Future version will also use p_target_relation_oid instead of
// p_target_relation.
void transformOnConflictArbiter(ParseState* pstate,
                                OnConflictClause* onConflictClause,
                                List** arbiterExpr, Node** arbiterWhere,
                                Oid* constraint);

// Infer a unique index from a list of indexElems, for ON
// CONFLICT clause
//
// Perform parse analysis of expressions and columns appearing within ON
// CONFLICT clause.  During planning, the returned list of expressions is used
// to infer which unique index to use.
//
// Exact same implementation as resolve_unique_index_expr, but the signature
// is missing the last parameter (Relation heapRel) because it is not used in
// the implementation.
List* resolve_unique_index_expr(ParseState* pstate, InferClause* infer);

// Process a window frame offset expression
//
// In RANGE mode, rangeopfamily is the sort opfamily for the input ORDER BY
// column, and rangeopcintype is the input data type the sort operator is
// registered with.  We expect the in_range function to be registered with
// that same type.  (In binary-compatible cases, it might be different from
// the input column's actual type, so we can't use that for the lookups.)
// We'll return the OID of the in_range function to *inRangeFunc.
Node* transformFrameOffset(ParseState* pstate, int frameOptions,
                           Oid rangeopfamily, Oid rangeopcintype,
                           Oid* inRangeFunc, Node* clause);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_coerce.c */

// Finds the cast function to coerce source to target types. Unlike the
// PostgreSQL version, this does not support complex types, records etc. It
// returns no pathway for those cases.
CoercionPathType find_coercion_pathway(Oid targetTypeId, Oid sourceTypeId,
                                       CoercionContext ccontext, Oid* funcid);

// Leans heavily on find_coercion_pathway to determine whether coercion is
// possible. Unlike the PostgreSQL version, this does not support generics or
// other complex types. It returns false for those cases.
bool can_coerce_type(int nargs, const Oid* input_typeids,
                     const Oid* target_typeids, CoercionContext ccontext);

// Constructs an expression tree for applying a pg_cast entry to the input tree.
// Spangres version of this function looks up the coercion function in the
// bootstrap_catalog instead of the Heap to validate its properties. The
// remaining behavior is left unchanged.
Node* build_coercion_expression(Node* node, CoercionPathType pathtype,
                                Oid funcId, Oid targetTypeId,
                                int32_t targetTypMod, CoercionContext ccontext,
                                CoercionForm cformat, int location);

// Check if `srctype` is binary-coercible to `targettype`.
// Spangres version uses various bootstrap catalogs to replace syscache lookups.
bool IsBinaryCoercible(Oid srctype, Oid targettype);

// Determine if the given type needs length coercion.
CoercionPathType find_typmod_coercion_function(Oid typeId, Oid* funcid);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_expr.c */

Node* transformParamRef(ParseState *pstate, ParamRef *pref);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_func.c */

// Lookup a function by name + args and return a variety of information about
// it. If an exact match isn't found, fallback checks look at coercion and
// ambiguous function resolution rules. This function is nearly identical to the
// PostgreSQL version except it uses bootstrap catalog and does not support
// default arguments (no bootstrap catalog support for them). In addition, this
// function depends on FuncnameGetCandidates, which does not include
// support for named arguments.
FuncDetailCode func_get_detail(List* funcname, List* fargs, List* fargnames,
                               int nargs, Oid* argtypes, bool expand_variadic,
                               bool expand_defaults, bool include_out_arguments,
                               Oid* funcid,          // return value
                               Oid* rettype,         // return value
                               bool* retset,         // return value
                               int* nvargs,          // return value
                               Oid* vatype,          // return value
                               Oid** true_typeids,   // return value
                               List** argdefaults);  // optional return value

// Parse (and disambiguate) a node that could represent either a column
// reference or a function call. This is an exact copy of the PostgreSQL code
// with two exceptions. First, when the function is an aggregate we use
// bootstrap_catalog to get the FormData struct instead of the syscache.
// Second, coercions are applied for specific functions so that string literals
// passed to "any" typed parameters are coerced from unknownoid to textoid.
Node* ParseFuncOrColumn(ParseState* pstate, List* funcname, List* fargs,
                        Node* last_srf, FuncCall* fn, bool proc_call,
                        int location);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_node.c */

// Functions to eliminate global state from parser error reporting. Shim
// versions do nothing with parser state and just return. We sacrifice detailed
// parser error reporting to maintain less error state.
void setup_parser_errposition_callback(ParseCallbackState* pcbstate,
                                       ParseState* pstate, int location);

void cancel_parser_errposition_callback(ParseCallbackState* pcbstate);

// Release a ParseState and any subsidiary resources.
// Same as free_parsestate except without freeing p_target_relation since the
// field is removed from ParseState.
void free_parsestate(ParseState* pstate);

// Convert a Value node (as returned by the grammar) to a Const node
// and assign a type for the constant. The original PostgreSQL function chooses
// int4, int8_t, or numeric for integers and floats but the Spangres function will
// only choose int8_t or numeric..
Const* make_const(ParseState* pstate, Value* value, int location);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_oper.c */

// Search for a binary or unary operator given a name and argument types.
// Replaces heap and cache lookups with bootstrap catalog.
Operator oper(ParseState* pstate, List* opname, Oid ltypeId, Oid rtypeId,
              bool noError, int location);
Operator left_oper(ParseState* pstate, List* op, Oid arg, bool noError,
                   int location);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_relation.c */

// Search the column names of a single RTE for the given name.
// If found, return the attnum (possibly negative, for a system column);
// else return InvalidAttrNumber.
// If the name proves ambiguous within this RTE, raise error.
// pstate and location are passed only for error-reporting purposes.

// Side effect: if fuzzystate is non-NULL, check non-system columns
// for an approximate match and update fuzzystate accordingly.

// Note: this is factored out of scanNSItemForColumn because error message
// creation may want to check RTEs that are not in the namespace.  To support
// that usage, minimize the number of validity checks performed here.  It's
// okay to complain about ambiguous-name cases, though, since if we are
// working to complain about an invalid name, we've already eliminated that.
int scanRTEForColumn(ParseState* pstate, RangeTblEntry* rte, Alias* eref,
                     const char* colname, int location, int fuzzy_rte_penalty,
                     FuzzyAttrMatchState* fuzzystate);

// This function is shimmed out to const false since dropped columns do not
// exist in googlesql catalogs.
bool get_rte_attribute_is_dropped(RangeTblEntry* rte, AttrNumber attnum);

// Given a relation specified by Oid (relid), looks it up in the user catalog
// and populates the outargs with attribute (column) information.
// The Spangres version of this function captures the behavior of PostgreSQL's
// expandRelation AND expandTupleDesc together to avoid supporting a proper
// TupleDesc for engine-defined tables. We can also safely skip dropped-column
// checks, thus `include_dropped` is ignored.
//
// In PostgreSQL, this is used when column names are not specified explicitly
// such as SELECT *...FROM or in building JOIN RTEs.
//
// SPANGRES: exclude pseudo columns like the Spanner _exists pseudo column
void expandRelation(Oid relid, Alias* eref, int rtindex, int sublevels_up,
                    int location, bool include_dropped, List** colnames,
                    List** colvars);

// Expand a ParseNamespaceItem. Signature differs from PG by including the
// ParseState.
List* expandNSItemVars(ParseState* pstate, ParseNamespaceItem* nsitem,
                       int sublevels_up, int location, List** colnames);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_target.c */

// This is used in INSERT and UPDATE statements only.  It prepares an
// expression for assignment to a column of the target table.
// This includes coercing the given value to the target column's type
// (if necessary), and dealing with any subfield names or subscripts
// attached to the target column itself.  The input expression has
// already been through transformExpr().
//
// pstate        parse state
// expr          expression to be modified
// exprKind      indicates which type of statement we're dealing with
// colname       target column name (ie, name of attribute to be assigned to)
// attrno        target attribute number
// indirection   subscripts/field names for target column, if any
// location      error cursor position for the target column, or -1
//
// Returns the modified expression.
//
// Note: location points at the target column name (SET target or INSERT
// column name list entry), and must therefore be -1 in an INSERT that
// omits the column name list.  So we should usually prefer to use
// exprLocation(expr) for errors that can happen in a default INSERT.
Expr* transformAssignedExpr(ParseState* pstate, Expr* expr,
                            ParseExprKind exprKind, char* colname, int attrno,
                            List* indirection, int location);

// Generate a list of INSERT column targets if not supplied, or
// test supplied column names to make sure they are in target table.
// Also return an integer list of the columns' attribute numbers.
// The shimmed version uses the p_target_relation_oid and the Engine User
// Catalog to populate the ResTarget columns and to get the column ids.
List* checkInsertTargets(ParseState* pstate, List* cols, List** attrnos);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from parser/parse_type.c */

// Both of these create a Type (HeapTuple) out of our bootstrap_catalog instead
// of the real heap.
Type typeidType(Oid id);

Type LookupTypeNameExtended(ParseState* pstate, const TypeName* typeName,
                            int32_t* typmod_p, bool temp_ok, bool missing_ok);

// Given a typeid, returns the type's typrelid (associated relation, if any).
// Spangres version uses bootstrap instead of syscache.
Oid typeidTypeRelid(Oid type_id);

// Given a typeid, return the type's typrelid (associated relation), if any.
// Returns InvalidOid if type is not a composite type or a domain over one.
// This is the same as typeidTypeRelid(getBaseType(type_id)), but
// faster.
Oid typeOrDomainTypeRelid(Oid type_id);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from rewrite/rewriteHandler.c */

// This is a no-op function. Its original version acquire PostgreSQL locks
// for future operations, but we don't need such locks for Spangres.
void AcquireRewriteLocks(Query* parsetree, bool forExecute,
                         bool forUpdatePushedDown);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/acl.c */

// Given a role name, look up the role's OID. If missing_ok is false,
// throw an error if role name not found.  If true, just return InvalidOid.
Oid get_role_oid(const char *rolname, bool missing_ok);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/adt/date.c */

// Given date text string, convert to internal date format.
Datum date_in(PG_FUNCTION_ARGS);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/adt/datetime.c */
int
DecodeDateTime(char **field, int *ftype, int nf,
               int *dtype, struct pg_tm *tm, fsec_t *fsec, int *tzp);
int
DecodeTimezoneAbbrev(int field, char *lowtoken,
                     int *offset, pg_tz **tz);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/adt/format_type.c */

// format_type_extended
//    Generate a possibly-qualified type name.
//
// The default behavior is to only qualify if the type is not in the search
// path, to ignore the given typmod, and to raise an error if a non-existent
// type_oid is given.
//
// The following bits in 'flags' modify the behavior:
// - FORMAT_TYPE_TYPEMOD_GIVEN
//      include the typmod in the output (typmod could still be -1 though)
// - FORMAT_TYPE_ALLOW_INVALID
//      if the type OID is invalid or unknown, return ??? or such instead
//      of failing
// - FORMAT_TYPE_FORCE_QUALIFY
//      always schema-qualify type names, regardless of search_path
//
// Note that TYPEMOD_GIVEN is not interchangeable with "typemod == -1";
// see the comments above for format_type().
//
// Returns a palloc'd string.
char* format_type_extended(Oid type_oid, int32_t typemod, bits16 flags);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/adt/ruleutils.c */

// Deparse an OpExpr node.
void get_oper_expr(OpExpr* expr, deparse_context* context);

// Gets a table name given its oid (relid). `namespaces` is not used as Spanner
// doesn't support namespaces. The result includes all necessary quoting.
char* generate_relation_name(Oid relid, List* namespaces);

// Compute the name to display for an operator specified by OID,
// given that it is being called with the specified actual arg types.
// (Arg types matter because of ambiguous-operator resolution rules.
// Pass InvalidOid for unused arg of a unary operator.)
// The result includes all necessary quoting and schema-prefixing,
// plus the OPERATOR() decoration needed to use a qualified operator name
// in an expression.
// This is an exact copy of the PostgreSQL code except that we use
// bootstrap_catalog to get the FormData struct instead of the syscache.
char* generate_operator_name(Oid operid, Oid arg1, Oid arg2);

// Compute the name to display for a function specified by OID,
// given that it is being called with the specified actual arg names and
// types.  (Those matter because of ambiguous-function resolution rules.)
//
// If we're dealing with a potentially variadic function (in practice, this
// means a FuncExpr or Aggref, not some other way of calling a function), then
// has_variadic must specify whether variadic arguments have been merged,
// and *use_variadic_p will be set to indicate whether to print VARIADIC in
// the output.  For non-FuncExpr cases, has_variadic should be FALSE and
// use_variadic_p can be NULL.
//
// The result includes all necessary quoting and schema-prefixing.
// This is an exact copy of the PostgreSQL code except that we use
// bootstrap_catalog to get the FormData struct instead of the syscache.
char* generate_function_name(Oid funcid, int nargs, List* argnames,
                             Oid* argtypes, bool has_variadic,
                             bool* use_variadic_p,
                             ParseExprKind special_exprkind);

// Sets column names of the table specififed in the RangeTblEntry object.
// Functionally the same as the PostgreSQL version, but uses a ZetaSQL catalog
// to look up table and columns information.
//
// Column alias info is saved in *colinfo, which is assumed to be pre-zeroed. If
// any colnames entries are already filled in, those override local choices.
void set_relation_column_names(deparse_namespace* dpns, RangeTblEntry* rte,
                               deparse_columns* colinfo);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/adt/timestamp.c */

// Convert a string to the internal timestamp form.
Datum timestamptz_in(PG_FUNCTION_ARGS);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/adt/ruleutils.c that are implemented in
 * catalog_shim_deparser.c
 */

// Deparsing the utility (DDL) statements.
// Original PostgreSQL supports deparsing only for NOTIFY statements out of all
// utility statement types. This function expands this support with all the
// utility statements supported by Spangres (CREATE/ALTER TABLE, CREATE/ALTER
// DATABASE, etc.). The support is provided only for the subset of features that
// is supported by Spangres, meaning that deparsing for arbitrary PostgreSQL
// parse trees is not guaranteed.
void get_utility_query_def(Query* query, deparse_context* context);

// Parse back a DELETE parsetree. The shimmed version excludes the target
// relation from the USING clause.
void get_delete_query_def(Query* query, deparse_context* context,
                          bool colNamesVisible);

// Parse back an UPDATE parsetree. The shimmed version excludes the target
// relation from the FROM clause.
void get_update_query_def(Query* query, deparse_context* context,
                          bool colNamesVisible);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/cache/catcache.c */

// Instead of releasing the tuple back to the cache, just
// does nothing since all heap tuples in postgres_translator are
// hand-constructed by us and not cache-backed. These tuples are allocated from
// the CurrentMemoryContext and memory is freed when the context is cleaned up.
void ReleaseCatCache(HeapTuple tuple);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/cache/lsyscache.c */

// Shim version of this function skips the domain lookup. Just returns the
// passed typid and ignores typmod.
Oid getBaseTypeAndTypmod(Oid typid, int32_t* typmod);

// Gets the typcollation field for a type passed by Oid. Uses bootstrap_catalog
// instead of catalog cache. Returns InvalidOid if the type is not found.
Oid get_typcollation(Oid typid);

// Gets the attribute name for a relation specified by Oid and attnum. Spangres
// version of this function uses the catalog adapter to look up the table name
// and then the googlesql catalog for table information.
char* get_attname(Oid relid, AttrNumber attnum, bool missing_ok);

// Gets typcategory and typispreferred for a type looked up in the bootstrap
// catalog. Functionally the same as the PostgreSQL version except for using
// bootstrap.
void get_type_category_preferred(Oid typid, char* typcategory,
                                 bool* typispreferred);

// Given procedure id, return the function's provariadic field.
Oid get_func_variadictype(Oid funcid);

// Given a function passed by Oid, get its proretset flag field.
// Spangres version uses bootstrap_catalog.
bool get_func_retset(Oid funcid);

// Gets a relation name based on its oid (relid). Functionally the same as the
// PostgreSQL version, but only supports tables for which the analyzer has built
// an RTE. These tables will have their oids and names stored in the catalog
// adapter.
// Note: it returns a palloc'd copy of the string, or NULL if no table is found
// from the input relid.
char* get_rel_name(Oid relid);

// Given a typeid, returns the type's typtype (basic type, complex type, etc.).
// Returns the null byte if the type is not found. Spangres version uses
// bootstrap catalog instead of syscache.
char get_typtype(Oid typid);

// Gets typOutput and typIsVarlena for a type looked up in the bootstrap
// catalog. Functionally the same as the PostgreSQL version except for using
// bootstrap.
void getTypeOutputInfo(Oid typid, Oid* typOutput, bool* typIsVarlena);

// Given the type OID, get the typelem (InvalidOid if not an array type).
//
// NB: this only considers varlena arrays to be true arrays; InvalidOid is
// returned if the input is a fixed-length array type.
Oid get_element_type(Oid typid);

// Given a type OID, fetches its typInput and typIOParam.
// Throws error on failure. Functionally the same as the
// PostgreSQL version except for using bootstrap catalog.
void getTypeInputInfo(Oid type, Oid* typInput, Oid* typIOParam);

// Given an operator's OID, find out which btree opfamilies it belongs to,
// and what properties it has within each one.  The results are returned
// as a palloc'd list of OpBtreeInterpretation structs.
// In addition to the normal btree operators, we consider a <> operator to be
// a "member" of an opfamily if its negator is an equality operator of the
// opfamily.  ROWCOMPARE_NE is returned as the strategy number for this case.
List* get_op_btree_interpretation(Oid opno);

// Given an Operator Class, returns the family that class belongs to.
// Spangres version uses bootstrap catalog lookup of opclass.
Oid get_opclass_family(Oid opclass);

// Given an Operator Class, returns the input type of that class.
// Spangres version uses bootstrap catalog lookup of opclass.
Oid get_opclass_input_type(Oid opclass);

// Gets the OID of the operator that implements the specified strategy with the
// specified datatypes for the specified opfamily.
// Spangres version uses bootstrap catalog lookup of amop (Access Method
// Operator).
Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype,
                        int16_t strategy);

// Given the OID of an ordering operator (a btree "<" or ">" operator),
// determine its opfamily, its declared input datatype, and its
// strategy number (BTLessStrategyNumber or BTGreaterStrategyNumber).

// Returns true if successful, false if no matching pg_amop entry exists.
// (This indicates that the operator is not a valid ordering operator.)

// Note: the operator could be registered in multiple families, for example
// if someone were to build a "reverse sort" opfamily.  This would result in
// uncertainty as to whether "ORDER BY USING op" would default to NULLS FIRST
// or NULLS LAST, as well as inefficient planning due to failure to match up
// pathkeys that should be the same.  So we want a determinate result here.
// Because of the way the syscache search works, we'll use the interpretation
// associated with the opfamily with smallest OID, which is probably
// determinate enough.  Since there is no longer any particularly good reason
// to build reverse-sort opfamilies, it doesn't seem worth expending any
// additional effort on ensuring consistency.
bool get_ordering_op_properties(Oid opno, Oid* opfamily, Oid* opcintype,
                                int16_t* strategy);

// Gets the OID of the specified support function for the specified opfamily and
// datatypes.
// Spangres version uses the bootstrap catalog lookup of amproc (Access Method
// Procedure).
Oid get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16_t procnum);

// Given an Operator identified by Oid, returns the regproc id (procedure Oid)
// of the routine used to implement that operator.
// Spangres version uses bootstrap catalog to look up the pg_operator struct.
RegProcedure get_opcode(Oid opno);

// Given an Operator identified by Oid, returns the oprcom (corresponding
// commutator operator).
// Spangres version uses bootstrap catalog to look up the pg_operator struct.
Oid get_commutator(Oid opno);

// Given an Operator identified by Oid, returns the corresponding negator of an
// operator.
// Spangres version uses bootstrap catalog to look up the pg_operator struct.
Oid get_negator(Oid opno);

// Returns the subtype of a given range type. Returns InvalidOid if the type is
// not a range type.
// Spangres doesn't yet support ranges and so always returns InvalidOid.
Oid get_range_subtype(Oid rangeOid);

// Given the type OID, get the typelem, looking "through" any domain to its
// underlying array type. In PostgreSQL, this includes special type resolution
// logic to avoid multiple cache hits. Our bootstrap_catalog is cheaper so we
// use the simpler version that hits the catalog multiple times.
Oid get_base_element_type(Oid typid);

// Gets multiple type fields for a type looked up in the bootstrap catalog.
// Functionally the same as the PostgreSQL version except for skipping bootstrap
// processing mode and using bootstrap catalog to get the type data instead of
// the syscache.
void get_type_io_data(Oid typid, IOFuncSelector which_func, int16_t* typlen,
                      bool* typbyval, char* typalign, char* typdelim,
                      Oid* typioparam, Oid* func);

// get_array_type
//
// Given the type OID, get the corresponding "true" array type.
// Returns InvalidOid if no array type can be found.
//
Oid get_array_type(Oid typid);

// Attempts to resolve a qualified relation name.
Oid get_relname_relid(const char* relname, Oid namespace_oid);

// Given the type OID, return the type's subscripting handler's OID,
// if it has one.
RegProcedure get_typsubscript(Oid typid, Oid *typelemp);

// Returns the range type of a given multirange
Oid get_multirange_range(Oid multirangeOid);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/cache/typcache.c */

// lookup_type_cache
//
// Fetch the type cache entry for the specified datatype, and make sure that
// all the fields requested by bits in 'flags' are valid.
//
// The result is never NULL --- we will ereport() if the passed type OID is
// invalid.  Note however that we may fail to find one or more of the
// values requested by 'flags'; the caller needs to check whether the fields
// are InvalidOid or not.
//
// SPANGRES MODIFICATIONS:  Don't cache; just look up directly in the bootstrap
// catalog.  We could cache these in the future.
TypeCacheEntry* lookup_type_cache(Oid type_id, int flags);

// assign_record_type_typmod
//
// Given a tuple descriptor for a RECORD type, find or create a cache entry
// for the type, and set the tupdesc's tdtypmod field to a value that will
// identify this cache entry to lookup_rowtype_tupdesc.
void assign_record_type_typmod(TupleDesc tupDesc);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/fmgr/fmgr.c */

// Populates FmgrInfo with function call information (based on Oid lookup).
// Spangres version is exactly the same for built-in functions, but doesn't
// support non-built-in functions at all. The PostgreSQL version falls back to a
// lookup in the pg_proc table. Since we don't have a pg_proc table (just
// bootstrap catalog), this doesn't miss anything.
void fmgr_info_cxt_security(Oid functionId, FmgrInfo* finfo, MemoryContext mcxt,
                            bool ignore_security);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from utils/fmgr/funcapi.c */

// internal_get_result_type -- workhorse code implementing various
// get_X_result_type public functions in funcapi.h.
//
// `funcid` must always be supplied. `call_expr` and `rsinfo` are unused because
// they apply only to unsupported cases. If supplied function returns a
// polymorphic or record (composite) type, throws an unsupported error for
// Spangres. This function is otherwise the same as the PostgreSQL version.
TypeFuncClass internal_get_result_type(Oid funcid, Node* call_expr,
                                       ReturnSetInfo* rsinfo, Oid* resultTypeId,
                                       TupleDesc* resultTupleDesc);

// Gets the name of the function's result parameter if any. Returns NULL
// otherwise.
// SPANGRES: This always returns NULL for now.
char* get_func_result_name(Oid functionId);

/*---------------------------------------------------------------------------*/
/* Shimmed functions from access/common/tupdesc.c */

// TupleDescInitEntry
//    This function initializes a single attribute structure in
//    a previously allocated tuple descriptor.
//
// If attributeName is NULL, the attname field is set to an empty string
// (this is for cases where we don't know or need a name for the field).
// Also, some callers use this function to change the datatype-related fields
// in an existing tupdesc; they pass attributeName = NameStr(att->attname)
// to indicate that the attname field shouldn't be modified.
//
// Note that attcollation is set to the default for the specified datatype.
// If a nondefault collation is needed, insert it afterwards using
// TupleDescInitEntryCollation.
//
// SPANGRES: Use bootstrap_catalog to look up pg_type information.
void TupleDescInitEntry(TupleDesc desc, AttrNumber attributeNumber,
                        const char* attributeName, Oid oidtypeid, int32_t typmod,
                        int attdim);

/*---------------------------------------------------------------------------*/
/* Shimmed functions that always return an error */

// Replace this function with an unsupported error throw. We don't support
// PostgreSQL's locking model and don't intend to support it.
// Implementation is in catalog_shim_expected_errors.c
LockAcquireResult LockAcquireExtended(const LOCKTAG* locktag, LOCKMODE lockmode,
                                      bool sessionLock, bool dontWait,
                                      bool reportMemoryError,
                                      LOCALLOCK** locallockp);

// This function replaces the PostgreSQL GUC (Global Unified Configuration)
// setup with an error to guarantee that Spangres does not create this global,
// mutable state management subsystem.
void InitializeGUCOptions(void);

// This funciton replaces an unsupported catalog lookup with a descriptive
// 'unsupported' error.
Oid get_rel_type_id(Oid relid);

/*---------------------------------------------------------------------------*/
/* Helper functions in catalog_shim.c */
// Performs a lookup in googlesql's Catalog and translates a looked-up
// zetasql::Table into an RTE.
ParseNamespaceItem* addRangeTableEntry(struct ParseState* pstate,
                                       RangeVar* relation, Alias* alias,
                                       bool inh, bool inFromCl);

// Like addRangeTableEntry, except looks up the table name in the catalog
// adapter instead of reading it off of the RangeVar*.
ParseNamespaceItem* addRangeTableEntryByOid(struct ParseState* pstate,
                                            Oid relation_oid, Alias* alias,
                                            bool inh, bool inFromCl);

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

// Given the type OID, gets typlen, typbyval, and typalign.
// Spangres version uses bootstrap catalog instead of SysCache.
void get_typlenbyvalalign(Oid typid, int16_t* typlen, bool* typbyval,
                          char* typalign);

// Gets a namespace Oid from the name by looking up in catalog adapter.
// Spangres version skips ACL checks and does not support pg_temp.
Oid LookupExplicitNamespace(const char* nspname, bool missing_ok);

// Given a set operation field from a query tree deparases it into the
// original statement. The spangres version skips the assert which checks
// if a subquery within a set operation has a set operation as this causes
// the deparser to fail on inputs such as
// 'SELECT 1 UNION ALL (SELECT 1 UNION SELECT 1 LIMIT 1)'.
// NB: The check was added along with code which adds paranthesses to the
// depasred query statement. At the current time, it is not completely clear
// why it is necessary for a subquery to not have a set operation in order
// for the parantheses to be correct.

void get_setop_query(Node* setOp, Query* query, deparse_context* context,
                     TupleDesc resultDesc, bool colNamesVisible);

#ifdef __cplusplus
}
#endif

#endif  // SHIMS_CATALOG_SHIM_H_
