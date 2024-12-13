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
