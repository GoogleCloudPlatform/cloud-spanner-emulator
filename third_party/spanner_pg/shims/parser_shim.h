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

#ifndef SHIMS_PARSER_SHIM_H_
#define SHIMS_PARSER_SHIM_H_

#include "third_party/spanner_pg/postgres_includes/all.h"

// This file is included by both Google C++ code and PostgreSQL C code.
#ifdef __cplusplus
#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"

// Simple scratch space for the PG Flex scanner as modified by Spangres to log
// start and end locations of tokens
struct SpangresTokenLocations {
  absl::optional<int> last_start_location;
  absl::flat_hash_map<int, int> start_end_pairs;
};

extern "C" {
#else
struct SpangresTokenLocations;
#endif

// Redefines the same struct as in PostgreSQL's include/nodes/parsenodes.h
typedef struct ViewStmt {
  NodeTag type;
  RangeVar* view;                  /* the view to be created */
  List* aliases;                   /* target column names */
  Node* query;                     /* the SELECT query (as a raw parse tree) */
  bool replace;                    /* replace an existing view? */
  List* options;                   /* options from WITH clause */
  ViewCheckOption withCheckOption; /* WITH ABSL_CHECK OPTION */
  /* BEGIN SPANGRES ADDITIONAL FIELDS */
  bool is_definer;    /* is security type DEFINER or INVOKER */
  char* query_string;  // sql string of the query field, it is set in gram.y by
                       // using token location information and extracted from
                       // the whole <CREATE VIEW> string.
  /* END SPANGRES ADDITIONAL FIELDS */
} ViewStmt;

typedef struct CreateChangeStreamStmt {
  NodeTag type;
  RangeVar *change_stream_name; /* Name of the change stream */
  List *opt_options;            /* Optional list of DefElem nodes */
  List *opt_for_tables;         /* Optional list of ChangeStreamTrackedTable */
  bool for_all; /* Whether change stream tracks all tables in database */
} CreateChangeStreamStmt;

typedef struct AlterChangeStreamStmt {
  NodeTag type;
  RangeVar *change_stream_name; /* Name of the change stream */

  /* parameters used for ALTER CHANGE STREAM ... WITH */
  List *opt_options; /* List of DefElem nodes */

  /* parameters used for ALTER CHANGE STREAM ... ADD/DROP TABLE */
  List *opt_for_tables;      /* List of ChangeStreamTrackedTable to set */
  List *opt_drop_for_tables; /* List of ChangeStreamTrackedTable to drop */
  bool for_all;
  bool drop_for_all;

  /* parameters used for ALTER CHANGE STREAM ... RESET */
  List *opt_reset_options; /* Special change stream to drop all tables in
                              db */
} AlterChangeStreamStmt;

typedef struct ChangeStreamTrackedTable {
  NodeTag type;
  RangeVar *table_name; /* Name of the tracked table */
  List *columns;        /* Name of the tracked columns */
  bool for_all_columns; /* Whether all columns in table are tracked */
} ChangeStreamTrackedTable;

// Redefines the same struct as in PostgreSQL's include/nodes/parsenodes.h
typedef struct Constraint {
  NodeTag type;
  ConstrType contype; /* see above */

  /* Fields used for most/all constraint types: */
  char *conname;     /* Constraint name, or NULL if unnamed */
  bool deferrable;   /* DEFERRABLE? */
  bool initdeferred; /* INITIALLY DEFERRED? */
  int location;      /* token location, or -1 if unknown */

  /* Fields used for constraints with expressions (ABSL_CHECK and DEFAULT): */
  bool is_no_inherit;  /* is constraint non-inheritable? */
  Node *raw_expr;      /* expr, as untransformed parse tree */
  char *cooked_expr;   /* expr, as nodeToString representation */
  char generated_when; /* ALWAYS or BY DEFAULT */

  /* Fields used for unique constraints (UNIQUE and PRIMARY KEY): */
  List *keys;      /* String nodes naming referenced key
                    * column(s) */
  List *including; /* String nodes naming referenced nonkey
                    * column(s) */

  /* Fields used for EXCLUSION constraints: */
  List *exclusions; /* list of (IndexElem, operator name) pairs */

  /* Fields used for index constraints (UNIQUE, PRIMARY KEY, EXCLUSION): */
  List *options;             /* options from WITH clause */
  char *indexname;           /* existing index to use; otherwise NULL */
  char *indexspace;          /* index tablespace; NULL for default */
  bool reset_default_tblspc; /* reset default_tablespace prior to
                              * creating the index */
  /* These could be, but currently are not, used for UNIQUE/PKEY: */
  char *access_method; /* index access method; NULL for default */
  Node *where_clause;  /* partial index predicate */

  /* Fields used for FOREIGN KEY constraints: */
  RangeVar *pktable;   /* Primary key table */
  List *fk_attrs;      /* Attributes of foreign key */
  List *pk_attrs;      /* Corresponding attrs in PK table */
  char fk_matchtype;   /* FULL, PARTIAL, SIMPLE */
  char fk_upd_action;  /* ON UPDATE action */
  char fk_del_action;  /* ON DELETE action */
  List *old_conpfeqop; /* pg_constraint.conpfeqop of my former self */
  Oid old_pktable_oid; /* pg_constraint.confrelid of my former
                        * self */

  /* Fields used for constraints that allow a NOT VALID specification */
  bool skip_validation; /* skip validation of existing rows? */
  bool initially_valid; /* mark the new constraint as valid? */

  /* BEGIN SPANGRES ADDITIONAL FIELDS */
  char *constraint_expr_string;  // `raw_expr` in its SQL text representation
                                 // used for the constraints with expression
                                 // (ABSL_CHECK, GENERATED and DEFAULT)

  /* END SPANGRES ADDITIONAL FIELDS */
} Constraint;

// Redefines the same struct as in PostgreSQL's include/nodes/parsenodes.h
typedef struct AlterTableCmd /* one subcommand of an ALTER TABLE */
{
  NodeTag type;
  AlterTableType subtype; /* Type of table alteration to apply */
  char *name; /* column, constraint, or trigger to act on, or tablespace */
  int16_t num;  /* attribute number for columns referenced by number */
  RoleSpec *newowner;
  Node *def; /* definition of new column, index, constraint, or parent table */
  DropBehavior behavior; /* RESTRICT or CASCADE for DROP cases */
  bool missing_ok;       /* skip error if missing? */
	bool		recurse;		/* exec-time recursion */

  /* BEGIN SPANGRES ADDITIONAL FIELDS */
  char *raw_expr_string;  // `raw_expr` in its SQL text representation
                          // used for the constraints with expression
                          // when altering a column of a table via SET DEFAULT.
  /* END SPANGRES ADDITIONAL FIELDS */
} AlterTableCmd;

// Like raw_parser from PostgreSQL, but also fills in token locations.  Defined
// in src/backend/parser/parser.c.
struct List* raw_parser_spangres(const char* str,
                                 RawParseMode mode,
                                 struct SpangresTokenLocations* locations);

// If locations is non-null, add location as both the (exclusive) end of the
// previous token (if any) and the start of the next one.  Defined in
// parser_shim.cc.
void add_token_end_start_spangres(int location,
                                  struct SpangresTokenLocations* locations);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // SHIMS_PARSER_SHIM_H_
