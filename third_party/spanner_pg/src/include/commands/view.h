/*-------------------------------------------------------------------------
 *
 * view.h
 *
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/view.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VIEW_H
#define VIEW_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

/* SPANGRES BEGIN */
// It is defined in third_party/spanner_pg/shims/parser_shim.h
// Forward declare here to workaround the cycle dependency.
struct ViewStmt;
/* SPANGRES BEGIN */

extern ObjectAddress DefineView(struct ViewStmt *stmt, const char *queryString,
								int stmt_location, int stmt_len);

extern void StoreViewQuery(Oid viewOid, Query *viewParse, bool replace);

#endif							/* VIEW_H */
