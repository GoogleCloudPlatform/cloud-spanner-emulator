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

#ifndef POSTGRES_INCLUDES_PG_INCLUDE_LIST_H_
#define POSTGRES_INCLUDES_PG_INCLUDE_LIST_H_
// Includes a list of postgres header files that spangres code uses. Meant to be
// included directly only within all.h.
//
// This file can be included within postgres code too. It should hence not
// include anything C++ or outside of the postgres codebase.
//
// Postgres headers are supposed to be included in a certain order, do not sort
// them alphabetically.
//
// clang-format off
#include "third_party/spanner_pg/src/include/postgres.h"

// These headers are generated and we don't bother to copy them into
// `include/` like PostgreSQL does, so they're found in `backend/`
#include "third_party/spanner_pg/src/backend/utils/fmgroids.h"
#include "third_party/spanner_pg/src/backend/utils/fmgrprotos.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_namespace_d.h"

#include "third_party/spanner_pg/src/include/fmgr.h"
#include "third_party/spanner_pg/src/include/funcapi.h"
#include "third_party/spanner_pg/src/include/miscadmin.h"
#include "third_party/spanner_pg/src/include/pgstat.h"
#include "third_party/spanner_pg/src/include/pgtime.h"
#include "third_party/spanner_pg/src/include/postgres_ext.h"
#include "third_party/spanner_pg/src/include/access/hash.h"
#include "third_party/spanner_pg/src/include/access/htup.h"
#include "third_party/spanner_pg/src/include/access/htup_details.h"
#include "third_party/spanner_pg/src/include/access/nbtree.h"
#include "third_party/spanner_pg/src/include/access/sysattr.h"
#include "third_party/spanner_pg/src/include/catalog/index.h"
#include "third_party/spanner_pg/src/include/catalog/namespace.h"
#include "third_party/spanner_pg/src/include/catalog/pg_aggregate.h"
#include "third_party/spanner_pg/src/include/catalog/pg_am.h"
#include "third_party/spanner_pg/src/include/catalog/pg_amop.h"
#include "third_party/spanner_pg/src/include/catalog/pg_amproc.h"
#include "third_party/spanner_pg/src/include/catalog/pg_cast.h"
#include "third_party/spanner_pg/src/include/catalog/pg_collation.h"
#include "third_party/spanner_pg/src/include/catalog/pg_constraint.h"
#include "third_party/spanner_pg/src/include/catalog/pg_class.h"
#include "third_party/spanner_pg/src/include/catalog/pg_inherits.h"
#include "third_party/spanner_pg/src/include/catalog/pg_opclass.h"
#include "third_party/spanner_pg/src/include/catalog/pg_operator.h"
#include "third_party/spanner_pg/src/include/catalog/pg_proc.h"
#include "third_party/spanner_pg/src/include/catalog/pg_type.h"
#include "third_party/spanner_pg/src/backend/catalog/pg_type_d.h"
#include "third_party/spanner_pg/src/include/commands/defrem.h"
#include "third_party/spanner_pg/src/include/common/string.h"
#include "third_party/spanner_pg/src/include/nodes/pg_list.h"
#include "third_party/spanner_pg/src/include/nodes/execnodes.h"
#include "third_party/spanner_pg/src/include/nodes/makefuncs.h"
#include "third_party/spanner_pg/src/include/nodes/nodeFuncs.h"
#include "third_party/spanner_pg/src/include/nodes/nodes.h"
#include "third_party/spanner_pg/src/include/nodes/parsenodes.h"
#include "third_party/spanner_pg/src/include/nodes/plannodes.h"
#include "third_party/spanner_pg/src/include/nodes/primnodes.h"
#include "third_party/spanner_pg/src/include/nodes/print.h"
#include "third_party/spanner_pg/src/include/nodes/readfuncs.h"
#include "third_party/spanner_pg/src/include/nodes/subscripting.h"
#include "third_party/spanner_pg/src/include/nodes/value.h"
#include "third_party/spanner_pg/src/include/optimizer/tlist.h"
#include "third_party/spanner_pg/src/include/parser/analyze.h"
#include "third_party/spanner_pg/src/include/parser/parser.h"
#include "third_party/spanner_pg/src/include/parser/parsetree.h"
#include "third_party/spanner_pg/src/include/parser/parse_agg.h"
#include "third_party/spanner_pg/src/include/parser/parse_clause.h"
#include "third_party/spanner_pg/src/include/parser/parse_coerce.h"
#include "third_party/spanner_pg/src/include/parser/parse_collate.h"
#include "third_party/spanner_pg/src/include/parser/parse_expr.h"
#include "third_party/spanner_pg/src/include/parser/parse_func.h"
#include "third_party/spanner_pg/src/include/parser/parse_oper.h"
#include "third_party/spanner_pg/src/include/parser/parse_relation.h"
#include "third_party/spanner_pg/src/include/parser/parse_target.h"
#include "third_party/spanner_pg/src/include/parser/parse_type.h"
#include "third_party/spanner_pg/src/include/utils/array.h"
#include "third_party/spanner_pg/src/include/utils/builtins.h"
#include "third_party/spanner_pg/src/include/utils/date.h"
#include "third_party/spanner_pg/src/include/utils/datetime.h"
#include "third_party/spanner_pg/src/include/utils/elog.h"
#include "third_party/spanner_pg/src/include/utils/fmgrtab.h"
#include "third_party/spanner_pg/src/include/utils/formatting.h"
#include "third_party/spanner_pg/src/include/utils/int8.h"  // DO_NOT_TRANSFORM
#include "third_party/spanner_pg/src/include/utils/lsyscache.h"
#include "third_party/spanner_pg/src/include/utils/memutils.h"
#include "third_party/spanner_pg/src/include/utils/numeric.h"
#include "third_party/spanner_pg/src/include/utils/ruleutils.h"
// TODO: remove syscache when get_func_arg_info_spangres is done
// with it.
#include "third_party/spanner_pg/src/include/utils/syscache.h"
#include "third_party/spanner_pg/src/include/utils/timestamp.h"
#include "third_party/spanner_pg/src/include/utils/typcache.h"
#include "third_party/spanner_pg/src/include/mb/pg_wchar.h"
// clang-format on

#endif  // POSTGRES_INCLUDES_PG_INCLUDE_LIST_H_
