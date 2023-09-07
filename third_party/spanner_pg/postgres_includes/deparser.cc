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

#include "third_party/spanner_pg/postgres_includes/deparser.h"

#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim.h"


extern "C" {
void get_query_def(Query *query, StringInfo buf, List *parentnamespace,
                   TupleDesc resultDesc, int prettyFlags, int wrapColumn,
                   int startIndent);

void set_deparse_for_query(deparse_namespace *dpns, Query *query,
                           List *parent_namespaces);

}

char* deparse_query(Query* query, bool prettyPrint) {
  StringInfoData buf;
  int prettyFlags = 0;
  if (prettyPrint) {
    prettyFlags = POSTGRES_PRETTYFLAG_PAREN | POSTGRES_PRETTYFLAG_INDENT |
                  POSTGRES_PRETTYFLAG_SCHEMA;
  }
  initStringInfo(&buf);
  get_query_def(query, &buf, NIL /* parentnamespace */, NULL /* resultDec */,
                prettyFlags /* prettyFlags */, 0 /* wrapColumn */,
                0 /* startIndent */);
  return buf.data;
}

char* deparse_expression_in_query(Node* expr, Query* query) {
  deparse_namespace dpns;
  set_deparse_for_query(&dpns, query, NIL);

  return deparse_expression(expr, list_make1(&dpns), false /* forceprefix */,
                            false /* showimplicit */);
}
