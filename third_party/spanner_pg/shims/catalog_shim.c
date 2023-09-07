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

#include <alloca.h>
#include <string.h>

#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim_cc_wrappers.h"

void get_rule_expr_paren(Node *node, deparse_context *context,
                         bool showimplicit, Node *parentNode);

// We made this function non-static to call from here. Forward declare it to
// limit the scope.
ParseNamespaceItem*
buildNSItemFromLists(RangeTblEntry* rte, Index rtindex,
                     List* coltypes, List* coltypmods, List* colcollations);

// Interceptor for addRangeTableEntry defined in parse_relation.c.
// Instead of building RTE from a PostgreSQL catalog lookup, builds by looking
// up a googlesql Table and transforming that into an RTE. This version also
// populates table column information, which the PostgreSQL version defers.
// Then, construct and return a ParseNamespaceItem for the new RTE.
// We do not link the ParseNamespaceItem into the pstate here; it's the
// caller's job to do that in the appropriate way.
ParseNamespaceItem* addRangeTableEntry(ParseState* pstate,
                                               RangeVar* relation,
                                               Alias* alias, bool inh,
                                               bool inFromCl) {
  if (!relation) {
    ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("Error occurred during RangeTblEntry construction: "
                    "invalid relation")));
  }

  if (!relation->relname) {
    ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("Error occurred during RangeTblEntry construction: "
                    "invalid relation 'relname'")));
  }

  RangeTblEntry *rte;

  rte = AddRangeTableEntryC(pstate, relation, alias, inh, inFromCl);

  if (!rte)
  {
    // An error occurred.
    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
             errmsg("Error occurred during RangeTblEntry construction of "
                    "relation: \"%s\"", relation->relname)));
  }

  // Transform table hints and attach.
  ListCell* lc;
  foreach (lc, relation->tableHints) {
    rte->tableHints =
        lappend(rte->tableHints,
                transformSpangresHint(pstate, castNode(DefElem, lfirst(lc))));
  }

  // Build a ParseNamespaceItem, but don't add it to the pstate's namespace
  // list --- caller must do that if appropriate.
  // SPANGRES: use buildNSItemFromLists instead of buildNSItemFromTupleDesc.
  int ncolumns = 0;
  List* coltypes;
  List* coltypmods;
  List* colcollations;
  GetColumnTypesC(
    rte->relid, &coltypes, &coltypmods, &colcollations, &ncolumns);
  return buildNSItemFromLists(
    rte, list_length(pstate->p_rtable), coltypes, coltypmods, colcollations);
}

ParseNamespaceItem* addRangeTableEntryByOid(ParseState* pstate,
                                            Oid relation_oid,
                                            Alias* alias, bool inh,
                                            bool inFromCl) {
  RangeTblEntry* rte = AddRangeTableEntryByOidC(pstate, relation_oid, alias, inh,
                        inFromCl);

  if (!rte) {
    // An error occurred.
    ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_TABLE),
             errmsg("Error occurred during RangeTblEntry construction of "
                    "a relation with oid: \"u\"", relation_oid)));
  }

  // Build a ParseNamespaceItem, but don't add it to the pstate's namespace
  // list --- caller must do that if appropriate.
  // SPANGRES: use buildNSItemFromLists instead of buildNSItemFromTupleDesc.
  int ncolumns = 0;
  List* coltypes;
  List* coltypmods;
  List* colcollations;
  GetColumnTypesC(
    rte->relid, &coltypes, &coltypmods, &colcollations, &ncolumns);
  return buildNSItemFromLists(
    rte, list_length(pstate->p_rtable), coltypes, coltypmods, colcollations);
}

// Helper function for HeapTuple creation out of FormData_pg_<X> structs.
// Allocates HeapTuple struct and sets the fields that are based only on size
// and attribute count.
// `data_size`: sizeof(FormData_pg_<X>)
// `num_attributes`: Natts_pg_<X>
// `has_nulls`: Whether this FormData struct has NULL values.
// Caller must set:
//   header->t_infomask based on tuple properties
//   Nulls bitmap if applicable
//   Struct payload data
static HeapTuple CreateTemplateHeapTuple(size_t data_size, int num_attributes,
                                         bool has_nulls) {
  size_t tuple_size = HEAPTUPLESIZE;
  size_t header_size = offsetof(HeapTupleHeaderData, t_bits);
  if (has_nulls) header_size += BITMAPLEN(num_attributes);
  header_size = MAXALIGN(header_size);

  HeapTuple tuple = palloc0(tuple_size + header_size + data_size);
  HeapTupleHeader header = (HeapTupleHeader)((char*)tuple + HEAPTUPLESIZE);

  // Set tuple fields, including header pointer.
  tuple->t_data = header;
  tuple->t_len = header_size + data_size;
  // Per htup.h: explicitly invalidate these fields for manufactured tuples.
  ItemPointerSetInvalid(&tuple->t_self);
  tuple->t_tableOid = InvalidOid;

  // Set basic header fields.
  header->t_hoff = header_size;
  HeapTupleHeaderSetTypMod(header, -1);         // Default value.
  ItemPointerSetInvalid(&(header->t_ctid));
  HeapTupleHeaderSetDatumLength(header, header_size + data_size);
  HeapTupleHeaderSetNatts(header, num_attributes);

  return tuple;
}

// Given a type oid, return a Type (HeapTuple) with the type's data copied in.
// This is similar to heap_form_tuple, except since we already have the payload
// data in struct form (variable stride), we memcpy it in instead of the
// complicated variable stride from an array of Datum pointers used by
// heap_form_tuple in PostgreSQL. We also hard-code certain data (like null
// columns) to avoid needing a TupleDesc.
// When the type oid is unknown, return NULL to let callers decide what error to
// report.
Type PgTypeFormHeapTuple(Oid type_id) {
  const FormData_pg_type* type_data = GetTypeFromBootstrapCatalog(type_id);
  if (type_data == NULL) {
    // Mimic the PostgreSQL behavior for failed lookups and return NULL. Callers
    // can decide how to report this error.
    return (Type) NULL;
  }

  size_t data_size = sizeof(FormData_pg_type);
  HeapTuple tuple =
      CreateTemplateHeapTuple(data_size, Natts_pg_type,
                              /*has_nulls=*/true);
  HeapTupleHeader header = tuple->t_data;

  // Set header infomasks based on pg_type data. These are hard-coded since the
  // alternative (iterating over an array of values as done in heap_fill_tuple
  // requires a full additional copy).
  header->t_infomask |= HEAP_HASNULL;  // pg_type rows have null attributres.
  header->t_infomask |= HEAP_HASVARWIDTH;  // pg_type.typedefault is text.
  header->t_infomask &=
      ~(HEAP_HASEXTERNAL);  // pg_type has no external data--the only
                            // variable-length fields are also NULL-valued. This
                            // setting is a no-op after the preceeding lines,
                            // but it's here as documentation and for easier
                            // comparison to the PostgresQL
                            // heap_fill_tuple/heap_form_tuple source.

  // Set the nulls bitmap (hard-coded to attributes {28,29,30} (1-indexed) based
  // on pg_type.h). Compare to the hard-coded initialization in TypeShellMake.
  header->t_bits[Anum_pg_type_typdefaultbin - 1] = 1;
  header->t_bits[Anum_pg_type_typdefault - 1] = 1;
  header->t_bits[Anum_pg_type_typacl - 1] = 1;

  // Copy in the payload.
  memcpy(GETSTRUCT(tuple), type_data, data_size);

  return (Type) tuple;  // Cast is for human-readability, they're the same type.
}

// Given an oid specifying an operator, return an Operator (HeapTuple) with the
// operator's data copied in. This is similar to heap_form_tuple, except since
// we already have the payload data in struct form (variable stride), we memcpy
// it in instead of the complicated variable stride from an array of Datum
// pointers used by heap_form_tuple in PostgreSQL. We also hard-code certain
// data (like null columns) to avoid needing a TupleDesc. When the oid is
// unknown, return NULL to let callers decide what error to report.
Operator PgOperatorFormHeapTuple(Oid operator_id) {
  const FormData_pg_operator* operator_data =
      GetOperatorFromBootstrapCatalog(operator_id);
  if (operator_data == NULL) {
    // Mimic the PostgreSQL behavior for failed lookups and return NULL. Callers
    // can decide how to report this error.
    return (Operator)NULL;
  }

  size_t data_size = sizeof(FormData_pg_operator);
  HeapTuple tuple =
      CreateTemplateHeapTuple(data_size, Natts_pg_operator,
                              /*has_nulls=*/false);
  HeapTupleHeader header = tuple->t_data;

  // Set header infomasks based on pg_operator data. These are hard-coded since
  // the alternative (iterating over an array of values as done in
  // heap_fill_tuple requires a full additional copy).
  // Note: the negative settings here are no-ops after the preceeding lines, but
  // we keep them here as documentation and for easier comparison to the
  // PostgresQL heap_fill_tuple/heap_form_tuple source.
  header->t_infomask &= ~(HEAP_HASNULL);      // pg_operator rows have no nulls.
  header->t_infomask &= ~(HEAP_HASVARWIDTH);  // pg_operator is all fixed-width.
  header->t_infomask &=
      ~(HEAP_HASEXTERNAL);  // pg_operator has no external data--there are no
                            // variable-length fields.

  // Copy in the payload.
  memcpy(GETSTRUCT(tuple), operator_data, data_size);

  return (Operator) tuple;  // Cast is for human-readability, they're the same type.
}

// Convert a ColumnRef into an A_Const string node for hints. Result is
// equivalent to what would be created if the hint string was originally
// single-quoted in the parser (instead of unquoted or double quoted, which
// produce ColumnRefs).
//
// The result is a *parse tree node* and still needs to be processed by the
// analyzer.
Node* HintColumnRefToString(ColumnRef* column_ref) {
  if (list_length(column_ref->fields) > 1) {
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("Too many names in hint value; if a string is intended, "
                    "please enclose it with quotation marks")));
  } else {
    Node* value = (Node*)linitial(column_ref->fields);
    if (IsA(value, A_Star)) {
      // Valid in the grammar, but not in this context. Shouldn't happen.
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Star in hint expression is invalid")));
    }
    Assert(IsA(value, String));
    A_Const* a_const = makeNode(A_Const);
    a_const->val.type = T_String;
    a_const->val.val.str = strVal(value);
    a_const->location = column_ref->location;
    return (Node*)a_const;
  }
}

// We treat DefElem here as a Key/Value pair, using its `defname` (key) and
// arg (value) fields, and optionally `defnamespace`. We do the transformation
// in place (same DefElem struct is returned), and we don't have to transform
// `defname` or `defnamespace`. We do have to transform the `arg` field from
// Parser types (A_Expr) to Analyzer types (Expr). In addition, for hints we
// apply the special rule that SQL identifiers (ColRef) are converted to string
// contants (we don't look up the identifier in the catalog). Other than this
// special identifier rule, this performs the same transformation on `arg` as
// transformExpr() plus the type coercion done on plain string types when
// necessary.
Node* transformSpangresHint(ParseState* pstate, DefElem* elem) {
  // Accept a single identifier as a hint value, transforming into a string
  // value. Otherwise, treat the arg as a standard expression.
  if (IsA(elem->arg, ColumnRef)) {
    elem->arg = HintColumnRefToString(castNode(ColumnRef, elem->arg));
  }

  elem->arg = transformExpr(pstate, elem->arg, EXPR_KIND_OTHER);

  // When the type is an unannotated string, PostgreSQL's resolver leaves it as
  // UNKNOWNOID to let context handle final type resolution. In most cases, like
  // in the target list, it defaults this resolution to TEXTOID. We apply the
  // same rule here.
  if (IsA(elem->arg, Const) &&
      castNode(Const, elem->arg)->consttype == UNKNOWNOID) {
    elem->arg = coerce_to_specific_type(pstate, elem->arg, TEXTOID, "HINT");
  }

  return (Node*)elem;
}

// Deparse a list of hints into a Spangres Hint container (/*@ hint1 = value1,
// hint2 = value2, ... */). If the list is empty, don't add an empty container.
//
// `statement`: indicates whether this is a statement-level hint or other
// location. Statement-level hints need an extra space after the hint, but not
// before: [/*@ hint... */ ]SELECT.... When the deparser is mid-statement, it
// expects new tokens to add their own prefix spaces, so non-statement-level
// hints must do the same: FROM <table>[ /*@ hint... */];
void get_hint_list_def(List* hints, deparse_context* context, bool statement) {
  if (list_length(hints) == 0) {
    return;
  }
  if (!statement) {
    appendStringInfoSpaces(context->buf, 1);
  }
  appendStringInfoString(context->buf, "/*@ ");
  ListCell* lc;
  foreach (lc, hints) {
    DefElem* hint = lfirst_node(DefElem, lc);
    if (hint->defnamespace != NULL) {
      appendStringInfoString(context->buf, hint->defnamespace);
      appendStringInfoString(context->buf, ".");
    }
    appendStringInfoString(context->buf, hint->defname);
    appendStringInfoString(context->buf, "=");
    // NB: dpcontext is NIL because no Vars are expected.
    //     forceprefix is irrelevant if no Vars are deparsed.
    appendStringInfoString(
        context->buf,
        deparse_expression(hint->arg, /*dpcontext=*/NIL, /*forceprefix=*/false,
                           /*showimplicit=*/false));
    if (lnext(hints, lc) != NULL) {
      appendStringInfoString(context->buf, ", ");
    } else {
      appendStringInfoSpaces(context->buf, 1);
    }
  }
  appendStringInfoString(context->buf, "*/");
  if (statement) {
    appendStringInfoSpaces(context->buf, 1);
  }
}
