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

// SysCache Shim
// A whole-file replacement of PostgreSQL's syscache.c.
// Most functions in this file throw an error (non-returning).

// NOTE: This is nominally implementing the src/includes/utils/syscache.h API,
// but due to Spangres include rules it includes postgres_incldues/all.h
// instead.

#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/catalog_shim.h"

// Error throwing function used to stop PostgreSQL execution when an unsupported
// catalog API is called. DOES NOT RETURN
#define ThrowSysCacheLookupError()                 \
  ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), \
                  errmsg("Attempted unsupported catalog lookup")))

void InitCatalogCache() { ThrowSysCacheLookupError(); }
void InitCatalogCachePhase2() { ThrowSysCacheLookupError(); }

HeapTuple SearchSysCache(int cacheId, Datum key1, Datum key2, Datum key3,
                         Datum key4) {
  ThrowSysCacheLookupError();
}

HeapTuple SearchSysCache1(int cacheId, Datum key1) {
  ThrowSysCacheLookupError();
}
HeapTuple SearchSysCache2(int cacheId, Datum key1, Datum key2) {
  ThrowSysCacheLookupError();
}
HeapTuple SearchSysCache3(int cacheId, Datum key1, Datum key2, Datum key3) {
  ThrowSysCacheLookupError();
}
HeapTuple SearchSysCache4(int cacheId, Datum key1, Datum key2, Datum key3,
                          Datum key4) {
  ThrowSysCacheLookupError();
}

// Spangres generates substitute heap tuples, so we must call Spangres' release.
void ReleaseSysCache(HeapTuple tuple) { ReleaseCatCache(tuple); }

/* convenience routines */
HeapTuple SearchSysCacheCopy(int cacheId, Datum key1, Datum key2, Datum key3,
                             Datum key4) {
  ThrowSysCacheLookupError();
}
bool SearchSysCacheExists(int cacheId, Datum key1, Datum key2, Datum key3,
                          Datum key4) {
  ThrowSysCacheLookupError();
}
Oid GetSysCacheOid(int cacheId, AttrNumber oidcol, Datum key1, Datum key2, Datum key3,
                   Datum key4) {
  ThrowSysCacheLookupError();
}

HeapTuple SearchSysCacheAttName(Oid relid, const char *attname) {
  ThrowSysCacheLookupError();
}
HeapTuple SearchSysCacheCopyAttName(Oid relid, const char *attname) {
  ThrowSysCacheLookupError();
}
bool SearchSysCacheExistsAttName(Oid relid, const char *attname) {
  ThrowSysCacheLookupError();
}

HeapTuple SearchSysCacheAttNum(Oid relid, int16_t attnum) {
  ThrowSysCacheLookupError();
}
HeapTuple SearchSysCacheCopyAttNum(Oid relid, int16_t attnum) {
  ThrowSysCacheLookupError();
}

Datum SysCacheGetAttr(int cacheId, HeapTuple tup, AttrNumber attributeNumber,
                      bool *isNull) {
  ThrowSysCacheLookupError();
}

uint32_t GetSysCacheHashValue(int cacheId, Datum key1, Datum key2, Datum key3,
                            Datum key4) {
  ThrowSysCacheLookupError();
}

struct catclist;
struct catclist *SearchSysCacheList(int cacheId, int nkeys, Datum key1,
                                    Datum key2, Datum key3) {
  ThrowSysCacheLookupError();
}

void SysCacheInvalidate(int cacheId, uint32_t hashValue) {
  ThrowSysCacheLookupError();
}

bool RelationInvalidatesSnapshotsOnly(Oid relid) { return false; }
bool RelationHasSysCache(Oid relid) { return false; }
bool RelationSupportsSysCache(Oid relid) { return false; }
