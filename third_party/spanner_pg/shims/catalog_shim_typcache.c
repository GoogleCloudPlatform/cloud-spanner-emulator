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

// Private flag bits in the TypeCacheEntry.flags field
// Copied from utils/cache/typcache.c
#define TCFLAGS_HAVE_PG_TYPE_DATA			0x000001
#define TCFLAGS_CHECKED_BTREE_OPCLASS		0x000002
#define TCFLAGS_CHECKED_HASH_OPCLASS		0x000004
#define TCFLAGS_CHECKED_EQ_OPR				0x000008
#define TCFLAGS_CHECKED_LT_OPR				0x000010
#define TCFLAGS_CHECKED_GT_OPR				0x000020
#define TCFLAGS_CHECKED_CMP_PROC			0x000040
#define TCFLAGS_CHECKED_HASH_PROC			0x000080
#define TCFLAGS_CHECKED_HASH_EXTENDED_PROC	0x000100
#define TCFLAGS_CHECKED_ELEM_PROPERTIES		0x000200
#define TCFLAGS_HAVE_ELEM_EQUALITY			0x000400
#define TCFLAGS_HAVE_ELEM_COMPARE			0x000800
#define TCFLAGS_HAVE_ELEM_HASHING			0x001000
#define TCFLAGS_HAVE_ELEM_EXTENDED_HASHING	0x002000
#define TCFLAGS_CHECKED_FIELD_PROPERTIES	0x004000
#define TCFLAGS_HAVE_FIELD_EQUALITY			0x008000
#define TCFLAGS_HAVE_FIELD_COMPARE			0x010000
#define TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS	0x020000
#define TCFLAGS_DOMAIN_BASE_IS_COMPOSITE	0x040000

// We made these functions non-static to call from here. Forward declare them to
// limit the scope.
bool array_element_has_equality(TypeCacheEntry *typentry);
bool array_element_has_compare(TypeCacheEntry *typentry);
bool array_element_has_hashing(TypeCacheEntry *typentry);
bool array_element_has_extended_hashing(TypeCacheEntry *typentry);
bool record_fields_have_equality(TypeCacheEntry *typentry);
bool record_fields_have_compare(TypeCacheEntry *typentry);
bool range_element_has_hashing(TypeCacheEntry *typentry);
bool range_element_has_extended_hashing(TypeCacheEntry *typentry);

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
TypeCacheEntry *
lookup_type_cache(Oid type_id, int flags)
{
  TypeCacheEntry *typentry;

  // SPANGRES: We're skipping hash/cache setup and lookup here.

  const FormData_pg_type* typtup = GetTypeFromBootstrapCatalog(type_id);
  if (typtup == NULL) {
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT),
          errmsg("type with OID %u does not exist", type_id)));
  }
  if (!typtup->typisdefined) {
    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_OBJECT),
          errmsg("type \"%s\" is only a shell",
            NameStr(typtup->typname))));
  }

  /* Now make the typcache entry */
  // SPANGRES: We're creating this on the heap instead of in the cache.
  typentry = (TypeCacheEntry*)palloc(sizeof(TypeCacheEntry));

  MemSet(typentry, 0, sizeof(TypeCacheEntry));

  /* These fields can never change, by definition */
  typentry->type_id = type_id;
  // SPANGRES: do not set the type_id_hash

  typentry->typlen = typtup->typlen;
  typentry->typbyval = typtup->typbyval;
  typentry->typalign = typtup->typalign;
  typentry->typstorage = typtup->typstorage;
  typentry->typtype = typtup->typtype;
  typentry->typrelid = typtup->typrelid;
  typentry->typelem = typtup->typelem;
  typentry->typcollation = typtup->typcollation;
  typentry->flags |= TCFLAGS_HAVE_PG_TYPE_DATA;

  // SPANGRES: We're skipping domain cache list updates here. This would be used
  // to invalidate this cache item in case the domain became invalid.

  /*
   * Look up opclasses if we haven't already and any dependent info is
   * requested.
   */
  if ((flags & (TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
          TYPECACHE_CMP_PROC |
          TYPECACHE_EQ_OPR_FINFO | TYPECACHE_CMP_PROC_FINFO |
          TYPECACHE_BTREE_OPFAMILY)) &&
    !(typentry->flags & TCFLAGS_CHECKED_BTREE_OPCLASS))
  {
    Oid opclass = GetDefaultOpClass(type_id, BTREE_AM_OID);
    if (OidIsValid(opclass)) {
      typentry->btree_opf = get_opclass_family(opclass);
      typentry->btree_opintype = get_opclass_input_type(opclass);
    }
    else
    {
      typentry->btree_opf = typentry->btree_opintype = InvalidOid;
    }

    /*
     * Reset information derived from btree opclass.  Note in particular
     * that we'll redetermine the eq_opr even if we previously found one;
     * this matters in case a btree opclass has been added to a type that
     * previously had only a hash opclass.
     */
    typentry->flags &= ~(TCFLAGS_CHECKED_EQ_OPR |
               TCFLAGS_CHECKED_LT_OPR |
               TCFLAGS_CHECKED_GT_OPR |
               TCFLAGS_CHECKED_CMP_PROC);
    typentry->flags |= TCFLAGS_CHECKED_BTREE_OPCLASS;
  }

  /*
   * If we need to look up equality operator, and there's no btree opclass,
   * force lookup of hash opclass.
   */
  if ((flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) &&
    !(typentry->flags & TCFLAGS_CHECKED_EQ_OPR) &&
    typentry->btree_opf == InvalidOid)
    flags |= TYPECACHE_HASH_OPFAMILY;

  if ((flags & (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO |
          TYPECACHE_HASH_EXTENDED_PROC |
          TYPECACHE_HASH_EXTENDED_PROC_FINFO |
          TYPECACHE_HASH_OPFAMILY)) &&
    !(typentry->flags & TCFLAGS_CHECKED_HASH_OPCLASS))
  {
    Oid      opclass = InvalidOid;

    opclass = GetDefaultOpClass(type_id, HASH_AM_OID);
    if (OidIsValid(opclass)) {
      typentry->hash_opf = get_opclass_family(opclass);
      typentry->hash_opintype = get_opclass_input_type(opclass);
    }
    else
    {
      typentry->hash_opf = typentry->hash_opintype = InvalidOid;
    }

    /*
     * Reset information derived from hash opclass.  We do *not* reset the
     * eq_opr; if we already found one from the btree opclass, that
     * decision is still good.
     */
    typentry->flags &= ~(TCFLAGS_CHECKED_HASH_PROC |
               TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
    typentry->flags |= TCFLAGS_CHECKED_HASH_OPCLASS;
  }

  /*
   * Look for requested operators and functions, if we haven't already.
   */
  if ((flags & (TYPECACHE_EQ_OPR | TYPECACHE_EQ_OPR_FINFO)) &&
    !(typentry->flags & TCFLAGS_CHECKED_EQ_OPR))
  {
    Oid      eq_opr = InvalidOid;

    if (typentry->btree_opf != InvalidOid)
      eq_opr = get_opfamily_member(typentry->btree_opf,
                     typentry->btree_opintype,
                     typentry->btree_opintype,
                     BTEqualStrategyNumber);
    if (eq_opr == InvalidOid &&
      typentry->hash_opf != InvalidOid)
      eq_opr = get_opfamily_member(typentry->hash_opf,
                     typentry->hash_opintype,
                     typentry->hash_opintype,
                     HTEqualStrategyNumber);

    /*
     * If the proposed equality operator is array_eq or record_eq, check
     * to see if the element type or column types support equality.  If
     * not, array_eq or record_eq would fail at runtime, so we don't want
     * to report that the type has equality.  (We can omit similar
     * checking for ranges because ranges can't be created in the first
     * place unless their subtypes support equality.)
     */
    if (eq_opr == ARRAY_EQ_OP &&
      !array_element_has_equality(typentry))
      eq_opr = InvalidOid;
    // SPANGRES: We're removing record type support here.
    // TODO: Add support for complex types.
    // else if (eq_opr == RECORD_EQ_OP &&
    //      !record_fields_have_equality(typentry))
    // eq_opr = InvalidOid;
    else if (eq_opr == RECORD_EQ_OP) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Record types are not supported.")));
    }

    /* Force update of eq_opr_finfo only if we're changing state */
    if (typentry->eq_opr != eq_opr)
      typentry->eq_opr_finfo.fn_oid = InvalidOid;

    typentry->eq_opr = eq_opr;

    /*
     * Reset info about hash functions whenever we pick up new info about
     * equality operator.  This is so we can ensure that the hash
     * functions match the operator.
     */
    typentry->flags &= ~(TCFLAGS_CHECKED_HASH_PROC |
               TCFLAGS_CHECKED_HASH_EXTENDED_PROC);
    typentry->flags |= TCFLAGS_CHECKED_EQ_OPR;
  }
  if ((flags & TYPECACHE_LT_OPR) &&
    !(typentry->flags & TCFLAGS_CHECKED_LT_OPR))
  {
    Oid      lt_opr = InvalidOid;

    if (typentry->btree_opf != InvalidOid)
      lt_opr = get_opfamily_member(typentry->btree_opf,
                     typentry->btree_opintype,
                     typentry->btree_opintype,
                     BTLessStrategyNumber);

    /*
     * As above, make sure array_cmp or record_cmp will succeed; but again
     * we need no special check for ranges.
     */
    if (lt_opr == ARRAY_LT_OP &&
      !array_element_has_compare(typentry))
      lt_opr = InvalidOid;
    // SPANGRES: We're removing record type support here.
    // TODO: Add support for complex types.
    // else if (lt_opr == RECORD_LT_OP &&
    //      !record_fields_have_compare(typentry))
    //   lt_opr = InvalidOid;
    else if (lt_opr == RECORD_LT_OP) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Record types are not supported.")));
    }

    typentry->lt_opr = lt_opr;
    typentry->flags |= TCFLAGS_CHECKED_LT_OPR;
  }
  if ((flags & TYPECACHE_GT_OPR) &&
    !(typentry->flags & TCFLAGS_CHECKED_GT_OPR))
  {
    Oid      gt_opr = InvalidOid;

    if (typentry->btree_opf != InvalidOid)
      gt_opr = get_opfamily_member(typentry->btree_opf,
                     typentry->btree_opintype,
                     typentry->btree_opintype,
                     BTGreaterStrategyNumber);

    /*
     * As above, make sure array_cmp or record_cmp will succeed; but again
     * we need no special check for ranges.
     */
    if (gt_opr == ARRAY_GT_OP &&
      !array_element_has_compare(typentry))
      gt_opr = InvalidOid;
    // SPANGRES: We're removing record type support here.
    // TODO: Add support for complex types.
    // else if (gt_opr == RECORD_GT_OP &&
    //      !record_fields_have_compare(typentry))
    //   gt_opr = InvalidOid;
    else if (gt_opr == RECORD_LT_OP) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Record types are not supported.")));
    }

    typentry->gt_opr = gt_opr;
    typentry->flags |= TCFLAGS_CHECKED_GT_OPR;
  }
  if ((flags & (TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO)) &&
    !(typentry->flags & TCFLAGS_CHECKED_CMP_PROC))
  {
    Oid      cmp_proc = InvalidOid;

    if (typentry->btree_opf != InvalidOid)
      cmp_proc = get_opfamily_proc(
          typentry->btree_opf, typentry->btree_opintype,
          typentry->btree_opintype, BTORDER_PROC);

    /*
     * As above, make sure array_cmp or record_cmp will succeed; but again
     * we need no special check for ranges.
     */
    if (cmp_proc == F_BTARRAYCMP &&
      !array_element_has_compare(typentry))
      cmp_proc = InvalidOid;
    // SPANGRES: We're removing record type support here.
    // TODO: Add support for complex types.
    // else if (cmp_proc == F_BTRECORDCMP &&
    //      !record_fields_have_compare(typentry))
    //   cmp_proc = InvalidOid;
    else if (cmp_proc == F_BTRECORDCMP) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Record types are not supported.")));
    }

    /* Force update of cmp_proc_finfo only if we're changing state */
    if (typentry->cmp_proc != cmp_proc)
      typentry->cmp_proc_finfo.fn_oid = InvalidOid;

    typentry->cmp_proc = cmp_proc;
    typentry->flags |= TCFLAGS_CHECKED_CMP_PROC;
  }
  if ((flags & (TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO)) &&
    !(typentry->flags & TCFLAGS_CHECKED_HASH_PROC))
  {
    Oid      hash_proc = InvalidOid;

    /*
     * We insist that the eq_opr, if one has been determined, match the
     * hash opclass; else report there is no hash function.
     */
    if (typentry->hash_opf != InvalidOid &&
      (!OidIsValid(typentry->eq_opr) ||
       typentry->eq_opr == get_opfamily_member(typentry->hash_opf,
                           typentry->hash_opintype,
                           typentry->hash_opintype,
                           HTEqualStrategyNumber)))
      hash_proc = get_opfamily_proc(
          typentry->hash_opf, typentry->hash_opintype, typentry->hash_opintype,
          HASHSTANDARD_PROC);

    /*
     * As above, make sure hash_array will succeed.  We don't currently
     * support hashing for composite types, but when we do, we'll need
     * more logic here to check that case too.
     */
    if (hash_proc == F_HASH_ARRAY &&
      !array_element_has_hashing(typentry))
      hash_proc = InvalidOid;

    /*
     * Likewise for hash_range.
     */
    // SPANGRES: We're removing record type support here.
    // TODO: Add support for complex types.
    // if (hash_proc == F_HASH_RANGE &&
    //   !range_element_has_hashing(typentry))
    //   hash_proc = InvalidOid;
    if (hash_proc == F_HASH_RANGE) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Record types are not supported.")));
    }

    /* Force update of hash_proc_finfo only if we're changing state */
    if (typentry->hash_proc != hash_proc)
      typentry->hash_proc_finfo.fn_oid = InvalidOid;

    typentry->hash_proc = hash_proc;
    typentry->flags |= TCFLAGS_CHECKED_HASH_PROC;
  }
  if ((flags & (TYPECACHE_HASH_EXTENDED_PROC |
          TYPECACHE_HASH_EXTENDED_PROC_FINFO)) &&
    !(typentry->flags & TCFLAGS_CHECKED_HASH_EXTENDED_PROC))
  {
    Oid      hash_extended_proc = InvalidOid;

    /*
     * We insist that the eq_opr, if one has been determined, match the
     * hash opclass; else report there is no hash function.
     */
    if (typentry->hash_opf != InvalidOid &&
      (!OidIsValid(typentry->eq_opr) ||
       typentry->eq_opr == get_opfamily_member(typentry->hash_opf,
                           typentry->hash_opintype,
                           typentry->hash_opintype,
                           HTEqualStrategyNumber)))
      hash_extended_proc = get_opfamily_proc(
          typentry->hash_opf, typentry->hash_opintype, typentry->hash_opintype,
          HASHEXTENDED_PROC);

    /*
     * As above, make sure hash_array_extended will succeed.  We don't
     * currently support hashing for composite types, but when we do,
     * we'll need more logic here to check that case too.
     */
    if (hash_extended_proc == F_HASH_ARRAY_EXTENDED &&
      !array_element_has_extended_hashing(typentry))
      hash_extended_proc = InvalidOid;

    /*
     * Likewise for hash_range_extended.
     */
    // SPANGRES: We're removing record type support here.
    // TODO: Add support for complex types.
    // if (hash_extended_proc == F_HASH_RANGE_EXTENDED &&
    //   !range_element_has_extended_hashing(typentry))
    //   hash_extended_proc = InvalidOid;
    if (hash_extended_proc == F_HASH_RANGE_EXTENDED) {
      ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                      errmsg("Range types are not supported.")));
    }

    /* Force update of proc finfo only if we're changing state */
    if (typentry->hash_extended_proc != hash_extended_proc)
      typentry->hash_extended_proc_finfo.fn_oid = InvalidOid;

    typentry->hash_extended_proc = hash_extended_proc;
    typentry->flags |= TCFLAGS_CHECKED_HASH_EXTENDED_PROC;
  }

  /*
   * Set up fmgr lookup info as requested
   *
   * Note: we tell fmgr the finfo structures live in CacheMemoryContext,
   * which is not quite right (they're really in the hash table's private
   * memory context) but this will do for our purposes.
   *
   * Note: the code above avoids invalidating the finfo structs unless the
   * referenced operator/function OID actually changes.  This is to prevent
   * unnecessary leakage of any subsidiary data attached to an finfo, since
   * that would cause session-lifespan memory leaks.
   */
  if ((flags & TYPECACHE_EQ_OPR_FINFO) &&
    typentry->eq_opr_finfo.fn_oid == InvalidOid &&
    typentry->eq_opr != InvalidOid)
  {
    Oid      eq_opr_func;

    eq_opr_func = get_opcode(typentry->eq_opr);
    if (eq_opr_func != InvalidOid)
      fmgr_info_cxt(eq_opr_func, &typentry->eq_opr_finfo,
              CacheMemoryContext);
  }
  if ((flags & TYPECACHE_CMP_PROC_FINFO) &&
    typentry->cmp_proc_finfo.fn_oid == InvalidOid &&
    typentry->cmp_proc != InvalidOid)
  {
    fmgr_info_cxt(typentry->cmp_proc, &typentry->cmp_proc_finfo,
            CacheMemoryContext);
  }
  if ((flags & TYPECACHE_HASH_PROC_FINFO) &&
    typentry->hash_proc_finfo.fn_oid == InvalidOid &&
    typentry->hash_proc != InvalidOid)
  {
    fmgr_info_cxt(typentry->hash_proc, &typentry->hash_proc_finfo,
            CacheMemoryContext);
  }
  if ((flags & TYPECACHE_HASH_EXTENDED_PROC_FINFO) &&
    typentry->hash_extended_proc_finfo.fn_oid == InvalidOid &&
    typentry->hash_extended_proc != InvalidOid)
  {
    fmgr_info_cxt(typentry->hash_extended_proc,
            &typentry->hash_extended_proc_finfo,
            CacheMemoryContext);
  }

  /*
   * If it's a composite type (row type), get tupdesc if requested
   */
  if ((flags & TYPECACHE_TUPDESC) &&
    typentry->tupDesc == NULL &&
    typentry->typtype == TYPTYPE_COMPOSITE)
  {
    // TODO: Add support for complex types.
    elog(ERROR, "Composite types are not supported in Spangres");
  }

  /*
   * If requested, get information about a range type
   */
  if ((flags & TYPECACHE_RANGE_INFO) &&
    typentry->typtype == TYPTYPE_RANGE)
  {
    // TODO: Add support for complex types.
    elog(ERROR, "Range types are not supported in Spangres");
  }

  /*
   * If requested, get information about a domain type
   *
   * This includes making sure that the basic info about the range element
   * type is up-to-date.
   */
  if ((flags & TYPECACHE_DOMAIN_BASE_INFO) &&
    typentry->domainBaseType == InvalidOid &&
    typentry->typtype == TYPTYPE_DOMAIN)
  {
    typentry->domainBaseTypmod = -1;
    typentry->domainBaseType =
      getBaseTypeAndTypmod(type_id, &typentry->domainBaseTypmod);
  }
  if ((flags & TYPECACHE_DOMAIN_CONSTR_INFO) &&
    (typentry->flags & TCFLAGS_CHECKED_DOMAIN_CONSTRAINTS) == 0 &&
    typentry->typtype == TYPTYPE_DOMAIN)
  {
    elog(ERROR, "Domain types are not supported in Spangres");
  }

  return typentry;
}

/*
 * assign_record_type_typmod
 *
 * Given a tuple descriptor for a RECORD type, find or create a cache entry
 * for the type, and set the tupdesc's tdtypmod field to a value that will
 * identify this cache entry to lookup_rowtype_tupdesc.
 *
 * SPANGRES: This function relies on RecordCacheHash which is not supported yet.
 * TODO: Implement this function with a storage replacement.
 */
void
assign_record_type_typmod(TupleDesc tupDesc)
{
  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                  errmsg("Record types are not supported.")));
}
