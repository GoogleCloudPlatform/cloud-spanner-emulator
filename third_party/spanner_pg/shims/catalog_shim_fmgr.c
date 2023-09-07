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

// We gave this normally-static function external linkage so that our
// shimmed-out fmgr_info_cxt_security below could use it. Forward
// declared here to limit the scope.
const FmgrBuiltin* fmgr_isbuiltin(Oid id);

void fmgr_info_cxt_security(Oid functionId, FmgrInfo* finfo,
							MemoryContext mcxt, bool ignore_security) {
	const FmgrBuiltin* fbp;

	/*
	 * fn_oid *must* be filled in last.  Some code assumes that if fn_oid is
	 * valid, the whole struct is valid.  Some FmgrInfo struct's do survive
	 * elogs.
	 */
	finfo->fn_oid = InvalidOid;
	finfo->fn_extra = NULL;
	finfo->fn_mcxt = mcxt;
	finfo->fn_expr = NULL;	// caller may set this later

	if ((fbp = fmgr_isbuiltin(functionId)) != NULL) {
		/*
		 * Fast path for builtin functions: don't bother consulting pg_proc
		 */
		finfo->fn_nargs = fbp->nargs;
		finfo->fn_strict = fbp->strict;
		finfo->fn_retset = fbp->retset;
		finfo->fn_stats = TRACK_FUNC_ALL; /* ie, never track */
		finfo->fn_addr = fbp->func;
		finfo->fn_oid = functionId;
		return;
	}

	ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
									errmsg("Unknown function or function is not a built-in: %u",
												 functionId)));
}
