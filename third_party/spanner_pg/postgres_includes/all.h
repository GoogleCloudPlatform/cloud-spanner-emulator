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

#ifndef POSTGRES_INCLUDES_ALL_H_
#define POSTGRES_INCLUDES_ALL_H_
// Including this file gives you the content from many postgres headers. It is
// designed to be included anywhere, including spangres source code, spangres
// headers, and postgres source code.
//
// If you include this file in C++ code (in practice: spangres code, but not
// postgres code), symbols and macros that have naming conflicts with common
// google3 symbols and macros will be overridden by the google3 version, and
// postgres macros whose definitions depend on overridden postgres macros or
// symbols will be "poisoned" (they cannot be referred to in the C++ code). Also
// see pg-exports.h and README.md for more.

#ifdef __cplusplus
extern "C" {
// The combination of start-postgres-header-region.inc and
// end-postgres-header-region.inc excludes postgres macros and symbols that have
// a naming conflict (with general google3 and spangres code) from being included
// when all.h is included.
//
// This wrapping *should not* be done if all.h is included in postgres code, due
// to the problem described in http://shortn/_ULPcTQZrHn. Most postgres source
// files that end up including (maybe transitively) all.h will otherwise be
// broken. We use "#ifdef __cplusplus" as a hacky replacement for detecting
// whether we are building spangres or postgres code. So, *-header-region.inc
// should activate only within "#ifdef __cplusplus".
#include "third_party/spanner_pg/postgres_includes/start-postgres-header-region.inc"
#endif

// pg-include-list.h directly includes postgres header files.
#include "third_party/spanner_pg/postgres_includes/pg-include-list.h"  // IWYU pragma: export
// pg-exports.h exports equivalent symbols or macros for some overridden or
// poisoned postgres ones.
#include "third_party/spanner_pg/postgres_includes/pg-exports.h"

#ifdef __cplusplus
#include "third_party/spanner_pg/postgres_includes/end-postgres-header-region.inc"
}  // extern "C"
#endif

#endif  // POSTGRES_INCLUDES_ALL_H_
