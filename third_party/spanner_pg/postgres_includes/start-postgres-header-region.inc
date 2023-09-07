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

// This file is meant to be included early inside all.h.
//
// For postgres macros that have google3 naming conflicts, this file undef-s
// the macros so including postgres headers after this file has been included
// will not throw errors even if the google3 names existed before. It also
// puts them in a stack with push_macro, see end-postgres-header-region.inc for
// how that is used.
//
// For symbols with google3 naming conflicts, this file replaces them with an
// unconflicting name using a pre-processor macro, so that any included
// postgres headers after this do not really end up defining/declaring/using
// the conflicting names.
#ifdef __cplusplus
extern "C" {
#endif

// Postgres macros that have naming conflicts ...
#pragma push_macro("Assert")
#undef Assert
#pragma push_macro("INFO")
#undef INFO
#pragma push_macro("WARNING")
#undef WARNING
#pragma push_macro("ERROR")
#undef ERROR
#pragma push_macro("FATAL")
#undef FATAL
#pragma push_macro("PANIC")
#undef PANIC
#pragma push_macro("INVALID")
#undef INVALID
#pragma push_macro("NOW")
#undef NOW
#pragma push_macro("Abs")
#undef Abs
#pragma push_macro("Max")
#undef Max
#pragma push_macro("Min")
#undef Min
#pragma push_macro("StrNCpy")
#undef StrNCpy
#pragma push_macro("ExclusiveLock")
#undef ExclusiveLock
#pragma push_macro("YEAR")
#undef YEAR
#pragma push_macro("MONTH")
#undef MONTH
#pragma push_macro("DAY")
#undef DAY
#pragma push_macro("HOUR")
#undef HOUR
#pragma push_macro("MINUTE")
#undef MINUTE
#pragma push_macro("SECOND")
#undef SECOND
#pragma push_macro("MILLISECOND")
#undef MILLISECOND
#pragma push_macro("MICROSECOND")
#undef MICROSECOND
#pragma push_macro("WEEK")
#undef WEEK

// gettext and friends are defined to null in Postgres unless you configure the
// build with --enable-nls (i.e. language translations). We don't do that.
#pragma push_macro("gettext")
#undef gettext
#pragma push_macro("dgettext")
#undef dgettext
#pragma push_macro("dngettext")
#undef dngettext
#pragma push_macro("ngettext")
#undef ngettext

// _ is an alias for gettext in postgres code
#pragma push_macro("_")
#undef _

#ifdef LOG
// ABSL_LOG is special. It's a frequently used macro in google3 code, and for
// whatever reason is particularly tickled by this trick. If we build this
// header in module mode (the default for workstation builds) then this push/pop
// structure doesn't correctly restore the google3 version of ABSL_LOG without the
// #ifdef surrounding this comment (which presumably results in some magic in
// module builds to handle ABSL_LOG being defined or not when this line is reached).
//
// One way to solve the problem is to _always_ build this header in C++ module
// mode. Then you don't need the push_macro/pop_macro pragmas, as #undef by
// itself would correctly hide the Postgres definitions within this header's
// module. Unfortunately Tricorder analyses, particularly coverage and builder
// analysis, don't work on targets unless they build with --config=no_modules.
// To support that, we _must_ have the push_macro/pop_macro in order to restore
// google3 definitions.
//
// Therefore we're stuck with this trick until Tricorder works on modules-only
// builds. Once we have that, the push_macro/pop_macro dance can be removed,
// this empty #ifdef can be removed, and we can just #undef the macros defined
// in the Postgres code that conflict with google3.
#endif
#pragma push_macro("LOG")
#undef LOG

// Postgres symbols that have naming conflicts ...
#define File CONFLICT_GOOGLE3_File
