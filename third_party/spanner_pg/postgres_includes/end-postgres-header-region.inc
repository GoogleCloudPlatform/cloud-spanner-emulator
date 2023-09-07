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

// This file is meant to be included late inside all.h. It is the companion to
// start-postgres-header-region.inc.
//
// Pop all the macros from the stack so future google3-includes can use their
// original defitions.
#undef Assert
#pragma pop_macro("Assert")
#undef LOG
#pragma pop_macro("LOG")
#undef INFO
#pragma pop_macro("INFO")
#undef WARNING
#pragma pop_macro("WARNING")
#undef ERROR
#pragma pop_macro("ERROR")
#undef FATAL
#pragma pop_macro("FATAL")
#undef PANIC
#pragma pop_macro("PANIC")
#undef INVALID
#pragma pop_macro("INVALID")
#undef NOW
#pragma pop_macro("NOW")
#undef Abs
#pragma pop_macro("Abs")
#undef Max
#pragma pop_macro("Max")
#undef Min
#pragma pop_macro("Min")
#undef StrNCpy
#pragma pop_macro("StrNCpy")
#undef ExclusiveLock
#pragma pop_macro("ExclusiveLock")
#undef gettext
#pragma pop_macro("gettext")
#undef dgettext
#pragma pop_macro("dgettext")
#undef dngettext
#pragma pop_macro("dngettext")
#undef ngettext
#pragma pop_macro("ngettext")
#undef _
#pragma pop_macro("_")
#undef YEAR
#pragma pop_macro("YEAR")
#undef MONTH
#pragma pop_macro("MONTH")
#undef DAY
#pragma pop_macro("DAY")
#undef HOUR
#pragma pop_macro("HOUR")
#undef MINUTE
#pragma pop_macro("MINUTE")
#undef SECOND
#pragma pop_macro("SECOND")
#undef MILLISECOND
#pragma pop_macro("MILLISECOND")
#undef MICROSECOND
#pragma pop_macro("MICROSECOND")
#undef WEEK
#pragma pop_macro("WEEK")

// undef all conflicting symbols so future google3-headers use/define/declare
// the real symbols.
#undef File

#ifdef __cplusplus
}  // extern "C"
#endif
