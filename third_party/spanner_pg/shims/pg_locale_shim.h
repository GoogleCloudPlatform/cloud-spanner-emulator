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

#ifndef SHIMS_PG_LOCALE_SHIM_H_
#define SHIMS_PG_LOCALE_SHIM_H_

#include <locale.h>

#ifdef __cplusplus
extern "C" {
#endif

// Localized months and days cache. These are initialized when the user calls
// `cache_locale_time`. This adheres to the existing native PostgreSQL
// behaviour.
extern char* localized_abbrev_days[];
extern char* localized_full_days[];
extern char* localized_abbrev_months[];
extern char* localized_full_months[];

// Returns the POSIX lconv struct (containing number/money formatting
// information) with locale information for all categories.
//
// This function returns a result based on the hardcoded `en_US.UTF-8` locale.
// This is because locale is not dynamic in Cloud Spanner. This function is
// thread safe.
extern struct lconv* PGLC_localeconv(void);

// Initializes and caches the localized time days and months.
//
// This function returns a result based on the hardcoded `en_US.UTF-8` locale.
// This is because locale is not dynamic in Cloud Spanner. This function is
// thread safe.
extern void cache_locale_time(void);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // SHIMS_PG_LOCALE_SHIM_H_
