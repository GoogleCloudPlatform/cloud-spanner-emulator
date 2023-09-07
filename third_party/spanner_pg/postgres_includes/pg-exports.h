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

#ifndef POSTGRES_INCLUDES_PG_EXPORTS_H_
#define POSTGRES_INCLUDES_PG_EXPORTS_H_
// Exports some macros and symbols as an alternative for those that were
// overridden or poisoned within all.h. Meant to be included directly only
// within all.h.
//
// This file has direct visibility into postgres symbols and macros, even if the
// file gets included while building C++ spangres code -- none of the overriding
// by google3 symbols and macros, or poisoning, has happened yet.
//
// This file can be included indirectly within postgres code too, and needs to
// compile within there. It should also not undefine or redefine any macros
// postgres already has when getting included in postgres code. Additionally, it
// is considered bad style to export new macros or symbols that are visible only
// within spangres code and not when this file becomes included in postgres code.

// Macros are evaluated at their place of use, not at their place of definition.
// So, if we have a name-conflicting macro called "ERROR", doing "#define
// PG_ERROR ERROR" will not make PG_ERROR work in Spangres code; PG_ERROR will
// be expanded to ERROR wherever it is used, and probably then result in a
// compiler error. This is also the reason some postgres macros are poisoned.
//
// One trick to force immediate evaluation of macro definitions is to not have
// them as preprocessor macros -- you can instead define actual C types or
// symbols.
//
// Therefore, re-exports can mostly be done by giving a new name to symbols or
// macros that have namespace conflicts, and transforming poisoned preprocessor
// macros to compiler tokens.
typedef enum {
  // Levels are documented in utils/elog.h
  PG_DEBUG5 = DEBUG5,
  PG_DEBUG4 = DEBUG4,
  PG_DEBUG3 = DEBUG3,
  PG_DEBUG2 = DEBUG2,
  PG_DEBUG1 = DEBUG1,
  PG_LOG = LOG,  // DO_NOT_TRANSFORM
  PG_LOG_SERVER_ONLY = LOG_SERVER_ONLY,
  PG_COMMERROR = PG_LOG_SERVER_ONLY,
  PG_INFO = INFO,
  PG_NOTICE = NOTICE,
  PG_WARNING = WARNING,
  PG_ERROR = ERROR,
  PG_FATAL = FATAL,
  PG_PANIC = PANIC,
} PgErrorLevel;

inline void PGAssertImpl(bool condition) {
  // Use the Assert macro in this context with the Postgres definition.
  Assert(condition);
}

// We reproduce disabling the Assert macro here so it has the same behavior as
// in the normal Postgres code - it only evaluates the condition if
// USE_ASSERT_CHECKING is defined.
#ifdef USE_ASSERT_CHECKING
#define PG_Assert(condition) PGAssertImpl(condition)
#else
#define PG_Assert(condition) ((void)true)
#endif

inline void PGEndCritSectionImpl() {
  // Use END_CRIT_SECTION in this context while Assert has the Postgres
  // definition.
  END_CRIT_SECTION();
}

#define PG_END_CRIT_SECTION() PGEndCritSectionImpl()

#endif  // POSTGRES_INCLUDES_PG_EXPORTS_H_
