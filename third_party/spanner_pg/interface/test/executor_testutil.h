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

#ifndef INTERFACE_TEST_EXECUTOR_TESTUTIL_H_
#define INTERFACE_TEST_EXECUTOR_TESTUTIL_H_

#include <string>

#include "absl/strings/string_view.h"

namespace postgres_translator {

// Performs stripping functionality on the AST strings that are written to the
// golden files.
std::string StripPgAstString(absl::string_view pg_ast_string);
std::string StripGsqlAstString(absl::string_view gsql_ast_string);

// Performs stripping functionality on the AST strings before comparing them.
// These transformed strings are not written to the golden files.
std::string StripPgDebugString(absl::string_view pg_debug_string);
std::string StripGsqlDebugString(absl::string_view gsql_debug_string);

}  // namespace postgres_translator

#endif  // INTERFACE_TEST_EXECUTOR_TESTUTIL_H_
