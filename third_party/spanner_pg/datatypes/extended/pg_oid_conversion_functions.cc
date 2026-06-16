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

#include "third_party/spanner_pg/datatypes/extended/pg_oid_conversion_functions.h"

#include "googlesql/public/function.h"
#include "googlesql/public/type.pb.h"
#include "googlesql/base/no_destructor.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"

namespace postgres_translator::spangres::datatypes {

const googlesql::Function* GetPgOidToInt64Conversion() {
  static const googlesql_base::NoDestructor<googlesql::Function*> kPgOidToInt64Conv(
      new googlesql::Function("pg_oid_to_int64_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastOidToInt64)));
  return *kPgOidToInt64Conv;
}

const googlesql::Function* GetInt64ToPgOidConversion() {
  static const googlesql_base::NoDestructor<googlesql::Function*> kInt64ToPgOidConv(
      new googlesql::Function("int64_to_pg_oid_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastToOid)));
  return *kInt64ToPgOidConv;
}

const googlesql::Function* GetPgOidToStringConversion() {
  static const googlesql_base::NoDestructor<googlesql::Function*> kPgOidToStringConv(
      new googlesql::Function("pg_oid_to_string_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastOidToString)));
  return *kPgOidToStringConv;
}

const googlesql::Function* GetStringToPgOidConversion() {
  static const googlesql_base::NoDestructor<googlesql::Function*> kStringToPgOidConv(
      new googlesql::Function("string_to_pg_oid_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastToOid)));
  return *kStringToPgOidConv;
}

}  // namespace postgres_translator::spangres::datatypes
