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

#include "zetasql/public/function.h"
#include "zetasql/public/type.pb.h"
#include "zetasql/base/no_destructor.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"

namespace postgres_translator::spangres::datatypes {

const zetasql::Function* GetPgOidToInt64Conversion() {
  static const zetasql_base::NoDestructor<zetasql::Function*> kPgOidToInt64Conv(
      new zetasql::Function("pg_oid_to_int64_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastOidToInt64)));
  return *kPgOidToInt64Conv;
}

const zetasql::Function* GetInt64ToPgOidConversion() {
  static const zetasql_base::NoDestructor<zetasql::Function*> kInt64ToPgOidConv(
      new zetasql::Function("int64_to_pg_oid_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastToOid)));
  return *kInt64ToPgOidConv;
}

const zetasql::Function* GetPgOidToStringConversion() {
  static const zetasql_base::NoDestructor<zetasql::Function*> kPgOidToStringConv(
      new zetasql::Function("pg_oid_to_string_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastOidToString)));
  return *kPgOidToStringConv;
}

const zetasql::Function* GetStringToPgOidConversion() {
  static const zetasql_base::NoDestructor<zetasql::Function*> kStringToPgOidConv(
      new zetasql::Function("string_to_pg_oid_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  postgres_translator::EvalCastToOid)));
  return *kStringToPgOidConv;
}

}  // namespace postgres_translator::spangres::datatypes
