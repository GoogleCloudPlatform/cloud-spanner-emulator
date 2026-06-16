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

#include "third_party/spanner_pg/datatypes/extended/pg_numeric_conversion_functions.h"

#include "googlesql/public/function.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"

namespace postgres_translator::spangres {
namespace datatypes {

const googlesql::Function* GetInt64ToPgNumericConversion() {
  static const googlesql::Function* kInt64ToPgNumericConv =
      new googlesql::Function("int64_to_pg_numeric_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kInt64ToPgNumericConv;
}

const googlesql::Function* GetDoubleToPgNumericConversion() {
  static const googlesql::Function* kDoubleToPgNumericConv =
      new googlesql::Function("double_to_pg_numeric_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kDoubleToPgNumericConv;
}

const googlesql::Function* GetFloatToPgNumericConversion() {
  static const googlesql::Function* kFloatToPgNumericConv =
      new googlesql::Function("float_to_pg_numeric_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kFloatToPgNumericConv;
}

const googlesql::Function* GetStringToPgNumericConversion() {
  static const googlesql::Function* kStringToPgNumericConv =
      new googlesql::Function("string_to_pg_numeric_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kStringToPgNumericConv;
}

const googlesql::Function* GetPgNumericToInt64Conversion() {
  static const googlesql::Function* kPgNumericToInt64Conv =
      new googlesql::Function("pg_numeric_to_int64_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastNumericToInt64)));
  return kPgNumericToInt64Conv;
}

const googlesql::Function* GetPgNumericToDoubleConversion() {
  static const googlesql::Function* kPgNumericToDoubleConv =
      new googlesql::Function(
          "pg_numeric_to_double_conv", "spanner", googlesql::Function::SCALAR,
          /*function_signatures=*/{},
          googlesql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(EvalCastNumericToDouble)));
  return kPgNumericToDoubleConv;
}

const googlesql::Function* GetPgNumericToFloatConversion() {
  static const googlesql::Function* kPgNumericToFloatConv =
      new googlesql::Function("pg_numeric_to_float_conv", "spanner",
                              googlesql::Function::SCALAR,
                              /*function_signatures=*/{},
                              googlesql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastNumericToFloat)));
  return kPgNumericToFloatConv;
}

const googlesql::Function* GetPgNumericToStringConversion() {
  static const googlesql::Function* kPgNumericToStringConv =
      new googlesql::Function(
          "pg_numeric_to_string_conv", "spanner", googlesql::Function::SCALAR,
          /*function_signatures=*/{},
          googlesql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(EvalCastNumericToString)));
  return kPgNumericToStringConv;
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres
