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

#include "zetasql/public/function.h"
#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"
#include "third_party/spanner_pg/catalog/emulator_functions.h"

namespace postgres_translator::spangres {
namespace datatypes {

const zetasql::Function* GetInt64ToPgNumericConversion() {
  static const zetasql::Function* kInt64ToPgNumericConv =
      new zetasql::Function("int64_to_pg_numeric_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kInt64ToPgNumericConv;
}

const zetasql::Function* GetDoubleToPgNumericConversion() {
  static const zetasql::Function* kDoubleToPgNumericConv =
      new zetasql::Function("double_to_pg_numeric_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kDoubleToPgNumericConv;
}

const zetasql::Function* GetFloatToPgNumericConversion() {
  static const zetasql::Function* kFloatToPgNumericConv =
      new zetasql::Function("float_to_pg_numeric_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kFloatToPgNumericConv;
}

const zetasql::Function* GetStringToPgNumericConversion() {
  static const zetasql::Function* kStringToPgNumericConv =
      new zetasql::Function("string_to_pg_numeric_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastToNumeric)));
  return kStringToPgNumericConv;
}

const zetasql::Function* GetPgNumericToInt64Conversion() {
  static const zetasql::Function* kPgNumericToInt64Conv =
      new zetasql::Function("pg_numeric_to_int64_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastNumericToInt64)));
  return kPgNumericToInt64Conv;
}

const zetasql::Function* GetPgNumericToDoubleConversion() {
  static const zetasql::Function* kPgNumericToDoubleConv =
      new zetasql::Function(
          "pg_numeric_to_double_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(EvalCastNumericToDouble)));
  return kPgNumericToDoubleConv;
}

const zetasql::Function* GetPgNumericToFloatConversion() {
  static const zetasql::Function* kPgNumericToFloatConv =
      new zetasql::Function("pg_numeric_to_float_conv", "spanner",
                              zetasql::Function::SCALAR,
                              /*function_signatures=*/{},
                              zetasql::FunctionOptions().set_evaluator(
                                  PGFunctionEvaluator(EvalCastNumericToFloat)));
  return kPgNumericToFloatConv;
}

const zetasql::Function* GetPgNumericToStringConversion() {
  static const zetasql::Function* kPgNumericToStringConv =
      new zetasql::Function(
          "pg_numeric_to_string_conv", "spanner", zetasql::Function::SCALAR,
          /*function_signatures=*/{},
          zetasql::FunctionOptions().set_evaluator(
              PGFunctionEvaluator(EvalCastNumericToString)));
  return kPgNumericToStringConv;
}

}  // namespace datatypes
}  // namespace postgres_translator::spangres
