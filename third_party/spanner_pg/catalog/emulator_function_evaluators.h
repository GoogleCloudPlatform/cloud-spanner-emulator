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

#ifndef CATALOG_EMULATOR_FUNCTION_EVALUATORS_H_
#define CATALOG_EMULATOR_FUNCTION_EVALUATORS_H_

#include <cstdint>
#include <functional>

#include "zetasql/public/function.h"
#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"

namespace postgres_translator {

// The default timezone used by the emulator query engine.
inline constexpr char kDefaultTimeZone[] = "America/Los_Angeles";

zetasql::FunctionEvaluator PGFunctionEvaluator(
    const zetasql::FunctionEvaluator& function,
    const std::function<void()>& on_compute_end = []() {});

// This function calls `F_JSONB_ARRAY_ELEMENT_TEXT` to compute the results on
// arguments `jsonb` and `element`. The function returns `Value::String`
// (`Value::NullString` to represent a SQL null).
absl::StatusOr<zetasql::Value> EmulatorJsonbArrayElementText(
    absl::string_view jsonb, int32_t element);

// This function calls `F_JSONB_OBJECT_FIELD_TEXT` to compute the results on
// arguments `jsonb` and `key`. The function returns `Value::String`
// (`Value::NullString` to represent a SQL null).
absl::StatusOr<zetasql::Value> EmulatorJsonbObjectFieldText(
    absl::string_view jsonb, absl::string_view key);

}  // namespace postgres_translator

#endif  // CATALOG_EMULATOR_FUNCTION_EVALUATORS_H_
