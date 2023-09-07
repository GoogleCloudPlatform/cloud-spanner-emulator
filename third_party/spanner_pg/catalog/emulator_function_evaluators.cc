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

#include "third_party/spanner_pg/catalog/emulator_function_evaluators.h"

#include <functional>
#include <memory>

#include "zetasql/public/function.h"
#include "zetasql/public/value.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/interface/pg_arena_factory.h"
#include "third_party/spanner_pg/interface/pg_timezone.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

zetasql::FunctionEvaluator PGFunctionEvaluator(
    const zetasql::FunctionEvaluator& function,
    const std::function<void()>& on_compute_end) {
  return [function, on_compute_end](absl::Span<const zetasql::Value> args)
             -> absl::StatusOr<zetasql::Value> {
    // Binds PG memory context arena to thread local
    ZETASQL_ASSIGN_OR_RETURN(
        std::unique_ptr<postgres_translator::interfaces::PGArena> pg_arena,
        postgres_translator::interfaces::CreatePGArena(nullptr));
    // Initializes the thread local timezone with the default timezone
    ZETASQL_RETURN_IF_ERROR(
        postgres_translator::interfaces::InitPGTimezone(kDefaultTimeZone));

    absl::StatusOr<zetasql::Value> result = function(args);

    // Call registered function for cleanup
    on_compute_end();
    // Clean up thread local timezone
    postgres_translator::interfaces::CleanupPGTimezone();

    return result;
  };
}

}  // namespace postgres_translator
