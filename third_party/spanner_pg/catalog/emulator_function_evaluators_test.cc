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

#include <cstdint>
#include <memory>

#include "zetasql/public/function.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "third_party/spanner_pg/interface/datetime_evaluators.h"
#include "third_party/spanner_pg/interface/pg_timezone.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace {

using ::postgres_translator::CheckedPgPalloc;
using ::zetasql_base::testing::IsOkAndHolds;

absl::StatusOr<zetasql::Value> EvalPgAlloc(
    const absl::Span<const zetasql::Value> arguments) {
  absl::StatusOr<void*> result = CheckedPgPalloc(sizeof(int));

  if (!result.ok()) {
    return result.status();
  }

  return zetasql::values::Int64(123);
}

absl::StatusOr<zetasql::Value> EvalCastToTimestamp(
  absl::Span<const zetasql::Value> args) {
ZETASQL_ASSIGN_OR_RETURN(absl::Time time, function_evaluators::PgTimestamptzIn(
                                      args[0].string_value()));
return zetasql::Value::Timestamp(time);
}

TEST(PGFunctionEvaluators, SetUpPGMemoryArena) {
  zetasql::FunctionEvaluator evaluator = PGFunctionEvaluator(EvalPgAlloc);

  EXPECT_THAT(evaluator({zetasql::values::Int64(1)}),
              IsOkAndHolds(zetasql::values::Int64(123)));
}

TEST(PGFunctionEvaluators, CallsGivenCleanupFunction) {
  bool function_called = false;
  zetasql::FunctionEvaluator evaluator =
      PGFunctionEvaluator(EvalPgAlloc, InitializePGTimezoneToDefault,
                          [&function_called]() { function_called = true; });

  EXPECT_THAT(evaluator({zetasql::values::Int64(1)}),
              IsOkAndHolds(zetasql::values::Int64(123)));
  EXPECT_TRUE(function_called);
}

TEST(PGFunctionEvaluators, TestTimeZoneDefault) {
  zetasql::FunctionEvaluator evaluator =
      PGFunctionEvaluator(EvalCastToTimestamp);

  EXPECT_THAT(evaluator({zetasql::values::String("2008-12-25 15:30:00")}),
              IsOkAndHolds(zetasql::values::Timestamp(
                  // 1230247800 is the unix timestamp for 2008-12-25T23:30:00+00
                  absl::FromUnixSeconds(1230247800))));
}

TEST(PGFunctionEvaluators, TestTimeZoneUTC) {
  auto initialize_pg_timezone = [&]() {
    absl::Status status = interfaces::InitPGTimezone("UTC");
  };
  zetasql::FunctionEvaluator evaluator =
      PGFunctionEvaluator(EvalCastToTimestamp, initialize_pg_timezone, []() {});

  EXPECT_THAT(evaluator({zetasql::values::String("2008-12-25 15:30:00")}),
              IsOkAndHolds(zetasql::values::Timestamp(
                  // 1230219000 is the unix timestamp for 2008-12-25T15:30:00+00
                  absl::FromUnixSeconds(1230219000))));
}
}  // namespace
}  // namespace postgres_translator

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
