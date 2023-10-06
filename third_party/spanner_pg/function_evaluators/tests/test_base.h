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

#ifndef FUNCTION_EVALUATORS_TEST_BASE_H_
#define FUNCTION_EVALUATORS_TEST_BASE_H_

#include <cstdint>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"

namespace postgres_translator::function_evaluators {

static absl::StatusOr<std::unique_ptr<spangres::MemoryContextPGArena>>
SetUpPgMemoryArena() {
  return spangres::MemoryContextPGArena::Init(
      std::make_unique<StubMemoryReservationManager>());
}

class PgEvaluatorTest : public testing::Test {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(pg_arena_, SetUpPgMemoryArena());
  }

 private:
  std::unique_ptr<spangres::MemoryContextPGArena> pg_arena_;
};

template <typename T>
class PgEvaluatorTestWithParam : public testing::TestWithParam<T> {
 protected:
  void SetUp() override {
    ZETASQL_ASSERT_OK_AND_ASSIGN(pg_arena_, SetUpPgMemoryArena());
  }

 private:
  std::unique_ptr<spangres::MemoryContextPGArena> pg_arena_;
};

}  // namespace postgres_translator::function_evaluators

#endif  // FUNCTION_EVALUATORS_TEST_BASE_H_
