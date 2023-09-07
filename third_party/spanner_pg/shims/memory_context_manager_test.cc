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

#include "third_party/spanner_pg/shims/memory_context_manager.h"

#include <cstddef>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/flags/flag.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/memory_reservation_holder.h"
#include "third_party/spanner_pg/shims/memory_shim_cc.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"

namespace postgres_translator {

namespace {

using zetasql_base::testing::StatusIs;

TEST(MemoryContextHolderTest, Simple) {
  // Bypass reservations to test only the Context and memory shim.
  auto res_manager = std::make_unique<StubMemoryReservationManager>();
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ASSERT_EQ(CurrentMemoryContext, nullptr);

  // Init and verify it creates a new context.
  ZETASQL_ASSERT_OK_AND_ASSIGN(ActiveMemoryContext context,
                       MemoryContextManager::Init("TestMemoryContext"));
  EXPECT_NE(CurrentMemoryContext, nullptr);

  // Try allocating with it.
  void* test_alloc = palloc(10);
  EXPECT_NE(test_alloc, nullptr);
  pfree(test_alloc);

  // Reset should clean up the current context.
  ZETASQL_ASSERT_OK(context.Reset());
  EXPECT_TRUE(MemoryContextIsEmpty(CurrentMemoryContext));

  // Try allocating with it again. This should still work.
  void* test_alloc_2 = palloc(10);
  EXPECT_NE(test_alloc_2, nullptr);
  pfree(test_alloc_2);

  // Clear should completely destroy the context and null the slot. We rely on
  // the leak checker a bit here too.
  ZETASQL_ASSERT_OK(context.Clear());
  EXPECT_EQ(CurrentMemoryContext, nullptr);
}

TEST(MemoryContextTest, ContextWithChild) {
  auto res_manager = std::make_unique<StubMemoryReservationManager>();
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ZETASQL_ASSERT_OK_AND_ASSIGN(ActiveMemoryContext context,
                       MemoryContextManager::Init("TestMemoryContext"));
  ASSERT_NE(CurrentMemoryContext, nullptr);

  // Try to create a child context and allocate from it.
  MemoryContext child = AllocSetContextCreate(
      CurrentMemoryContext, "test child context", ALLOCSET_DEFAULT_SIZES);
  ASSERT_TRUE(MemoryContextIsValid(child));
  EXPECT_NE(palloc(123), nullptr);

  // Make sure we can reset and also delete the child.
  MemoryContextReset(child);
  MemoryContextDelete(child);
}
}  // namespace
}  // namespace postgres_translator
