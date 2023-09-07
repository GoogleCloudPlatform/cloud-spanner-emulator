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

#include "third_party/spanner_pg/shims/timezone_helper.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/memory_reservation_holder.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"

namespace postgres_translator {
namespace {

TEST(TimezoneHelperTest, BasicSetup) {
  auto reservation_manager =
      std::make_unique<postgres_translator::StubMemoryReservationManager>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto memory_reservation_holder,
      MemoryReservationHolder::Create(reservation_manager.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context, MemoryContextManager::Init("test"));
  ASSERT_EQ(session_timezone, nullptr);
  ASSERT_EQ(log_timezone, nullptr);

  ZETASQL_ASSERT_OK(InitTimezone());
  ASSERT_NE(session_timezone, nullptr);
  ASSERT_NE(log_timezone, nullptr);
  CleanupTimezone();

  EXPECT_EQ(session_timezone, nullptr);
  EXPECT_EQ(log_timezone, nullptr);
  ZETASQL_EXPECT_OK(memory_context.Clear());
}

// Ensure that cleanup sets us up for another successful setup.
TEST(TimezoneHelperTest, RepeatedSetup) {
  auto reservation_manager =
      std::make_unique<postgres_translator::StubMemoryReservationManager>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto memory_reservation_holder,
      MemoryReservationHolder::Create(reservation_manager.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context, MemoryContextManager::Init("test"));

  ZETASQL_ASSERT_OK(InitTimezone());
  CleanupTimezone();
  ZETASQL_ASSERT_OK(InitTimezone());
  CleanupTimezone();

  ZETASQL_ASSERT_OK(memory_context.Clear());
}

using ::zetasql_base::testing::StatusIs;
extern "C" bool TimezoneCleared();
TEST(TimezoneHelperTest, FailedInitCleansUp) {
  auto reservation_manager =
      std::make_unique<postgres_translator::StubMemoryReservationManager>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto memory_reservation_holder,
      MemoryReservationHolder::Create(reservation_manager.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context, MemoryContextManager::Init("test"));

  ASSERT_THAT(TimeZoneScope::Create("no such time zone with this name"),
              StatusIs(absl::StatusCode::kInternal));

  EXPECT_EQ(session_timezone, nullptr);
  EXPECT_EQ(log_timezone, nullptr);
  EXPECT_TRUE(TimezoneCleared());
}

// Ensure low memory exception is converted to status code.
TEST(TimezoneHelperTest, LowMemory) {
  auto reservation_manager =
      std::make_unique<postgres_translator::StubMemoryReservationManager>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto memory_reservation_holder,
      MemoryReservationHolder::Create(reservation_manager.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context, MemoryContextManager::Init("test"));

  reservation_manager->SetAvailableMemory(1);

  ASSERT_THAT(InitTimezone(), StatusIs(absl::StatusCode::kResourceExhausted));
  CleanupTimezone();

  ASSERT_THAT(InitTimezone("UTC"),
              StatusIs(absl::StatusCode::kResourceExhausted));
  CleanupTimezone();

  ASSERT_THAT(InitTimezone("GMT"),
              StatusIs(absl::StatusCode::kResourceExhausted));
  CleanupTimezone();
}

TEST(TimezoneHelperTest, InitTimezoneOffset) {
  auto reservation_manager =
      std::make_unique<postgres_translator::StubMemoryReservationManager>();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      MemoryReservationHolder memory_reservation_holder,
      MemoryReservationHolder::Create(reservation_manager.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context, MemoryContextManager::Init("test"));
  ASSERT_EQ(session_timezone, nullptr);
  ASSERT_EQ(log_timezone, nullptr);

  ZETASQL_ASSERT_OK(InitTimezoneOffset(3600));
  ASSERT_NE(session_timezone, nullptr);
  ASSERT_NE(log_timezone, nullptr);
  CleanupTimezone();

  EXPECT_EQ(session_timezone, nullptr);
  EXPECT_EQ(log_timezone, nullptr);
  ZETASQL_EXPECT_OK(memory_context.Clear());
}
}  // namespace
}  // namespace postgres_translator
