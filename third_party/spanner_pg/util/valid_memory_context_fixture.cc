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

#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

#include "gmock/gmock.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "third_party/spanner_pg/shims/timezone_helper.h"

namespace postgres_translator::test {

ValidMemoryContext::ValidMemoryContext()
    : reservation_manager_(
          std::make_unique<
              postgres_translator::StubMemoryReservationManager>()),
      reservation_holder_(postgres_translator::MemoryReservationHolder::Create(
          reservation_manager_.get())) {
  ABSL_CHECK(reservation_holder_.ok());
}

void ValidMemoryContext::SetUp() {
  memory_context_ = postgres_translator::MemoryContextManager::Init(
      "ValidMemoryContext test");
  ZETASQL_ASSERT_OK(memory_context_);
  ZETASQL_ASSERT_OK(InitTimezone());
}

void ValidMemoryContext::TearDown() {
  CleanupTimezone();
  if (memory_context_.ok()) {
    ZETASQL_ASSERT_OK(memory_context_->Clear());
  }
}

}  // namespace postgres_translator::test
