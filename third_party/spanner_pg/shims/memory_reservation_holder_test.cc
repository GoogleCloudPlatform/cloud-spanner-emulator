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

#include "third_party/spanner_pg/shims/memory_reservation_holder.h"

#include <memory>
#include <string>
#include <vector>

#include "zetasql/base/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/types/optional.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/memory_shim_cc.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"

namespace postgres_translator {

namespace {

using ::zetasql_base::testing::StatusIs;

class FakeMemoryReservationManager
    : public interfaces::MemoryReservationManager {
 public:
  explicit FakeMemoryReservationManager(int64_t available_memory)
      : available_memory_(available_memory) {}

  bool TryAdditionalReserve(size_t size) override {
    if (reserved_memory_ + size <= available_memory_) {
      reserved_memory_ += size;
      return true;
    } else {
      reserved_memory_ = 0;
      return false;
    }
  }

  int64_t GetReservedBytes() const { return reserved_memory_; }

 private:
  int64_t available_memory_;
  int64_t reserved_memory_ = 0;
};

class TrackedMemoryReservationTest : public testing::Test {
 public:
  static constexpr int kBlockSize = MemoryContextManager::kInitBlockSize;
  static constexpr int kAllocatorOverhead = 8192;
};

TEST(MemoryReservationTest, SetsThreadLocal) {
  // Should not initially be set
  EXPECT_EQ(thread_memory_reservation, nullptr);

  {
    auto res_manager = std::make_unique<StubMemoryReservationManager>();
    auto res_holder = MemoryReservationHolder::Create(res_manager.get());
    // The current implementation of MemoryReservationHolder should set the
    // thread-local variable `thread_memory_reservation` to point to a memory
    // reservation manager
    ZETASQL_ASSERT_OK(res_holder);
    EXPECT_NE(thread_memory_reservation, nullptr);
  }

  // Make sure the pointer is reset when the holder goes out of scope
  EXPECT_EQ(thread_memory_reservation, nullptr);
}

// Simple series of palloc calls within the reservation amount. Verifies that
// the allocations succeed and Spanner tracks the reserved amount.
TEST_F(TrackedMemoryReservationTest, SimpleReservation) {
  // Create a reservation tracker with memory for first block plus allocator
  // overhead.
  auto res_manager = std::make_unique<FakeMemoryReservationManager>(
      kAllocatorOverhead + kBlockSize * 2);
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ZETASQL_ASSERT_OK(res_holder);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context,
                       MemoryContextManager::Init("TestMemoryContext"));
  EXPECT_EQ(res_manager->GetReservedBytes(), kAllocatorOverhead);

  // Ensure we can allocate a few small pieces of memory. The first allocation
  // should fit into the existing 8k block, but the second should force a new
  // block to be allocated.
  void* mem_small = palloc(8);
  EXPECT_NE(mem_small, nullptr);
  EXPECT_EQ(res_manager->GetReservedBytes(), kAllocatorOverhead);
  // Try a larger piece of memory that should require allocating a new block.
  // Note that the allocator already allocated one block of size kBlockSize,
  // so it's going to double its next allocation size.
  void* mem_large = palloc(kBlockSize);
  EXPECT_NE(mem_large, nullptr);
  EXPECT_EQ(res_manager->GetReservedBytes(),
            kAllocatorOverhead + kBlockSize * 2);
  // Free should work and have no effect on the reservation logic.
  pfree(mem_large);
  EXPECT_EQ(res_manager->GetReservedBytes(),
            kAllocatorOverhead + kBlockSize * 2);
  // Test cleanup to insure hermiticity.
  ZETASQL_ASSERT_OK(memory_context.Clear());
}

// Test that palloc fails as expected when it asks for too much memory. Test
// that the reservation mechanism still reflects only granted requests.
TEST_F(TrackedMemoryReservationTest, RejectedReservation) {
  // Create a reservation tracker with memory for allocator overhead only. The
  // first Spangres request should try (and fail) to allocate an 8kiB block.
  auto res_manager =
      std::make_unique<FakeMemoryReservationManager>(kAllocatorOverhead);
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ZETASQL_ASSERT_OK(res_holder);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context,
                       MemoryContextManager::Init("TestMemoryContext"));
  EXPECT_EQ(res_manager->GetReservedBytes(), kAllocatorOverhead);
  // This allocation should trigger a new block request and fail. The
  // rejection request revokes our full grant, reducing the reserved size to
  // 0.
  EXPECT_FALSE(CheckedPgPalloc(kBlockSize).ok());
  EXPECT_EQ(res_manager->GetReservedBytes(), 0);
  // This allocation should trigger a new 100MB block request and fail. No new
  // grant is created.
  EXPECT_FALSE(CheckedPgPalloc(100 * 1024 * 1024).ok());
  EXPECT_EQ(res_manager->GetReservedBytes(), 0);
  // Also try other allocation functions.
  EXPECT_FALSE(CheckedPgPalloc0(kBlockSize).ok());
  EXPECT_FALSE(CheckedPgPalloc0fast(kBlockSize).ok());
  EXPECT_FALSE(CheckedPgPallocExtended(kBlockSize, MCXT_ALLOC_ZERO).ok());
  EXPECT_FALSE(
      CheckedPgMemoryContextAlloc(CurrentMemoryContext, kBlockSize).ok());
  EXPECT_FALSE(
      CheckedPgMemoryContextAllocZero(CurrentMemoryContext, kBlockSize).ok());
  EXPECT_FALSE(
      CheckedPgMemoryContextAllocZeroAligned(CurrentMemoryContext, kBlockSize)
          .ok());
  EXPECT_FALSE(
      CheckedPgMemoryContextAllocHuge(CurrentMemoryContext, kBlockSize).ok());
  EXPECT_EQ(res_manager->GetReservedBytes(), 0);

  // Test cleanup to insure hermiticity.
  ZETASQL_ASSERT_OK(memory_context.Clear());
}

// Addendum to the above test for realloc because it needs a valid pointer.
TEST_F(TrackedMemoryReservationTest, RejectedReallocReservation) {
  // Create a reservation tracker with memory for allocator overhead + a small
  // allocation only. The first small request should fit into the overhead block
  // and succeed. The second request (to realloc that block larger than fits in
  // the overhead) should try to allocate a new chunk and fail.
  auto res_manager =
      std::make_unique<FakeMemoryReservationManager>(kAllocatorOverhead);
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ZETASQL_ASSERT_OK(res_holder);

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context,
                       MemoryContextManager::Init("TestMemoryContext"));
  EXPECT_EQ(res_manager->GetReservedBytes(), kAllocatorOverhead);
  // This allocation should not trigger a new block request.
  ZETASQL_ASSERT_OK_AND_ASSIGN(void* pointer, CheckedPgPalloc(1));
  EXPECT_EQ(res_manager->GetReservedBytes(), kAllocatorOverhead);
  // This allocation should trigger a new block request and fail. No new
  // grant is created and the old one is revoked.
  EXPECT_FALSE(CheckedPgRepalloc(pointer, kBlockSize + kBlockSize).ok());
  EXPECT_EQ(res_manager->GetReservedBytes(), 0);

  // Test cleanup to insure hermiticity.
  ZETASQL_ASSERT_OK(memory_context.Clear());
}

// Test that the PostgreSQL parser correctly uses spanner memory reservations.
TEST_F(TrackedMemoryReservationTest, Parser) {
  // Create a reservation tracker with plenty of memory for parsing.
  auto res_manager = std::make_unique<FakeMemoryReservationManager>(100 * 1024);
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ZETASQL_ASSERT_OK(res_holder);
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto memory_context,
                       MemoryContextManager::Init("TestMemoryContext"));

  int64_t memory_accum = 0;  // Used to separate memory used for each parse.

  // Invoke the parser on a few simple strings and see that it works.
  std::vector<std::string> test_cases{"select 1;", "select 4 + 5;",
                                      "select a from t;",
                                      "select t.a, s.b, sum(t.c) from t "
                                      "inner join s on t.d=s.d group by t.a, "
                                      "s.b order by s.b, t.a;"};
  for (const auto& test_case : test_cases) {
    List* parse_tree = raw_parser(test_case.c_str(), RAW_PARSE_DEFAULT);
    EXPECT_NE(parse_tree, nullptr);  // Parser dies if it can't allocate.
    std::string parse_tree_str =
        pretty_format_node_dump(nodeToString(parse_tree));
    int64_t new_mem =
        res_manager->GetReservedBytes() - kAllocatorOverhead - memory_accum;
    ZETASQL_VLOG(1) << "Test case string:\n" << test_case;
    ZETASQL_VLOG(1) << "Parse tree:\n" << parse_tree_str;
    ZETASQL_VLOG(1) << "Additional memory used on this parse: " << new_mem;
    memory_accum += new_mem;
  }
  // This test gets into the 2nd double-sizing chunk, thus uses 8+16kB of
  // user-space memory plus the overhead of the memory system.
  EXPECT_EQ(res_manager->GetReservedBytes(),
            ((8 + 16) * 1024) + kAllocatorOverhead);

  // Test cleanup to insure hermiticity.
  ZETASQL_ASSERT_OK(memory_context.Clear());
}

// Failing to initialize the MemoryContextManager should not terminate
// the server.
TEST_F(TrackedMemoryReservationTest, MemoryContextInitFailure) {
  // Create a reservation tracker without any memory. The MemoryContext init
  // should fail.
  auto res_manager = std::make_unique<StubMemoryReservationManager>(
      /*available_memory=*/1);
  auto res_holder = MemoryReservationHolder::Create(res_manager.get());
  ZETASQL_ASSERT_OK(res_holder);
  EXPECT_EQ(CurrentMemoryContext, nullptr);
  EXPECT_THAT(MemoryContextManager::Init("TestMemoryContext"),
              StatusIs(absl::StatusCode::kResourceExhausted));
}

}  // namespace
}  // namespace postgres_translator
