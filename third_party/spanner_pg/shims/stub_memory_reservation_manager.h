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

#ifndef SHIMS_STUB_MEMORY_RESERVATION_MANAGER_H_
#define SHIMS_STUB_MEMORY_RESERVATION_MANAGER_H_

#include "third_party/spanner_pg/interface/memory_reservation_manager.h"

namespace postgres_translator {

// This class is a MemoryReservationManager that simply returns that it's always
// ok to reserve more memory.
// It's intended for use in tests and in other environments where we need to
// run code that supports memory reservations, but we have no need to
// manage or constrain memory usage.
class StubMemoryReservationManager
    : public interfaces::MemoryReservationManager {
 public:
  explicit StubMemoryReservationManager(int64_t available_memory)
      : available_memory_(available_memory) {}

  explicit StubMemoryReservationManager() : available_memory_(0) {}

  // TryAdditionalReserve checks to see if there's enough memory left in the
  // memory pool for a malloc to succeed. This stub version assumes that every
  // check results in a subsequent malloc which will not be freed since we have
  // no visibility into the actual malloc/free system. If available_memory_ is
  // zero then no limits are set.
  bool TryAdditionalReserve(size_t size) override {
    if (available_memory_ == 0) {
      // Track the amount of memory asked for, even if we aren't limiting it.
      reserved_memory_ += size;
      return true;
    }
    if (reserved_memory_ + size <= available_memory_) {
      reserved_memory_ += size;
      return true;
    } else {
      reserved_memory_ = 0;
      return false;
    }
  }

  // SetAvailableMemory changes the amount of memory in the fake memory
  // allocation pool.
  void SetAvailableMemory(int64_t available_memory) {
    available_memory_ = available_memory;
  }

 private:
  int64_t available_memory_;
  int64_t reserved_memory_ = 0;
};

}  // namespace postgres_translator

#endif  // SHIMS_STUB_MEMORY_RESERVATION_MANAGER_H_
