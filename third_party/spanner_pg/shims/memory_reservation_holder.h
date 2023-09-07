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

#ifndef SHIMS_MEMORY_RESERVATION_HOLDER_H_
#define SHIMS_MEMORY_RESERVATION_HOLDER_H_

#include <memory>

#include "absl/status/statusor.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"

namespace postgres_translator {

// This class is a scoped holder class.  It manages
// `thread_memory_reservation` thread-local variable, one of the
// thread-local variables required by the PostgreSQL Parser and Analyzer.
//
// Only one instance of this class may be on the stack at a time.
//
// See the comment in `memory_shim_cc.h` for details about
// what `thread_memory_reservation` does.
class MemoryReservationHolder {
 public:
  // Construct a MemoryReservationHolder.
  // MemoryReservationHolder's constructor can fail.  Force the failure to be
  // handled by the caller by wrapping the result of construction in a StatusOr.
  static absl::StatusOr<MemoryReservationHolder> Create(
      interfaces::MemoryReservationManager* manager);

  // Required for transferring ownership of MemoryReservationHolder objects.
  // (For example, when returning as part of a StatusOr<>.)
  MemoryReservationHolder(MemoryReservationHolder&& other);
  ~MemoryReservationHolder();

  // This can be implemented if we find that we need it.
  MemoryReservationHolder& operator=(const MemoryReservationHolder& other) =
      delete;

 private:
  explicit MemoryReservationHolder(
      interfaces::MemoryReservationManager* manager);

  // If our ownership of the thread-local has been transferred via a
  // move constructor, our destructor shouldn't try to restore it.
  bool owns_thread_memory_reservation_;
};

}  // namespace postgres_translator

#endif  // SHIMS_MEMORY_RESERVATION_HOLDER_H_
