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

#include "zetasql/base/logging.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/shims/memory_shim_cc.h"
#include "zetasql/base/status_builder.h"

namespace postgres_translator {

absl::StatusOr<MemoryReservationHolder> MemoryReservationHolder::Create(
    interfaces::MemoryReservationManager* manager) {
  if (thread_memory_reservation) {
    return zetasql_base::InternalErrorBuilder()
           << "Nesting MemoryReservationHolders is not supported";
  }

  return MemoryReservationHolder(manager);
}

MemoryReservationHolder::MemoryReservationHolder(
    interfaces::MemoryReservationManager* manager)
    : owns_thread_memory_reservation_(true) {
  // `thread_memory_reservation` is being used in a specific and rather
  // unusual way.  See the comment next to its forward declaration for context.
  thread_memory_reservation = manager;
}

MemoryReservationHolder::MemoryReservationHolder(
    MemoryReservationHolder&& other)
    : owns_thread_memory_reservation_(true) {
  other.owns_thread_memory_reservation_ = false;
}

MemoryReservationHolder::~MemoryReservationHolder() {
  if (owns_thread_memory_reservation_) {
    thread_memory_reservation = nullptr;
  }
}

}  // namespace postgres_translator
