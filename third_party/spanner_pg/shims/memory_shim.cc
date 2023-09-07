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

#include "third_party/spanner_pg/shims/memory_shim.h"

#include <stddef.h>
#include <stdlib.h>

#include <memory>

#include "zetasql/base/logging.h"
#include "absl/flags/flag.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/memory_shim_cc.h"

// Thread-local pointer to Spanner's memory reservation structure so that we
// don't have to thread it through PostgreSQL and back.
namespace postgres_translator {
thread_local postgres_translator::interfaces::MemoryReservationManager*
    thread_memory_reservation;
}  // namespace postgres_translator

// Functions below are UN-NAMESPACED because they are 'extern "C"'
// so any namespace declaration would be ignored anyway.

// Request memory from Spanner. If granted, do the allocation and send it back
// to PostgreSQL. If not granted or the reserver is not available, deny the
// allocation.
extern "C" void* reserved_alloc(size_t size) {
  using postgres_translator::thread_memory_reservation;

  if (thread_memory_reservation &&
      thread_memory_reservation->TryAdditionalReserve(size)) {
    return malloc(size);
  } else {
    // Reservation denied or not available. Don't allocate.
    return nullptr;
  }
}

// Same as alloc, except for shrinking reallocs, just do the realloc. We will
// reclaim this reservation in bulk when the operation ends after translation.
extern "C" void* reserved_realloc(void* block, size_t old_size,
                                  size_t new_size) {
  using postgres_translator::thread_memory_reservation;

  int64_t size_diff = new_size - old_size;
  if (size_diff > 0) {
    if (thread_memory_reservation &&
        thread_memory_reservation->TryAdditionalReserve(size_diff)) {
      return realloc(block, new_size);
    } else {
      // Reservation denied or not available. Don't allocate.
      return nullptr;
    }
  } else {
    // Shrinking the allocation, no permission needed.
    return realloc(block, new_size);
  }
}

