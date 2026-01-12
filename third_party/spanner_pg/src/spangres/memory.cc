#include "third_party/spanner_pg/src/spangres/memory.h"

#include <stddef.h>
#include <stdlib.h>

#include <cstdint>

#include "absl/log/log.h"
#include "third_party/spanner_pg/interface/memory_reservation_manager.h"
#include "third_party/spanner_pg/src/spangres/memory_cc.h"

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

