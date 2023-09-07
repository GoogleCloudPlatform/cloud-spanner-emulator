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

#ifndef SHIMS_MEMORY_CONTEXT_PG_ARENA_H_
#define SHIMS_MEMORY_CONTEXT_PG_ARENA_H_

#include <memory>

#include "third_party/spanner_pg/interface/pg_arena.h"
#include "third_party/spanner_pg/shims/memory_context_manager.h"
#include "third_party/spanner_pg/shims/memory_reservation_holder.h"
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {
namespace spangres {

// Implementation of PGArena that initializes and clears a MemoryContext via an
// internal ActiveMemoryContext object.
class MemoryContextPGArena : public interfaces::PGArena {
 public:
  MemoryContextPGArena(std::unique_ptr<interfaces::MemoryReservationManager>
                           memory_reservation_manager,
                       MemoryReservationHolder memory_reservation_holder,
                       ActiveMemoryContext memory_context)
      : PGArena(std::move(memory_reservation_manager)),
        memory_reservation_holder_(std::move(memory_reservation_holder)),
        memory_context_(std::move(memory_context)) {}

  ~MemoryContextPGArena() override = default;

  static absl::StatusOr<std::unique_ptr<MemoryContextPGArena>> Init(
      std::unique_ptr<interfaces::MemoryReservationManager>
          memory_reservation_manager) {
    if (memory_reservation_manager == nullptr) {
      memory_reservation_manager =
          std::make_unique<StubMemoryReservationManager>();
    }
    ZETASQL_ASSIGN_OR_RETURN(
        MemoryReservationHolder holder,
        MemoryReservationHolder::Create(memory_reservation_manager.get()));
    ZETASQL_ASSIGN_OR_RETURN(ActiveMemoryContext context,
                     MemoryContextManager::Init("MemoryContextPGArena"));
    return std::make_unique<MemoryContextPGArena>(
        std::move(memory_reservation_manager), std::move(holder),
        std::move(context));
  }

 private:
  MemoryReservationHolder memory_reservation_holder_;
  ActiveMemoryContext memory_context_;
};

}  // namespace spangres
}  // namespace postgres_translator

#endif  // SHIMS_MEMORY_CONTEXT_PG_ARENA_H_
