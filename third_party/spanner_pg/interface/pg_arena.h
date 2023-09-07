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

#ifndef INTERFACE_PG_ARENA_H_
#define INTERFACE_PG_ARENA_H_

#include <memory>

#include "third_party/spanner_pg/interface/memory_reservation_manager.h"

namespace postgres_translator {
namespace interfaces {

// Represents a memory arena that the PostgreSQL parser, analyzer, or parser
// output deserializer uses to allocate objects against.  Those objects are only
// valid during the lifetime of the PGArena they were allocated against.
//
// Each instance also owns an instance of MemoryReservationManager by which
// Spangres will request permission from the caller to allocate memory.
class PGArena {
 public:
  explicit PGArena(
      std::unique_ptr<MemoryReservationManager> memory_reservation_manager)
      : memory_reservation_manager_(std::move(memory_reservation_manager)) {}

  virtual ~PGArena() = default;

  // Not copyable or movable.  Meant as an abstract base class; should be
  // accessed via pointers.
  PGArena(const PGArena&) = delete;
  PGArena& operator=(const PGArena&) = delete;
  PGArena(PGArena&&) = delete;
  PGArena& operator=(PGArena&&) = delete;

  MemoryReservationManager* memory_reservation_manager() {
    return memory_reservation_manager_.get();
  }

 private:
  std::unique_ptr<MemoryReservationManager> memory_reservation_manager_;
};

}  // namespace interfaces
}  // namespace postgres_translator

#endif  // INTERFACE_PG_ARENA_H_
