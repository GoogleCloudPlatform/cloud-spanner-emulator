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

#ifndef INTERFACE_MEMORY_RESERVATION_MANAGER_H_
#define INTERFACE_MEMORY_RESERVATION_MANAGER_H_

#include <cstddef>

namespace postgres_translator {
namespace interfaces {

// MemoryReservationManager
//
// Abstract base class representing a class whose instances tell a given
// Spangres thread (parser instance, analyzer instance, etc)
// whether it is permitted to allocate more memory.  Spangres only uses this
// interface through a thread local, so implementations need not be thread-safe.
// TODO: Move this to the Spanner dev branch once it's stabilized.
class MemoryReservationManager {
 public:
  // Return true if an additional memory reservation of `size` is permitted.
  virtual bool TryAdditionalReserve(size_t size) = 0;

  virtual ~MemoryReservationManager() {}
};

}  // namespace interfaces
}  // namespace postgres_translator

#endif  // INTERFACE_MEMORY_RESERVATION_MANAGER_H_
