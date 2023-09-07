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

#ifndef SHIMS_MEMORY_CONTEXT_MANAGER_H_
#define SHIMS_MEMORY_CONTEXT_MANAGER_H_

#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"

typedef struct MemoryContextData* MemoryContext;

namespace postgres_translator {

// Represents a PG MemoryContext that is active in the current thread (i.e.
// occupies the relevant thread local variables within PG).
class ActiveMemoryContext {
 public:
  // Calls Clear(), logging and swallowing any non-ok status returned. See note
  // on Clear() about which thread it can be called in. Since we swallow any
  // failed status, calling this on a thread other than the one where this
  // instance was initialized can lead to leaks.
  ~ActiveMemoryContext();

  // Not copyable
  ActiveMemoryContext(const ActiveMemoryContext&) = delete;
  ActiveMemoryContext& operator=(const ActiveMemoryContext&) = delete;

  // Movable. In both cases, the target instance (either the move-constructed
  // one or the left side of the move assignment operator) are considered to
  // have been initialized on whatever thread the input instance was, regardless
  // of which thread invokes the move, and the input will no longer represent an
  // active memory context (i.e. IsActive() will return false).
  ActiveMemoryContext(ActiveMemoryContext&&) noexcept;
  ActiveMemoryContext& operator=(ActiveMemoryContext&&) noexcept;

  // Clear deletes the context completely, including metadata and leaves
  // CurrentMemoryContext nulled. After calling this,
  // MemoryContextManager::Init() must be called before using the PG memory
  // subsystem. This method has no effect once it has already been called
  // (idempotent). The first invocation must be on the same thread where this
  // object was initialized. If it is not on the same thread, and InternalError
  // status is returned and no action is performed. Subsequent calls, which do
  // nothing, can be on any thread.
  absl::Status Clear();

  // Reset resets the context, freeing all memory but the context metadata and
  // up to 1 kInitBlockSize block (to avoid excessively cycling malloc). Objects
  // allocated into the context previously are no longer valid. After resetting,
  // context is again ready for PostgreSQL operation. Must be called on the same
  // thread where this object was initialized. If it is not on the same thread,
  // and InternalError status is returned and no action is performed.
  absl::Status Reset();

  // Returns true iff this instance still represents an active memory context on
  // any thread.
  bool IsActive() const;

 private:
  // To be called only from MemoryContextManager::Init(...)
  ActiveMemoryContext();

  // Returns the internal state, which still represents allocated PG memory, and
  // leaves this object in a state as if Clear() had been called. The returned
  // state must start being managed by another ActiveMemoryContext instance to
  // avoid leaks.
  absl::optional<MemoryContext*> Release();

  // Implementation of Clear(), which just grabs the lock and calls this.
  absl::Status ClearLocked() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  // Returns an InternalError status if this object manages active state (i.e.
  // active_memory_context_.has_value()) and the calling thread is different
  // than the thread where that state was initialized, which means that Clear()
  // and Reset() operations are invalid.
  absl::Status CheckSameThread() ABSL_SHARED_LOCKS_REQUIRED(mu_);

  // Calls ClearLocked(), but logs and swallows any error status returned.
  void ClearAndLogErrors() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

  mutable absl::Mutex mu_;

  // If this optional contains no value, this instance does not manage any
  // active memory, as in the cases where Clear() was called or it was the input
  // to a move constructor/assignment. If it does contain a value, it is the
  // address of the thread-local CurrentMemoryContext on the thread where
  // initialization occurred. Thread-locals have a different address when
  // observed from each thread, so taking the address of CurrentMemoryContext
  // again and comparing the two tells us if this object is incorrectly being
  // used across threads. (Calling MemoryContextManager::Init() on one thread
  // and MemoryContextManager::Clear() on another is very bad.)
  absl::optional<MemoryContext*> active_memory_context_ ABSL_GUARDED_BY(mu_);

  friend class MemoryContextManager;
};

// MemoryContextManager is responsible for maintaining the PostgreSQL
// MemoryContext object and the CurrentMemoryContext thread-specific pointer
// used by the PostgreSQL allocators. Init() before invoking the PostgreSQL
// parser and analyzer. Note that this context must outlive any objects
// allocated by PostgreSQL (i.e. Query* trees returned by the analyzer). To free
// up memory but leave the context in place (i.e. for a parse-analyze loop),
// call Reset(). To completely remove the object and null the thread pointer,
// call Clear().
class MemoryContextManager {
 public:
  // Creates a default context named 'name' and sets CurrentMemoryContext to it.
  // Must be called before using the PostgreSQL memory subsystem.
  // `name` is used in debug information and must be statically allocated (or at
  // least outlive the context).
  // If a context already exists, does nothing and returns an error.
  static absl::StatusOr<ActiveMemoryContext> Init(const char* name);

  // Size constants for initial allocation amount, first block size, and largest
  // pooled block size allocated by this memory context.
  static constexpr size_t kMinContextSize = 0;
  static constexpr size_t kInitBlockSize = 8 * 1024;
  static constexpr size_t kMaxBlockSize = 8 * 1024 * 1024;

 private:
  // Reset resets the context, freeing all memory but the context metadata and
  // up to 1 kInitBlockSize block (to avoid excessively cycling malloc). Objects
  // allocated into the context previously are no longer valid. After resetting,
  // context is again ready for PostgreSQL operation.
  static absl::Status Reset();
  // Clear deletes the context completely, including metadata and leaves
  // CurrentMemoryContext nulled. After calling this, must call Init() before
  // using the PostgreSQL memory subsystem.
  static absl::Status Clear();
  // Internal helper function to create the default-spec context.
  static absl::StatusOr<MemoryContext> CreateDefaultMemoryContext(
      const char* name);

  friend class ActiveMemoryContext;
};

}  // namespace postgres_translator

#endif  // SHIMS_MEMORY_CONTEXT_MANAGER_H_
