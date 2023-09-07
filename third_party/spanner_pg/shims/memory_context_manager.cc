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

#include "third_party/spanner_pg/shims/memory_context_manager.h"

#include "zetasql/base/logging.h"
#include "absl/status/status.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "absl/utility/utility.h"
#include "third_party/spanner_pg/postgres_includes/all.h"
#include "third_party/spanner_pg/shims/error_shim.h"
#include "zetasql/base/logging.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_builder.h"

extern "C" {
// Internal PG function for freeing memory inside PG's thread-local freelists
void AsetDeleteFreelists(void);
}

namespace postgres_translator {

// Convert a generic error message into one that is specific to MemoryContext
// initialization failure.
//
// Creating the initial MemoryContext is a special case where an OOM can prevent
// us from reporting the real error since much of ereport depends on allocating.
// This happens often in systests (deliberately).
static zetasql_base::StatusBuilder FailedMemoryContextCreation(
    zetasql_base::StatusBuilder builder) {
  if (builder.code() == absl::StatusCode::kInternal) {
    // Log the original error just in case, but usually it's not informative.
    absl::Status status =
        builder.LogError()
        << "error occurred during MemoryContextManager::Init; assuming OOM and "
           "converting to Resource Exhausted";
    // New message matches what Postgres would report for any other OOM.
    return zetasql_base::ResourceExhaustedErrorBuilder() << "out of memory";
  } else {
    // Another error is unlikely here, but we won't encode that assumption.
    return builder;
  }
}

absl::StatusOr<ActiveMemoryContext> MemoryContextManager::Init(
    const char* name) {
  ZETASQL_RET_CHECK_EQ(CurrentMemoryContext, nullptr)
      << "Memory context already present in slot.";
  ZETASQL_RET_CHECK_EQ(TopMemoryContext, nullptr)
      << "Memory context already present in top slot.";
  ZETASQL_ASSIGN_OR_RETURN(CurrentMemoryContext, CreateDefaultMemoryContext(name),
                   _.With(FailedMemoryContextCreation));
  TopMemoryContext = CurrentMemoryContext;
  ZETASQL_RET_CHECK_NE(CurrentMemoryContext, nullptr)
      << "Failed to create CurrentMemoryContext.";

  return ActiveMemoryContext();
}

// CacheMemoryContext is initialized on-demand. Clear it if necessary.
static void DeleteCacheMemoryContext() {
  if (MemoryContext temp_cache = CacheMemoryContext; temp_cache != nullptr) {
    CacheMemoryContext = nullptr;
    MemoryContextDelete(temp_cache);
  }
}

absl::Status MemoryContextManager::Reset() {
  ZETASQL_RET_CHECK_NE(CurrentMemoryContext, nullptr) << "No memory context in slot.";

  DeleteCacheMemoryContext();  // Must delete because it lives inside Current.
  CurrentMemoryContext = TopMemoryContext;  // Reset to the original context.
  MemoryContextReset(CurrentMemoryContext);
  return absl::OkStatus();
}

absl::Status MemoryContextManager::Clear() {
  ZETASQL_RET_CHECK_NE(CurrentMemoryContext, nullptr) << "No memory context in slot.";

  DeleteCacheMemoryContext();
  MemoryContext context = TopMemoryContext;
  CurrentMemoryContext = nullptr;
  TopMemoryContext = nullptr;
  MemoryContextDelete(context);

  // Clear thread-local freelists as well
  AsetDeleteFreelists();

  return absl::OkStatus();
}

absl::StatusOr<MemoryContext> MemoryContextManager::CreateDefaultMemoryContext(
    const char* name) {
  // Create a memory context for this test to allocate against with default
  // settings.
  return CheckedPgAllocSetContextCreateInternal(nullptr, name, kMinContextSize,
                                                kInitBlockSize, kMaxBlockSize);
}

ActiveMemoryContext::ActiveMemoryContext() {
  absl::MutexLock lock(&mu_);
  active_memory_context_ = &CurrentMemoryContext;
}

ActiveMemoryContext::~ActiveMemoryContext() {
  absl::MutexLock lock(&mu_);
  ClearAndLogErrors();
}

absl::Status ActiveMemoryContext::CheckSameThread() {
  if (!active_memory_context_.has_value()) {
    return absl::OkStatus();
  }

  return *active_memory_context_ == &CurrentMemoryContext
             ? absl::OkStatus()
             : absl::InternalError(
                   "attempting to use ActiveMemoryContext on a different "
                   "fiber "
                   "from where it was initialized");
}

void ActiveMemoryContext::ClearAndLogErrors() {
  auto status = ClearLocked();
  if (!status.ok()) {
    ABSL_LOG(ERROR) << "MemoryContext cleanup failed: " << status.message();
  }
}

ActiveMemoryContext::ActiveMemoryContext(ActiveMemoryContext&& other) noexcept {
  // Either order works, since nothing else can possibly be trying to grab the
  // lock for this->mu_ yet.
  absl::MutexLock my_lock(&mu_);
  absl::MutexLock other_lock(&other.mu_);
  active_memory_context_ = other.active_memory_context_;
  other.active_memory_context_ = absl::nullopt;
}

absl::optional<MemoryContext*> ActiveMemoryContext::Release() {
  absl::MutexLock lock(&mu_);
  absl::optional<MemoryContext*> ctx(active_memory_context_);
  active_memory_context_ = absl::nullopt;
  return ctx;
}

ActiveMemoryContext& ActiveMemoryContext::operator=(
    ActiveMemoryContext&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  absl::optional<MemoryContext*> other_ctx = other.Release();
  absl::MutexLock lock(&mu_);
  ClearAndLogErrors();
  active_memory_context_ = other_ctx;
  return *this;
}

absl::Status ActiveMemoryContext::Clear() {
  absl::MutexLock lock(&mu_);
  return ClearLocked();
}

absl::Status ActiveMemoryContext::ClearLocked() {
  ZETASQL_RETURN_IF_ERROR(CheckSameThread());

  if (!active_memory_context_.has_value()) {
    return absl::OkStatus();
  }

  active_memory_context_ = absl::nullopt;
  return MemoryContextManager::Clear();
}

absl::Status ActiveMemoryContext::Reset() {
  absl::ReaderMutexLock lock(&mu_);

  ZETASQL_RETURN_IF_ERROR(CheckSameThread());

  if (!active_memory_context_.has_value()) {
    return absl::OkStatus();
  }

  return MemoryContextManager::Reset();
}

bool ActiveMemoryContext::IsActive() const {
  absl::ReaderMutexLock lock(&mu_);
  return active_memory_context_.has_value();
}

}  // namespace postgres_translator
