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

#include "third_party/spanner_pg/catalog/catalog_adapter_holder.h"

#include <memory>
#include <utility>

#include "zetasql/public/catalog.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator {

// Pointer to the actual CatalogAdapter object. It should always be initialized
// through a scoped CatalogAdapterHolder object, and retrieved through
// the helper function below.
static thread_local CatalogAdapter* thread_local_catalog_adapter = nullptr;

absl::StatusOr<CatalogAdapter*> GetCatalogAdapter() {
  if (thread_local_catalog_adapter == nullptr) {
    return absl::InternalError(
        "CatalogAdapter is not initialized on this thread.");
  }
  return thread_local_catalog_adapter;
}

absl::StatusOr<std::unique_ptr<CatalogAdapterHolder>>
CatalogAdapterHolder::Create(
    std::unique_ptr<EngineUserCatalog> engine_user_catalog,
    EngineSystemCatalog* engine_system_catalog,
    const zetasql::AnalyzerOptions& analyzer_options,
    absl::flat_hash_map<int, int> token_locations) {
  if (engine_user_catalog == nullptr) {
    return absl::InternalError(
        "The catalog adapter holder needs to be created with a valid ZetaSQL "
        "catalog instance");
  }

  if (engine_system_catalog == nullptr) {
    return absl::InternalError(
        "The catalog adapter holder needs to be created with a valid "
        "EngineSystemCatalog instance");
  }

  // Multiple holders cannot co-exist, so make sure the
  // thread_local_catalog_adapter pointer is currently null, i.e. it's not being
  // managed by any other holder.
  if (thread_local_catalog_adapter) {
    return absl::InternalError(
        "The thread-local catalog adapter is already initialized");
  }
  ZETASQL_ASSIGN_OR_RETURN(auto adapter,
                   CatalogAdapter::Create(
                       std::move(engine_user_catalog), engine_system_catalog,
                       analyzer_options, std::move(token_locations)));

  // Use WrapUnique() to return a unique pointer from this factory method,
  // as CatalogAdapterHolder has a private constructor.
  return absl::WrapUnique(new CatalogAdapterHolder(std::move(adapter)));
}

absl::StatusOr<std::unique_ptr<CatalogAdapterHolder>>
CatalogAdapterHolder::Create(std::unique_ptr<CatalogAdapter> adapter) {
  if (adapter == nullptr) {
    return absl::InternalError(
        "The catalog adapter holder needs to be created with a valid "
        "CatalogAdapter instance");
  }

  // Multiple holders cannot co-exist, so make sure the
  // thread_local_catalog_adapter pointer is currently null, i.e. it's not being
  // managed by any other holder.
  if (thread_local_catalog_adapter) {
    return absl::InternalError(
        "The thread-local catalog adapter is already initialized");
  }
  // Use WrapUnique() to return a unique pointer from this factory method,
  // as CatalogAdapterHolder has a private constructor.
  return absl::WrapUnique(new CatalogAdapterHolder(std::move(adapter)));
}

std::unique_ptr<CatalogAdapter> CatalogAdapterHolder::ReleaseCatalogAdapter() {
  // Prevent the thread-local from being dangling
  thread_local_catalog_adapter = nullptr;
  return absl::WrapUnique(catalog_adapter_.release());
}

CatalogAdapterHolder::CatalogAdapterHolder(
    std::unique_ptr<CatalogAdapter> adapter)
    : catalog_adapter_(std::move(adapter)) {
  // Transfer ownership of `adapter` to the current holder, and set the
  // thread-local to reference the underlying `CatalogAdapter` object.
  thread_local_catalog_adapter = catalog_adapter_.get();
}

CatalogAdapterHolder::~CatalogAdapterHolder() {
  // Prevent the thread-local from being dangling
  thread_local_catalog_adapter = nullptr;
}

}  // namespace postgres_translator
