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

#ifndef SHIMS_CATALOG_ADAPTER_HOLDER_H_
#define SHIMS_CATALOG_ADAPTER_HOLDER_H_

#include <memory>

#include "zetasql/public/catalog.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/catalog/engine_system_catalog.h"

namespace postgres_translator {

// A scoped holder class to manage the lifetime of a CatalogAdapter instance.
// The underlying instance is accessed through a thread-local pointer. After a
// holder object is constructed with a CatalogAdapter pointer, callers from the
// current thread can access the thread-local pointer through
// GetCatalogAdapter(). If the holder still owns the underlying CatalogAdapter
// instance when the holder goes out of scope, the instance is deleted as well,
// and calls to GetCatalogAdapter() will receive an error. In other words, if a
// CatalogAdapterHolder object is constructed in thread A, GetCatalogAdapter()
// will return a valid CatalogAdapter pointer in calls from thread A, and
// an error for calls from other threads.
//
// A user of CatalogAdapterHolder can decide to transfer ownership of the
// underlying CatalogAdapter instance to another object, such as a transformer.
// This can be done through ReleaseCatalogAdapter(). Upon transfer, the
// thread-local CatalogAdapter pointer will become nullptr.
//
// It is expected that there should exist only one CatalogAdapterHolder object
// at a time in a given thread. This also means that different threads can have
// different holders, as long as there is only one holder per thread.
//
// It is also expected that a holder is defined in a clearly defined scope, so
// that it is constructed and deconstructed in the same thread. If the holder is
// deconstructed in a different thread, the behavior is undefined.
// Example usage:
//
// // The thread-local catalog adapter is uninitialized.
// EXPECT_THAT(GetCatalogAdapter(),
//             StatusIs(absl::StatusCode::kInternal,
//                      HasSubstr("is not initialized")));
// zetasql::EnumerableCatalog* engine_provided_catalog =
//             GetSpangresTestCatalog();
// std::unique_ptr<EngineUserCatalog> engine_user_catalog =
//             std::make_unique<SpangresUserCatalog>(engine_provided_catalog);
// EngineSystemCatalog* engine_system_catalog = GetSpangresTestSystemCatalog()
// auto analyzer_options = GetSpangresTestAnalyzerOptions();
// {
//    ZETASQL_ASSIGN_OR_RETURN(auto holder,
//                     CatalogAdapterHolder::Create(
//                         std::move(engine_user_catalog),
//                         engine_system_catalog), analyzer_options);
//
//    // The thread-local catalog adapter is now initialized.
//    ZETASQL_ASSIGN_OR_RETURN(CatalogAdapter* catalog_adapter, GetCatalogAdapter());
//    // The thread-local ZetaSQL catalog is also initialized.
//    ZETASQL_RETURN_IF_ERROR(catalog_adapter->GetEngineUserCatalog()
//                    ->FindTable({"KeyValue"}, &key_value_table));
// }
// // The holder goes out of scope, the thread-local catalog adapter is now
// // cleared.
// EXPECT_THAT(GetCatalogAdapter(),
//             StatusIs(absl::StatusCode::kInternal,
//                      HasSubstr("is not initialized")));

class CatalogAdapterHolder {
 public:
  // A factory method to create a `CatalogAdapterHolder` object and let callers
  // owned it. This method will fail if `engine_user_catalog` or
  // `engine_system_catalog` are nullptrs, or if the thread-local catalog
  // adapter is already initialized.
  static absl::StatusOr<std::unique_ptr<CatalogAdapterHolder>> Create(
      std::unique_ptr<EngineUserCatalog> engine_user_catalog,
      EngineSystemCatalog* engine_system_catalog,
      const zetasql::AnalyzerOptions& analyzer_options,
      absl::flat_hash_map<int, int> token_locations = {});

  static absl::StatusOr<std::unique_ptr<CatalogAdapterHolder>> Create(
      std::unique_ptr<CatalogAdapter> adapter);
  ~CatalogAdapterHolder();

  // Releases ownership of the underlying CatalogAdapter instance and sets the
  // thread-local pointer to nullptr. Call sites own the instance after this
  // function returns and are expected to properly handle memory used by the
  // instance.
  std::unique_ptr<CatalogAdapter> ReleaseCatalogAdapter();

  // Disallow default constructor, copy/move constructors and operator, make
  // sure that there is no two holders concurrently holding on to a
  // CatalogAdapter object.
  CatalogAdapterHolder() = delete;
  CatalogAdapterHolder(const CatalogAdapterHolder& holder) = delete;
  CatalogAdapterHolder& operator=(const CatalogAdapterHolder&) = delete;
  CatalogAdapterHolder(CatalogAdapterHolder&& holder) = delete;
  CatalogAdapterHolder& operator=(CatalogAdapterHolder&&) = delete;

 private:
  // The holder should always be created through the following constructor,
  // holding on to the CatalogAdapter object used by the holder's callers.
  explicit CatalogAdapterHolder(std::unique_ptr<CatalogAdapter> adapter);

  // A catalog adapter owned by this holder. Its memory usage will be cleaned up
  // as the holder is destructed, unless it is released to a call site through
  // ReleaseCatalogAdapter(). This adapter is accessed through the thread-local
  // `catalog_adapter` pointer.
  std::unique_ptr<CatalogAdapter> catalog_adapter_ = nullptr;
};

// Helper function for easy access to the underlying adapter.
// This will return an error if there is no CatalogAdapter object registered for
// the current thread. Use a CatalogAdapterHolder object to register a
// CatalogAdapter object for this thread.
//
// Note that the returned pointer is not owned by the caller and should not be
// released. The pointer will remain valid for the duration of life of the
// managing CatalogAdapterHolder object. See the documentation for
// CatalogAdapterHolder for details.
absl::StatusOr<CatalogAdapter*> GetCatalogAdapter();

}  // namespace postgres_translator

#endif  // SHIMS_CATALOG_ADAPTER_HOLDER_H_
