//
// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "backend/schema/catalog/versioned_catalog.h"

#include <memory>

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/time/time.h"
#include "common/errors.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

VersionedCatalog::VersionedCatalog() {
  schemas_[absl::InfinitePast()] = absl::make_unique<const Schema>();
}

VersionedCatalog::VersionedCatalog(
    std::unique_ptr<const Schema> initial_schema) {
  schemas_[absl::InfinitePast()] = std::move(initial_schema);
}

const Schema* VersionedCatalog::GetSchema(absl::Time timestamp) const {
  absl::MutexLock lock(&mu_);
  auto itr = schemas_.upper_bound(timestamp);
  itr--;
  return itr->second.get();
}

const Schema* VersionedCatalog::GetLatestSchema() const {
  return GetSchema(absl::InfiniteFuture());
}

absl::Status VersionedCatalog::AddSchema(absl::Time creation_time,
                                         std::unique_ptr<const Schema> schema) {
  absl::MutexLock lock(&mu_);
  ZETASQL_RET_CHECK(creation_time > schemas_.rbegin()->first)
      << "Failed to insert schema at " << absl::FormatTime(creation_time)
      << ": the latest schema creation timestamp is "
      << absl::FormatTime(schemas_.rbegin()->first);
  schemas_[creation_time] = std::move(schema);
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
