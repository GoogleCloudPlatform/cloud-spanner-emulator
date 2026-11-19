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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <utility>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/utils.h"
#include "backend/schema/catalog/schema.h"
#include "googlesql/base/ret_check.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::StatusOr<absl::Duration> ParseVersionRetentionPeriod(
    absl::string_view version_retention_period) {
  int64_t retention_period = ParseSchemaTimeSpec(version_retention_period);

  if (retention_period <= 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid retention period: ", version_retention_period));
  } else if (retention_period > (7 * 24 * 60 * 60)) {
    return absl::InvalidArgumentError(
        absl::StrCat("Retention period must be less than 7 days: ",
                     version_retention_period));
  } else {
    if (retention_period < 60 * 60) {
      ABSL_LOG(WARNING) << "Retention period has been set to less than 1 hour. "
                   << "This is only supported in the emulator. "
                   << "This is not supported by Spanner.";
    }
    return absl::Seconds(retention_period);
  }
}

std::optional<absl::Duration>& VersionRetentionOverride() {
  static std::optional<absl::Duration> retention = [] {
    const char* value = std::getenv("SPANNER_EMULATOR_VERSION_RETENTION_S");
    std::optional<absl::Duration> parsed =
        VersionRetentionOverrideFromEnvValue(value);
    if (parsed.has_value()) {
      ABSL_LOG(INFO) << "Keeping versions for at most "
                     << absl::FormatDuration(*parsed)
                     << " (SPANNER_EMULATOR_VERSION_RETENTION_S)";
    } else if (value != nullptr && *value != '\0') {
      ABSL_LOG(ERROR) << "Ignoring SPANNER_EMULATOR_VERSION_RETENTION_S=" << value
                      << ": not a whole number of seconds of at least 1";
    }
    return parsed;
  }();
  return retention;
}

}  // namespace

std::optional<absl::Duration> VersionRetentionOverrideFromEnvValue(
    const char* value) {
  if (value == nullptr || *value == '\0') return std::nullopt;
  char* end = nullptr;
  const long long seconds = std::strtoll(value, &end, 10);
  if (end == value || *end != '\0' || seconds < 1) return std::nullopt;
  return absl::Seconds(seconds);
}

void VersionedCatalog::SetVersionRetentionOverrideForTesting(
    std::optional<absl::Duration> retention) {
  VersionRetentionOverride() = retention;
}

absl::Duration VersionedCatalog::version_retention_period() const {
  absl::MutexLock lock(mu_);
  const std::optional<absl::Duration>& retention = VersionRetentionOverride();
  if (retention.has_value() && *retention < version_retention_period_) {
    return *retention;
  }
  return version_retention_period_;
}

VersionedCatalog::VersionedCatalog() {
  schemas_[absl::InfinitePast()] = std::make_unique<const Schema>();
}

VersionedCatalog::VersionedCatalog(
    std::unique_ptr<const Schema> initial_schema) {
  schemas_[absl::InfinitePast()] = std::move(initial_schema);
}

const Schema* VersionedCatalog::GetSchema(absl::Time timestamp) const {
  absl::MutexLock lock(mu_);
  auto itr = schemas_.upper_bound(timestamp);
  itr--;
  return itr->second.get();
}

const Schema* VersionedCatalog::GetLatestSchema() const {
  return GetSchema(absl::InfiniteFuture());
}

absl::Status VersionedCatalog::AddSchema(absl::Time creation_time,
                                         std::unique_ptr<const Schema> schema) {
  absl::MutexLock lock(mu_);
  GOOGLESQL_RET_CHECK(creation_time > schemas_.rbegin()->first)
      << "Failed to insert schema at " << absl::FormatTime(creation_time)
      << ": the latest schema creation timestamp is "
      << absl::FormatTime(schemas_.rbegin()->first);
  auto version_retention_period =
      ParseVersionRetentionPeriod(schema->version_retention_period());
  if (!version_retention_period.ok()) {
    return version_retention_period.status();
  }
  version_retention_period_ = *version_retention_period;
  schemas_[creation_time] = std::move(schema);
  return absl::OkStatus();
}

void VersionedCatalog::RemoveExpiredSchemas(absl::Time timestamp) {
  const absl::Duration retention = version_retention_period();
  absl::MutexLock lock(mu_);
  auto upper_bound = schemas_.upper_bound(timestamp - retention);
  auto it = ++schemas_.begin();  // Skip the infinite past schema.
  while (it != upper_bound) {
    auto next = it;
    if (++next == upper_bound) {
      // The current schema needs to be kept to cover the retention period.
      break;
    }
    it = schemas_.erase(it);
  }
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
