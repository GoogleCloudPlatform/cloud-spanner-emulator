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


#include "backend/database/schema_cache.h"

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"
#include "common/feature_flags.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Appended with its length, so that no two lists of parts make the same key.
void AppendPart(std::string_view part, std::string* key) {
  const uint64_t size = part.size();
  key->append(reinterpret_cast<const char*>(&size), sizeof(size));
  key->append(part);
}

}  // namespace

absl::StatusOr<std::unique_ptr<const Schema>> BuiltSchema::CloneFor(
    std::string_view database_id) const {
  absl::MutexLock lock(&mu_);
  return CloneSchema(*schema_, database_id);
}

// What became of the build of one key. Shared by the cache, the builder and
// the callers waiting for it, so it outlives its place in the cache.
class SchemaCache::Entry {
 public:
  enum class State { kBuilding, kBuilt, kNotShareable, kFailed };

  // Ends the build. `built` is set only for kBuilt.
  void Finish(State state, std::shared_ptr<const BuiltSchema> built)
      ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    state_ = state;
    built_ = std::move(built);
  }

  bool Finished() ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return state_ != State::kBuilding;
  }

  // Waits for the build to end. Null unless it ended as kBuilt.
  std::shared_ptr<const BuiltSchema> Await() ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    mu_.Await(absl::Condition(
        +[](State* state) { return *state != State::kBuilding; }, &state_));
    return built_;
  }

 private:
  absl::Mutex mu_;
  State state_ ABSL_GUARDED_BY(mu_) = State::kBuilding;
  std::shared_ptr<const BuiltSchema> built_ ABSL_GUARDED_BY(mu_);
};

SchemaCache& SchemaCache::Instance() {
  static SchemaCache* instance = new SchemaCache();
  return *instance;
}

bool SchemaCache::Enabled() {
  // Read on every create rather than once, so that a test can turn it off and
  // on; a create costs far more than the lookup.
  return std::getenv("SPANNER_EMULATOR_DISABLE_SCHEMA_CACHE") == nullptr;
}

std::string SchemaCache::KeyFor(const SchemaChangeOperation& operation) {
  // Every feature flag, whether or not the schema updater reads it today.
  const EmulatorFeatureFlags::Flags flags =
      EmulatorFeatureFlags::instance().flags();
  static_assert(
      std::has_unique_object_representations_v<EmulatorFeatureFlags::Flags>,
      "the flags are part of the key as bytes, so they must have no padding");

  std::string key;
  const int64_t dialect = operation.database_dialect;
  AppendPart(
      std::string_view(reinterpret_cast<const char*>(&dialect), sizeof(dialect)),
      &key);
  AppendPart(
      std::string_view(reinterpret_cast<const char*>(&flags), sizeof(flags)),
      &key);
  AppendPart(operation.proto_descriptor_bytes, &key);
  for (const std::string& statement : operation.statements) {
    AppendPart(statement, &key);
  }
  return key;
}

SchemaCache::Lookup SchemaCache::Acquire(const std::string& key) {
  std::shared_ptr<Entry> entry;
  {
    absl::MutexLock lock(&mu_);
    for (auto it = entries_.begin(); it != entries_.end(); ++it) {
      if (it->first == key) {
        entry = it->second;
        entries_.splice(entries_.begin(), entries_, it);
        break;
      }
    }
    if (entry == nullptr) {
      entry = std::make_shared<Entry>();
      entries_.emplace_front(key, entry);
      // An entry pushed out while it is being built stays whole: its builder
      // and its waiters hold it, and only later callers no longer find it.
      while (entries_.size() > kMaxEntries) {
        entries_.pop_back();
      }
      return Lookup{.build = std::make_unique<Build>(this, std::move(entry))};
    }
  }

  std::shared_ptr<const BuiltSchema> built = entry->Await();
  if (built == nullptr) {
    return Lookup{};
  }
  absl::MutexLock lock(&mu_);
  ++stats_.adopted;
  return Lookup{.built = std::move(built)};
}

void SchemaCache::Build::Publish(std::shared_ptr<const BuiltSchema> built) {
  entry_->Finish(Entry::State::kBuilt, std::move(built));
  absl::MutexLock lock(&cache_->mu_);
  ++cache_->stats_.built;
}

void SchemaCache::Build::MarkNotShareable() {
  entry_->Finish(Entry::State::kNotShareable, nullptr);
  absl::MutexLock lock(&cache_->mu_);
  ++cache_->stats_.not_shareable;
}

SchemaCache::Build::~Build() {
  if (entry_->Finished()) {
    return;
  }
  // The build failed. The entry goes first, so that the next caller builds
  // anew instead of finding a failure that may not repeat; the callers already
  // waiting build their own.
  cache_->Remove(entry_);
  entry_->Finish(Entry::State::kFailed, nullptr);
}

void SchemaCache::Remove(const std::shared_ptr<Entry>& entry) {
  absl::MutexLock lock(&mu_);
  entries_.remove_if(
      [&entry](const auto& item) { return item.second == entry; });
}

SchemaCache::Stats SchemaCache::stats() const {
  absl::MutexLock lock(&mu_);
  return stats_;
}

void SchemaCache::Clear() {
  absl::MutexLock lock(&mu_);
  entries_.clear();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
