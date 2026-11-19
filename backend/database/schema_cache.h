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


#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_SCHEMA_CACHE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_SCHEMA_CACHE_H_

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "googlesql/public/types/type_factory.h"
#include "absl/base/thread_annotations.h"
#include "absl/status/statusor.h"
#include "absl/synchronization/mutex.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/updater/schema_updater.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// The schema one database was created with, kept so that a database created
// from the same statements copies it instead of building its own.
//
// Building a schema applies its statements one by one, and each statement
// clones and validates the whole schema graph: seconds for a few hundred
// statements. A copy is one clone of the graph.
//
// Nothing here is handed to a database as it is. A database gets its own copy
// of the schema nodes, so later DDL on it, its query engine and whatever that
// keeps per node never meet another database's. What the copies do share is
// the column types, which the type factory of the database that built the
// schema owns; so this holds that factory, and a database that adopts the
// schema holds this.
class BuiltSchema {
 public:
  // `schema` must share no node with a schema a database uses, and its types
  // must be owned by `type_factory` or be GoogleSQL's static ones. The three
  // numbers are where the builder's ID generators stood when it was done.
  BuiltSchema(std::shared_ptr<googlesql::TypeFactory> type_factory,
              std::unique_ptr<const Schema> schema, int64_t next_table_id,
              int64_t next_column_id, int64_t next_queue_id)
      : next_table_id_(next_table_id),
        next_column_id_(next_column_id),
        next_queue_id_(next_queue_id),
        type_factory_(std::move(type_factory)),
        schema_(std::move(schema)) {}

  // A copy of the schema for the database `database_id`, valid for as long as
  // this object lives.
  absl::StatusOr<std::unique_ptr<const Schema>> CloneFor(
      std::string_view database_id) const ABSL_LOCKS_EXCLUDED(mu_);

  // Where a database that adopts the schema must set its ID generators, so
  // that the tables and columns later DDL adds to it get the IDs they would
  // have got had it built the schema itself.
  int64_t next_table_id() const { return next_table_id_; }
  int64_t next_column_id() const { return next_column_id_; }
  int64_t next_queue_id() const { return next_queue_id_; }

 private:
  const int64_t next_table_id_;
  const int64_t next_column_id_;
  const int64_t next_queue_id_;

  // Declared before the schema, so that the types outlive the columns.
  const std::shared_ptr<googlesql::TypeFactory> type_factory_;

  // One copy at a time. The schema is never modified, so this guards nothing
  // known; a copy takes milliseconds and a create is rare.
  mutable absl::Mutex mu_;
  const std::unique_ptr<const Schema> schema_;
};

// The schemas of the last few distinct CreateDatabase requests, for the whole
// process.
//
// A test run starts several JVMs that each create a database from the same
// statements on one emulator, at the same moment. The first request to arrive
// builds; the others wait for it and copy its schema.
//
// Only a schema whose build left nothing belonging to the one database is
// kept. The database that builds decides that from what the build produced
// (see Database::Create) and reports it through its `Build`.
class SchemaCache {
 public:
  // How many distinct statement lists are remembered. An entry that is pushed
  // out is freed once the last database that adopted it is dropped.
  static constexpr size_t kMaxEntries = 4;

  static SchemaCache& Instance();

  // False when the process runs with SPANNER_EMULATOR_DISABLE_SCHEMA_CACHE
  // set: every database then builds its own schema.
  static bool Enabled();

  // Everything the schema built from `operation` depends on: the dialect, the
  // feature flags, the proto descriptors and every statement, byte for byte.
  static std::string KeyFor(const SchemaChangeOperation& operation);

  class Entry;

  // The duty to build the schema of a key, held by the one caller of
  // `Acquire` that found nothing. The callers that arrive meanwhile wait for
  // it to end one of three ways: `Publish`, `MarkNotShareable`, or being
  // destroyed with neither, which is how a failed build ends.
  class Build {
   public:
    Build(SchemaCache* cache, std::shared_ptr<Entry> entry)
        : cache_(cache), entry_(std::move(entry)) {}
    Build(const Build&) = delete;
    Build& operator=(const Build&) = delete;
    ~Build();

    // The schema is built and can be copied: waiting and later callers adopt
    // `built`.
    void Publish(std::shared_ptr<const BuiltSchema> built);

    // The schema is built but belongs to its database alone: waiting and
    // later callers build their own, without waiting for one another.
    void MarkNotShareable();

   private:
    SchemaCache* cache_;
    std::shared_ptr<Entry> entry_;
  };

  // At most one of the two is set. With `built`, adopt it. With `build`, build
  // and report. With neither, build and report nothing.
  struct Lookup {
    std::shared_ptr<const BuiltSchema> built;
    std::unique_ptr<Build> build;
  };

  // Finds the schema of `key`, waiting while another caller builds it.
  Lookup Acquire(const std::string& key) ABSL_LOCKS_EXCLUDED(mu_);

  struct Stats {
    // Schemas published.
    int64_t built = 0;
    // Lookups answered with a published schema.
    int64_t adopted = 0;
    // Builds reported as belonging to their database alone.
    int64_t not_shareable = 0;
  };
  Stats stats() const ABSL_LOCKS_EXCLUDED(mu_);

  // Forgets every entry. Databases that adopted one keep it alive.
  void Clear() ABSL_LOCKS_EXCLUDED(mu_);

 private:
  SchemaCache() = default;

  // Takes `entry` out, if it is still in.
  void Remove(const std::shared_ptr<Entry>& entry) ABSL_LOCKS_EXCLUDED(mu_);

  mutable absl::Mutex mu_;
  // Most recently used first.
  std::list<std::pair<std::string, std::shared_ptr<Entry>>> entries_
      ABSL_GUARDED_BY(mu_);
  Stats stats_ ABSL_GUARDED_BY(mu_);
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATABASE_SCHEMA_CACHE_H_
