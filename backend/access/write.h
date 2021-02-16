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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACCESS_WRITE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACCESS_WRITE_H_

#include <ostream>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "backend/datamodel/key_set.h"
#include "backend/datamodel/value.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// MutationOpType enumerates the type of mutation operations.
enum class MutationOpType {
  kInsert,
  kUpdate,
  kInsertOrUpdate,
  kReplace,
  kDelete,
};

// MutationOp represents an operation within a mutation.
struct MutationOp {
  MutationOp() {}

  MutationOp(MutationOpType type, const std::string& table,
             std::vector<std::string>&& columns, std::vector<ValueList>&& rows)
      : type(type),
        table(table),
        columns(std::move(columns)),
        rows(std::move(rows)) {}

  MutationOp(MutationOpType type, const std::string& table,
             const KeySet& key_set)
      : type(type), table(table), key_set(key_set) {}

  std::string DebugString() const;

  // The type of the mutation operation.
  MutationOpType type;

  // The table on which the mutation operation operates.
  std::string table;

  // Mutation data for kInsert, kUpdate, kInsertOrUpdate, and kReplace.
  std::vector<std::string> columns;
  std::vector<ValueList> rows;

  // Mutation data for kDelete.
  KeySet key_set;
};

// Streams a debug string representation of MutationOp to out.
std::ostream& operator<<(std::ostream& out, const MutationOp& op);

// A Mutation represents a collection of mutation operations applied
// atomically.
class Mutation {
 public:
  // Get the current list of ops in the Mutation.
  const std::vector<MutationOp>& ops() const { return ops_; }

  // Adds an Insert, Update, InsertOrUpdate, or Replace MutationOp to
  // this Mutation.
  void AddWriteOp(MutationOpType type, const std::string& table,
                  std::vector<std::string> columns,
                  std::vector<ValueList> values);

  // Adds a Delete MutationOp to this Mutation.
  void AddDeleteOp(const std::string& table, const KeySet& key_set);

 private:
  std::vector<MutationOp> ops_;
};

// Streams a debug string representation of Mutation to out.
std::ostream& operator<<(std::ostream& out, const Mutation& mut);

// RowWriter defines an abstract interface for applying mutations to a database.
//
// This interface is used to decouple the query subsystem from the transaction
// subsystem. The query system deals with a RowWriter interface and is unaware
// of the underlying concrete transaction class.
class RowWriter {
 public:
  virtual ~RowWriter() {}

  // Write applies the mutation m to the database.
  virtual absl::Status Write(const Mutation& m) = 0;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACCESS_WRITE_H_
