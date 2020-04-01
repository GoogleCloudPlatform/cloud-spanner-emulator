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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_IDS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_IDS_H_

#include <cstddef>
#include <cstdlib>
#include <string>

#include "absl/base/thread_annotations.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

template <typename IdType>
class UniqueIdGenerator {
 public:
  UniqueIdGenerator() : next_seq_(0) {}
  explicit UniqueIdGenerator(int64_t starting_seq) : next_seq_(starting_seq) {}

  // Generate the next unique ID.
  IdType NextId(absl::string_view prefix) ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return IdType{absl::StrCat(prefix, ":", next_seq_++)};
  }

  // Generate the next unique ID.
  IdType NextId() ABSL_LOCKS_EXCLUDED(mu_) {
    absl::MutexLock lock(&mu_);
    return IdType{next_seq_++};
  }

 private:
  absl::Mutex mu_;
  int64_t next_seq_ ABSL_GUARDED_BY(mu_);
};

// Unique identifier associated with a table. TableID is guaranteed to be unique
// within a single database.
using TableID = std::string;

// A TableID generator. Each database has a single TableIDGenerator.
using TableIDGenerator = UniqueIdGenerator<TableID>;

// Unique identifier associated with a column. ColumnID is guaranteed to be
// unique within a single table.
using ColumnID = std::string;

// A ColumnID generator. Each table has a single ColumnIDGenerator.
using ColumnIDGenerator = UniqueIdGenerator<ColumnID>;

// Unique identifier associated with a transaction.
using TransactionID = int64_t;

// A TransactionID generator. Each database has a single TransactionIDGenerator.
using TransactionIDGenerator = UniqueIdGenerator<TransactionID>;

// The priority associated with a transaction.
using TransactionPriority = int64_t;

// A sentinel transaction ID which will never be assigned to valid transactions.
constexpr TransactionID kInvalidTransactionID = -1;

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_IDS_H_
