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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_ITERATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_ITERATOR_H_

#include <vector>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "backend/datamodel/key.h"
#include "backend/storage/iterator.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// FixedRowStorageIterator is an in-memory implementation of the
// StorageIterator.
//
// All rows are buffered in-memory and yielded in same order as specified. This
// class is not thread-safe.
class FixedRowStorageIterator : public StorageIterator {
 public:
  using Row = std::pair<class Key, std::vector<zetasql::Value>>;

  FixedRowStorageIterator() = default;
  explicit FixedRowStorageIterator(std::vector<Row>&& rows);

  // Implementation of the StorageIterator interface.
  bool Next() override;
  absl::Status Status() const override;
  const class Key& Key() const override;
  int NumColumns() const override;
  const zetasql::Value& ColumnValue(int i) const override;

 private:
  const std::vector<zetasql::Value>& GetColumns() const;

  // Memory buffer of rows.
  const std::vector<Row> rows_;

  // Current index of the iterator.
  int pos_ = -1;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_IN_MEMORY_ITERATOR_H_
