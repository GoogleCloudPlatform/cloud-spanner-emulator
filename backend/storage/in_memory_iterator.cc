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

#include "backend/storage/in_memory_iterator.h"

#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

FixedRowStorageIterator::FixedRowStorageIterator(std::vector<Row>&& rows)
    : rows_(std::move(rows)) {}

bool FixedRowStorageIterator::Next() { return ++pos_ < rows_.size(); }

absl::Status FixedRowStorageIterator::Status() const {
  return absl::OkStatus();
}

const Key& FixedRowStorageIterator::Key() const { return rows_[pos_].first; }

const std::vector<zetasql::Value>& FixedRowStorageIterator::GetColumns()
    const {
  return rows_[pos_].second;
}

int FixedRowStorageIterator::NumColumns() const { return GetColumns().size(); }

const zetasql::Value& FixedRowStorageIterator::ColumnValue(int i) const {
  return GetColumns()[i];
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
