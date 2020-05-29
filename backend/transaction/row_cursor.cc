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

#include "backend/transaction/row_cursor.h"

#include "backend/storage/iterator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

bool StorageIteratorRowCursor::Next() {
  // Loop over underlying iterators until a Next value is obtained.
  while (current_ < iterators_.size()) {
    if (iterators_.at(current_)->Next()) {
      // Current iterator yeilded a next element.
      return true;
    }
    if (iterators_.at(current_)->Status().ok()) {
      // Current iterator is exhausted, try next iterator.
      current_ += 1;
      continue;
    }
    return false;
  }
  // End of all iterators.
  return false;
}

absl::Status StorageIteratorRowCursor::Status() const {
  if (current_ >= iterators_.size()) {
    return absl::OkStatus();
  }
  return iterators_.at(current_)->Status();
}

const zetasql::Value StorageIteratorRowCursor::ColumnValue(int i) const {
  auto value = iterators_.at(current_)->ColumnValue(i);
  if (!value.is_valid()) {
    return zetasql::Value::Null(ColumnType(i));
  }
  return value;
}

int StorageIteratorRowCursor::NumColumns() const { return columns_.size(); }

const std::string StorageIteratorRowCursor::ColumnName(int i) const {
  return columns_.at(i)->Name();
}

const zetasql::Type* StorageIteratorRowCursor::ColumnType(int i) const {
  return columns_.at(i)->GetType();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
