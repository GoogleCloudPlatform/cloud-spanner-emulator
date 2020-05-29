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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ROW_CURSOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ROW_CURSOR_H_

#include "backend/access/read.h"
#include "backend/schema/catalog/table.h"
#include "backend/storage/in_memory_iterator.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// StorageIteratorRowCursor is an implementation of RowCursor that reads from
// storage using an array of storage iterators.
//
// This class is not thread-safe.
class StorageIteratorRowCursor : public RowCursor {
 public:
  StorageIteratorRowCursor(
      std::vector<std::unique_ptr<StorageIterator>> iterators,
      std::vector<const Column*> columns)
      : iterators_(std::move(iterators)), columns_(std::move(columns)) {}

  // Implementation of the RowCursor interface
  bool Next() override;
  absl::Status Status() const override;
  int NumColumns() const override;
  const std::string ColumnName(int i) const override;
  const zetasql::Value ColumnValue(int i) const override;
  const zetasql::Type* ColumnType(int i) const override;

 private:
  const std::vector<std::unique_ptr<StorageIterator>> iterators_;

  // Index of the iterator being read by row cursor in iterators_.
  int current_ = 0;

  // Set of columns to read.
  const std::vector<const Column*> columns_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_TRANSACTION_ROW_CURSOR_H_
