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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_ITERATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_ITERATOR_H_

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "backend/datamodel/key.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// StorageIterator allows sequential iteration through the rows returned
// from a call to Storage::Read().
//
// Rows are guaranteed to be yielded in sorted order of the key.
//
// Each iterator position has three states:
//    State                     Next()       Status()
//    Row is available          true         ok
//    No more rows available    false        ok
//    An error while yielding   false        error status
//
// For concurrent access by multiple threads, an external lock must be used.
//
// Usage:
//    std::unique_ptr<StorageIterator> itr;
//    ZETASQL_RETURN_IF_ERROR(storage.Read(..., &itr)) << "Failed to read from storage."
//    while (itr->Next()) {
//      const Key& key = itr->Key();
//      for (int i = 0; i < itr->NumColumns(); i++) {
//        const zetasql::Value& value = itr->Value(i);
//      }
//    }
//    ZETASQL_RETURN_IF_ERROR(itr->Status()) << "Failed to read rows."
class StorageIterator {
 public:
  virtual ~StorageIterator() {}

  // Returns true on successfully advancing to the next row. Returns false if
  // there are no rows or if there is an error. Use Status() to distinguish
  // between these two cases.
  virtual bool Next() = 0;

  // Returns error status for error caused while yielding rows.
  virtual absl::Status Status() const = 0;

  // Returns the key associated with the current row. A call to Key() is only
  // valid if a previous call to Next() returned true.
  virtual const class Key& Key() const = 0;

  // Returns the number of columns in this row.
  virtual int NumColumns() const = 0;

  // Returns the column value associated with the current row at the specified
  // index. A call to ColumnValue() is only valid if a previous call to Next()
  // returned true.
  //
  // REQUIRES: 0 <= i < NumColumns()
  virtual const zetasql::Value& ColumnValue(int i) const = 0;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_STORAGE_ITERATOR_H_
