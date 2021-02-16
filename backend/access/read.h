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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACCESS_READ_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACCESS_READ_H_

#include <ostream>
#include <string>
#include <vector>

#include "zetasql/public/type.h"
#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "backend/datamodel/key_set.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ReadArg specifies a read request for a single database table.
//
// key_set is allowed to have overlapping keys and ranges. Each unique row that
// belongs to the key set will only be returned once. Rows are returned in key
// order. If index is non-empty, key_set refers to index keys and not table keys
// and rows are returned in index-key order. The keys are not expected to be
// canonicalized (i.e. they do not need to have their sort order set).
struct ReadArg {
  // The table from which to read rows.
  std::string table;

  // If non-empty, key_set below specifies keys from this index, not the table.
  std::string index;

  // Set of keys to read.
  KeySet key_set;

  // Set of columns to read.
  std::vector<std::string> columns;
};

// Streams a debug string representation of ReadArg to out.
std::ostream& operator<<(std::ostream& out, const ReadArg& arg);

// RowCursor is an abstract interface for iterating over rows.
//
// All rows will have the same number of columns, column types, and column
// names. The RowCursor is intended to be used by a consumer in a single thread.
// Implementations of this interface are not required to be thread-safe.
//
// Usage:
//     std::unique_ptr<RowCursor> cursor;
//     ZETASQL_RETURN_IF_ERROR(SomeMethodThatYieldsA(&cursor));
//     while (cursor->Next()) {
//       for (int i = 0; i < cursor->NumColumns(); ++i) {
//         const zetasql::value& value = cursor->ColumnValue(i);
//       }
//     }
//     ZETASQL_RETURN_IF_ERROR(cursor->Status());
class RowCursor {
 public:
  virtual ~RowCursor() {}

  // Moves to the next row. If the result set is exhausted, or the iterator
  // encountered an error, returns false. To distinguish between these two
  // cases, use Status().
  virtual bool Next() = 0;

  // Returns the status of the iterator. Once the iterator enters an error
  // state, it remains in the error state. Calls to Next() will return
  // false in this state.
  virtual absl::Status Status() const = 0;

  // Returns the number of columns.
  virtual int NumColumns() const = 0;

  // Returns the name of the specified column.
  virtual const std::string ColumnName(int i) const = 0;

  // Returns the value at the specified column of the current row. A call to
  // ColumnValue() is only valid if a previous call to Next() returned true.
  virtual const zetasql::Value ColumnValue(int i) const = 0;

  // Returns the type of the specified column. Types are owned by the database's
  // TypeFactory and not by the RowCursor.
  virtual const zetasql::Type* ColumnType(int i) const = 0;
};

// RowReader defines an abstract interface for reading rows from a database.
//
// This interface is used to decouple the query subsystem from the transaction
// subsystem. The query system deals with a RowReader interface and is unaware
// of the underlying concrete transaction class.
class RowReader {
 public:
  virtual ~RowReader() {}

  // Reads rows from a database based on the provided read_arg.
  virtual absl::Status Read(const ReadArg& read_arg,
                            std::unique_ptr<RowCursor>* cursor) = 0;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACCESS_READ_H_
