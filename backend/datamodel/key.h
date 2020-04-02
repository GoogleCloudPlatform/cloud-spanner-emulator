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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_H_

#include <ostream>
#include <string>
#include <vector>

#include "zetasql/public/value.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Key represents the primary key or a prefix of the primary key for a table.
//
// A Key consists of a tuple of column values. Each column can be marked as
// ascending or descending. Comparison of keys with mismatching column types or
// order attributes is undefined.
//
// There are two special key values:
//   Key::Empty()    which points to the beginning of a key space
//   Key::Infinity() which points to the end of a key space
//
// In addition, there is support for "prefix limit" keys. Given a key K, the
// prefix limit key K+ (obtained by Key::ToPrefixLimit()) is a point in the key
// space larger than any key with prefix K. This is useful in implementing
// prefix ranges as the range [K, K+) will cover all keys with prefix K.
class Key {
 public:
  // Constructs an empty key.
  Key();

  // Constructs a key with initial column values.
  explicit Key(std::vector<zetasql::Value> columns);

  // Adds a column to the key.
  void AddColumn(zetasql::Value value, bool desc = false);

  // Returns the number of columns in a key.
  int NumColumns() const;

  // Sets the column at a specific index.
  void SetColumnValue(int i, zetasql::Value value);
  void SetColumnDescending(int i, bool value);

  // Returns the column at a specific index.
  const zetasql::Value& ColumnValue(int i) const;
  bool IsColumnDescending(int i) const;

  // Returns all column values in the key.
  const std::vector<zetasql::Value>& column_values() const {
    return columns_;
  }

  // Performs a three-way comparison against another key.
  // k1.Compare(k2) returns
  //   -1 if k1 < k2
  //    0 if k1 = k2
  //    1 if k1 > k2
  int Compare(const Key& other) const;

  // Returns a key pointing to the start of the keyspace.
  static Key Empty();

  // Returns a key pointing to the end of the keyspace.
  static Key Infinity();

  // Returns a special key which sorts after any key with the same prefix.
  Key ToPrefixLimit() const;

  // Returns a prefix of this key with the specified number of columns.
  Key Prefix(int n) const;

  // Returns true if *this is a prefix of the given key.
  bool IsPrefixOf(const Key& other) const;

  // Returns true if the key does not have any columns.
  bool IsEmpty() const { return columns_.empty(); }

  // Returns the logical size of the key in bytes.
  int64_t LogicalSizeInBytes() const;

  // Returns a debug string suitable to be included in error messages.
  std::string DebugString() const;

 private:
  // Individual columns that make up the key.
  std::vector<zetasql::Value> columns_;

  // Key metadata.
  bool is_infinity_ = false;
  bool is_prefix_limit_ = false;

  // Column metadata.
  std::vector<bool> is_descending_;

  // Friend for member access.
  friend std::ostream& operator<<(std::ostream& out, const Key& k);
};

// Streams out a string representation of the key (same as DebugString).
std::ostream& operator<<(std::ostream& out, const Key& k);

// Various comparison operators for convenience.
bool operator<(const Key& k1, const Key& k2);
bool operator<=(const Key& k1, const Key& k2);
bool operator==(const Key& k1, const Key& k2);
bool operator>(const Key& k1, const Key& k2);
bool operator>=(const Key& k1, const Key& k2);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_H_
