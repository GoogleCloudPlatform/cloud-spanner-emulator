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

#include "backend/datamodel/key.h"

#include <sstream>

#include "zetasql/public/numeric_value.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

// Returns the logical size in bytes of the given value.
int64_t LogicalBytesInternal(const zetasql::Value& value) {
  if (value.is_null()) {
    return 2;
  }
  // TODO: Refactor this to account for physical not null vs
  // logical not null keys. This causes a difference of 1 byte in the size.
  switch (value.type_kind()) {
    case zetasql::TYPE_BOOL:
      return 1;
    case zetasql::TYPE_DATE:
      return 4;
    case zetasql::TYPE_INT64:
    case zetasql::TYPE_DOUBLE:
      return 8;
    case zetasql::TYPE_TIMESTAMP:
      return 12;
    case zetasql::TYPE_STRING:
      return value.string_value().size();
    case zetasql::TYPE_BYTES:
      return value.bytes_value().size();
    case zetasql::TYPE_NUMERIC:
      // Here we use the maximal bytes of a numeric value.
      return 25;
    default:
      // Key columns should have already been validated, so invalid key columns
      // should not occur.
      return 0;
  }
}

}  // namespace

Key::Key() {}

Key::Key(std::vector<zetasql::Value> columns)
    : columns_(std::move(columns)), is_descending_(columns_.size()) {}

void Key::AddColumn(zetasql::Value value, bool desc) {
  columns_.emplace_back(std::move(value));
  is_descending_.push_back(desc);
}

int Key::NumColumns() const { return columns_.size(); }

void Key::SetColumnValue(int i, zetasql::Value value) {
  columns_[i] = std::move(value);
}

void Key::SetColumnDescending(int i, bool value) { is_descending_[i] = value; }

const zetasql::Value& Key::ColumnValue(int i) const { return columns_[i]; }

bool Key::IsColumnDescending(int i) const { return is_descending_[i]; }

int Key::Compare(const Key& other) const {
  // Handle infinity keys first.
  if (is_infinity_ || other.is_infinity_) {
    if (is_infinity_ == other.is_infinity_) {
      return 0;
    }
    return other.is_infinity_ ? -1 : 1;
  }

  // Perform left-to-right column comparisons.
  for (int i = 0; i < columns_.size(); ++i) {
    if (i >= other.columns_.size()) {
      // If we reached here, other is a prefix of *this.
      return other.is_prefix_limit_ ? -1 : 1;
    }

    if (columns_[i].LessThan(other.columns_[i])) {
      return is_descending_[i] ? 1 : -1;
    }

    if (columns_[i].Equals(other.columns_[i])) {
      continue;
    }

    return is_descending_[i] ? -1 : 1;
  }

  // If we reached here, *this is a prefix of other.
  if (other.columns_.size() > columns_.size()) {
    return is_prefix_limit_ ? 1 : -1;
  }

  // If we reached here, all columns are equal.
  if (is_prefix_limit_ != other.is_prefix_limit_) {
    return is_prefix_limit_ ? 1 : -1;
  }

  return 0;
}

// static
Key Key::Empty() { return Key(); }

// static
Key Key::Infinity() {
  Key k;
  k.is_infinity_ = true;
  return k;
}

Key Key::ToPrefixLimit() const {
  Key k = (*this);
  k.is_prefix_limit_ = true;
  return k;
}

Key Key::Prefix(int n) const {
  Key k = (*this);
  k.columns_.resize(n);
  k.is_descending_.resize(n);
  return k;
}

bool Key::IsPrefixOf(const Key& other) const {
  // Infinity is not a prefix of any other key except itself.
  if (is_infinity_) {
    return other.is_infinity_;
  }

  // To be a prefix, *this needs to have fewer or equal columns.
  if (columns_.size() > other.columns_.size()) {
    return false;
  }

  // If any columns of *this mismatch, *this is not a prefix.
  for (int i = 0; i < columns_.size(); ++i) {
    if (columns_[i] != other.columns_[i]) {
      return false;
    }
  }

  // If we reached here, all columns in *this match.
  return is_prefix_limit_ == other.is_prefix_limit_;
}

int64_t Key::LogicalSizeInBytes() const {
  int64_t key_size = 0;
  for (int i = 0; i < NumColumns(); ++i) {
    key_size += LogicalBytesInternal(ColumnValue(i));
  }
  return key_size;
}

std::string Key::DebugString() const {
  std::stringstream out;
  out << (*this);
  return out.str();
}

std::ostream& operator<<(std::ostream& out, const Key& k) {
  if (k.is_infinity_) {
    out << "{∞}";
    return out;
  }

  out << "{";

  for (int i = 0; i < k.NumColumns(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << k.ColumnValue(i);
    if (k.is_descending_[i]) {
      out << "↓";
    }
  }

  out << "}";

  if (k.is_prefix_limit_) {
    out << "+";
  }

  return out;
}

bool operator<(const Key& k1, const Key& k2) { return k1.Compare(k2) < 0; }
bool operator<=(const Key& k1, const Key& k2) { return k1.Compare(k2) <= 0; }
bool operator==(const Key& k1, const Key& k2) { return k1.Compare(k2) == 0; }
bool operator>(const Key& k1, const Key& k2) { return k2 < k1; }
bool operator>=(const Key& k1, const Key& k2) { return k2 <= k1; }

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
