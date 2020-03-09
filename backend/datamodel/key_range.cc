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

#include "backend/datamodel/key_range.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

KeyRange::KeyRange()
    : KeyRange(EndpointType::kClosed, Key(), EndpointType::kOpen, Key()) {}

KeyRange::KeyRange(EndpointType start_type, const Key& start_key,
                   EndpointType limit_type, const Key& limit_key)
    : start_type_(start_type),
      start_key_(start_key),
      limit_type_(limit_type),
      limit_key_(limit_key) {}

// static
KeyRange KeyRange::Empty() {
  return KeyRange::ClosedOpen(Key::Empty(), Key::Empty());
}

// static
KeyRange KeyRange::All() {
  return KeyRange::ClosedOpen(Key::Empty(), Key::Infinity());
}

// static
KeyRange KeyRange::Point(const Key& key) {
  return KeyRange::ClosedOpen(key, key.ToPrefixLimit());
}

// static
KeyRange KeyRange::Prefix(const Key& key) {
  return KeyRange::ClosedOpen(key, key.ToPrefixLimit());
}

// static
KeyRange KeyRange::ClosedOpen(const Key& start_key, const Key& limit_key) {
  return KeyRange(EndpointType::kClosed, start_key, EndpointType::kOpen,
                  limit_key);
}

// static
KeyRange KeyRange::ClosedClosed(const Key& start_key, const Key& limit_key) {
  return KeyRange(EndpointType::kClosed, start_key, EndpointType::kClosed,
                  limit_key);
}

// static
KeyRange KeyRange::OpenOpen(const Key& start_key, const Key& limit_key) {
  return KeyRange(EndpointType::kOpen, start_key, EndpointType::kOpen,
                  limit_key);
}

// static
KeyRange KeyRange::OpenClosed(const Key& start_key, const Key& limit_key) {
  return KeyRange(EndpointType::kOpen, start_key, EndpointType::kClosed,
                  limit_key);
}

bool KeyRange::operator==(const KeyRange& other) const {
  return start_key_ == other.start_key_ && limit_key_ == other.limit_key_ &&
         start_type_ == other.start_type_ && limit_type_ == other.limit_type_;
}

bool KeyRange::IsClosedOpen() const {
  return start_type_ == EndpointType::kClosed &&
         limit_type_ == EndpointType::kOpen;
}

bool KeyRange::Contains(const Key& k) const {
  // Note that, based on the interval endpoint type, we could have converted
  // keys at construction time using Key::ToPrefixLimit which would allow us to
  // the natural operator overloads to test for inclusion, but we want to
  // preserve the keys given to us in the constructor.
  switch (start_type_) {
    case EndpointType::kOpen:
      if (k <= start_key_ || start_key_.IsPrefixOf(k)) {
        return false;
      }
      break;
    case EndpointType::kClosed:
      if (k < start_key_) {
        return false;
      }
      break;
  }

  switch (limit_type_) {
    case EndpointType::kOpen:
      if (k >= limit_key_) {
        return false;
      }
      break;
    case EndpointType::kClosed:
      if (k > limit_key_ && !limit_key_.IsPrefixOf(k)) {
        return false;
      }
      break;
  }

  return true;
}

KeyRange KeyRange::ToClosedOpen() const {
  // For Open intervals, we want to exclude all keys with the given prefix. This
  // is (by definition) equivalent to a closed interval starting at the prefix
  // limit key.
  Key start_key = start_type_ == EndpointType::kClosed
                      ? start_key_
                      : start_key_.ToPrefixLimit();
  // For Closed intervals, we want to include all keys with the given prefix.
  // This is (by definition) equivalent to a open interval ending in the prefix
  // limit key.
  Key limit_key = limit_type_ == EndpointType::kOpen
                      ? limit_key_
                      : limit_key_.ToPrefixLimit();

  return KeyRange::ClosedOpen(start_key, limit_key);
}

std::string KeyRange::DebugString() const {
  std::stringstream out;
  out << (*this);
  return out.str();
}

std::ostream& operator<<(std::ostream& out, const KeyRange& range) {
  if (range.start_type() == EndpointType::kClosed) {
    out << "[";
  } else {
    out << "(";
  }

  out << range.start_key();
  out << " ... ";
  out << range.limit_key();

  if (range.limit_type() == EndpointType::kClosed) {
    out << "]";
  } else {
    out << ")";
  }

  return out;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
