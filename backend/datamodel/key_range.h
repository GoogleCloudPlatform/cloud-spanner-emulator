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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_RANGE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_RANGE_H_

#include <ostream>
#include <string>

#include "backend/datamodel/key.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Types of key range endpoints.
enum class EndpointType { kOpen, kClosed };

// KeyRange represents a range in a key space.
//
// A KeyRange consists of two keys - start and limit, along with their interval
// endpoint types - closed or open. Given two keys k1, k2, KeyRange supports all
// possible combinations of endpoint types:
//   [k1, k2)
//   [k1, k2]
//   (k1, k2)
//   (k1, k2]
//
// For fully specified keys, the endpoint types have a natural interpretation.
// For prefix keys, the prefix key collectively represents all keys with the
// same prefix, e.g. (k1, ... will exclude all keys with prefix k1 and [k1, ...
// will include all keys with prefix k1 (similarly for the limit key). For more
// details, see the test cases and
//     https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#keyrange
//
// Within the database, key ranges are usually canonicalized into a closed-open
// form via KeyRange::ToClosedOpen. ClosedOpen ranges have the useful property
// that a key can be compared against the start and limit keys with natural
// operator overloads (>=, <) to test for inclusion. Since we preserve the
// keys passed to the constructor for start_key()/limit_key(), due to the
// prefix-related comparisons, this is not possible for the other intervals (and
// you must use KeyRange::Contains()).
class KeyRange {
 public:
  // Constructs an empty key range [(), ()).
  KeyRange();

  // Constructs a fully-specified key range.
  KeyRange(EndpointType start_type_, const Key& start_key,
           EndpointType limit_type_, const Key& limit_key);

  // Modifiers.
  Key& start_key() { return start_key_; }
  Key& limit_key() { return limit_key_; }
  EndpointType& start_type() { return start_type_; }
  EndpointType& limit_type() { return limit_type_; }

  // Accessors.
  const Key& start_key() const { return start_key_; }
  const Key& limit_key() const { return limit_key_; }
  EndpointType start_type() const { return start_type_; }
  EndpointType limit_type() const { return limit_type_; }

  // Convenience constructors.
  static KeyRange Empty();
  static KeyRange All();
  static KeyRange Point(const Key& key);
  static KeyRange Prefix(const Key& key);
  static KeyRange ClosedOpen(const Key& start_key, const Key& limit_key);
  static KeyRange ClosedClosed(const Key& start_key, const Key& limit_key);
  static KeyRange OpenOpen(const Key& start_key, const Key& limit_key);
  static KeyRange OpenClosed(const Key& start_key, const Key& limit_key);

  // Convenience predicates.
  bool operator==(const KeyRange& other) const;
  bool IsClosedOpen() const;
  bool Contains(const Key& k) const;

  // Converts this key range to an equivalent ClosedOpen range.
  KeyRange ToClosedOpen() const;

  // Returns a debug string suitable to be included in error messages.
  std::string DebugString() const;

 private:
  // Start of the range.
  EndpointType start_type_;
  Key start_key_;

  // Limit of the range.
  EndpointType limit_type_;
  Key limit_key_;
};

// Streams a debug string representation of the key range (same as DebugString).
std::ostream& operator<<(std::ostream& out, const KeyRange& range);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_RANGE_H_
