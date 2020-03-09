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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_SET_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_SET_H_

#include <ostream>
#include <string>
#include <vector>

#include "backend/datamodel/key.h"
#include "backend/datamodel/key_range.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// KeySet represents a collection of point keys and key ranges.
//
// KeySet acts as a simple container for multiple keys and key ranges. These
// keys and ranges are not canonicalized by the KeySet in any way (e.g. the
// class itself does not ensure that the keys and ranges are disjoint).
//
// For more details, see
//    https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#keyset
class KeySet {
 public:
  // Constructs an empty key set.
  KeySet();

  // Constructs a key set containing a single key.
  explicit KeySet(const Key& key);

  // Constructs a key set containing a single range.
  explicit KeySet(const KeyRange& key_range);

  // Modifiers.
  void AddKey(const Key& key);
  void AddRange(const KeyRange& range);

  // Accessors.
  const std::vector<Key>& keys() const { return keys_; }
  const std::vector<KeyRange>& ranges() const { return ranges_; }

  // Returns a key set representing the entire key space.
  static KeySet All();

  // Returns a debug string suitable to be included in error messages.
  std::string DebugString() const;

 private:
  // Keys in the key set.
  std::vector<Key> keys_;

  // Key ranges in the key set.
  std::vector<KeyRange> ranges_;
};

// Streams a debug string representation of the KeySet.
std::ostream& operator<<(std::ostream& out, const KeySet& set);

// Converts a key set to a sorted list of disjoint closed-open key ranges.
void MakeDisjointKeyRanges(const KeySet& set, std::vector<KeyRange>* ranges);

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_DATAMODEL_KEY_SET_H_
