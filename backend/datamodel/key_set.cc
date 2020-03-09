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

#include "backend/datamodel/key_set.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

KeySet::KeySet() {}

KeySet::KeySet(const Key& key) { AddKey(key); }

KeySet::KeySet(const KeyRange& key_range) { AddRange(key_range); }

void KeySet::AddKey(const Key& key) { keys_.push_back(key); }

void KeySet::AddRange(const KeyRange& range) { ranges_.push_back(range); }

// static
KeySet KeySet::All() { return KeySet(KeyRange::All()); }

std::string KeySet::DebugString() const {
  std::stringstream out;
  out << (*this);
  return out.str();
}

std::ostream& operator<<(std::ostream& out, const KeySet& set) {
  const std::vector<Key>& keys = set.keys();
  const std::vector<KeyRange>& ranges = set.ranges();

  if (keys.empty() && ranges.empty()) {
    out << "<none>";
  }

  for (int i = 0; i < keys.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << "Key" << keys[i];
  }

  for (int i = 0; i < ranges.size(); ++i) {
    if (!keys.empty() || i > 0) {
      out << ", ";
    }
    out << "Range" << ranges[i];
  }

  return out;
}

void MakeDisjointKeyRanges(const KeySet& set, std::vector<KeyRange>* ranges) {
  // Clear output.
  ranges->clear();

  // First convert all keys and ranges to closed-open ranges.
  for (const Key& key : set.keys()) {
    ranges->push_back(KeyRange::Point(key));
  }
  for (const KeyRange& range : set.ranges()) {
    ranges->push_back(range.ToClosedOpen());
  }

  // Early exit if we have nothing to do.
  if (ranges->empty()) {
    return;
  }

  // Now sort the ranges by start key.
  std::sort(ranges->begin(), ranges->end(),
            [](const KeyRange& kr1, const KeyRange& kr2) {
              return kr1.start_key() < kr2.start_key();
            });

  // Finally, walk through the ranges, merging them as we go.
  // Invariants:
  // - All ranges before last are disjoint
  // - All ranges in [last, next) have been merged into last
  // - next moves forward in every iteration
  auto last = ranges->begin();
  auto next = last + 1;
  while (next != ranges->end()) {
    // Skip invalid or empty ranges.
    if (next->start_key() >= next->limit_key()) {
      ++next;
      continue;
    }

    // Consider merging the next range into the last disjoint range.
    switch (next->start_key().Compare(last->limit_key())) {
      case 1: {
        // The next range's start key is after the last range's limit key.
        // Everything before next is now disjoint, so make next the new last.
        ++last;
        *last = *next;
        break;
      }

      case 0: {
        // The next range's start key coincides with the last range's limit key.
        // The two ranges are contiguous, make last extend to cover both ranges.
        last->limit_key() = next->limit_key();
        break;
      }

      case -1: {
        // The next range's start key is before the last range's limit key.
        // The two ranges overlap, extend last to the greater of the two ranges.
        last->limit_key() = std::max(next->limit_key(), last->limit_key());
        break;
      }
    }

    // Now consider the next un-merged range.
    ++next;
  }

  // Clear any ranges after last (they have already been merged into last).
  ranges->erase(last + 1, next);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
