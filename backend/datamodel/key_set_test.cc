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

#include <cstdlib>
#include <ctime>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/flags/flag.h"
#include "backend/datamodel/key_range.h"

ABSL_FLAG(int, cloud_spanner_emulator_canonicalize_key_set_test_seed, -1,
          "If non-negative, specifies the seed to use for the randomly "
          "generated dataset used in the KeySet.CanonicalizesRandomKeySet "
          "test case.");

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql::values::Int64;
using zetasql::values::String;

TEST(KeySet, ConstructorsInitializeCorrectly) {
  KeySet empty_set;
  EXPECT_TRUE(empty_set.keys().empty());
  EXPECT_TRUE(empty_set.ranges().empty());

  KeySet single_key_set(Key({Int64(1)}));
  ASSERT_EQ(1, single_key_set.keys().size());
  EXPECT_EQ(Key({Int64(1)}), single_key_set.keys()[0]);
  EXPECT_TRUE(single_key_set.ranges().empty());

  KeyRange range = KeyRange::ClosedOpen(Key({Int64(1)}), Key({Int64(2)}));
  KeySet single_range_set(range);
  ASSERT_EQ(1, single_range_set.ranges().size());
  EXPECT_EQ(range, single_range_set.ranges()[0]);
  EXPECT_TRUE(single_range_set.keys().empty());
}

TEST(KeySet, ModifiersAddKeysAndRanges) {
  Key k1({Int64(1)});
  Key k2({Int64(2)});
  KeyRange kr1 = KeyRange::ClosedOpen(Key({Int64(1)}), Key({Int64(2)}));
  KeyRange kr2 = KeyRange::ClosedOpen(Key({Int64(3)}), Key({Int64(4)}));

  KeySet ks;
  ks.AddKey(k1);
  ks.AddKey(k2);
  ks.AddRange(kr1);
  ks.AddRange(kr2);

  ASSERT_EQ(2, ks.keys().size());
  ASSERT_EQ(2, ks.ranges().size());
  EXPECT_EQ(k1, ks.keys()[0]);
  EXPECT_EQ(k2, ks.keys()[1]);
  EXPECT_EQ(kr1, ks.ranges()[0]);
  EXPECT_EQ(kr2, ks.ranges()[1]);
}

TEST(KeySet, AllKeySetContainsAllKeyRange) {
  KeySet all_set(KeyRange::All());
  ASSERT_EQ(1, all_set.ranges().size());
  EXPECT_EQ(KeyRange::All(), all_set.ranges()[0]);
  EXPECT_TRUE(all_set.keys().empty());
}

TEST(KeySet, GeneratesDebugString) {
  KeySet empty_set;
  EXPECT_EQ("<none>", empty_set.DebugString());

  Key k1({Int64(1)});
  Key k2({Int64(2)});
  KeyRange kr1 = KeyRange::ClosedOpen(Key({Int64(1)}), Key({Int64(2)}));
  KeyRange kr2 = KeyRange::ClosedOpen(Key({Int64(3)}), Key({Int64(4)}));

  KeySet ks;
  ks.AddKey(k1);
  ks.AddKey(k2);
  ks.AddRange(kr1);
  ks.AddRange(kr2);

  EXPECT_EQ(
      "Key{Int64(1)}, Key{Int64(2)}, Range[{Int64(1)} ... {Int64(2)}), "
      "Range[{Int64(3)} ... {Int64(4)})",
      ks.DebugString());
}

// Convenience helpers for canonicalization tests below.
typedef std::pair<int, int> Interval;
typedef std::vector<Interval> IntervalList;
KeyRange MakeClosedOpenRange(int a, int b) {
  return KeyRange::ClosedOpen(Key({Int64(a)}), Key({Int64(b)}));
}

TEST(KeySet, CanonicalizesEmptyRange) {
  KeySet ks;
  std::vector<KeyRange> expected_ranges;

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);

  EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
}

TEST(KeySet, CanonicalizesSingleRange) {
  KeySet ks;
  ks.AddRange(MakeClosedOpenRange(1, 2));
  std::vector<KeyRange> expected_ranges{MakeClosedOpenRange(1, 2)};

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);

  EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
}

TEST(KeySet, CanonicalizesOverlappingKeyRangesWithMultipleParts) {
  Key b1 = Key({String("b"), Int64(1)});
  Key b2 = Key({String("b"), Int64(2)});
  Key a = Key({String("a")});
  Key c = Key({String("c")});

  KeySet ks;
  ks.AddRange(KeyRange::ClosedOpen(b1, b2));
  ks.AddRange(KeyRange::ClosedOpen(a, c));
  std::vector<KeyRange> expected_ranges{KeyRange::ClosedOpen(a, c)};

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);
  EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
}

TEST(KeySet, CanonicalizeKeyRangesWithInvalidRanges) {
  Key a = Key({String("a")});

  KeySet ks;
  ks.AddRange(KeyRange::Point(a));
  ks.AddRange(KeyRange::OpenOpen(a, a));
  std::vector<KeyRange> expected_ranges{KeyRange::Point(a)};

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);
  EXPECT_THAT(actual_ranges, testing::ElementsAreArray(expected_ranges));
}

TEST(KeySet, CanonicalizeOverlappingKeyRangesWithAll) {
  Key b1 = Key({String("b"), Int64(1)});
  Key b2 = Key({String("b"), Int64(2)});
  Key a = Key({String("a")});
  Key c = Key({String("c")});

  KeySet ks;
  ks.AddRange(KeyRange::ClosedOpen(b1, b2));
  ks.AddRange(KeyRange::ClosedOpen(a, c));
  ks.AddRange(KeyRange::All());
  std::vector<KeyRange> expected_ranges{KeyRange::All()};

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);
  EXPECT_THAT(actual_ranges, testing::ElementsAreArray(expected_ranges));
}

TEST(KeySet, CanonicalizeOverlappingKeyRangesWithEmpty) {
  Key b1 = Key({String("b"), Int64(1)});
  Key b2 = Key({String("b"), Int64(2)});
  Key a = Key({String("a")});
  Key c = Key({String("c")});

  KeySet ks;
  ks.AddRange(KeyRange::ClosedOpen(b1, b2));
  ks.AddRange(KeyRange::ClosedOpen(a, c));
  ks.AddRange(KeyRange::OpenOpen(Key::Empty(), Key::Infinity()));

  // This particular empty range sorts after all ranges. We don't collapse it
  // and there is no harm in not doing so.
  std::vector<KeyRange> expected_ranges{
      KeyRange::ClosedOpen(a, c),
      KeyRange::ClosedOpen(Key::Empty().ToPrefixLimit(), Key::Infinity())};

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);
  EXPECT_THAT(actual_ranges, testing::ElementsAreArray(expected_ranges));
}

TEST(KeySet, CanonicalizeRangesContiguousWithPrefixLimit) {
  Key one = Key({Int64(1)});
  Key one_plus = one.ToPrefixLimit();
  Key two = Key({Int64(2)});
  Key two_plus = two.ToPrefixLimit();

  KeySet ks;
  ks.AddRange(KeyRange::ClosedOpen(two, two_plus));
  ks.AddRange(KeyRange::ClosedOpen(one, one_plus));

  // We do not collapse [1, 1+) and [2, 2+) into one range as that would need
  // knowledge of a successor function. We still expect them to be sorted.
  std::vector<KeyRange> expected_ranges{KeyRange::ClosedOpen(one, one_plus),
                                        KeyRange::ClosedOpen(two, two_plus)};

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);
  EXPECT_THAT(actual_ranges, testing::ElementsAreArray(expected_ranges));
}

TEST(KeySet, CanonicalizesAllRange) {
  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(KeySet::All(), &actual_ranges);
  ASSERT_EQ(1, actual_ranges.size());
  EXPECT_EQ(KeyRange::All(), actual_ranges[0]);
}

TEST(KeySet, CanonicalizesTwoRangeClosedOpenCases) {
  // Setup cases for canonicalizing two ranges. In the test case lines below,
  // the first two intervals are the input ranges, the remaining intervals are
  // the expected output ranges. All input ranges are ClosedOpen.
  std::vector<std::pair<IntervalList, IntervalList>> test_cases = {
      // Range A and B overlap and will be merged.
      {{{2, 7}, {0, 5}}, {{0, 7}}},

      // Range A and B are disjoint and will be kept disjoint.
      {{{0, 5}, {7, 9}}, {{0, 5}, {7, 9}}},

      // Range A is included in range B and will be subsumed.
      {{{2, 3}, {0, 5}}, {{0, 5}}},

      // Range B perfectly extends range A and will be merged.
      {{{0, 5}, {5, 9}}, {{0, 9}}},
  };

  for (const auto& test_case : test_cases) {
    const IntervalList& input = test_case.first;
    const IntervalList& output = test_case.second;

    KeySet ks;
    for (const Interval& interval : input) {
      ks.AddRange(MakeClosedOpenRange(interval.first, interval.second));
    }

    std::vector<KeyRange> expected_ranges;
    for (const Interval& interval : output) {
      expected_ranges.push_back(
          MakeClosedOpenRange(interval.first, interval.second));
    }

    std::vector<KeyRange> actual_ranges;
    MakeDisjointKeyRanges(ks, &actual_ranges);

    EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
  }
}

TEST(KeySet, CanonicalizesTwoDescendingClosedOpenRanges) {
  // Setup cases for canonicalizing two ranges where keys are descending. In the
  // test case lines below, the first two intervals are the input ranges, the
  // remaining intervals are the expected output ranges.  All input ranges are
  // ClosedOpen.
  std::vector<std::pair<IntervalList, IntervalList>> test_cases = {
      // Range A and B overlap and will be merged.
      {{{7, 2}, {5, 0}}, {{7, 0}}},

      // Range A and B are disjoint and will be kept disjoint.
      {{{5, 0}, {9, 7}}, {{9, 7}, {5, 0}}},

      // Range A is included in range B and will be subsumed.
      {{{3, 2}, {5, 0}}, {{5, 0}}},

      // Range B perfectly extends range A and will be merged.
      {{{5, 0}, {9, 5}}, {{9, 0}}},
  };

  for (const auto& test_case : test_cases) {
    const IntervalList& input = test_case.first;
    const IntervalList& output = test_case.second;

    KeySet ks;
    for (const Interval& interval : input) {
      Key k1;
      k1.AddColumn(Int64(interval.first), true);
      Key k2;
      k2.AddColumn(Int64(interval.second), true);

      ks.AddRange(KeyRange::ClosedOpen(k1, k2));
    }

    std::vector<KeyRange> expected_ranges;
    for (const Interval& interval : output) {
      Key k1;
      k1.AddColumn(Int64(interval.first), true);
      Key k2;
      k2.AddColumn(Int64(interval.second), true);

      expected_ranges.push_back(KeyRange::ClosedOpen(k1, k2));
    }

    std::vector<KeyRange> actual_ranges;
    MakeDisjointKeyRanges(ks, &actual_ranges);

    EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
  }
}

TEST(KeySet, CanonicalizesMultipleClosedOpenRanges) {
  IntervalList input = {{1, 7}, {10, 13}, {0, 5}, {11, 16}, {2, 4}};
  IntervalList output = {{0, 7}, {10, 16}};

  KeySet ks;
  for (const Interval& interval : input) {
    ks.AddRange(MakeClosedOpenRange(interval.first, interval.second));
  }

  std::vector<KeyRange> expected_ranges;
  for (const Interval& interval : output) {
    expected_ranges.push_back(
        MakeClosedOpenRange(interval.first, interval.second));
  }

  std::vector<KeyRange> actual_ranges;
  MakeDisjointKeyRanges(ks, &actual_ranges);

  EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
}

TEST(KeySet, CanonicalizesMultipleDescendingOpenOpenRanges) {
  // In the test case lines below, the first two intervals are the input ranges,
  // the remaining intervals are the expected output ranges.  All input ranges
  // are OpenOpen. All elements are positive, the negative sign is used as a
  // trick to identify prefix-limit keys.
  std::vector<std::pair<IntervalList, IntervalList>> test_cases = {
      // Range A and B do not overlap and should be kept separate.
      {{{7, 5}, {5, 1}}, {{-7, 5}, {-5, 1}}},

      // Same case above but positions of input ranges switched.
      {{{5, 1}, {7, 5}}, {{-7, 5}, {-5, 1}}},

      // Range A and B overlap and should be merged.
      {{{9, 6}, {7, 3}}, {{-9, 3}}},
  };

  for (const auto& test_case : test_cases) {
    const IntervalList& input = test_case.first;
    const IntervalList& output = test_case.second;

    KeySet ks;
    for (const Interval& interval : input) {
      Key k1;
      k1.AddColumn(Int64(interval.first), true);
      Key k2;
      k2.AddColumn(Int64(interval.second), true);

      ks.AddRange(KeyRange(EndpointType::kOpen, k1, EndpointType::kOpen, k2));
    }

    std::vector<KeyRange> expected_ranges;
    for (const Interval& interval : output) {
      Key k1;
      k1.AddColumn(Int64(std::abs(interval.first)), true);
      if (interval.first < 0) {
        k1 = k1.ToPrefixLimit();
      }
      Key k2;
      k2.AddColumn(Int64(std::abs(interval.second)), true);
      if (interval.second < 0) {
        k2 = k2.ToPrefixLimit();
      }

      expected_ranges.push_back(KeyRange::ClosedOpen(k1, k2));
    }

    std::vector<KeyRange> actual_ranges;
    MakeDisjointKeyRanges(ks, &actual_ranges);

    EXPECT_THAT(expected_ranges, testing::ElementsAreArray(actual_ranges));
  }
}

// Returns true if the given KeySet contains the input key.
bool Contains(const KeySet& keyset, const Key& input) {
  for (const Key& key : keyset.keys()) {
    if (key == input) {
      return true;
    }
  }
  for (const KeyRange& range : keyset.ranges()) {
    if (range.Contains(input)) {
      return true;
    }
  }
  return false;
}

TEST(KeySet, CanonicalizesRandomKeySet) {
  // Choose a random seed unless one is explicitly provided by the test.
  int seed = absl::GetFlag(
      FLAGS_cloud_spanner_emulator_canonicalize_key_set_test_seed);
  if (seed < 0) {
    seed = std::time(nullptr);
  }
  ZETASQL_LOG(INFO) << "Seeding CanonicalizesRandomKeySet with test seed " << seed;
  std::srand(seed);

  // This test generates several test cases randomly with multiple interval
  // types and point keys and then checks that the union of the output ranges
  // contains the same elements as the original key set. The key set under
  // consideration is [0, kNumElements).
  const int kNumElements = 100;
  const int kMaxPointKeys = 5;
  const int kMaxRanges = 10;

  for (int i = 0; i < 1000; ++i) {
    KeySet ks;

    // Generate random keys.
    int np = std::rand() % kMaxPointKeys;
    for (int i = 0; i < np; ++i) {
      ks.AddKey(Key({Int64(std::rand() % kNumElements)}));
    }

    // Generate random ranges.
    int nr = std::rand() % kMaxRanges;
    for (int i = 0; i < nr; ++i) {
      int start = std::rand() % kNumElements;
      int end = std::rand() % kNumElements;
      if (start > end) {
        std::swap(start, end);
      }
      ks.AddRange(KeyRange(
          std::rand() % 2 ? EndpointType::kClosed : EndpointType::kOpen,
          Key({Int64(start)}),
          std::rand() % 2 ? EndpointType::kClosed : EndpointType::kOpen,
          Key({Int64(end)})));
    }

    // Canonicalize.
    std::vector<KeyRange> output_ranges;
    MakeDisjointKeyRanges(ks, &output_ranges);

    // Check that the output ranges are disjoint.
    if (!output_ranges.empty()) {
      auto itr1 = output_ranges.begin();
      auto itr2 = itr1 + 1;
      while (itr2 != output_ranges.end()) {
        EXPECT_TRUE(itr1->IsClosedOpen()) << (*itr1);
        EXPECT_GT(itr2->start_key(), itr1->limit_key());
        ++itr1;
        ++itr2;
      }
    }

    // Figure out which elements are present in the original key set.
    std::vector<Key> expected_elements;
    for (int i = 0; i < kNumElements; ++i) {
      Key element = Key({Int64(i)});
      if (Contains(ks, element)) {
        expected_elements.push_back(element);
      }
    }

    // Figure out which elements are present in the canonicalized key set.
    std::vector<Key> actual_elements;
    for (int i = 0; i < kNumElements; ++i) {
      Key element = Key({Int64(i)});
      for (const KeyRange& range : output_ranges) {
        if (range.Contains(element)) {
          actual_elements.push_back(element);
          break;
        }
      }
    }

    // Check that these are equal.
    EXPECT_THAT(expected_elements, testing::ElementsAreArray(actual_elements))
        << "When processing key set " << ks;
  }
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
