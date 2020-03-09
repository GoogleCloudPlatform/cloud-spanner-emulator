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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

using zetasql::values::Int64;
using zetasql::values::String;

TEST(KeyRange, DefaultConstructorIsEmpty) {
  EXPECT_EQ(KeyRange(), KeyRange::Empty());
}

TEST(KeyRange, EmptyRangeHasClosedOpenEmptyKeys) {
  KeyRange empty_range = KeyRange::Empty();

  EXPECT_EQ(Key::Empty(), empty_range.start_key());
  EXPECT_EQ(Key::Empty(), empty_range.limit_key());
  EXPECT_EQ(EndpointType::kClosed, empty_range.start_type());
  EXPECT_EQ(EndpointType::kOpen, empty_range.limit_type());
}

TEST(KeyRange, ConstructorInitializesKeyRange) {
  KeyRange simple_range = KeyRange(EndpointType::kOpen, Key({Int64(2)}),
                                   EndpointType::kClosed, Key({Int64(3)}));

  EXPECT_EQ(Key({Int64(2)}), simple_range.start_key());
  EXPECT_EQ(Key({Int64(3)}), simple_range.limit_key());
  EXPECT_EQ(EndpointType::kOpen, simple_range.start_type());
  EXPECT_EQ(EndpointType::kClosed, simple_range.limit_type());
}

TEST(KeyRange, KeyRangeAllSpansAllKeys) {
  KeyRange all = KeyRange::All();

  EXPECT_EQ(Key::Empty(), all.start_key());
  EXPECT_EQ(Key::Infinity(), all.limit_key());
  EXPECT_EQ(EndpointType::kClosed, all.start_type());
  EXPECT_EQ(EndpointType::kOpen, all.limit_type());
  EXPECT_TRUE(all.Contains(Key::Empty()));
  EXPECT_FALSE(all.Contains(Key::Infinity()));
}

TEST(KeyRange, KeyRangePointSpansSingleKey) {
  Key key({Int64(2)});
  KeyRange point = KeyRange::Point(key);

  EXPECT_EQ(key, point.start_key());
  EXPECT_LT(key, point.limit_key());
  EXPECT_EQ(key.ToPrefixLimit(), point.limit_key());
  EXPECT_EQ(EndpointType::kClosed, point.start_type());
  EXPECT_EQ(EndpointType::kOpen, point.limit_type());
}

TEST(KeyRange, ClosedOpenHelperInitializesCorrectly) {
  KeyRange co = KeyRange::ClosedOpen(Key({Int64(2)}), Key({Int64(3)}));

  EXPECT_EQ(EndpointType::kClosed, co.start_type());
  EXPECT_EQ(EndpointType::kOpen, co.limit_type());
  EXPECT_EQ(Key({Int64(2)}), co.start_key());
  EXPECT_EQ(Key({Int64(3)}), co.limit_key());
}

TEST(KeyRange, ImplementsContainsPredicate) {
  // Build a key space.
  Key s = Key::Empty();
  Key a({String("a")});
  Key a1({String("a"), Int64(1)});
  Key a2({String("a"), Int64(2)});
  Key b({String("b")});
  Key b1({String("b"), Int64(1)});
  Key b2({String("b"), Int64(2)});
  Key e = Key::Infinity();

  // Set of full keys in the key space.
  std::vector<Key> full{a1, a2, b1, b2};

  // Returns all full keys which belong in the key range.
  auto elementsIn = [&](const KeyRange& kr) {
    std::vector<Key> out;
    for (const Key& k : full) {
      if (kr.Contains(k)) {
        out.push_back(k);
      }
    }
    return out;
  };

  // In the tests below, the following combinations are tested:
  // - prefix key, prefix key
  // - prefix key, full key
  // - start, infinity
  // - start, start
  // - full key, full key
  // - full key, prefix key

  // Closed-Open
  // ===========
  // Excludes all keys with prefix b.
  EXPECT_THAT(elementsIn(KeyRange::ClosedOpen(a, b)),
              testing::ElementsAre(a1, a2));
  // Excludes only key b2.
  EXPECT_THAT(elementsIn(KeyRange::ClosedOpen(a, b2)),
              testing::ElementsAre(a1, a2, b1));
  // Includes all keys from start to infinity.
  EXPECT_THAT(elementsIn(KeyRange::ClosedOpen(s, e)),
              testing::ElementsAre(a1, a2, b1, b2));
  // Does not include any keys as the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::ClosedOpen(s, s)), testing::ElementsAre());
  // Only includes a2 as anything at or past b1 is excluded.
  EXPECT_THAT(elementsIn(KeyRange::ClosedOpen(a2, b1)),
              testing::ElementsAre(a2));
  // Only includes a2 as anything at or past b is excluded.
  EXPECT_THAT(elementsIn(KeyRange::ClosedOpen(a2, b)),
              testing::ElementsAre(a2));

  // Closed-Closed
  // =============
  // Includes all keys as closed endpoint includes all matching prefixes.
  EXPECT_THAT(elementsIn(KeyRange::ClosedClosed(a, b)),
              testing::ElementsAre(a1, a2, b1, b2));
  // Still includes all keys as b2 is inclusive.
  EXPECT_THAT(elementsIn(KeyRange::ClosedClosed(a, b2)),
              testing::ElementsAre(a1, a2, b1, b2));
  // Includes all keys since the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::ClosedClosed(s, s)),
              testing::ElementsAre(a1, a2, b1, b2));
  // Includes all keys since the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::ClosedClosed(s, e)),
              testing::ElementsAre(a1, a2, b1, b2));
  // Includes all keys between a2 and b1 inclusive.
  EXPECT_THAT(elementsIn(KeyRange::ClosedClosed(a2, b1)),
              testing::ElementsAre(a2, b1));
  // Includes all keys between a2 and a2 inclusive.
  EXPECT_THAT(elementsIn(KeyRange::ClosedClosed(a2, a2)),
              testing::ElementsAre(a2));

  // Open-Open
  // =========
  // Excludes all keys as both prefixes are excluded.
  EXPECT_THAT(elementsIn(KeyRange::OpenOpen(a, b)), testing::ElementsAre());
  // Excludes keys with prefix a and anything at or past b2.
  EXPECT_THAT(elementsIn(KeyRange::OpenOpen(a, b2)), testing::ElementsAre(b1));
  // Excludes all keys since the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::OpenOpen(s, e)), testing::ElementsAre());
  // Excludes all keys since the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::OpenOpen(s, s)), testing::ElementsAre());
  // Excludes all keys since there are not keys between a1 and b1.
  EXPECT_THAT(elementsIn(KeyRange::OpenOpen(a2, b1)), testing::ElementsAre());
  // Excludes all keys since a2 and any key with prefix b is excluded.
  EXPECT_THAT(elementsIn(KeyRange::OpenOpen(a2, b)), testing::ElementsAre());

  // Open-Closed
  // ===========
  // Includes only keys with prefix b.
  EXPECT_THAT(elementsIn(KeyRange::OpenClosed(a, b)),
              testing::ElementsAre(b1, b2));
  // Still includes keys with prefix b as b2 is the last such key.
  EXPECT_THAT(elementsIn(KeyRange::OpenClosed(a, b2)),
              testing::ElementsAre(b1, b2));
  // Excludes all keys since the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::OpenClosed(s, e)), testing::ElementsAre());
  // Excludes all keys since the empty key is a prefix of all keys.
  EXPECT_THAT(elementsIn(KeyRange::OpenClosed(s, s)), testing::ElementsAre());
  // Only includes b1 as it is the only key between a2 and itself-inclusive.
  EXPECT_THAT(elementsIn(KeyRange::OpenClosed(a2, b1)),
              testing::ElementsAre(b1));
  // Includes all keys with prefix b.
  EXPECT_THAT(elementsIn(KeyRange::OpenClosed(a2, b)),
              testing::ElementsAre(b1, b2));
}

TEST(KeyRange, ContainsPredicateHandlesPrefixLimitKeys) {
  Key a({String("a")});
  Key b({String("b")});
  Key a_plus = a.ToPrefixLimit();
  Key b_plus = b.ToPrefixLimit();

  // a+ lies between a and b and will get included in all range types.
  EXPECT_TRUE(KeyRange::ClosedOpen(a, b).Contains(a_plus));
  EXPECT_TRUE(KeyRange::ClosedClosed(a, b).Contains(a_plus));
  EXPECT_TRUE(KeyRange::OpenOpen(a, b).Contains(a_plus));
  EXPECT_TRUE(KeyRange::OpenClosed(a, b).Contains(a_plus));

  // b+ lies past b and will get excluded from all range types.
  EXPECT_FALSE(KeyRange::ClosedOpen(a, b).Contains(b_plus));
  EXPECT_FALSE(KeyRange::ClosedClosed(a, b).Contains(b_plus));
  EXPECT_FALSE(KeyRange::OpenOpen(a, b).Contains(b_plus));
  EXPECT_FALSE(KeyRange::OpenClosed(a, b).Contains(b_plus));

  // a+ is past a and will get excluded from all range types.
  EXPECT_FALSE(KeyRange::ClosedOpen(a, a).Contains(a_plus));
  EXPECT_FALSE(KeyRange::ClosedClosed(a, a).Contains(a_plus));
  EXPECT_FALSE(KeyRange::OpenOpen(a, a).Contains(a_plus));
  EXPECT_FALSE(KeyRange::OpenClosed(a, a).Contains(a_plus));
}

TEST(KeyRange, ConvertsRangesToClosedOpen) {
  // Build a key space.
  Key s = Key::Empty();
  Key a({String("a")});
  Key a1({String("a"), Int64(1)});
  Key a2({String("a"), Int64(2)});
  Key b({String("b")});
  Key b1({String("b"), Int64(1)});
  Key b2({String("b"), Int64(2)});
  Key e = Key::Infinity();

  // Set of all points in the key space.
  std::vector<Key> all{s, a, a1, a2, b, b1, b2, e};

  // Set of full keys in the key space.
  std::vector<Key> full{a1, a2, b1, b2};

  // Returns all full keys which belong in the key range.
  auto elementsIn = [&](const KeyRange& kr) {
    std::vector<Key> out;
    for (const Key& k : full) {
      if (kr.Contains(k)) {
        out.push_back(k);
      }
    }
    return out;
  };

  // Consider every possible interval in the domain.
  for (int i = 0; i < all.size(); ++i) {
    for (int j = 0; j < all.size(); ++j) {
      const Key& start = all[i];
      const Key& limit = all[j];

      // Check that every range is equivalent to its ClosedOpen form.
      KeyRange co = KeyRange::ClosedOpen(start, limit);
      EXPECT_THAT(elementsIn(co),
                  testing::ElementsAreArray(elementsIn(co.ToClosedOpen())))
          << "When converting " << co << " to ClosedOpen";

      KeyRange cc = KeyRange::ClosedClosed(start, limit);
      EXPECT_THAT(elementsIn(cc),
                  testing::ElementsAreArray(elementsIn(cc.ToClosedOpen())))
          << "When converting " << cc << " to ClosedOpen";

      KeyRange oo = KeyRange::OpenOpen(start, limit);
      EXPECT_THAT(elementsIn(oo),
                  testing::ElementsAreArray(elementsIn(oo.ToClosedOpen())))
          << "When converting " << oo << " to ClosedOpen";

      KeyRange oc = KeyRange::OpenClosed(start, limit);
      EXPECT_THAT(elementsIn(oc),
                  testing::ElementsAreArray(elementsIn(oc.ToClosedOpen())))
          << "When converting " << oc << " to ClosedOpen";
    }
  }
}

TEST(KeyRange, GeneratesDebugString) {
  EXPECT_EQ("[{} ... {∞})", KeyRange::All().DebugString());
  EXPECT_EQ("[{} ... {})", KeyRange::Empty().DebugString());
  EXPECT_EQ(
      "[{Int64(2)} ... {Int64(3)})",
      KeyRange::ClosedOpen(Key({Int64(2)}), Key({Int64(3)})).DebugString());
}

}  // namespace

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
