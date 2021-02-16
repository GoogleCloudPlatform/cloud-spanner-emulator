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

#include "frontend/converters/chunking.h"

#include <vector>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/random/random.h"
#include "absl/time/time.h"
#include "common/limits.h"
#include "frontend/converters/reads.h"
#include "tests/common/chunking.h"
#include "tests/common/row_cursor.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

using ::google::spanner::v1::PartialResultSet;
using ::google::spanner::v1::ResultSet;
using zetasql_base::testing::StatusIs;

TEST(ChunkingTest, CopiesMetadataFromResultSet) {
  const size_t kChunkSize = 100;

  {
    ResultSet result = PARSE_TEXT_PROTO(R"(
      metadata {
        row_type {
          fields {
            name: "list"
            type {
              code: ARRAY
              array_element_type {
                code: STRUCT
                struct_type {
                  fields {
                    name: "a"
                    type { code: STRING }
                  }
                }
              }
            }
          }
        }
      }
    )");
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                         ChunkResultSet(result, kChunkSize));

    std::string expected_results = R"(
      metadata {
        row_type {
          fields {
            name: "list"
            type {
              code: ARRAY
              array_element_type {
                code: STRUCT
                struct_type {
                  fields {
                    name: "a"
                    type { code: STRING }
                  }
                }
              }
            }
          }
        }
      }
    )";
    EXPECT_THAT(results,
                testing::ElementsAre(test::EqualsProto(expected_results)));
  }
}

TEST(ChunkingTest, InvalidTypeReturnsError) {
  const size_t kChunkSize = 20;
  // Attempt to chunk a struct type which is not valid.
  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {
      row_type {
        fields {
          name: "struct"
          type { code: STRUCT }
        }
      }
    }
    rows { values { struct_value { fields {} } } }
  )");

  EXPECT_THAT(ChunkResultSet(result, kChunkSize),
              StatusIs(absl::StatusCode::kInternal));
}

TEST(ChunkingTest, ChunkString) {
  const size_t kChunkSize = 18;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    rows { values { string_value: "abcdefghijklmnopqrstuvwxyz" } }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values { string_value: "abcdefghijklmnop" }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values { string_value: "qrstuvwxyz" }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkNestedLists) {
  const size_t kChunkSize = 44;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values {
            list_value { values { string_value: "abcdefghijklmnoqrstu" } }
          }
          values {
            list_value { values { string_value: "abcdefghijklmnoqrstu" } }
          }
          values {
            list_value { values { string_value: "abcdefghijklmnoqrstu" } }
          }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(3);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values {
          list_value {
            values { string_value: "abcdefghijklmnoqrstu" }
          }
        }
        values {
          list_value {
            values { string_value: "abcdefghijkl" }
          }
        }
      }
    }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values {
          list_value {
            values { string_value: "mnoqrstu" }
          }
        }
        values {
          list_value {
            values { string_value: "abcdefghijklmnoqrstu" }
          }
        }
      }
    }
    chunked_value: true
  )";
  expected_results[2] = R"(
    values {
      list_value {
        values {
          list_value {
          }
        }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1]),
                                   test::EqualsProto(expected_results[2])));
}

TEST(ChunkingTest, ChunkStringLists) {
  const size_t kChunkSize = 40;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values { string_value: "The beginning" }
          values { string_value: "abcdefghijklmnopqrstuvwxyz" }
          values { string_value: "The end" }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values { string_value: "The beginning" }
        values { string_value: "abcdefghijklmnopqrstu" }
      }
    }
    chunked_value:  true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values { string_value: "vwxyz" }
        values { string_value: "The end" }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkStringListsAtStringBoundary) {
  const size_t kChunkSize = 40;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values { string_value: "The beginning" }
          values { string_value: "abcdefghijklmnopqrstu" }
          values { string_value: "vwxyz" }
          values { string_value: "The end" }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values { string_value: "The beginning" }
        values { string_value: "abcdefghijklmnopqrstu" }
      }
    }
    chunked_value:  true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values { string_value: "" }
        values { string_value: "vwxyz" }
        values { string_value: "The end" }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkStringListsAtListBoundary) {
  const size_t kChunkSize = 40;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values { string_value: "The beginning" }
          values { string_value: "abcdefghijklmno" }
        }
      }
      values {
        list_value {
          values { string_value: "pqrstuvwxyz" }
          values { string_value: "The end" }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values { string_value: "The beginning" }
        values { string_value: "abcdefghijklmno" }
      }
    }
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values { string_value: "pqrstuvwxyz" }
        values { string_value: "The end" }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkStringListsAtNestedListBoundary) {
  const size_t kChunkSize = 40;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values {
            list_value {
              values { string_value: "abcdefghijklmnoqrstuvwxyz-------" }
            }
          }
          values { list_value { values { string_value: "1234567890" } } }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values { list_value { values { string_value: "abcdefghijklmnoqrstuvwxyz-------" } } }
      }
    }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values { list_value { values { string_value: "" } } }
        values {
          list_value { values { string_value: "1234567890" } }
        }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkEmptyLists) {
  const size_t kChunkSize = 20;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
          values { list_value {} }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
      }
    }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values {
          list_value {}
        }
        values {
          list_value {}
        }
        values {
          list_value {}
        }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkNumberLists) {
  const size_t kChunkSize = 64;

  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows {
      values {
        list_value {
          values { number_value: 0 }
          values { number_value: 1 }
          values { number_value: 2 }
          values { number_value: 3 }
          values { number_value: 4 }
          values { number_value: 5 }
          values { number_value: 6 }
          values { number_value: 7 }
          values { number_value: 8 }
          values { number_value: 9 }
        }
      }
    }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(2);
  expected_results[0] = R"(
    metadata {}
    values {
      list_value {
        values {  number_value: 0 }
        values {  number_value: 1 }
        values {  number_value: 2 }
        values {  number_value: 3 }
        values {  number_value: 4 }
        values {  number_value: 5 }
        values {  number_value: 6 }
      }
    }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values {  number_value: 7 }
        values {  number_value: 8 }
        values {  number_value: 9 }
      }
    }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0]),
                                   test::EqualsProto(expected_results[1])));
}

TEST(ChunkingTest, ChunkUTFCharacters) {
  const size_t kChunkSize = 6;
  std::vector<std::string> expected_results(2);
  std::vector<PartialResultSet> results;

  // Multibyte UTF characters should not be split up and instead the entire
  // character should be pushed to the next chunk.
  {
    // Test a 2 byte UTF character { 0xC3, 0x88 }
    ResultSet result = PARSE_TEXT_PROTO(R"(
      metadata {}
      rows { values { string_value: "\x00\x00\x00\x00\xC3\x88" } }
    )");
    ZETASQL_ASSERT_OK_AND_ASSIGN(results, ChunkResultSet(result, kChunkSize));

    expected_results[0] = R"(
      metadata {}
      values { string_value: "\x00\x00\x00\x00" }
      chunked_value: true
    )";
    expected_results[1] = R"(
      values { string_value: "\xC3\x88" }
    )";

    EXPECT_THAT(results, testing::ElementsAre(
                             test::EqualsProto(expected_results[0]),
                             test::EqualsProto(expected_results[1])));
  }

  {
    // Test a 3 byte UTF character { 0xE0, 0xB0, 0x81 }
    ResultSet result = PARSE_TEXT_PROTO(R"(
      metadata {}
      rows { values { string_value: "\x00\x00\x00\xE0\xB0\x81" } }
    )");
    ZETASQL_ASSERT_OK_AND_ASSIGN(results, ChunkResultSet(result, kChunkSize));

    expected_results[0] = R"(
      metadata {}
      values { string_value: "\x00\x00\x00" }
      chunked_value: true
    )";
    expected_results[1] = R"(
      values { string_value: "\xE0\xB0\x81" }
    )";

    EXPECT_THAT(results, testing::ElementsAre(
                             test::EqualsProto(expected_results[0]),
                             test::EqualsProto(expected_results[1])));
  }

  {
    // Test a 4 byte UTF character { 0xF7, 0x81, 0x83, 0x87 }
    ResultSet result = PARSE_TEXT_PROTO(R"(
      metadata {}
      rows { values { string_value: "\x00\x00\xF7\x81\x83\x87" } }
    )");
    ZETASQL_ASSERT_OK_AND_ASSIGN(results, ChunkResultSet(result, kChunkSize));

    expected_results[0] = R"(
      metadata {}
      values { string_value: "\x00\x00" }
      chunked_value: true
    )";
    expected_results[1] = R"(
      values { string_value: "\xF7\x81\x83\x87" }
    )";

    EXPECT_THAT(results, testing::ElementsAre(
                             test::EqualsProto(expected_results[0]),
                             test::EqualsProto(expected_results[1])));
  }

  {
    // Test a group of single byte UTF characters { 0x61, 0x62, 0x63, 0x64,
    // 0x65, 0x66, 0x67 } which is "abcdefgh", so it should get split up across
    // chunks.
    ResultSet result = PARSE_TEXT_PROTO(R"(
      metadata {}
      rows { values { string_value: "\x61\x62\x63\x64\x65\x66\x67" } }
    )");
    ZETASQL_ASSERT_OK_AND_ASSIGN(results, ChunkResultSet(result, kChunkSize));

    expected_results[0] = R"(
      metadata {}
      values { string_value: "\x61\x62\x63\x64" }
      chunked_value: true
    )";
    expected_results[1] = R"(
      values { string_value: "\x65\x66\x67" }
    )";

    EXPECT_THAT(results, testing::ElementsAre(
                             test::EqualsProto(expected_results[0]),
                             test::EqualsProto(expected_results[1])));
  }
}

TEST(ChunkingTest, ChunkEmptyString) {
  const size_t kChunkSize = 20;
  ResultSet result = PARSE_TEXT_PROTO(R"(
    metadata {}
    rows { values { string_value: "" } }
  )");
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  std::vector<std::string> expected_results(1);
  expected_results[0] = R"(
    metadata {}
    values { string_value: "" }
  )";

  EXPECT_THAT(results,
              testing::ElementsAre(test::EqualsProto(expected_results[0])));
}

TEST(ChunkingTest, CheckSizeLimit) {
  const size_t kChunkSize = limits::kMaxStreamingChunkSize;
  ResultSet result;
  result.mutable_metadata();
  auto row = result.add_rows();

  for (int i = 0; i < 600000; ++i) {
    row->add_values()->set_bool_value(true);
  }
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                       ChunkResultSet(result, kChunkSize));

  // Check that the total size is within a factor of 2 of the streaming chunk
  // limit.
  ASSERT_EQ(results.size(), 2);
  for (const auto& set : results) {
    ASSERT_LE(set.ByteSizeLong(), limits::kMaxStreamingChunkSize * 2);
  }
}

TEST(ChunkingTest, RandomChunking) {
  int64_t time = absl::ToUnixNanos(absl::Now());
  std::seed_seq seed({time});
  absl::BitGen gen(seed);
  ZETASQL_LOG(INFO) << "Testing random chunking with seed: " << time;

  const int kNumColumnsMin = 10;
  const int kNumColumnsMax = 200;
  // We set the minimum chunk size to 40 since there needs to be sufficient room
  // to handle all of the recursive lists that are transferred to the next
  // chunk.
  const int kChunkSizeMin = 40;
  const int kChunkSizeMax = 1000;
  for (int i = 0; i < 50; ++i) {
    const size_t kChunkSize = absl::Uniform<size_t>(
        absl::IntervalClosedClosed, gen, kChunkSizeMin, kChunkSizeMax);
    const int kNumColumns = absl::Uniform<int>(absl::IntervalClosedClosed, gen,
                                               kNumColumnsMin, kNumColumnsMax);

    // Generate random result set.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResultSet result,
        backend::test::GenerateRandomResultSet(&gen, kNumColumns));

    // Chunk result.
    ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> results,
                         ChunkResultSet(result, kChunkSize));

    // Merge resulting chunks and check that it is equal to the original result.
    ZETASQL_ASSERT_OK_AND_ASSIGN(
        ResultSet merged_result,
        backend::test::MergePartialResultSets(results, kNumColumns));
    EXPECT_THAT(merged_result, test::EqualsProto(result));
  }
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
