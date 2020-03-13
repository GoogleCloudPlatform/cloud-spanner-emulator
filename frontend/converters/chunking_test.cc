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

#include "google/spanner/v1/result_set.pb.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "frontend/converters/reads.h"
#include "tests/common/row_cursor.h"
#include "zetasql/base/status.h"

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
              StatusIs(zetasql_base::StatusCode::kInternal));
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
    values { string_value: "abcdefghijkl" }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values { string_value: "mnopqrstuvwxyz" }
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
            values { string_value: "ab" }
          }
        }
      }
    }
    chunked_value:  true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values {
          list_value {
            values { string_value: "cdefghijklmnoqrstu" }
          }
        }
        values {
          list_value {
            values { string_value: "abcdef" }
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
            values { string_value: "ghijklmnoqrstu" }
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
        values { string_value: "abcdefghijklm" }
      }
    }
    chunked_value:  true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values { string_value: "nopqrstuvwxyz" }
        values { string_value: "The end" }
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
      }
    }
    chunked_value: true
  )";
  expected_results[1] = R"(
    values {
      list_value {
        values {  number_value: 5 }
        values {  number_value: 6 }
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
  const size_t kChunkSize = 11;
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
      values { string_value: "\x61\x62\x63\x64\x65" }
      chunked_value: true
    )";
    expected_results[1] = R"(
      values { string_value: "\x66\x67" }
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

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
