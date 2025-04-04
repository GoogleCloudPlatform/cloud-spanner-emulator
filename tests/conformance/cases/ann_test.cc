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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "common/errors.h"
#include "tests/conformance/common/database_test_base.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using testing::HasSubstr;
using zetasql_base::testing::StatusIs;

class ANNTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"sql(
          CREATE TABLE Base (
            MyKey INT64 NOT NULL,
            MyData STRING(MAX),
            Embedding ARRAY<FLOAT32>(vector_length=>2),
            Embedding2 ARRAY<FLOAT32>(vector_length=>2),
            Embedding3 ARRAY<FLOAT64>(vector_length=>2),
          ) PRIMARY KEY(MyKey)
        )sql"}));
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"sql(
        CREATE VECTOR INDEX vec_index ON Base(Embedding)
        WHERE Embedding IS NOT NULL
        OPTIONS(distance_type = 'COSINE', tree_depth = 2)
        )sql",
    }));
    ZETASQL_EXPECT_OK(SetSchema({
        R"sql(
        CREATE VECTOR INDEX vec_index_dot_product ON Base(Embedding)
        WHERE Embedding IS NOT NULL
        OPTIONS(distance_type = 'DOT_PRODUCT', tree_depth = 3)
        )sql",
    }));
    ZETASQL_EXPECT_OK(SetSchema({
        R"sql(
      CREATE VECTOR INDEX vec_index_euclidean ON Base(Embedding2)
      WHERE Embedding2 IS NOT NULL
      OPTIONS(distance_type = 'EUCLIDEAN', tree_depth = 2)
      )sql",
    }));
    ZETASQL_EXPECT_OK(SetSchema({
        R"sql(
        CREATE VECTOR INDEX vec_index_store ON Base(Embedding)
        STORING (MyData)
        WHERE Embedding IS NOT NULL
        OPTIONS(distance_type = 'COSINE', tree_depth = 2)
        )sql",
    }));
    ZETASQL_EXPECT_OK(SetSchema({
        R"sql(
        CREATE VECTOR INDEX vec_index_double ON Base(Embedding3)
        WHERE Embedding3 IS NOT NULL
        OPTIONS(distance_type = 'COSINE', tree_depth = 2)
        )sql",
    }));
    ZETASQL_EXPECT_OK(SetSchema({
        R"sql(
          CREATE INDEX index2 ON Base(MyData)
        )sql"}));
    return PopulateDatabase();
  }

 protected:
  absl::Status PopulateDatabase() {
    ZETASQL_RETURN_IF_ERROR(
        MultiInsert(
            "Base",
            {"MyKey", "MyData", "Embedding", "Embedding2", "Embedding3"},
            {{1, "datastr", std::vector<float>{1.0, 0.8},
              std::vector<float>{1.0, 0.8}, std::vector<double>{1.0, 0.8}},
             {2, "datastr", std::vector<float>{0.1, 1.0},
              std::vector<float>{0.1, 1.0}, std::vector<double>{0.1, 1.0}}})
            .status());
    return absl::OkStatus();
  }
};

TEST_F(ANNTest, BasicANNQuery) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            ARRAY<FLOAT32>[1.0, 0.1], b.Embedding,
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(ANNTest, BasicANNQueryWithParams) {
  EXPECT_THAT(QueryWithParams(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index_double} b
          WHERE b.Embedding3 IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            @embedding, b.Embedding3,
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql",
                  {{"embedding", Value(std::vector<double>{1.0, 0.1})}}),
              IsOkAndHoldsRows({{1}, {2}}));
  EXPECT_THAT(QueryWithParams(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index_double} b
          WHERE b.Embedding3 IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding3, @embedding,
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql",
                  {{"embedding", Value(std::vector<double>{1.0, 0.1})}}),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(ANNTest, ANNQueryNoForceIndex) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_DOT_PRODUCT(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{2}, {1}}));
}

TEST_F(ANNTest, ANNQueryNoForceIndexDifferentColumn) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding2 IS NOT NULL
          ORDER BY APPROX_EUCLIDEAN_DISTANCE(
            b.Embedding2, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(ANNTest, ANNQueryWrongDistanceType) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_EUCLIDEAN_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
      error::VectorIndexesUnusableForceIndexWrongDistanceType(
          "vec_index", "COSINE", "APPROX_EUCLIDEAN_DISTANCE", "Embedding"));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_EUCLIDEAN_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::VectorIndexesUnusable("EUCLIDEAN", "Embedding",
                                           "approx_euclidean_distance"));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding2 IS NOT NULL
          ORDER BY APPROX_DOT_PRODUCT(
            b.Embedding2, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::VectorIndexesUnusable("DOT_PRODUCT", "Embedding2",
                                           "approx_dot_product"));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=index2} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_EUCLIDEAN_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::NotVectorIndexes("index2"));
}

TEST_F(ANNTest, ANNQueryWrongColumn) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding2 IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding2, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::VectorIndexesUnusableForceIndexWrongColumn(
                  "vec_index", "APPROX_COSINE_DISTANCE", "Embedding2"));
}

TEST_F(ANNTest, ANNQueryNoOptions) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1])
          LIMIT 2)sql"),
      error::ApproxDistanceFunctionOptionsRequired("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryWrongOptions) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves": 1}')
          LIMIT 2)sql"),
      error::ApproxDistanceFunctionInvalidJsonOption("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryInvalidNumLeavesToSearch) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": "abc"}')
          LIMIT 2)sql"),
      error::ApproxDistanceFunctionInvalidJsonOption("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryNegativeNumLeavesToSearch) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": -1}')
          LIMIT 2)sql"),
      error::ApproxDistanceFunctionInvalidJsonOption("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryNoOrderBy) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          LIMIT 2)sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryMustUnderOrderBy) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY b.MyKey
          LIMIT 2)sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryNoJoin) {
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
          CREATE TABLE Base2 (
            MyKey INT64 NOT NULL,
            MyData STRING(MAX),
            Embedding ARRAY<FLOAT32>(vector_length=>2),
          ) PRIMARY KEY(MyKey)
        )sql"}));
  ZETASQL_EXPECT_OK(MultiInsert("Base2", {"MyKey", "MyData", "Embedding"},
                        {{1, "datastr2", std::vector<float>{1.0, 0.8}},
                         {2, "datastr2", std::vector<float>{0.0, 1.0}}})
                .status());
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey, b2.MyData FROM Base@{FORCE_INDEX=vec_index} b
          JOIN Base2 b2 ON b.MyKey = b2.MyKey
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryOrderByNoOtherColumns) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}'),
            b.MyKey
          LIMIT 2)sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
}

TEST_F(ANNTest, ANNQueryComplexJoins) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT MyKey FROM Base@{FORCE_INDEX=index2}
          JOIN
          (
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2
          ) USING (MyKey)
          )sql"),
              IsOkAndHoldsRows({{1}, {2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT MyKey FROM
          (
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2
          )
          JOIN
          (
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2
          ) USING (MyKey)
          ORDER BY MyKey ASC
          )sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(ANNTest, ANNQueryMultipleWhere) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.MyKey != 1 AND b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{2}}));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE (b.Embedding2 IS NULL OR b.MyData IS NOT NULL) AND b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(ANNTest, BasicANNQueryNoWhere) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
      error::VectorIndexesUnusableNotNullFiltered("vec_index", "Embedding"));
}

TEST_F(ANNTest, BasicANNQueryWrongWhere) {
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding2 IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
      error::VectorIndexesUnusableNotNullFiltered("vec_index", "Embedding"));
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
      error::VectorIndexesUnusableNotNullFiltered("vec_index", "Embedding"));
  EXPECT_THAT(
      Query(
          R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding2 IS NOT NULL AND b.Embedding IS NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
      error::VectorIndexesUnusableNotNullFiltered("vec_index", "Embedding"));
}

TEST_F(ANNTest, ANNQueryOffset) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base@{FORCE_INDEX=vec_index} b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2 OFFSET 1)sql"),
              IsOkAndHoldsRows({{2}}));
}

TEST_F(ANNTest, ANNQueryStoreColumn) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyData FROM Base@{FORCE_INDEX=vec_index_store} b
          WHERE b.Embedding IS NOT NULL AND b.MyData IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              IsOkAndHoldsRows({{"datastr"}, {"datastr"}}));
}

TEST_F(ANNTest, ANNQueryWrongInput) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<INT64>[1, 0],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "APPROX_COSINE_DISTANCE")));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            ARRAY<INT64>[1, 0], ARRAY<INT64>[1, 0],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "APPROX_COSINE_DISTANCE")));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT64>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "APPROX_COSINE_DISTANCE")));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            (SELECT 1), b.Embedding,
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "APPROX_COSINE_DISTANCE")));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, b.Embedding,
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, b.Embedding2,
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[0.0, 0.0],
            options => JSON '{"num_leaves_to_search": 1}')
          LIMIT 2)sql"),
              absl::InvalidArgumentError(
                  "Cannot compute cosine distance against zero vector."));
}

TEST_F(ANNTest, ANNQueryNoLimit) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT b.MyKey FROM Base b
          WHERE b.Embedding IS NOT NULL
          ORDER BY APPROX_COSINE_DISTANCE(
            b.Embedding, ARRAY<FLOAT32>[1.0, 0.1],
            options => JSON '{"num_leaves_to_search": 1}')
          )sql"),
              error::ApproxDistanceInvalidShape("approx_cosine_distance"));
}

TEST_F(ANNTest, AlterVectorIndex) {
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
      CREATE VECTOR INDEX VI_alter ON Base(Embedding) WHERE Embedding IS NOT NULL
        OPTIONS(distance_type = 'EUCLIDEAN')
    )sql"}));
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
      ALTER VECTOR INDEX VI_alter ADD STORED COLUMN MyData
    )sql"}));
  EXPECT_THAT(SetSchema({
                  R"sql(
      ALTER VECTOR INDEX VI_alter ADD STORED COLUMN NonExistent
    )sql"}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr(error::VectorIndexStoredColumnNotFound(
                                     "VI_alter", "NonExistent")
                                     .message())));
  EXPECT_THAT(SetSchema({
                  R"sql(
      ALTER VECTOR INDEX VI_alter ADD STORED COLUMN MyData
    )sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(error::VectorIndexStoredColumnAlreadyExists(
                                     "VI_alter", "MyData")
                                     .message())));
  EXPECT_THAT(SetSchema({
                  R"sql(
      ALTER VECTOR INDEX VI_alter ADD STORED COLUMN MyKey
    )sql"}),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr(error::VectorIndexStoredColumnIsKey(
                                     "VI_alter", "MyKey", "Base")
                                     .message())));
  EXPECT_THAT(
      SetSchema({
          R"sql(
      ALTER VECTOR INDEX VI_alter DROP STORED COLUMN NonExistent
    )sql"}),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr(
              error::ColumnNotFound("VI_alter", "NonExistent").message())));
  EXPECT_THAT(
      SetSchema({
          R"sql(
      ALTER VECTOR INDEX VI_alter DROP STORED COLUMN MyKey
    )sql"}),
      StatusIs(
          absl::StatusCode::kNotFound,
          HasSubstr(error::ColumnNotFound("VI_alter", "MyKey").message())));
  EXPECT_THAT(SetSchema({
                  R"sql(
      ALTER VECTOR INDEX VI_alter DROP STORED COLUMN Embedding
    )sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr(error::VectorIndexNotStoredColumn("VI_alter",
                                                                   "Embedding")
                                     .message())));
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
      ALTER VECTOR INDEX VI_alter DROP STORED COLUMN MyData
    )sql"}));
}

TEST_F(ANNTest, DropVectorIndex) {
  EXPECT_THAT(SetSchema({
                  R"sql(
      DROP VECTOR INDEX VI_drop
    )sql"}),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr(error::IndexNotFound("VI_drop").message())));
  EXPECT_THAT(SetSchema({
                  R"sql(
      DROP VECTOR INDEX index2
    )sql"}),
              StatusIs(absl::StatusCode::kNotFound,
                       HasSubstr(error::IndexNotFound("index2").message())));
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
      CREATE VECTOR INDEX VI_drop ON Base(Embedding) WHERE Embedding IS NOT NULL
        OPTIONS(distance_type = 'EUCLIDEAN')
    )sql"}));
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
      DROP VECTOR INDEX VI_drop
    )sql"}));
}
}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
