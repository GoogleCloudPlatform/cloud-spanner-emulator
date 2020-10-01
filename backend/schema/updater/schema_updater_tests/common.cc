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

#include "backend/schema/updater/schema_updater_tests/base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace {

TEST_F(SchemaUpdaterTest, CreationOrder) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"(
      CREATE TABLE T1 (
        k1 INT64,
        k2 INT64
      ) PRIMARY KEY (k1)
    )",
                                        R"(
      CREATE TABLE T2 (
        k1 INT64,
        k2 INT64,
        c1 BYTES(MAX),
        CONSTRAINT fk2 FOREIGN KEY (k2) REFERENCES T1 (k2)
      ) PRIMARY KEY (k1,k2), INTERLEAVE IN PARENT T1
    )",
                                        R"(
      CREATE INDEX Idx1 ON T1(k1) STORING(k2)
    )",
                                        R"(
      CREATE INDEX Idx2 ON T2(k1,c1), INTERLEAVE IN T1
    )"}));

  auto t1 = schema->FindTable("T1");
  auto t2 = schema->FindTable("T2");
  auto fk2 = t2->FindForeignKey("fk2");
  auto fk2_idx1 = fk2->referenced_index();
  auto fk2_idx1_dt = fk2_idx1->index_data_table();
  auto fk2_idx2 = fk2->referencing_index();
  auto fk2_idx2_dt = fk2_idx2->index_data_table();
  auto idx1 = schema->FindIndex("Idx1");
  auto idx1_dt = idx1->index_data_table();
  auto idx2 = schema->FindIndex("Idx2");
  auto idx2_dt = idx2->index_data_table();

  // Check that the nodes are added in topological order so that they
  // are cloned and validated in topological order.
  // clang-format off
  std::vector<const SchemaNode*> expected = {
      t1->FindColumn("k1"),
      t1->FindColumn("k2"),
      t1->FindKeyColumn("k1"),
      t1,
      t2->FindColumn("k1"),
      t2->FindColumn("k2"),
      t2->FindColumn("c1"),
      t2->FindKeyColumn("k1"),
      t2->FindKeyColumn("k2"),
      fk2,
      fk2_idx1_dt->FindColumn("k2"),
      fk2_idx1_dt->FindColumn("k1"),
      fk2_idx1_dt->FindKeyColumn("k2"),
      fk2_idx1_dt->FindKeyColumn("k1"),
      fk2_idx1,
      fk2_idx1_dt,
      fk2_idx2_dt->FindColumn("k2"),
      fk2_idx2_dt->FindColumn("k1"),
      fk2_idx2_dt->FindKeyColumn("k2"),
      fk2_idx2_dt->FindKeyColumn("k1"),
      fk2_idx2,
      fk2_idx2_dt,
      t2,
      idx1_dt->FindColumn("k1"),
      idx1_dt->FindKeyColumn("k1"),
      idx1_dt->FindColumn("k2"),
      idx1,
      idx1_dt,
      idx2_dt->FindColumn("k1"),
      idx2_dt->FindColumn("c1"),
      idx2_dt->FindColumn("k2"),
      idx2_dt->FindKeyColumn("k1"),
      idx2_dt->FindKeyColumn("c1"),
      idx2_dt->FindKeyColumn("k2"),
      idx2,
      idx2_dt,
  };
  // clang-format on

  EXPECT_THAT(schema->GetSchemaGraph()->GetSchemaNodes(),
              testing::ElementsAreArray(expected));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
