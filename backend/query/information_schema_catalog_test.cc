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

#include "backend/query/information_schema_catalog.h"

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

namespace google::spanner::emulator::backend {

namespace {

TEST(InformationSchemaCatalogTest, ColumnsMetadataCount) {
  Schema schema;
  InformationSchemaCatalog catalog(&schema);
  EXPECT_EQ(catalog.ColumnsMetadata().size(), 162);
}

TEST(InformationSchemaCatalogTest, IndexColumnsMetadataCount) {
  Schema schema;
  InformationSchemaCatalog catalog(&schema);
  EXPECT_EQ(catalog.IndexColumnsMetadata().size(), 101);
}

}  // namespace
}  // namespace google::spanner::emulator::backend
