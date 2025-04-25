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

#include <string>

#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "backend/query/info_schema_columns_metadata_values.h"
#include "backend/query/spanner_sys_catalog.h"

namespace google::spanner::emulator::backend {

namespace {

TEST(InformationSchemaCatalogTest, ColumnsMetadataCount) {
  Schema schema;
  SpannerSysCatalog spanner_sys_catalog;
  InformationSchemaCatalog catalog(InformationSchemaCatalog::kName, &schema,
                                   &spanner_sys_catalog);
  EXPECT_EQ(ColumnsMetadata().size(), 261);
}

TEST(InformationSchemaCatalogTest, IndexColumnsMetadataCount) {
  Schema schema;
  SpannerSysCatalog spanner_sys_catalog;
  InformationSchemaCatalog catalog(InformationSchemaCatalog::kName, &schema,
                                   &spanner_sys_catalog);
  EXPECT_EQ(IndexColumnsMetadata().size(), 168);
}

TEST(InformationSchemaCatalogTest, SpannerSysColumnsMetadataCount) {
  Schema schema;
  SpannerSysCatalog spanner_sys_catalog;
  InformationSchemaCatalog catalog(InformationSchemaCatalog::kName, &schema,
                                   &spanner_sys_catalog);
  EXPECT_EQ(SpannerSysColumnsMetadata().size(), 293);
}

TEST(InformationSchemaCatalogTest, PGColumnsMetadataCount) {
  Schema schema;
  SpannerSysCatalog spanner_sys_catalog;
  InformationSchemaCatalog catalog(InformationSchemaCatalog::kPGName, &schema,
                                   &spanner_sys_catalog);
  EXPECT_EQ(PGColumnsMetadata().size(), 411);
}

TEST(InformationSchemaCatalogTest, PGIndexColumnsMetadataCount) {
  Schema schema;
  SpannerSysCatalog spanner_sys_catalog;
  InformationSchemaCatalog catalog(InformationSchemaCatalog::kPGName, &schema,
                                   &spanner_sys_catalog);
  EXPECT_EQ(PGIndexColumnsMetadata().size(), 1);
}

TEST(InformationSchemaCatalogTest, PGSpannerSysColumnsMetadataCount) {
  Schema schema;
  SpannerSysCatalog spanner_sys_catalog;
  InformationSchemaCatalog catalog(InformationSchemaCatalog::kPGName, &schema,
                                   &spanner_sys_catalog);
  EXPECT_EQ(SpannerSysColumnsMetadata().size(), 293);
}
}  // namespace
}  // namespace google::spanner::emulator::backend
