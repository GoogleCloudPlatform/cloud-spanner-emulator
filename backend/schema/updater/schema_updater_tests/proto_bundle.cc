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

#include <memory>
#include <string>

#include "google/protobuf/descriptor.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/btree_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "tests/common/test.pb.h"
#include "tests/common/test_2.pb.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

// Proto bundles are not supported in PG yet and we skip the tests for PG.
using database_api::DatabaseDialect::POSTGRESQL;
using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

namespace {
std::string read_descriptors() {
  google::protobuf::FileDescriptorSet proto_files;
  ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
      proto_files.add_file());
  ::emulator::tests::common::ImportingParent::descriptor()->file()->CopyTo(
      proto_files.add_file());
  return proto_files.SerializeAsString();
}

TEST_P(SchemaUpdaterTest, CreateProtoBundle_SetsTheProtoBundle) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
      emulator.tests.common.ImportingAndParentingLevelTwo,
      emulator.tests.common.TestEnum,
      emulator.tests.common.EnumContainer.TestEnum,
    )
  )sql"},
                                        read_descriptors()));

  EXPECT_THAT(schema->proto_bundle()->types(),
              testing::UnorderedElementsAre(
                  "emulator.tests.common.Simple",
                  "emulator.tests.common.ImportingAndParentingLevelTwo",
                  "emulator.tests.common.TestEnum",
                  "emulator.tests.common.EnumContainer.TestEnum"));
  ZETASQL_EXPECT_OK(schema->proto_bundle()->GetTypeDescriptor(
      "emulator.tests.common.Simple"));
  ZETASQL_EXPECT_OK(schema->proto_bundle()->GetEnumTypeDescriptor(
      "emulator.tests.common.TestEnum"));
}

TEST_P(SchemaUpdaterTest, CreateProtoBundle_InUpdateSchema_SetsTheProtoBundle) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )sql"}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
    )
  )sql"},
                                            read_descriptors()));

  EXPECT_THAT(schema->proto_bundle()->types(),
              testing::UnorderedElementsAre("emulator.tests.common.Simple"));
  ZETASQL_EXPECT_OK(schema->proto_bundle()->GetTypeDescriptor(
      "emulator.tests.common.Simple"));
}

TEST_P(SchemaUpdaterTest, ProtoBundle_WithoutCreateProtoBundle_IsEmpty) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )sql"},
                                        read_descriptors()));

  EXPECT_TRUE(schema->proto_bundle()->empty());
  EXPECT_THAT(
      schema->proto_bundle()->GetTypeDescriptor("emulator.tests.common.Simple"),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(SchemaUpdaterTest, AlterProtoBundle_WithoutCreateProtoBundle_Fails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema(
                  {
                      R"sql(
    ALTER PROTO BUNDLE INSERT (
      emulator.tests.common.TestMessage,
    )
  )sql"},
                  read_descriptors()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, AlterProtoBundleAltersTypes) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
      emulator.tests.common.TestEnum
    )
  )sql",
                                        },
                                        read_descriptors()));
  ASSERT_THAT(schema->proto_bundle()->types(),
              testing::UnorderedElementsAre("emulator.tests.common.Simple",
                                            "emulator.tests.common.TestEnum"));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
    ALTER PROTO BUNDLE INSERT (
      emulator.tests.common.ImportingParent,
      emulator.tests.common.EnumContainer.TestEnum,
    ) UPDATE (emulator.tests.common.Simple)
    DELETE (emulator.tests.common.TestEnum)
  )sql",
                                            },
                                            read_descriptors()));

  EXPECT_THAT(schema->proto_bundle()->types(),
              testing::UnorderedElementsAre(
                  "emulator.tests.common.Simple",
                  "emulator.tests.common.ImportingParent",
                  "emulator.tests.common.EnumContainer.TestEnum"));
  ZETASQL_EXPECT_OK(schema->proto_bundle()->GetTypeDescriptor(
      "emulator.tests.common.ImportingParent"));
  ZETASQL_EXPECT_OK(schema->proto_bundle()->GetEnumTypeDescriptor(
      "emulator.tests.common.EnumContainer.TestEnum"));
}

TEST_P(SchemaUpdaterTest, AlterProtoBundle_UpdateNonExistent_Fails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
    )
  )sql",
                                        },
                                        read_descriptors()));

  EXPECT_THAT(UpdateSchema(schema.get(),
                           {
                               R"sql(
    ALTER PROTO BUNDLE UPDATE (
      emulator.tests.common.Parent
    )
  )sql",
                           },
                           read_descriptors()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(SchemaUpdaterTest, AlterProtoBundle_DeleteNonExistent_Fails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
    )
  )sql",
                                        },
                                        read_descriptors()));

  EXPECT_THAT(UpdateSchema(schema.get(),
                           {
                               R"sql(
    ALTER PROTO BUNDLE DELETE (
      emulator.tests.common.Parent
    )
  )sql",
                           },
                           read_descriptors()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(SchemaUpdaterTest, DropProtoBundle_Succeeds) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
      emulator.tests.common.TestEnum
    )
  )sql",
                                        },
                                        read_descriptors()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
                                                  DROP PROTO BUNDLE
                                                )sql",
                                            },
                                            read_descriptors()));

  EXPECT_TRUE(schema->proto_bundle()->empty());
  EXPECT_THAT(
      schema->proto_bundle()->GetTypeDescriptor("emulator.tests.common.Simple"),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(SchemaUpdaterTest, DropProtoBundleWithoutExistingProtoBundle_Fails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema(
                  {
                      R"sql(
                        DROP PROTO BUNDLE
                      )sql",
                  },
                  read_descriptors()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, CreateAfterDropProtoBundle_Succeeds) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
      emulator.tests.common.TestEnum
    )
  )sql",
                                        },
                                        read_descriptors()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
                                                  DROP PROTO BUNDLE
                                                )sql",
                                            },
                                            read_descriptors()));

  ASSERT_TRUE(schema->proto_bundle()->empty());

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
    )
                                                )sql",
                                            },
                                            read_descriptors()));

  ZETASQL_EXPECT_OK(schema->proto_bundle()->GetTypeDescriptor(
      "emulator.tests.common.Simple"));
  EXPECT_THAT(schema->proto_bundle()->GetEnumTypeDescriptor(
                  "emulator.tests.common.TestEnum"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(SchemaUpdaterTest, AlterAfterDropProtoBundle_Fails) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
      emulator.tests.common.TestEnum
    )
  )sql",
                                        },
                                        read_descriptors()));

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
                                                  DROP PROTO BUNDLE
                                                )sql",
                                            },
                                            read_descriptors()));

  ASSERT_TRUE(schema->proto_bundle()->empty());

  EXPECT_THAT(UpdateSchema(schema.get(),
                           {
                               R"sql(
    ALTER PROTO BUNDLE INSERT (
      emulator.tests.common.Simple,
    )
                                                )sql",
                           },
                           read_descriptors()),
              zetasql_base::testing::StatusIs(absl::StatusCode::kFailedPrecondition));
}

TEST_P(SchemaUpdaterTest, AlterProtoBundleUpdatesProtoColumnTypes) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.Simple,
    )
  )sql",
                                            R"sql(
          CREATE TABLE Albums (
            AlbumId INT64 NOT NULL,
            AlbumInfo emulator.tests.common.Simple,
            ArrayAlbumInfo ARRAY<emulator.tests.common.Simple>,
          ) PRIMARY KEY(AlbumId)
        )sql",
                                        },
                                        read_descriptors()));
  auto table = schema->FindTable("Albums");
  auto column = table->FindColumn("AlbumInfo");
  auto array_column = table->FindColumn("ArrayAlbumInfo");
  const zetasql::Type* column_type = column->GetType();
  const zetasql::Type* array_column_element_type =
      array_column->GetType()->AsArray()->element_type();
  ASSERT_NE(column_type->AsProto()->descriptor()->DebugString().find(
                "optional string field = 1"),
            std::string::npos);
  ASSERT_NE(
      array_column_element_type->AsProto()->descriptor()->DebugString().find(
          "optional string field = 1"),
      std::string::npos);
  google::protobuf::FileDescriptorSet file_descriptor_set;
  ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
      file_descriptor_set.add_file());

  google::protobuf::FileDescriptorProto* file_descriptor_proto =
      file_descriptor_set.mutable_file(0);
  google::protobuf::DescriptorProto* descriptor_proto =
      file_descriptor_proto->mutable_message_type(0);
  descriptor_proto->mutable_field(0)->set_name("new_field");
  std::string serialized_descriptor_bytes =
      file_descriptor_set.SerializeAsString();

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
                                                  ALTER PROTO BUNDLE UPDATE (
                                                    emulator.tests.common.Simple,
                                                  )
                                                )sql",
                                            },
                                            serialized_descriptor_bytes));
  auto new_table = schema->FindTable("Albums");
  auto new_column = new_table->FindColumn("AlbumInfo");
  auto new_array_column = new_table->FindColumn("ArrayAlbumInfo");
  const zetasql::Type* new_column_type = new_column->GetType();
  const zetasql::Type* new_array_column_element_type =
      new_array_column->GetType()->AsArray()->element_type();
  EXPECT_NE(new_column_type->AsProto()->descriptor()->DebugString().find(
                "optional string new_field = 1"),
            std::string::npos);
  EXPECT_NE(new_array_column_element_type->AsProto()
                ->descriptor()
                ->DebugString()
                .find("optional string new_field = 1"),
            std::string::npos);
}

TEST_P(SchemaUpdaterTest, AlterProtoBundleUpdatesEnumColumnTypes) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema(
                                        {
                                            R"sql(
    CREATE PROTO BUNDLE (
      emulator.tests.common.TestEnum,
    )
  )sql",
                                            R"sql(
          CREATE TABLE Albums (
            AlbumId INT64 NOT NULL,
            AlbumTypes emulator.tests.common.TestEnum,
            AlbumTypesArray ARRAY<emulator.tests.common.TestEnum>,
          ) PRIMARY KEY(AlbumId)
        )sql",
                                        },
                                        read_descriptors()));
  auto table = schema->FindTable("Albums");
  auto column = table->FindColumn("AlbumTypes");
  auto array_column = table->FindColumn("AlbumTypesArray");
  const zetasql::Type* column_type = column->GetType();
  const zetasql::Type* array_column_element_type =
      array_column->GetType()->AsArray()->element_type();
  ASSERT_NE(column_type->AsEnum()->enum_descriptor()->DebugString().find(
                "TEST_ENUM_ONE"),
            std::string::npos);
  ASSERT_NE(array_column_element_type->AsEnum()
                ->enum_descriptor()
                ->DebugString()
                .find("TEST_ENUM_ONE"),
            std::string::npos);
  google::protobuf::FileDescriptorSet file_descriptor_set;
  ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
      file_descriptor_set.add_file());

  google::protobuf::FileDescriptorProto* file_descriptor_proto =
      file_descriptor_set.mutable_file(0);
  google::protobuf::EnumDescriptorProto* descriptor_proto =
      file_descriptor_proto->mutable_enum_type(0);
  descriptor_proto->mutable_value(1)->set_name("TEST_ENUM_CHANGED");
  std::string serialized_descriptor_bytes =
      file_descriptor_set.SerializeAsString();

  ZETASQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(),
                                            {
                                                R"sql(
                                                  ALTER PROTO BUNDLE UPDATE (
                                                    emulator.tests.common.TestEnum,
                                                  )
                                                )sql",
                                            },
                                            serialized_descriptor_bytes));
  auto new_table = schema->FindTable("Albums");
  auto new_column = new_table->FindColumn("AlbumTypes");
  auto new_array_column = new_table->FindColumn("AlbumTypesArray");
  const zetasql::Type* new_column_type = new_column->GetType();
  const zetasql::Type* new_array_column_element_type =
      new_array_column->GetType()->AsArray()->element_type();
  EXPECT_NE(new_column_type->AsEnum()->enum_descriptor()->DebugString().find(
                "TEST_ENUM_CHANGED"),
            std::string::npos);
  EXPECT_NE(new_array_column_element_type->AsEnum()
                ->enum_descriptor()
                ->DebugString()
                .find("TEST_ENUM_CHANGED"),
            std::string::npos);
}

TEST_P(SchemaUpdaterTest,
       CreateProtoBundle_AddColumnWithUnrecognizedProtoType) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"sql(
    CREATE TABLE T (
      col1 INT64,
      col2 emulator.tests.common.Simple
    ) PRIMARY KEY(col1)
  )sql"}),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Unrecognized ddl::ColumnDefinition:")));
}

TEST_P(SchemaUpdaterTest,
       CreateProtoBundle_AlterColumnWithUnrecognizedProtoType) {
  if (GetParam() == POSTGRESQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema, CreateSchema({
                                        R"sql(
    CREATE TABLE T (
      col1 INT64,
      col2 STRING(MAX)
    ) PRIMARY KEY(col1)
  )sql"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"sql(
    ALTER TABLE T ALTER COLUMN col2 emulator.tests.common.Simple
  )sql"}),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("Unrecognized ddl::ColumnDefinition:")));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
