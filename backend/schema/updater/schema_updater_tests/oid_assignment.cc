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

#include <cstdint>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

namespace {

using google::spanner::admin::database::v1::DatabaseDialect::
    GOOGLE_STANDARD_SQL;

absl::Status ValidateSchema(const Schema& schema, uint32_t expected_oid_count) {
  absl::flat_hash_set<uint32_t> in_use_oids;
  for (const auto& node : schema.GetSchemaGraph()->GetSchemaNodes()) {
    auto object_oid = node->postgresql_oid();
    if (object_oid.has_value()) {
      if (in_use_oids.contains(object_oid.value())) {
        return absl::AlreadyExistsError(
            absl::StrCat("Duplicate OID: ", object_oid.value()));
      }
      in_use_oids.insert(object_oid.value());
    }
    if (node->GetSchemaNameInfo().has_value()) {
      if (node->GetSchemaNameInfo()->kind == "Table") {
        auto table = node->As<Table>();
        auto primary_key_index_oid = table->primary_key_index_postgresql_oid();
        if (primary_key_index_oid.has_value()) {
          if (in_use_oids.contains(primary_key_index_oid.value())) {
            return absl::AlreadyExistsError(
                absl::StrCat("Duplicate OID: ", primary_key_index_oid.value()));
          }
          in_use_oids.insert(primary_key_index_oid.value());
        }
        auto interleave_in_parent_oid =
            table->interleave_in_parent_postgresql_oid();
        if (interleave_in_parent_oid.has_value()) {
          if (in_use_oids.contains(interleave_in_parent_oid.value())) {
            return absl::AlreadyExistsError(absl::StrCat(
                "Duplicate OID: ", interleave_in_parent_oid.value()));
          }
          in_use_oids.insert(interleave_in_parent_oid.value());
        }
      } else if (node->GetSchemaNameInfo()->kind == "ChangeStream") {
        auto change_stream = node->As<ChangeStream>();
        auto tvf_postgresql_oid = change_stream->tvf_postgresql_oid();
        if (tvf_postgresql_oid.has_value()) {
          if (in_use_oids.contains(tvf_postgresql_oid.value())) {
            return absl::AlreadyExistsError(
                absl::StrCat("Duplicate OID: ", tvf_postgresql_oid.value()));
          }
          in_use_oids.insert(tvf_postgresql_oid.value());
        }
      }
    }
  }
  if (in_use_oids.size() != expected_oid_count) {
    return absl::UnknownError(
        absl::StrCat("Unexpected number of OIDs : ", in_use_oids.size(), " vs ",
                     expected_oid_count));
  }
  return absl::OkStatus();
}

TEST_P(SchemaUpdaterTest, OidAssignment_RevertOnError) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  EXPECT_THAT(CreateSchema({R"(
    CREATE TABLE T(
      col1 bigint PRIMARY KEY,
      col1 character varying(100)))"},
                           /*proto_descriptor_bytes=*/"",
                           database_api::DatabaseDialect::POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false),
              StatusIs(error::DuplicateColumnName("t.col1")));
  // Expect oid assigner to revert any assigned oids for unsuccessful changes.
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
    CREATE TABLE T(
      col1 bigint PRIMARY KEY,
      col2 character varying(100)))"},
                                    /*proto_descriptor_bytes=*/"",
                                    database_api::DatabaseDialect::POSTGRESQL,
                                    /*use_gsql_to_pg_translation=*/false));
  // Expect 3 oids to be assigned:
  // 1 table, 1 primary key index, 1 primary key constraint
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 3));
}

TEST_P(SchemaUpdaterTest, OidAssignment_BasicTable) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(
  CREATE TABLE T (
    id bigint,
    id2 bigint CHECK (id2 > 0),
    v1 character varying(100) DEFAULT 'some text',
    v2 character varying(100) GENERATED ALWAYS AS (v1 || 'generated') STORED,
    v3 bigint,
    PRIMARY KEY(id, id2)))"},
                                    /*proto_descriptor_bytes=*/"",
                                    database_api::DatabaseDialect::POSTGRESQL,
                                    /*use_gsql_to_pg_translation=*/false));
  // Expect 7 oids to be assigned:
  // 1 table, 1 primary key index, 2 primary key constraints,
  // 1 check constraint, 2 columns (default and generated)
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 7));
}

TEST_P(SchemaUpdaterTest, OidAssignment_InterleaveTable) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(
                           {
                               R"(CREATE TABLE parent(
      col1 bigint PRIMARY KEY,
      col2 character varying(100)))",
                               R"(CREATE TABLE child(
      col1 bigint PRIMARY KEY,
      col2 character varying(100))
      INTERLEAVE IN PARENT parent)"},
                           /*proto_descriptor_bytes=*/"",
                           database_api::DatabaseDialect::POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  // Expect 7 oids to be assigned:
  // 2 tables, 2 primary key indexes, 2 primary key constraints,
  // 1 interleave IN PARENT constraint
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 7));
}

TEST_P(SchemaUpdaterTest, OidAssignment_ForeignKey) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(
                           {
                               R"(CREATE TABLE Customers (
      CustomerId bigint NOT NULL,
      CustomerName character varying(1024) NOT NULL,
      PRIMARY KEY(CustomerId)
      ))",
                               R"(CREATE TABLE ShoppingCarts (
      CartId bigint NOT NULL,
      CustomerId bigint NOT NULL,
      CustomerName character varying(1024) NOT NULL,
      PRIMARY KEY(CartId),
      CONSTRAINT fkshoppingcartscustomers FOREIGN KEY (CustomerId, CustomerName)
        REFERENCES Customers(CustomerId, CustomerName) ON DELETE CASCADE
      ))"},
                           /*proto_descriptor_bytes=*/"",
                           database_api::DatabaseDialect::POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  // Expect 9 oids to be assigned:
  // 2 tables, 2 primary key indexes, 2 primary key constraints,
  // 1 foreign key constraint, 1 foreign key index, 1 unique constraint index
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 9));
}

TEST_P(SchemaUpdaterTest, OidAssignment_NamedSchema) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema({R"(CREATE SCHEMA named_schema)"},
                                    /*proto_descriptor_bytes=*/"",
                                    database_api::DatabaseDialect::POSTGRESQL,
                                    /*use_gsql_to_pg_translation=*/false));
  // Expect 1 oid to be assigned:
  // 1 schema
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 1));
}

TEST_P(SchemaUpdaterTest, OidAssignment_View) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateSchema(
          {R"(CREATE VIEW named_view SQL SECURITY INVOKER AS select 123;)"},
          /*proto_descriptor_bytes=*/"",
          database_api::DatabaseDialect::POSTGRESQL,
          /*use_gsql_to_pg_translation=*/false));
  // Expect 1 oid to be assigned:
  // 1 view
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 1));
}

TEST_P(SchemaUpdaterTest, OidAssignment_Index) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(
                           {
                               R"(CREATE TABLE T(
      col1 bigint PRIMARY KEY,
      col2 character varying(100)))",
                               R"(CREATE INDEX t_idx ON T(col2))"},
                           /*proto_descriptor_bytes=*/"",
                           database_api::DatabaseDialect::POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  // Expect 4 oids to be assigned:
  // 1 tables, 1 primary key index, 1 primary key constraint, 1 index
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 4));
}

TEST_P(SchemaUpdaterTest, OidAssignment_Sequence) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto schema,
      CreateSchema({R"(CREATE SEQUENCE Seq BIT_REVERSED_POSITIVE)"},
                   /*proto_descriptor_bytes=*/"",
                   database_api::DatabaseDialect::POSTGRESQL,
                   /*use_gsql_to_pg_translation=*/false));
  // Expect 1 oid to be assigned:
  // 1 sequence
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 1));
}

TEST_P(SchemaUpdaterTest, OidAssignment_ChangeStream) {
  if (GetParam() == GOOGLE_STANDARD_SQL) GTEST_SKIP();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto schema,
                       CreateSchema(
                           {
                               R"(CREATE TABLE T(
      col1 bigint PRIMARY KEY,
      col2 character varying(100)))",
                               R"(CREATE CHANGE STREAM changestream)"},
                           /*proto_descriptor_bytes=*/"",
                           database_api::DatabaseDialect::POSTGRESQL,
                           /*use_gsql_to_pg_translation=*/false));
  // Expect 5 oids to be assigned:
  // 1 table, 1 primary key index, 1 primary key constraint, 1 change stream,
  // 1 change stream TVF
  ZETASQL_EXPECT_OK(ValidateSchema(*schema.get(), 5));
}

}  // namespace

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
