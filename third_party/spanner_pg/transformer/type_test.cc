//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/catalog/type.h"

#include "zetasql/public/types/type_factory.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/memory/memory.h"
#include "third_party/spanner_pg/catalog/catalog_adapter.h"
#include "third_party/spanner_pg/test_catalog/test_catalog.h"
#include "third_party/spanner_pg/transformer/forward_transformer.h"
#include "third_party/spanner_pg/transformer/transformer.h"
#include "third_party/spanner_pg/transformer/transformer_test.h"

namespace postgres_translator::spangres::test {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

using TypeTransformerTest = ::postgres_translator::TransformerTest;

TEST_F(TypeTransformerTest, PgToGsql_Bool) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(const zetasql::Type* gsql_type,
                       forward_transformer_->BuildGsqlType(BOOLOID));
  EXPECT_TRUE(gsql_type->IsBool());
}

TEST_F(TypeTransformerTest, PgToGsql_Unsupported) {
  EXPECT_THAT(
      forward_transformer_->BuildGsqlType(POINTOID),
      StatusIs(absl::StatusCode::kUnimplemented, HasSubstr("not supported")));
}

TEST_F(TypeTransformerTest, InvalidParameterNames) {
  // Invalid parameter name.
  std::map<std::string, const zetasql::Type*> parameter_types = {
      {"param1", zetasql::types::BoolType()},
      {"p2", zetasql::types::Int64Type()}};
  int num_params = 0;

  std::unique_ptr<CatalogAdapter> adapter =
      GetSpangresTestCatalogAdapter(analyzer_options_);
  EXPECT_THAT(Transformer::BuildPgParameterTypeList(*adapter, parameter_types,
                                                    &num_params),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid parameter name: param1")));

  parameter_types = {{"p1p", zetasql::types::BoolType()}};
  EXPECT_THAT(Transformer::BuildPgParameterTypeList(*adapter, parameter_types,
                                                    &num_params),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid parameter name: p1p")));

  parameter_types = {{"p", zetasql::types::BoolType()}};
  EXPECT_THAT(Transformer::BuildPgParameterTypeList(*adapter, parameter_types,
                                                    &num_params),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("Invalid parameter name: p")));
}

TEST_F(TypeTransformerTest, MissingParameterNames) {
  // Missing type for parameter p2. The returned array should contain 0 for the
  // unspecified parameter.
  std::map<std::string, const zetasql::Type*> parameter_types = {
    {"p1", zetasql::types::BoolType()},
    {"p3", zetasql::types::Int64Type()}};
  int num_params = 0;

  std::unique_ptr<CatalogAdapter> adapter =
      GetSpangresTestCatalogAdapter(analyzer_options_);
  Oid* parameter_oids;
  ZETASQL_ASSERT_OK_AND_ASSIGN(parameter_oids,
                       Transformer::BuildPgParameterTypeList(
                           *adapter, parameter_types, &num_params));
  EXPECT_EQ(parameter_oids[0], BOOLOID);
  EXPECT_EQ(parameter_oids[1], 0);
  EXPECT_EQ(parameter_oids[2], INT8OID);
  EXPECT_EQ(num_params, 3);

  parameter_types = {{"p4", zetasql::types::BoolType()}};
  ZETASQL_ASSERT_OK_AND_ASSIGN(parameter_oids,
                       Transformer::BuildPgParameterTypeList(
                           *adapter, parameter_types, &num_params));
  EXPECT_EQ(parameter_oids[0], 0);
  EXPECT_EQ(parameter_oids[1], 0);
  EXPECT_EQ(parameter_oids[2], 0);
  EXPECT_EQ(parameter_oids[3], BOOLOID);
  EXPECT_EQ(num_params, 4);

  parameter_types = {
    {"p2", zetasql::types::BoolType()},
    {"p4", zetasql::types::Int64Type()},
    {"p6", zetasql::types::DateType()},
  };
  ZETASQL_ASSERT_OK_AND_ASSIGN(parameter_oids,
                       Transformer::BuildPgParameterTypeList(
                           *adapter, parameter_types, &num_params));
  EXPECT_EQ(parameter_oids[0], 0);
  EXPECT_EQ(parameter_oids[1], BOOLOID);
  EXPECT_EQ(parameter_oids[2], 0);
  EXPECT_EQ(parameter_oids[3], INT8OID);
  EXPECT_EQ(parameter_oids[4], 0);
  EXPECT_EQ(parameter_oids[5], DATEOID);
  EXPECT_EQ(num_params, 6);
}

TEST_F(TypeTransformerTest, InvalidParameterTypes) {
  std::map<std::string, const zetasql::Type*> parameter_types = {
      {"p1", zetasql::types::BoolType()},
      {"p2", zetasql::types::Int64Type()},
      {"p3", zetasql::types::NumericType()}};
  int num_params = 0;

  std::unique_ptr<CatalogAdapter> adapter =
      GetSpangresTestCatalogAdapter(analyzer_options_);
  EXPECT_THAT(Transformer::BuildPgParameterTypeList(*adapter, parameter_types,
                                                    &num_params),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("Unsupported ZetaSQL Type")));
}

TEST_F(TypeTransformerTest, ValidParameters) {
  std::map<std::string, const zetasql::Type*> parameter_types = {
      {"p2", zetasql::types::BoolType()},
      {"p3", zetasql::types::Int64Type()},
      {"p1", zetasql::types::DoubleType()}};

  std::unique_ptr<CatalogAdapter> adapter =
      GetSpangresTestCatalogAdapter(analyzer_options_);
  Oid* parameter_oids;
  int num_params = 0;
  ZETASQL_ASSERT_OK_AND_ASSIGN(parameter_oids,
                       Transformer::BuildPgParameterTypeList(
                           *adapter, parameter_types, &num_params));
  EXPECT_EQ(parameter_oids[0], FLOAT8OID);
  EXPECT_EQ(parameter_oids[1], BOOLOID);
  EXPECT_EQ(parameter_oids[2], INT8OID);
  EXPECT_EQ(num_params, 3);
}

}  // namespace
}  // namespace postgres_translator::spangres::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
