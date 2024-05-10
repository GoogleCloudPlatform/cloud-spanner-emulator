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

#include "third_party/spanner_pg/interface/bootstrap_catalog_accessor.h"

#include "net/proto2/contrib/parse_proto/parse_text_proto.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/interface/bootstrap_catalog_data.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"

using ::google::protobuf::contrib::parse_proto::ParseTextProtoOrDie;

namespace postgres_translator {

class PgCollationDataTest : public testing::TestWithParam<absl::string_view> {};
class PgNamespaceDataTest : public testing::TestWithParam<absl::string_view> {};
class PgProcDataTest : public testing::TestWithParam<absl::string_view> {};
class PgTypeDataTest : public testing::TestWithParam<absl::string_view> {};


TEST_P(PgCollationDataTest, GetPgCollationDataFromBootstrapSuccess) {
  PgCollationData expected = ParseTextProtoOrDie(GetParam());
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgCollationData actual,
                       GetPgCollationDataFromBootstrap(GetPgBootstrapCatalog(),
                                                       expected.collname()));
  EXPECT_THAT(actual, testing::EqualsProto(expected));
}

INSTANTIATE_TEST_SUITE_P(
  PgCollationDataTestData,
  PgCollationDataTest,
  testing::Values(
    R"pb(
      oid: 100
      collname: "default"
      collprovider: 'd'
      collisdeterministic: true
      collencoding: -1
    )pb",
    R"pb(
      oid: 950
      collname: "C"
      collprovider: 'c'
      collisdeterministic: true
      collencoding: -1
    )pb"));

TEST_F(PgCollationDataTest, GetPgCollationDataFromBootstrapFailure) {
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "invalid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_P(PgNamespaceDataTest, GetPgNamespaceDataFromBootstrapSuccess) {
  PgNamespaceData expected = ParseTextProtoOrDie(GetParam());
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgNamespaceData actual,
                       GetPgNamespaceDataFromBootstrap(GetPgBootstrapCatalog(),
                                                      expected.nspname()));
  EXPECT_THAT(actual, testing::EqualsProto(expected));
}

TEST_F(PgNamespaceDataTest, GetPgNamespaceDataFromBootstrapFailure) {
  ASSERT_THAT(GetPgNamespaceDataFromBootstrap(GetPgBootstrapCatalog(),
                                              "invalid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

INSTANTIATE_TEST_SUITE_P(
  PgNamespaceDataTestData,
  PgNamespaceDataTest,
  testing::Values(
    R"pb(
      oid: 11
      nspname: "pg_catalog"
    )pb",
    R"pb(
      oid: 2200
      nspname: "public"
    )pb",
    R"pb(
      oid: 50000
      nspname: "spanner"
    )pb"));

TEST_P(PgProcDataTest, GetPgProcDataFromBootstrapSuccess) {
  PgProcData expected = ParseTextProtoOrDie(GetParam());
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PgProcData actual,
      GetPgProcDataFromBootstrap(GetPgBootstrapCatalog(), expected.oid()));
  EXPECT_THAT(actual, testing::EqualsProto(expected));
}

TEST_F(PgProcDataTest, GetPgProcDataFromBootstrapFailure) {
  // default (100) is a collation, not a proc.
  ASSERT_THAT(
      GetPgProcDataFromBootstrap(GetPgBootstrapCatalog(), 100),
      zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

INSTANTIATE_TEST_SUITE_P(PgProcDataTestData, PgProcDataTest,
                         testing::Values(
                             R"pb(
                               oid: 1973
                               proname: "div"
                               pronamespace: 11
                               prorows: 0
                               provariadic: 0
                               prokind: 'f'
                               proretset: false
                               provolatile: 'i'
                               pronargs: 2
                               prorettype: 1700
                               proargtypes: 1700
                               proargtypes: 1700
                             )pb",
                             R"pb(
                               oid: 3058
                               proname: "concat"
                               pronamespace: 11
                               prorows: 0
                               provariadic: 2276
                               prokind: 'f'
                               proretset: false
                               provolatile: 's'
                               pronargs: 1
                               prorettype: 25
                               proargtypes: 2276
                             )pb",
                             R"pb(
                               oid: 3219
                               proname: "jsonb_array_elements"
                               pronamespace: 11
                               prorows: 100
                               provariadic: 0
                               prokind: 'f'
                               proretset: true
                               provolatile: 'i'
                               pronargs: 1
                               prorettype: 3802
                               proargtypes: 3802
                             )pb"));

TEST_P(PgTypeDataTest, GetPgTypeDataFromBootstrapNameSuccess) {
  PgTypeData expected = ParseTextProtoOrDie(GetParam());
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgTypeData actual,
                       GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(),
                                                  expected.typname()));
  EXPECT_THAT(actual, testing::EqualsProto(expected));
}

TEST_P(PgTypeDataTest, GetPgTypeDataFromBootstrapOidSuccess) {
  PgTypeData expected = ParseTextProtoOrDie(GetParam());
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgTypeData actual,
                       GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(),
                                                  expected.oid()));
  EXPECT_THAT(actual, testing::EqualsProto(expected));
}

INSTANTIATE_TEST_SUITE_P(
  PgTypeDataTestData,
  PgTypeDataTest,
  testing::Values(
    R"pb(
      oid: 16
      typname: "bool"
      typlen: 1,
      typbyval: true,
      typtype: "b",
      typcategory: "B",
      typispreferred: true
      typisdefined: true
      typdelim: ","
      typelem: 0
      typarray: 1000
    )pb",
    R"pb(
      oid: 1000
      typname: "_bool"
      typlen: -1
      typbyval: false
      typtype: "b"
      typcategory: "A"
      typispreferred: false
      typisdefined: true
      typdelim: ","
      typelem: 16
      typarray: 0
    )pb"));

TEST_F(PgTypeDataTest, GetPgTypeDataFromBootstrapNameFailure) {
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "invalid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(PgTypeDataTest, GetPgTypeDataFromBootstrapOidFailure) {
  // default (100) is a collation, not a type.
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), 100),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace postgres_translator
