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
      collcollate: ""
      collctype: ""
    )pb",
    R"pb(
      oid: 950
      collname: "C"
      collprovider: 'c'
      collisdeterministic: true
      collencoding: -1
      collcollate: "C"
      collctype: "C"
    )pb"));

TEST_F(PgCollationDataTest, GetPgCollationDataFromBootstrapFailure) {
  // POSIX is an unsupported PG collation.
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "POSIX"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "invalid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

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
  // xid is an unsupported PG type.
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "xid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), "invalid"),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(PgTypeDataTest, GetPgTypeDataFromBootstrapOidFailure) {
  // xml (142) is an unsupported PG type.
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), 142),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
  // default (100) is a collation, not a type.
  ASSERT_THAT(GetPgTypeDataFromBootstrap(GetPgBootstrapCatalog(), 100),
              zetasql_base::testing::StatusIs(absl::StatusCode::kNotFound));
}

}  // namespace postgres_translator
