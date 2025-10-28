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

#include "third_party/spanner_pg/catalog/spangres_function_verifier.h"

#include "zetasql/public/function.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "google/protobuf/text_format.h"

namespace postgres_translator {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::IsOk;
using ::zetasql_base::testing::StatusIs;

TEST(SpangresFunctionVerifierTest, ReturnsErrorWhenFunctionHasNoSignatures) {

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "error" }
      )pb",
      &fn));

  ASSERT_THAT(ValidateCatalogFunctions({fn}),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("must have at least one signature")));
}

TEST(SpangresFunctionVerifierTest,
     ReturnsErrorWhenFunctionSignatureNamePathIsNotNamespaced) {

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "error" }
        signatures: { postgresql_name_paths: { name_path: "error" } }
      )pb",
      &fn));

  ASSERT_THAT(
      ValidateCatalogFunctions({fn}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "postgresql_name_path must have 2 parts (namespace and name)")));
}

TEST(SpangresFunctionVerifierTest,
     ReturnsErrorWhenFunctionSignatureNamePathHasNestedNamespaces) {

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "error" }
        signatures: {
          postgresql_name_paths: {
            name_path: "spanner"
            name_path: "safe"
            name_path: "error"
          }
        }
      )pb",
      &fn));

  ASSERT_THAT(
      ValidateCatalogFunctions({fn}),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "postgresql_name_path must have 2 parts (namespace and name)")));
}

TEST(SpangresFunctionVerifierTest,
     ReturnsErrorWhenFunctionSignatureWithNamedArgumentHasNoName) {

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "error" }
        signatures: {
          postgresql_name_paths: { name_path: "spanner" name_path: "error" }
          arguments: {
            type: { oid: 25 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_OR_NAMED
          }
        }
      )pb",
      &fn));

  ASSERT_THAT(
      ValidateCatalogFunctions({fn}),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("name must be defined")));
}

TEST(SpangresFunctionVerifierTest, ReturnsOkWhenSignatureIsValid) {

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "success" }
        signatures: {
          postgresql_name_paths: { name_path: "spanner" name_path: "success" }
          arguments: {
            type: { oid: 25 }
            cardinality: REQUIRED
            named_argument_kind: POSITIONAL_OR_NAMED
            name: "arg1"
          }
        }
      )pb",
      &fn));

  ASSERT_THAT(ValidateCatalogFunctions({fn}), IsOk());
}

}  // namespace
}  // namespace postgres_translator
