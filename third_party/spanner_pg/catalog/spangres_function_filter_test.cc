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

#include "third_party/spanner_pg/catalog/spangres_function_filter.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/flags/flag.h"
#include "google/protobuf/text_format.h"

ABSL_FLAG(bool, spangres_test_enable_function, true,
          "Test flag to enable a function");
ABSL_FLAG(bool, spangres_test_enable_signature, true,
          "Test flag to enable a signature");

namespace postgres_translator {
namespace {

using testing::Eq;
using testing::IsEmpty;
using testing::SizeIs;
using zetasql_base::testing::IsOkAndHolds;

TEST(SpangresFunctionFilterTest, RemovesManuallyRegisteredFunctions) {
  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "pg" name_path: "array_all_equal" }
      )pb",
      &fn));

  ASSERT_THAT(FilterEnabledFunctionsAndSignatures({fn}),
              IsOkAndHolds(IsEmpty()));
}

TEST(SpangresFunctionFilterTest, RemovesCatalogDisabledFunctions) {
  absl::SetFlag(&FLAGS_spangres_test_enable_function, false);

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "pg" name_path: "fn" }
        enable_in_catalog: "spangres_test_enable_function"
      )pb",
      &fn));

  ASSERT_THAT(FilterEnabledFunctionsAndSignatures({fn}),
              IsOkAndHolds(IsEmpty()));
}

TEST(SpangresFunctionFilterTest, RemovesCatalogDisabledSignatures) {
  absl::SetFlag(&FLAGS_spangres_test_enable_signature, false);

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "spanner" name_path: "fn" }
        signatures: {
          oid: 50001
          enable_in_catalog: "spangres_test_enable_signature"
        }
        signatures: { oid: 50002 }
      )pb",
      &fn));

  ZETASQL_ASSERT_OK_AND_ASSIGN(const std::vector<FunctionProto>& result,
                       FilterEnabledFunctionsAndSignatures({fn}));
  ASSERT_THAT(result, SizeIs(1));
  ASSERT_THAT(result[0].signatures(), SizeIs(1));
  ASSERT_THAT(result[0].signatures()[0].oid(), Eq(50002));
}

TEST(SpangresFunctionFilterTest, RemovesFunctionsWithNoEnabledSignatures) {
  absl::SetFlag(&FLAGS_spangres_test_enable_signature, false);

  FunctionProto fn;
  ASSERT_TRUE(google::protobuf::TextFormat::ParseFromString(
      R"pb(
        mapped_name_path: { name_path: "spanner" name_path: "fn" }
        signatures: {
          oid: 50001
          enable_in_catalog: "spangres_test_enable_signature"
        }
        signatures: {
          oid: 50002
          enable_in_catalog: "spangres_test_enable_signature"
        }
      )pb",
      &fn));

  ASSERT_THAT(FilterEnabledFunctionsAndSignatures({fn}),
              IsOkAndHolds(IsEmpty()));
}

}  // namespace
}  // namespace postgres_translator
