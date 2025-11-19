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

#include "third_party/spanner_pg/codegen/default_values_reader.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "absl/status/status.h"
#include "third_party/spanner_pg/codegen/default_values_embed.h"

namespace postgres_translator {
namespace {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

TEST(ProcsDefaultValuesReaderTest, ParsesProductionCatalogCorrectly) {
  ZETASQL_ASSERT_OK(GetProcsDefaultValues(kDefaultValuesEmbed));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenJsonIsMalformed) {

  std::string json_string = R"(
    abc
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("failed to parse default values JSON")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenRootElementIsNotAnArray) {

  std::string json_string = R"(
    "abc"
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("json document is not an array")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenJsonEntryHasNoProcOid) {

  std::string json_string = R"(
    [{
      "abc": 1
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("proc_oid entry could not be found")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenProcOidIsNotANumber) {

  std::string json_string = R"(
    [{
      "proc_oid": "abc"
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("proc_oid is not an integer number")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenProcOidIsNotAnInteger) {

  std::string json_string = R"(
    [{
      "proc_oid": 1.0
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("proc_oid is not an integer number")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenProcOidOverflowsUint32) {

  // uint32_t + 1
  std::string json_string = R"(
    [{
      "proc_oid": 4294967296
    }]
  )";

  ASSERT_THAT(
      GetProcsDefaultValues(json_string),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("proc_oid overflow")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenProcOidUnderflowsUint32) {

  std::string json_string = R"(
    [{
      "proc_oid": -1
    }]
  )";

  ASSERT_THAT(
      GetProcsDefaultValues(json_string),
      StatusIs(absl::StatusCode::kInternal, HasSubstr("proc_oid underflow")));
}

TEST(ProcsDefaultValuesReaderTest,
     ReturnsErrorWhenJsonEntryHasNoDefaultValues) {

  std::string json_string = R"(
    [{
      "proc_oid": 50050
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("default_values entry could not be found")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenDefaultValuesIsNotAnArray) {

  std::string json_string = R"(
    [{
      "proc_oid": 50050,
      "default_values": "abc"
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("default_values is not an array")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenDefaultValuesIsEmpty) {

  std::string json_string = R"(
    [{
      "proc_oid": 50050,
      "default_values": []
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("default_values is empty")));
}

TEST(ProcsDefaultValuesReaderTest,
     ReturnsErrorWhenDefaultValuesElementIsNotAString) {

  std::string json_string = R"(
    [{
      "proc_oid": 50050,
      "default_values": [1]
    }]
  )";

  ASSERT_THAT(GetProcsDefaultValues(json_string),
              StatusIs(absl::StatusCode::kInternal,
                       HasSubstr("default_values element is not a string")));
}

TEST(ProcsDefaultValuesReaderTest, ReturnsErrorWhenInsertingDuplicateOid) {

  std::string json_string = R"(
    [
      {
        "proc_oid": 50050,
        "default_values": ["null::bigint"]
      },
      {
        "proc_oid": 50050,
        "default_values": ["null::bigint"]
      }
    ]
  )";

  ASSERT_THAT(
      GetProcsDefaultValues(json_string),
      StatusIs(
          absl::StatusCode::kInternal,
          HasSubstr(
              "duplicate OID found when building PostgreSQL default values")));
}
}  // namespace
}  // namespace postgres_translator
