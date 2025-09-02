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

#include "third_party/spanner_pg/datatypes/common/jsonb/jsonb_value.h"

#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "third_party/spanner_pg/util/valid_memory_context_fixture.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {
namespace {

using postgres_translator::spangres::datatypes::common::jsonb::PgJsonbValue;
using postgres_translator::spangres::datatypes::common::jsonb::TreeNode;
using ::postgres_translator::test::ValidMemoryContext;

using JsonbValueTest = ValidMemoryContext;

constexpr char kJSONBStr[] = R"(
  {
    "pi": 3.141,
    "happy": true,
    "name": "Niels",
    "nothing": null,
    "answer": {
      "everything": 42
    },
    "list": [1, 0, 2],
    "object": {
      "currency": "USD",
      "value": 42.99
    }
  }
)";

TEST_F(JsonbValueTest, JsonbString) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse(R"("foo")", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), true);
  EXPECT_THAT(jsonb.Serialize(), "\"foo\"");
}

TEST_F(JsonbValueTest, JsonbBool) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("true", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsBoolean(), true);
  EXPECT_THAT(jsonb.Serialize(), "true");
}

TEST_F(JsonbValueTest, JsonbObject) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse(R"({"a": 1})", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_THAT(jsonb.Serialize(), R"({"a": 1})");
}

TEST_F(JsonbValueTest, JsonbArray) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse(R"([1, 2, 3])", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), true);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_THAT(jsonb.Serialize(), R"([1, 2, 3])");
}

TEST_F(JsonbValueTest, JsonbNumeric) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("123.456", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), true);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_THAT(jsonb.Serialize(), "123.456");
}

TEST_F(JsonbValueTest, SimpleJsonbObject) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("{\"a\": 1}", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_THAT(jsonb.Serialize(), "{\"a\": 1}");
  EXPECT_TRUE(jsonb.HasMember("a"));
  auto member = jsonb.GetMemberIfExists("a");
  EXPECT_TRUE(member.has_value());
  EXPECT_THAT(member->Serialize(), "1");
  std::vector<std::pair<absl::string_view, PgJsonbValue>> members =
      jsonb.GetMembers();
  EXPECT_THAT(members.size(), 1);
  EXPECT_THAT(members[0].first, "a");
  EXPECT_THAT(members[0].second.Serialize(), "1");
  EXPECT_FALSE(jsonb.RemoveMember("b"));
  // Expected mutation on the object.
  EXPECT_TRUE(jsonb.RemoveMember("a"));
  EXPECT_FALSE(jsonb.HasMember("a"));
  EXPECT_THAT(jsonb.GetMembers().size(), 0);
  ZETASQL_EXPECT_OK(jsonb.CreateMemberIfNotExists("a"));
  auto null_member = jsonb.GetMemberIfExists("a");
  EXPECT_TRUE(null_member.has_value());
  EXPECT_TRUE(jsonb.RemoveMember("a"));
  EXPECT_THAT(jsonb.Serialize(), "{}");
  EXPECT_THAT(jsonb.IsEmpty(), true);
}

TEST_F(JsonbValueTest, EmptyObject) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("{}", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.Serialize(), "{}");
  EXPECT_THAT(jsonb.GetMembers().size(), 0);
  EXPECT_THAT(jsonb.IsEmpty(), true);
  auto member = jsonb.GetMemberIfExists("a");
  EXPECT_FALSE(member.has_value());
  ZETASQL_EXPECT_OK(jsonb.CreateMemberIfNotExists("a"));
  EXPECT_TRUE(jsonb.RemoveMember("a"));
}

TEST_F(JsonbValueTest, NullJson) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("null", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), true);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_THAT(jsonb.Serialize(), "null");
  auto member = jsonb.GetMemberIfExists("a");
  EXPECT_FALSE(member.has_value());
}

TEST_F(JsonbValueTest, CreateEmptyArray) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  PgJsonbValue jsonb = PgJsonbValue::CreateEmptyArray(&tree_nodes);
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), true);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), true);
  EXPECT_THAT(jsonb.Serialize(), "[]");
}

TEST_F(JsonbValueTest, CreateEmptyObject) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  PgJsonbValue jsonb = PgJsonbValue::CreateEmptyObject(&tree_nodes);
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), true);
  EXPECT_THAT(jsonb.Serialize(), "{}");
}

TEST_F(JsonbValueTest, ParseJsonbObject) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse(kJSONBStr, &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_THAT(jsonb.IsArray(), false);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  std::vector<std::pair<absl::string_view, PgJsonbValue>> members =
      jsonb.GetMembers();
  EXPECT_THAT(members.size(), 7);
  for (auto& elem : members) {
    ABSL_LOG(ERROR) << elem.first << ": " << elem.second.Serialize();
  }
  EXPECT_EQ(members[0].first, "pi");
  EXPECT_THAT(jsonb.GetMemberIfExists("pi").value().Serialize(), "3.141");
  EXPECT_THAT(jsonb.GetMemberIfExists("happy").value().Serialize(), "true");
  EXPECT_THAT(jsonb.GetMemberIfExists("name").value().Serialize(), "\"Niels\"");
  EXPECT_THAT(jsonb.GetMemberIfExists("nothing").value().Serialize(), "null");
  EXPECT_THAT(jsonb.GetMemberIfExists("answer").value().Serialize(),
              "{\"everything\": 42}");
  EXPECT_THAT(jsonb.GetMemberIfExists("list").value().Serialize(), "[1, 0, 2]");
  PgJsonbValue nested_object = jsonb.GetMemberIfExists("object").value();
  EXPECT_THAT(nested_object.GetMemberIfExists("currency").value().Serialize(),
              "\"USD\"");
  EXPECT_THAT(nested_object.GetMemberIfExists("value").value().Serialize(),
              "42.99");
  auto member = jsonb.GetMemberIfExists("a");
  EXPECT_FALSE(member.has_value());
  EXPECT_THAT(jsonb.GetMemberIfExists("happy").value().Serialize(), "true");
  // Expected mutation on the object.
  EXPECT_TRUE(jsonb.RemoveMember("happy"));
  auto null_member_one = jsonb.GetMemberIfExists("happy");
  EXPECT_FALSE(null_member_one.has_value());
  EXPECT_FALSE(jsonb.RemoveMember("happy"));
  EXPECT_THAT(jsonb.GetMembers().size(), 6);
  // Expected mutation on the object.
  EXPECT_TRUE(jsonb.RemoveMember("name"));
  auto null_member_two = jsonb.GetMemberIfExists("name");
  EXPECT_FALSE(null_member_two.has_value());
  EXPECT_FALSE(jsonb.RemoveMember("name"));
  EXPECT_THAT(jsonb.GetMembers().size(), 5);
  auto pi_member = jsonb.GetMemberIfExists("pi");
  EXPECT_TRUE(pi_member.has_value());
  EXPECT_THAT(pi_member->Serialize(), "3.141");
}

TEST_F(JsonbValueTest, JsonbStringsAndNumbers) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse(kJSONBStr, &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_TRUE(jsonb.GetMemberIfExists("name").value().IsString());
  EXPECT_EQ(jsonb.GetMemberIfExists("name").value().GetString(), "Niels");

  // Validate the numbers APIs.
  PgJsonbValue number = jsonb.GetMemberIfExists("answer")
                            .value()
                            .GetMemberIfExists("everything")
                            .value();
  EXPECT_TRUE(number.IsNumeric());
  EXPECT_EQ(number.GetNumeric(), "42");

  number = jsonb.GetMemberIfExists("pi").value();
  EXPECT_TRUE(number.IsNumeric());
  EXPECT_EQ(number.GetNumeric(), "3.141");

  number = jsonb.GetMemberIfExists("object")
               .value()
               .GetMemberIfExists("value")
               .value();
  EXPECT_TRUE(number.IsNumeric());
  EXPECT_EQ(number.GetNumeric(), "42.99");
}

TEST_F(JsonbValueTest, CreateNewMember) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("{}", &tree_nodes));
  EXPECT_THAT(jsonb.IsEmpty(), true);
  ZETASQL_EXPECT_OK(jsonb.CreateMemberIfNotExists("a"));
  EXPECT_THAT(jsonb.Serialize(), R"({"a": null})");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_value,
                       PgJsonbValue::Parse("[1,2,3]", &tree_nodes));
  jsonb.GetMemberIfExists("a")->SetValue(new_value);
  EXPECT_THAT(jsonb.Serialize(), R"({"a": [1, 2, 3]})");
  EXPECT_THAT(jsonb.IsEmpty(), false);
}

TEST_F(JsonbValueTest, SimpleJsonbArray) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("[1, 2, 3]", &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), false);
  EXPECT_THAT(jsonb.IsArray(), true);
  EXPECT_THAT(jsonb.IsNull(), false);
  EXPECT_THAT(jsonb.IsBoolean(), false);
  EXPECT_THAT(jsonb.IsNumeric(), false);
  EXPECT_THAT(jsonb.IsString(), false);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_THAT(jsonb.Serialize(), "[1, 2, 3]");
}

TEST_F(JsonbValueTest, JsonbArrayGettersOnNull) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("null", &tree_nodes));
  EXPECT_FALSE(jsonb.GetArrayElementIfExists(0).has_value());
}

TEST_F(JsonbValueTest, JsonbArrayGetters) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("[1, 2, 3]", &tree_nodes));
  EXPECT_THAT(jsonb.GetArrayElements().size(), 3);
  // GetArrayElementIfExists.
  EXPECT_THAT(jsonb.GetArrayElementIfExists(0)->Serialize(), "1");
  EXPECT_THAT(jsonb.GetArrayElementIfExists(1)->Serialize(), "2");
  EXPECT_THAT(jsonb.GetArrayElementIfExists(2)->Serialize(), "3");
  EXPECT_FALSE(jsonb.GetArrayElementIfExists(3).has_value());
  EXPECT_THAT(jsonb.GetArrayElementIfExists(-1)->Serialize(), "3");
  EXPECT_THAT(jsonb.GetArrayElementIfExists(-2)->Serialize(), "2");
  EXPECT_THAT(jsonb.GetArrayElementIfExists(-3)->Serialize(), "1");
  EXPECT_FALSE(jsonb.GetArrayElementIfExists(-4).has_value());
}

TEST_F(JsonbValueTest, JsonbArrayInsertElementTest) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  // Parse the array and check correctness.
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue base_array,
                       PgJsonbValue::Parse("[1, 2, 3]", &tree_nodes));
  ASSERT_THAT(base_array.GetArraySize(), 3);
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "1");
  EXPECT_THAT(base_array.GetArrayElementIfExists(1)->Serialize(), "2");
  EXPECT_THAT(base_array.GetArrayElementIfExists(2)->Serialize(), "3");
  EXPECT_THAT(base_array.Serialize(), "[1, 2, 3]");

  // Insert one element to the end.
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_four,
                       PgJsonbValue::Parse("4", &tree_nodes));
  ZETASQL_ASSERT_OK(base_array.InsertArrayElement(new_element_four, 3));
  ASSERT_THAT(base_array.GetArraySize(), 4);
  EXPECT_THAT(base_array.GetArrayElementIfExists(3)->Serialize(), "4");
  EXPECT_THAT(base_array.Serialize(), "[1, 2, 3, 4]");

  // Insert an element past the end of the array. Should only append to the end.
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_five,
                       PgJsonbValue::Parse("5", &tree_nodes));
  ZETASQL_EXPECT_OK(base_array.InsertArrayElement(new_element_five, 10));
  ASSERT_THAT(base_array.GetArraySize(), 5);
  EXPECT_THAT(base_array.GetArrayElementIfExists(4)->Serialize(), "5");
  EXPECT_THAT(base_array.Serialize(), "[1, 2, 3, 4, 5]");

  // Insert element to the beginning of the array.
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_zero,
                       PgJsonbValue::Parse("0", &tree_nodes));
  ZETASQL_ASSERT_OK(base_array.InsertArrayElement(new_element_zero, 0));
  ASSERT_THAT(base_array.GetArraySize(), 6);
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "0");
  EXPECT_THAT(base_array.GetArrayElementIfExists(1)->Serialize(), "1");
  EXPECT_THAT(base_array.GetArrayElementIfExists(2)->Serialize(), "2");
  EXPECT_THAT(base_array.GetArrayElementIfExists(3)->Serialize(), "3");
  EXPECT_THAT(base_array.GetArrayElementIfExists(4)->Serialize(), "4");
  EXPECT_THAT(base_array.GetArrayElementIfExists(5)->Serialize(), "5");
  EXPECT_THAT(base_array.Serialize(), "[0, 1, 2, 3, 4, 5]");

  // Insert elements in the middle of the array
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_small_object,
                       PgJsonbValue::Parse(R"({"a": 1})", &tree_nodes));
  ZETASQL_ASSERT_OK(base_array.InsertArrayElement(new_element_small_object, 2));
  ASSERT_THAT(base_array.GetArraySize(), 7);
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "0");
  EXPECT_THAT(base_array.GetArrayElementIfExists(1)->Serialize(), "1");
  EXPECT_THAT(base_array.GetArrayElementIfExists(2)->Serialize(), "{\"a\": 1}");
  EXPECT_THAT(base_array.GetArrayElementIfExists(3)->Serialize(), "2");
  EXPECT_THAT(base_array.GetArrayElementIfExists(4)->Serialize(), "3");
  EXPECT_THAT(base_array.GetArrayElementIfExists(5)->Serialize(), "4");
  EXPECT_THAT(base_array.GetArrayElementIfExists(6)->Serialize(), "5");
  EXPECT_THAT(base_array.Serialize(), R"([0, 1, {"a": 1}, 2, 3, 4, 5])");

  // Insert into the array until the max array size.
  while (base_array.GetArraySize() < kJSONBMaxArraySize) {
    ZETASQL_EXPECT_OK(
        base_array.InsertArrayElement(new_element_zero, kJSONBMaxArraySize));
  }
  ASSERT_THAT(base_array.GetArraySize(), kJSONBMaxArraySize);
  EXPECT_THAT(
      base_array.GetArrayElementIfExists(kJSONBMaxArraySize - 1)->Serialize(),
      "0");
  // Inserting an element anywhere should fail as it causes the array to exceed
  // the max size.
  EXPECT_THAT(
      base_array.InsertArrayElement(new_element_five, kJSONBMaxArraySize),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kOutOfRange,
          absl::StrCat("JSONB array size exceeds the limit of ",
                       kJSONBMaxArraySize)));
  ASSERT_THAT(base_array.GetArraySize(), kJSONBMaxArraySize);
  EXPECT_THAT(
      base_array.InsertArrayElement(new_element_five, kJSONBMaxArraySize / 2),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kOutOfRange,
          absl::StrCat("JSONB array size exceeds the limit of ",
                       kJSONBMaxArraySize)));
}

TEST_F(JsonbValueTest, JsonbArrayModifyArrayValue) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue object,
                       PgJsonbValue::Parse(R"({"a": [1,2,4]})", &tree_nodes));
  EXPECT_THAT(object.GetMemberIfExists("a").value().Serialize(), "[1, 2, 4]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_three,
                       PgJsonbValue::Parse("3", &tree_nodes));
  ZETASQL_ASSERT_OK(object.GetMemberIfExists("a").value().InsertArrayElement(
      new_element_three, 2));
  EXPECT_THAT(object.GetMemberIfExists("a").value().Serialize(),
              "[1, 2, 3, 4]");
  EXPECT_THAT(object.Serialize(), R"({"a": [1, 2, 3, 4]})");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_five,
                       PgJsonbValue::Parse("5", &tree_nodes));
  ZETASQL_ASSERT_OK(object.GetMemberIfExists("a").value().InsertArrayElement(
      new_element_five, 1000));
  EXPECT_THAT(object.GetMemberIfExists("a").value().Serialize(),
              "[1, 2, 3, 4, 5]");
  EXPECT_THAT(object.Serialize(), R"({"a": [1, 2, 3, 4, 5]})");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_element_zero,
                       PgJsonbValue::Parse("0", &tree_nodes));
  ZETASQL_ASSERT_OK(object.GetMemberIfExists("a").value().InsertArrayElement(
      new_element_zero, -1000));
  EXPECT_THAT(object.GetMemberIfExists("a").value().Serialize(),
              "[0, 1, 2, 3, 4, 5]");
  EXPECT_THAT(object.Serialize(), R"({"a": [0, 1, 2, 3, 4, 5]})");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue new_bool_element,
                       PgJsonbValue::Parse("true", &tree_nodes));
  ZETASQL_ASSERT_OK(object.GetMemberIfExists("a").value().InsertArrayElement(
      new_bool_element, -2));
  EXPECT_THAT(object.GetMemberIfExists("a").value().Serialize(),
              "[0, 1, 2, 3, true, 4, 5]");
  EXPECT_THAT(object.Serialize(), R"({"a": [0, 1, 2, 3, true, 4, 5]})");
}

TEST_F(JsonbValueTest, InaccurateEstimates) {
  // These are test cases where we may lose track of the `max_depth_estimate_`.
  // Make sure that serialization is still correct.
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue base_array,
                       PgJsonbValue::Parse(R"([1, 2, 3])", &tree_nodes));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue insert_object,
                       PgJsonbValue::Parse(R"({"a": 1})", &tree_nodes));
  ZETASQL_ASSERT_OK(base_array.InsertArrayElement(insert_object, 0));
  EXPECT_THAT(base_array.Serialize(), R"([{"a": 1}, 1, 2, 3])");
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "{\"a\": 1}");
}

TEST_F(JsonbValueTest, JsonbArrayRemoveElementTest) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue base_array,
                       PgJsonbValue::Parse("[1, 2, 3]", &tree_nodes));
  EXPECT_THAT(base_array.GetArraySize(), 3);
  EXPECT_TRUE(base_array.RemoveArrayElement(0));
  EXPECT_THAT(base_array.GetArraySize(), 2);
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "2");
  EXPECT_THAT(base_array.GetArrayElementIfExists(1)->Serialize(), "3");
  // Index doesn't exist
  EXPECT_FALSE(base_array.RemoveArrayElement(2));
  EXPECT_FALSE(base_array.RemoveArrayElement(-3));
  EXPECT_THAT(base_array.GetArraySize(), 2);
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "2");
  EXPECT_THAT(base_array.GetArrayElementIfExists(1)->Serialize(), "3");
  // Support negative index
  EXPECT_TRUE(base_array.RemoveArrayElement(-1));
  EXPECT_THAT(base_array.GetArraySize(), 1);
  EXPECT_THAT(base_array.GetArrayElementIfExists(0)->Serialize(), "2");
  // Remove the final element
  EXPECT_TRUE(base_array.RemoveArrayElement(-1));
  EXPECT_THAT(base_array.GetArraySize(), 0);
  EXPECT_FALSE(base_array.RemoveArrayElement(0));
}

TEST_F(JsonbValueTest, CleanUpJsonbObject) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PgJsonbValue jsonb,
      PgJsonbValue::Parse(R"({"a": 1, "b": null})", &tree_nodes));
  EXPECT_THAT(jsonb.Serialize(), R"({"a": 1, "b": null})");
  EXPECT_THAT(jsonb.GetMembers().size(), 2);
  EXPECT_THAT(jsonb.GetMemberIfExists("a").value().Serialize(), "1");
  EXPECT_THAT(jsonb.GetMemberIfExists("b").value().Serialize(), "null");
  jsonb.CleanUpJsonbObject();
  EXPECT_THAT(jsonb.GetMembers().size(), 1);
  EXPECT_THAT(jsonb.Serialize(), R"({"a": 1})");
}

TEST_F(JsonbValueTest, CleanUpJsonbObjectMultipleLevels) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb, PgJsonbValue::Parse(
                                               R"({"a": {"c": null, "d": []},
                              "e": null, "f": {}, "g": []})",
                                               &tree_nodes));
  EXPECT_THAT(jsonb.GetMembers().size(), 4);
  EXPECT_THAT(jsonb.GetMemberIfExists("a").value().GetMembers().size(), 2);
  EXPECT_THAT(jsonb.GetMemberIfExists("a")
                  .value()
                  .GetMemberIfExists("c")
                  .value()
                  .Serialize(),
              "null");
  EXPECT_THAT(jsonb.GetMemberIfExists("a")
                  .value()
                  .GetMemberIfExists("d")
                  .value()
                  .Serialize(),
              "[]");
  EXPECT_THAT(jsonb.GetMemberIfExists("e").value().Serialize(), "null");
  EXPECT_THAT(jsonb.GetMemberIfExists("f").value().Serialize(), "{}");
  EXPECT_THAT(jsonb.GetMemberIfExists("g").value().Serialize(), "[]");
  // Only removes the nulls from the top level
  jsonb.CleanUpJsonbObject();
  EXPECT_THAT(jsonb.GetMembers().size(), 3);
  EXPECT_THAT(jsonb.GetMemberIfExists("a").value().GetMembers().size(), 2);
  EXPECT_THAT(jsonb.GetMemberIfExists("a")
                  .value()
                  .GetMemberIfExists("c")
                  .value()
                  .Serialize(),
              "null");
  EXPECT_THAT(jsonb.GetMemberIfExists("a")
                  .value()
                  .GetMemberIfExists("d")
                  .value()
                  .Serialize(),
              "[]");
  EXPECT_THAT(jsonb.GetMemberIfExists("f").value().Serialize(), "{}");
  EXPECT_THAT(jsonb.GetMemberIfExists("g").value().Serialize(), "[]");
  // Clean up second level object.
  jsonb.GetMemberIfExists("a").value().CleanUpJsonbObject();
  EXPECT_THAT(jsonb.GetMemberIfExists("a").value().GetMembers().size(), 1);
  EXPECT_THAT(jsonb.GetMemberIfExists("a").value().Serialize(), R"({"d": []})");
}

TEST_F(JsonbValueTest, PathElementToIndex) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("null", &tree_nodes));
  EXPECT_THAT(jsonb.PathElementToIndex("a", 1),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  "path element at position 1 is not an integer: \"a\""));

  int64_t max = std::numeric_limits<int32_t>::max();
  ZETASQL_ASSERT_OK_AND_ASSIGN(int32_t index,
                       jsonb.PathElementToIndex(absl::StrCat(max), 1));
  EXPECT_EQ(index, max);
  // For any index that is greater than max int32_t return an error.
  EXPECT_THAT(
      jsonb.PathElementToIndex(absl::StrCat(max + 1), 1),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kInvalidArgument,
          absl::StrCat("path element at position 1 is not an integer: \"",
                       max + 1, "\"")));
}

// Helper function to call FindAtPath for successful cases.
std::optional<PgJsonbValue> FindAtPathHelper(
    PgJsonbValue jsonb, std::vector<std::string> path,
    PgJsonbValue::FindAtPathMode mode =
        PgJsonbValue::FindAtPathMode::kDefault) {
  auto result = jsonb.FindAtPath(path, mode);
  ABSL_CHECK_OK(result);
  return result.value();
}

TEST_F(JsonbValueTest, FindAtPath) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PgJsonbValue jsonb,
      PgJsonbValue::Parse(R"({"a": {"b": ["hello", [1,2,3]]}})", &tree_nodes));
  EXPECT_THAT(FindAtPathHelper(jsonb, {"a", "b"})->Serialize(),
              R"(["hello", [1, 2, 3]])");
  EXPECT_THAT(FindAtPathHelper(jsonb, {"a", "b", "1"})->Serialize(),
              "[1, 2, 3]");
  // Support negative indices
  EXPECT_THAT(FindAtPathHelper(jsonb, {"a", "b", "-1"})->Serialize(),
              "[1, 2, 3]");
  EXPECT_THAT(FindAtPathHelper(jsonb, {})->Serialize(),
              R"({"a": {"b": ["hello", [1, 2, 3]]}})");
  EXPECT_EQ(FindAtPathHelper(jsonb, {"a", "b", "5"}), std::nullopt);
  EXPECT_EQ(FindAtPathHelper(jsonb, {"a", "b", "-10"}), std::nullopt);
  // Fail if path index at array is not integer
  EXPECT_THAT(jsonb.FindAtPath({"a", "b", "32a"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  "path element at position 3 is not an integer: \"32a\""));
  EXPECT_EQ(FindAtPathHelper(
                jsonb, {"a", "b", "32a"},
                PgJsonbValue::FindAtPathMode::kIgnoreStringPathOnArrayError),
            std::nullopt);
  // Non-existent member.
  EXPECT_EQ(FindAtPathHelper(jsonb, {"d"}), std::nullopt);
  // String at array index.
  EXPECT_THAT(jsonb.FindAtPath({"a", "b", "hi"}),
              zetasql_base::testing::StatusIs(
                  absl::StatusCode::kInvalidArgument,
                  "path element at position 3 is not an integer: \"hi\""));
  EXPECT_EQ(FindAtPathHelper(
                jsonb, {"a", "b", "hi"},
                PgJsonbValue::FindAtPathMode::kIgnoreStringPathOnArrayError),
            std::nullopt);
  // Path element at scalar.
  EXPECT_EQ(FindAtPathHelper(jsonb, {"a", "b", "0", "a"}), std::nullopt);
}

TEST(DeathTest, JsonbString) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse(R"("foo")", &tree_nodes));
  EXPECT_DEATH(jsonb.RemoveMember("a"), "Expected object but got string");
}

TEST(DeathTest, JsonbBool) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("true", &tree_nodes));
  EXPECT_DEATH(jsonb.GetMembers(), "Expected object, null but got scalar");
}

TEST(DeathTest, SimpleJsonbArray) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("[1, 2, 3]", &tree_nodes));
  EXPECT_DEATH(absl::Status status = jsonb.CreateMemberIfNotExists("a"),
               "Expected object but got array");
}

TEST(DeathTest, JsonbGetArrayOnNull) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("null", &tree_nodes));
  EXPECT_DEATH(jsonb.GetArrayElements(), "Expected array but got null");
}

TEST_F(JsonbValueTest, JsonbObjectWithSpecialCharactersInKeys) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PgJsonbValue jsonb,
      PgJsonbValue::Parse(R"({ "\n_is_ascii":   10, "\t_is_ascii":  9.0})",
                          &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_THAT(jsonb.IsEmpty(), false);
  EXPECT_TRUE(jsonb.HasMember("\n_is_ascii"));
  EXPECT_TRUE(jsonb.HasMember("\t_is_ascii"));
  EXPECT_THAT(jsonb.GetMemberIfExists("\n_is_ascii").value().Serialize(), "10");
  EXPECT_THAT(jsonb.GetMemberIfExists("\t_is_ascii").value().Serialize(),
              "9.0");

  // Check the order by raw object keys.
  std::vector<std::pair<absl::string_view, PgJsonbValue>> members =
      jsonb.GetMembers();
  EXPECT_THAT(members.size(), 2);
  EXPECT_THAT(members.at(0).first, "\t_is_ascii");
  EXPECT_THAT(members.at(1).first, "\n_is_ascii");

  EXPECT_THAT(jsonb.Serialize(), R"({"\t_is_ascii": 9.0, "\n_is_ascii": 10})");

  EXPECT_TRUE(jsonb.RemoveMember("\n_is_ascii"));
  EXPECT_FALSE(jsonb.GetMemberIfExists("\n_is_ascii").has_value());
  ZETASQL_EXPECT_OK(jsonb.CreateMemberIfNotExists("\n_is_ascii"));
  EXPECT_THAT(jsonb.GetMemberIfExists("\n_is_ascii").value().Serialize(),
              "null");

  EXPECT_THAT(jsonb.Serialize(),
              R"({"\t_is_ascii": 9.0, "\n_is_ascii": null})");
}

TEST_F(JsonbValueTest, JsonbObjectWithSpecialCharactersInValues) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PgJsonbValue jsonb,
      PgJsonbValue::Parse(R"({ "\n_0": "\t_0" , "\t_1":  "\n_1" })",
                          &tree_nodes));
  EXPECT_THAT(jsonb.IsObject(), true);
  EXPECT_EQ(jsonb.GetMemberIfExists("\n_0").value().GetString(), "\t_0");
  EXPECT_EQ(jsonb.GetMemberIfExists("\n_0").value().Serialize(), R"("\t_0")");
  EXPECT_EQ(jsonb.GetMemberIfExists("\t_1").value().GetString(), "\n_1");
  EXPECT_EQ(jsonb.GetMemberIfExists("\t_1").value().Serialize(), R"("\n_1")");
}

TEST_F(JsonbValueTest, JsonbArrayWithSpecialCharacters) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      PgJsonbValue jsonb,
      PgJsonbValue::Parse(R"([ "\n_0", "\t_1" ])", &tree_nodes));
  EXPECT_THAT(jsonb.IsArray(), true);
  EXPECT_THAT(jsonb.GetArraySize(), 2);
  EXPECT_THAT(jsonb.GetArrayElementIfExists(0).value().GetString(), "\n_0");
  EXPECT_THAT(jsonb.GetArrayElementIfExists(0).value().Serialize(),
              R"("\n_0")");
  EXPECT_THAT(jsonb.Serialize(), R"(["\n_0", "\t_1"])");
}

TEST_F(JsonbValueTest, JsonbSetValue) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("{\"a\": 1}", &tree_nodes));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_two,
                       PgJsonbValue::Parse("{\"b\": 2}", &tree_nodes));
  jsonb.SetValue(jsonb_two);
  EXPECT_THAT(jsonb.Serialize(), "{\"b\": 2}");
  ZETASQL_EXPECT_OK(jsonb.CreateMemberIfNotExists("a"));
  EXPECT_THAT(jsonb.Serialize(), "{\"a\": null, \"b\": 2}");
  EXPECT_THAT(jsonb_two.Serialize(), "{\"b\": 2}");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_three,
                       PgJsonbValue::Parse("3", &tree_nodes));
  jsonb.GetMemberIfExists("a").value().SetValue(jsonb_three);
  EXPECT_THAT(jsonb.Serialize(), "{\"a\": 3, \"b\": 2}");
  jsonb.GetMemberIfExists("b").value().SetValue(jsonb_three);
  EXPECT_THAT(jsonb.Serialize(), "{\"a\": 3, \"b\": 3}");
  EXPECT_THAT(jsonb_two.Serialize(), "{\"b\": 3}");
}

TEST_F(JsonbValueTest, JsonbSetValueWithDifferentArena) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  std::vector<std::unique_ptr<TreeNode>> tree_nodes_two;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("{\"a\": 1}", &tree_nodes));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_two,
                       PgJsonbValue::Parse("{\"b\": 2}", &tree_nodes_two));
  jsonb.SetValue(jsonb_two);
}

TEST_F(JsonbValueTest, JsonbSetMemberValue) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  std::vector<std::unique_ptr<TreeNode>> tree_nodes_two;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("{\"a\": 1}", &tree_nodes));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_two,
                       PgJsonbValue::Parse("{\"b\": 2}", &tree_nodes_two));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_three,
                       PgJsonbValue::Parse("3", &tree_nodes_two));
  jsonb.GetMemberIfExists("a").value().SetValue(jsonb_three);
  EXPECT_THAT(jsonb.Serialize(), "{\"a\": 3}");
  jsonb.GetMemberIfExists("a").value().SetValue(jsonb_two);
  EXPECT_THAT(jsonb.Serialize(), "{\"a\": {\"b\": 2}}");
}

TEST_F(JsonbValueTest, JsonbSetArrayValue) {
  std::vector<std::unique_ptr<TreeNode>> tree_nodes;
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb,
                       PgJsonbValue::Parse("[1, 2, 3]", &tree_nodes));
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_two,
                       PgJsonbValue::Parse("[4, 5, 6]", &tree_nodes));
  jsonb.SetValue(jsonb_two);
  EXPECT_THAT(jsonb.Serialize(), "[4, 5, 6]");
  ZETASQL_ASSERT_OK_AND_ASSIGN(PgJsonbValue jsonb_three,
                       PgJsonbValue::Parse("3", &tree_nodes));
  jsonb.GetArrayElements()[0].SetValue(jsonb_three);
  EXPECT_THAT(jsonb.Serialize(), "[3, 5, 6]");
}

}  // namespace
}  // namespace postgres_translator::spangres::datatypes::common::jsonb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
