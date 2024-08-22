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

#include "backend/schema/catalog/proto_bundle.h"

#include <memory>
#include <string>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/container/btree_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/substitute.h"
#include "tests/common/proto_matchers.h"
#include "tests/common/test.pb.h"
#include "tests/common/test_2.pb.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

using ::testing::HasSubstr;
using ::zetasql_base::testing::StatusIs;

MATCHER_P(EqualsMessageDescriptor, expected_descriptor,
          negation ? "does not equal message descriptor"
                   : "equals message descriptor") {
  google::protobuf::DescriptorProto result;
  arg->CopyTo(&result);

  google::protobuf::DescriptorProto expected;
  expected_descriptor->CopyTo(&expected);

  *result_listener << "\nExpected:\n" << expected.DebugString();
  *result_listener << "\nActual:\n" << result.DebugString();

  // Use partial matching as protoc generates field descriptors with json_name
  // added by default, while the actual field descriptors may not have it.
  return testing::ExplainMatchResult(
      test::proto::Partially(test::EqualsProto(expected)), result,
      result_listener);
}

MATCHER_P(EqualsEnumDescriptor, expected_descriptor,
          negation ? "does not equal enum descriptor"
                   : "equals enum descriptor") {
  google::protobuf::EnumDescriptorProto result;
  arg->CopyTo(&result);

  google::protobuf::EnumDescriptorProto expected;
  expected_descriptor->CopyTo(&expected);

  *result_listener << "\nExpected:\n" << expected.DebugString();
  *result_listener << "\nActual:\n" << result.DebugString();

  // Use partial matching as protoc generates field descriptors with json_name
  // added by default, while the actual field descriptors may not have it.
  return testing::ExplainMatchResult(
      test::proto::Partially(test::EqualsProto(expected)), result,
      result_listener);
}

namespace {
class ProtoBundleTest : public ::testing::Test {
 public:
  ProtoBundleTest() = default;
  std::string read_descriptors() {
    google::protobuf::FileDescriptorSet proto_files;
    ::emulator::tests::common::Simple::descriptor()->file()->CopyTo(
        proto_files.add_file());
    ::emulator::tests::common::ImportingParent::descriptor()->file()->CopyTo(
        proto_files.add_file());
    return proto_files.SerializeAsString();
  }

  std::string GenerateProtoDescriptorBytesAsString(
      std::string annotation_suffix, std::string annotation_value,
      std::string package) {
    std::string annotation_prefix =
    "zetasql";

    std::string annotation_name =
        "[" + annotation_prefix + "." + annotation_suffix + "]";

    const google::protobuf::FileDescriptorProto file_descriptor = PARSE_TEXT_PROTO(
        absl::Substitute(R"pb(
                           syntax: "proto2"
                           name: "0"
                           package: "$2"
                           message_type {
                             name: "User"
                             field {
                               name: "int_field"
                               type: TYPE_INT64
                               number: 2
                               label: LABEL_OPTIONAL
                               options { $0: $1 }
                             }
                           }
                           message_type {
                             name: "NestedUser"
                             field {
                               name: "nested_message"
                               type: TYPE_MESSAGE
                               label: LABEL_OPTIONAL
                               number: 3
                               type_name: "$2.User"
                             }
                           }
                           message_type {
                             name: "Point"
                             field {
                               name: "int_field"
                               type: TYPE_INT64
                               number: 2
                               label: LABEL_OPTIONAL
                             }
                           }
                           enum_type {
                             name: "State"
                             value { name: "UNSPECIFIED" number: 0 }
                           }
                         )pb",
                         annotation_name, annotation_value, package));
    google::protobuf::FileDescriptorSet file_descriptor_set;
    *file_descriptor_set.add_file() = file_descriptor;
    return file_descriptor_set.SerializeAsString();
  }

  const std::string kSimpleProtoName = "emulator.tests.common.Simple";
  const std::string kParentProtoName = "emulator.tests.common.Parent";
  const std::string kImportingParentProtoName =
      "emulator.tests.common.ImportingParent";
  const std::string kImportingParentingLevelTwoProtoName =
      "emulator.tests.common.ImportingAndParentingLevelTwo";
  const std::string kEnumContainerProtoName =
      "emulator.tests.common.EnumContainer";
  const std::string kImportingNestingEnumContainerProtoName =
      "emulator.tests.common.ImportingAndNestingEnumContainer";
  const std::string kGlobalEnumName = "emulator.tests.common.TestEnum";
  const std::string kNamespacedEnumName =
      "emulator.tests.common.EnumContainer.TestEnum";
  const std::string kImportingNestingEnumName =
      "emulator.tests.common.ImportingAndNestingEnumContainer.TestEnum";
};

using zetasql_base::testing::StatusIs;

TEST_F(ProtoBundleTest, CreateEmpty_CreatesAnEmptyBundle) {
  auto proto_bundle = ProtoBundle::CreateEmpty();
  EXPECT_TRUE(proto_bundle->empty());
}

TEST_F(ProtoBundleTest, UnsupportedFormatAnnotations) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v1,
      ProtoBundle::Builder::New(GenerateProtoDescriptorBytesAsString(
          "format", "NUMERIC", "customer.app")));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.User"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("`customer.app.User.int_field` options has "
                                 "reserved extension tag `68711883`.")));
}

TEST_F(ProtoBundleTest, UnsupportedNestedFormatAnnotations) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v1,
      ProtoBundle::Builder::New(GenerateProtoDescriptorBytesAsString(
          "format", "NUMERIC", "customer.app")));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.NestedUser"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("`customer.app.User.int_field` options has "
                                 "reserved extension tag `68711883`.")));
}

TEST_F(ProtoBundleTest, UnsupportedUseDefaultsAnnotations) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v1,
      ProtoBundle::Builder::New(GenerateProtoDescriptorBytesAsString(
          "use_defaults", "true", "customer.app")));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.User"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("`customer.app.User.int_field` options has "
                                 "reserved extension tag `49779519`.")));
}

TEST_F(ProtoBundleTest, UnsupportedTypeAnnotations) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v1,
      ProtoBundle::Builder::New(GenerateProtoDescriptorBytesAsString(
          "type", "DATE_DECIMAL", "customer.app")));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.User"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("`customer.app.User.int_field` options has "
                                 "reserved extension tag `49796453`.")));
}

TEST_F(ProtoBundleTest, UnsupportedUseFieldDefaultsAnnotations) {
  std::string prefix =
  "zetasql";
  const google::protobuf::FileDescriptorProto file_descriptor =
      PARSE_TEXT_PROTO(absl::Substitute(R"pb(
                                          syntax: "proto2"
                                          name: "0"
                                          package: "customer.app"
                                          message_type {
                                            name: "User"
                                            field {
                                              name: "int_field"
                                              type: TYPE_INT64
                                              number: 2
                                              label: LABEL_OPTIONAL
                                            }
                                            options {
                                              [$0.use_field_defaults]: false
                                            }
                                          }
                                        )pb",
                                        prefix));
  google::protobuf::FileDescriptorSet file_descriptor_set;
  *file_descriptor_set.add_file() = file_descriptor;
  std::string descriptor_bytes = file_descriptor_set.SerializeAsString();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_builder_v1,
                       ProtoBundle::Builder::New(descriptor_bytes));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.User"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("`customer.app.User` options has reserved "
                                 "extension tag `49659010`.")));
}

TEST_F(ProtoBundleTest, AllowsRecursiveProtos) {
  const google::protobuf::FileDescriptorProto file_descriptor = PARSE_TEXT_PROTO(R"pb(
    syntax: "proto2"
    name: "0"
    package: "customer.app"
    message_type {
      name: "Simple"
      field {
        name: "Recursive"
        type: TYPE_MESSAGE
        number: 2
        label: LABEL_OPTIONAL
        type_name: ".customer.app.Complex"
      }
    }
    message_type {
      name: "Complex"
      field {
        name: "Simple"
        type: TYPE_MESSAGE
        number: 2
        label: LABEL_OPTIONAL
        type_name: ".customer.app.Simple"
      }
    }
  )pb");
  google::protobuf::FileDescriptorSet file_descriptor_set;
  *file_descriptor_set.add_file() = file_descriptor;
  std::string descriptor_bytes = file_descriptor_set.SerializeAsString();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_builder_v1,
                       ProtoBundle::Builder::New(descriptor_bytes));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.Simple"}));
  ZETASQL_EXPECT_OK(proto_bundle_builder_v1->Build());
}

TEST_F(ProtoBundleTest, UnsupportedMessageSetExtensions) {
  const google::protobuf::FileDescriptorProto file_descriptor = PARSE_TEXT_PROTO(R"pb(
    syntax: "proto2"
    name: "0"
    package: "customer.app"
    message_type {
      name: "User"
      extension {
        name: "message_set_extension"
        extendee: ".proto2.bridge.MessageSet"
        number: 10071
        label: LABEL_OPTIONAL
        type: TYPE_MESSAGE
        type_name: ".util.StatusProto"
      }
    }
  )pb");
  google::protobuf::FileDescriptorSet file_descriptor_set;
  *file_descriptor_set.add_file() = file_descriptor;
  std::string descriptor_bytes = file_descriptor_set.SerializeAsString();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_builder_v1,
                       ProtoBundle::Builder::New(descriptor_bytes));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.User"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kUnimplemented,
                       HasSubstr("Message extensions are not supported for "
                                 "message `customer.app.User`.")));
}

TEST_F(ProtoBundleTest, UnsupportedMessageSetFields) {
  const google::protobuf::FileDescriptorProto file_descriptor = PARSE_TEXT_PROTO(R"pb(
    syntax: "proto2"
    name: "0"
    package: "customer.app"
    message_type {
      name: "User"
      field {
        name: "message_set"
        number: 5
        label: LABEL_OPTIONAL
        type: TYPE_MESSAGE
        type_name: ".proto2.bridge.MessageSet"
      }
    }
  )pb");
  google::protobuf::FileDescriptorSet file_descriptor_set;
  *file_descriptor_set.add_file() = file_descriptor;
  std::string descriptor_bytes = file_descriptor_set.SerializeAsString();
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_builder_v1,
                       ProtoBundle::Builder::New(descriptor_bytes));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"customer.app.User"}));
  EXPECT_THAT(
      proto_bundle_builder_v1->Build(),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kUnimplemented,
          testing::HasSubstr(
              "Message type `proto2.bridge.MessageSet` is not supported")));
}

TEST_F(ProtoBundleTest, CannotUseRestrictedSpannerPackages) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v1,
      ProtoBundle::Builder::New(GenerateProtoDescriptorBytesAsString(
          "format", "NUMERIC", "spanner.geometry")));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"spanner.geometry.Point"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Message `spanner.geometry.Point` has "
                                 "reserved package `spanner`.")));
}

TEST_F(ProtoBundleTest, CannotUseRestrictedTechSpannerPackages) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v1,
      ProtoBundle::Builder::New(GenerateProtoDescriptorBytesAsString(
          "format", "NUMERIC", "tech.spanner")));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{"tech.spanner.Point"}));
  EXPECT_THAT(proto_bundle_builder_v1->Build(),
              StatusIs(absl::StatusCode::kFailedPrecondition,
                       HasSubstr("Message `tech.spanner.Point` has "
                                 "reserved package `tech.spanner`.")));
}

TEST_F(ProtoBundleTest,
       ProtoBundleUsedAsPrevBundle_SubsequentBundleBuildersCanBeCreated) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_builder_v1,
                       ProtoBundle::Builder::New(read_descriptors()));
  ZETASQL_ASSERT_OK(proto_bundle_builder_v1->InsertTypes(
      std::vector<std::string>{kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v1, proto_bundle_builder_v1->Build());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto proto_bundle_builder_v2,
      ProtoBundle::Builder::New(read_descriptors(), proto_bundle_v1.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v2, proto_bundle_builder_v2->Build());

  EXPECT_THAT(proto_bundle_v2->types(), testing::ElementsAre(kSimpleProtoName));
  auto descriptor_result = proto_bundle_v1->GetTypeDescriptor(kSimpleProtoName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsMessageDescriptor(::emulator::tests::common::Simple::descriptor()));
}

TEST_F(ProtoBundleTest,
       EmptyBundleUsedAsPrevBundle_SubsequentBundleBuildersFail) {
  auto empty_proto_bundle = ProtoBundle::CreateEmpty();
  EXPECT_THAT(
      ProtoBundle::Builder::New(read_descriptors(), empty_proto_bundle.get()),
      StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(ProtoBundleTest, OnlyInsert) {
  auto insert_proto_types = std::vector<std::string>{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  EXPECT_THAT(proto_bundle->types(), testing::ElementsAre(kSimpleProtoName));
}

TEST_F(ProtoBundleTest, InsertFailsWithUnknownDescriptor) {
  auto insert_proto_types = std::vector<std::string>{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  EXPECT_THAT(
      builder->InsertTypes(std::vector<std::string>{"customer.app.User"}),
      zetasql_base::testing::StatusIs(
          absl::StatusCode::kNotFound,
          testing::HasSubstr("Missing descriptor for `customer.app.User`")));
}

TEST_F(ProtoBundleTest, InsertDuplicates_Fails) {
  auto insert_proto_types = std::vector<std::string>{};
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}));
  EXPECT_THAT(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}),
              StatusIs(absl::StatusCode::kAlreadyExists));
}

TEST_F(ProtoBundleTest, OnlyUpdateWithoutPreviousBundle_Fails) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ASSERT_THAT(builder->UpdateTypes(std::vector<std::string>{kSimpleProtoName}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ProtoBundleTest, OnlyUpdateWithPreviousBundle_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));
  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v1, builder->Build());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto builder_v2,
      ProtoBundle::Builder::New(read_descriptors(), proto_bundle_v1.get()));

  ZETASQL_EXPECT_OK(
      builder_v2->UpdateTypes(std::vector<std::string>{kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v2, builder_v2->Build());

  EXPECT_THAT(proto_bundle_v2->types(),
              testing::UnorderedElementsAre(kSimpleProtoName));
}

TEST_F(ProtoBundleTest, OnlyDeleteWithoutPreviousBundle_Fails) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  EXPECT_THAT(builder->DeleteTypes(std::vector<std::string>{kSimpleProtoName}),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ProtoBundleTest, OnlyDeleteWithPreviousLoad_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));
  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v1, builder->Build());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto builder_v2,
      ProtoBundle::Builder::New(read_descriptors(), proto_bundle_v1.get()));

  ZETASQL_ASSERT_OK(
      builder_v2->DeleteTypes(std::vector<std::string>{kSimpleProtoName}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v2, builder_v2->Build());

  EXPECT_TRUE(proto_bundle_v2->empty());
  EXPECT_THAT(proto_bundle_v2->GetTypeDescriptor(kSimpleProtoName),
              StatusIs(absl::StatusCode::kNotFound));
  // Sanity check v1 to verify that there were no updates there.
  EXPECT_THAT(proto_bundle_v1->types(), testing::ElementsAre(kSimpleProtoName));
}

TEST_F(ProtoBundleTest, InsertAndDelete_DoesNotLoadDescriptor) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}));
  ZETASQL_ASSERT_OK(builder->DeleteTypes(std::vector<std::string>{kSimpleProtoName}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  EXPECT_TRUE(proto_bundle->empty());
  EXPECT_THAT(proto_bundle->GetTypeDescriptor(kSimpleProtoName),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ProtoBundleTest, AllOperations) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(
      std::vector<std::string>{kSimpleProtoName, kParentProtoName,
                               kGlobalEnumName, kImportingNestingEnumName}));
  ZETASQL_ASSERT_OK(builder->UpdateTypes(
      std::vector<std::string>{kParentProtoName, kGlobalEnumName}));
  ZETASQL_ASSERT_OK(builder->DeleteTypes(
      std::vector<std::string>{kSimpleProtoName, kGlobalEnumName}));

  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  EXPECT_THAT(proto_bundle->types(),
              testing::UnorderedElementsAre(kParentProtoName,
                                            kImportingNestingEnumName));
}

TEST_F(ProtoBundleTest, GetTypeDescriptor) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result = proto_bundle->GetTypeDescriptor(kSimpleProtoName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsMessageDescriptor(::emulator::tests::common::Simple::descriptor()));

  EXPECT_THAT(proto_bundle->GetTypeDescriptor("does.not.exist"),
              StatusIs(absl::StatusCode::kNotFound));

  // Another proto descriptor from the same file descriptor, but not explicitly
  // inserted.
  EXPECT_THAT(proto_bundle->GetTypeDescriptor(kParentProtoName),
              StatusIs(absl::StatusCode::kNotFound));

  // kImporting* use message fields belonging to a different file descriptor.
  EXPECT_THAT(proto_bundle->GetTypeDescriptor(kImportingParentProtoName),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ProtoBundleTest, GetTypeDescriptor_SecondGenerationProtoBundle) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder_v1,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(
      builder_v1->InsertTypes(std::vector<std::string>{kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v1, builder_v1->Build());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto builder_v2,
      ProtoBundle::Builder::New(read_descriptors(), proto_bundle_v1.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v2, builder_v2->Build());

  auto descriptor_result_v2 =
      proto_bundle_v2->GetTypeDescriptor(kSimpleProtoName);
  ZETASQL_EXPECT_OK(descriptor_result_v2.status());
  EXPECT_THAT(
      descriptor_result_v2.value(),
      EqualsMessageDescriptor(::emulator::tests::common::Simple::descriptor()));

  // Sanity check v1 too.
  auto descriptor_result_v1 =
      proto_bundle_v1->GetTypeDescriptor(kSimpleProtoName);
  ZETASQL_EXPECT_OK(descriptor_result_v1.status());
  EXPECT_THAT(
      descriptor_result_v1.value(),
      EqualsMessageDescriptor(::emulator::tests::common::Simple::descriptor()));
}

TEST_F(ProtoBundleTest, GetEnumTypeDescriptor) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kGlobalEnumName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result = proto_bundle->GetEnumTypeDescriptor(kGlobalEnumName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsEnumDescriptor(::emulator::tests::common::TestEnum_descriptor()));

  EXPECT_THAT(proto_bundle->GetEnumTypeDescriptor("does.not.exist"),
              StatusIs(absl::StatusCode::kNotFound));

  // Another enum descriptor from the same file descriptor, but not explicitly
  // inserted.
  EXPECT_THAT(proto_bundle->GetEnumTypeDescriptor(kNamespacedEnumName),
              StatusIs(absl::StatusCode::kNotFound));

  // kImporting* belongs to a different file descriptor.
  EXPECT_THAT(proto_bundle->GetEnumTypeDescriptor(kImportingParentProtoName),
              StatusIs(absl::StatusCode::kNotFound));
}

TEST_F(ProtoBundleTest, GetEnumTypeDescriptor_SecondGenerationBundle) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder_v1,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder_v1->InsertTypes(std::vector<std::string>{kGlobalEnumName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v1, builder_v1->Build());

  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto builder_v2,
      ProtoBundle::Builder::New(read_descriptors(), proto_bundle_v1.get()));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle_v2, builder_v2->Build());

  auto descriptor_result_v2 =
      proto_bundle_v2->GetEnumTypeDescriptor(kGlobalEnumName);
  ZETASQL_EXPECT_OK(descriptor_result_v2.status());
  EXPECT_THAT(
      descriptor_result_v2.value(),
      EqualsEnumDescriptor(::emulator::tests::common::TestEnum_descriptor()));

  // Sanity check v1 too.
  auto descriptor_result_v1 =
      proto_bundle_v1->GetEnumTypeDescriptor(kGlobalEnumName);
  ZETASQL_EXPECT_OK(descriptor_result_v1.status());
  EXPECT_THAT(
      descriptor_result_v1.value(),
      EqualsEnumDescriptor(::emulator::tests::common::TestEnum_descriptor()));
}

// If the `Parent` proto type is added to the protodb before adding the
// child's proto type (`Simple`), there should not be an issue.
TEST_F(ProtoBundleTest, MessageTopologicalOrder_WithoutImports_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(
      std::vector<std::string>{kParentProtoName, kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result = proto_bundle->GetTypeDescriptor(kParentProtoName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsMessageDescriptor(::emulator::tests::common::Parent::descriptor()));
}

// If the `ImportingParent` proto type is added to the protodb before adding
// the child's proto type (`Simple`), there should not be an issue.
TEST_F(ProtoBundleTest, MessageTopologicalOrder_WithImports_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(
      std::vector<std::string>{kImportingParentingLevelTwoProtoName,
                               kImportingParentProtoName, kSimpleProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result =
      proto_bundle->GetTypeDescriptor(kImportingParentingLevelTwoProtoName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsMessageDescriptor(::emulator::tests::common::
                                  ImportingAndParentingLevelTwo::descriptor()));
}

// If only the `Parent` proto type are added to the protodb with no
// reference to the child type of `Simple` which exists in the same
// FileDescriptor. This should work as expected with the complete descriptor
// being returned.
TEST_F(ProtoBundleTest,
       MessageTopologicalOrder_MissingDependentMessageType_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{kParentProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result = proto_bundle->GetTypeDescriptor(kParentProtoName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsMessageDescriptor(::emulator::tests::common::Parent::descriptor()));
}

// If only the `Importing*` proto types are added to the protodb without
// adding the FileDescriptors containing the `Simple` and `Parent` types
// required by `Importing*` protos, we only get the partial descriptor
// with placeholders for missing FieldDescriptor's message types.
// `ImportingAndParentingLevelTwo` also has an enum field whose definition is in
// another file.
TEST_F(
    ProtoBundleTest,
    MessageTopologicalOrder_MissingImportedDependency_ReturnsPlaceholderDescriptor) {  // NOLINT
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{
      kImportingParentingLevelTwoProtoName, kImportingParentProtoName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result_level_two =
      proto_bundle->GetTypeDescriptor(kImportingParentingLevelTwoProtoName);
  ZETASQL_EXPECT_OK(descriptor_result_level_two.status());
  EXPECT_TRUE(descriptor_result_level_two.value()
                  ->FindFieldByName("from_another_file")
                  ->message_type()
                  ->is_placeholder());
  EXPECT_TRUE(descriptor_result_level_two.value()
                  ->FindFieldByName("imported_enum")
                  ->enum_type()
                  ->is_placeholder());
  EXPECT_FALSE(descriptor_result_level_two.value()
                   ->FindFieldByName("importing_parent_but_local_container")
                   ->message_type()
                   ->is_placeholder());
  // TODO: Find a better way to verify the returned descriptor.

  auto descriptor_result =
      proto_bundle->GetTypeDescriptor(kImportingParentProtoName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_TRUE(descriptor_result.value()
                  ->FindFieldByName("imported")
                  ->message_type()
                  ->is_placeholder());
  // TODO: Find a better way to verify the returned descriptor.
}

// If the `EnumContainer.TestEnum`'s container type is added to the protodb
// before adding the enum type and the global namespaced `TestEnum`, there
// should not be an issue.
TEST_F(ProtoBundleTest, EnumLocalTopologicalOrder_WithoutImports_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{
      kNamespacedEnumName, kEnumContainerProtoName, kGlobalEnumName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result =
      proto_bundle->GetEnumTypeDescriptor(kNamespacedEnumName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(
      descriptor_result.value(),
      EqualsEnumDescriptor(
          ::emulator::tests::common::EnumContainer::TestEnum_descriptor()));
}

// If the `ImportingNestingEnumContainer` type is added to the protodb
// before adding the inner enum types which need to be imported, there should
// not be an issue.
TEST_F(ProtoBundleTest, EnumTopologicalOrder_WithImports_Succeeds) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto builder,
                       ProtoBundle::Builder::New(read_descriptors()));

  ZETASQL_ASSERT_OK(builder->InsertTypes(std::vector<std::string>{
      kImportingNestingEnumName, kNamespacedEnumName, kGlobalEnumName}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto proto_bundle, builder->Build());

  auto descriptor_result =
      proto_bundle->GetEnumTypeDescriptor(kImportingNestingEnumName);
  ZETASQL_EXPECT_OK(descriptor_result.status());
  EXPECT_THAT(descriptor_result.value(),
              EqualsEnumDescriptor(
                  ::emulator::tests::common::ImportingAndNestingEnumContainer::
                      TestEnum_descriptor()));
}

}  // namespace
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
