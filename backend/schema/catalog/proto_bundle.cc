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
#include <utility>
#include <vector>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor_database.h"
#include "zetasql/public/types/proto_type.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "common/errors.h"
#include "google/protobuf/descriptor.h"
#include "zetasql/base/ret_check.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

absl::Status ProtoBundle::Builder::ParseProtoDescriptorBytes(
    absl::string_view proto_descriptor_bytes) {
  if (proto_descriptor_bytes.empty()) {
    return absl::InvalidArgumentError(
        "Contents of proto_descriptor_file cannot be empty");
  }

  // The FileDescriptorSet provided by the client may have unrelated
  // descriptors, but we want to store only the descriptors for the types that
  // are listed explicitly. So we parse and store the provided FileDescriptorSet
  // in a temporary protodb before adding the listed types to the correct
  // protodb.
  google::protobuf::FileDescriptorSet provided_descriptor_set;
  ZETASQL_RET_CHECK(provided_descriptor_set.ParseFromString(proto_descriptor_bytes))
          .SetErrorCode(absl::StatusCode::kInvalidArgument)
      << "Could not parse contents of proto_descriptor_file";

  for (const auto& file : provided_descriptor_set.file()) {
    ZETASQL_RET_CHECK(unfiltered_protodb_->Add(file)) << absl::Substitute(
        "Could not add FileDescriptor of `$0` into the protodb: the file "
        "descriptor already exists in the protodb.",
        file.name());
  }

  return absl::OkStatus();
}

absl::Status ProtoBundle::Builder::LoadTypesFromPreviousProtoBundle(
    const ProtoBundle* prev_proto_bundle) {
  ZETASQL_RET_CHECK(!prev_proto_bundle->empty())
          .SetErrorCode(absl::StatusCode::kInvalidArgument)
      << "Previous ProtoBundle is empty";

  ZETASQL_RETURN_IF_ERROR(InsertTypes(std::vector(prev_proto_bundle->types_.begin(),
                                          prev_proto_bundle->types_.end())))
          .SetPrepend()
      << "Failed to load type list from previous proto bundle into new proto "
         "bundle. Error: ";

  return absl::OkStatus();
}

absl::Status ProtoBundle::Builder::InsertTypes(
    absl::Span<const std::string> types) {
  for (const auto& t : types) {
    ZETASQL_RETURN_IF_ERROR(CheckIfTypeExistsInUnfilteredDescriptors(t));
    auto ret = instance_->types_.insert(t);
    ZETASQL_RET_CHECK(ret.second).SetErrorCode(absl::StatusCode::kAlreadyExists)
        << absl::Substitute("Type already exists: `$0` the proto bundle", t);
  }
  return absl::OkStatus();
}

absl::Status ProtoBundle::Builder::UpdateTypes(
    absl::Span<const std::string> types) {
  for (const auto& t : types) {
    if (!instance_->types_.contains(t)) {
      return absl::NotFoundError(absl::Substitute(
          "Tried to update `$0` which does not already exist.", t));
    }
    ZETASQL_RETURN_IF_ERROR(CheckIfTypeExistsInUnfilteredDescriptors(t));
  }
  return absl::OkStatus();
}

absl::Status ProtoBundle::Builder::DeleteTypes(
    absl::Span<const std::string> types) {
  for (const auto& t : types) {
    // TODO: Implement behavior to validate removals.
    ZETASQL_RET_CHECK_EQ(instance_->types_.erase(t), 1)
            .SetErrorCode(absl::StatusCode::kNotFound)
        << absl::Substitute(
               "Tried to remove `$0` which does not already exist in the proto "
               "bundle.",
               t);
  }
  return absl::OkStatus();
}

absl::StatusOr<std::unique_ptr<const ProtoBundle>>
ProtoBundle::Builder::Build() {
  absl::flat_hash_set<std::string> encountered_files;
  for (const std::string& proto_type : instance_->types_) {
    google::protobuf::FileDescriptorProto file_descriptor_proto;
    // This is a sanity check as the types were already verified to exist in
    // the proto bundle.
    ZETASQL_RET_CHECK(unfiltered_protodb_->FindFileContainingSymbol(
                  proto_type, &file_descriptor_proto))
            .SetErrorCode(absl::StatusCode::kNotFound)
        << absl::Substitute("Could not find FileDescriptor for `$0`",
                            proto_type);

    if (!encountered_files.contains(file_descriptor_proto.name())) {
      // Add() should only fail for double insertions which is already taken
      // care of. We return an error if it fails for anything else.
      ZETASQL_RET_CHECK(instance_->protodb_->Add(file_descriptor_proto))
          << absl::Substitute(
                 "Could not insert FileDescriptor (`$0`) of `$1` in the "
                 "database",
                 file_descriptor_proto.name(), proto_type);

      encountered_files.insert(file_descriptor_proto.name());
    }
  }

  instance_->descriptor_pool_ =
      std::make_unique<google::protobuf::DescriptorPool>(instance_->protodb_.get());
  instance_->descriptor_pool_->AllowUnknownDependencies();
  ZETASQL_RETURN_IF_ERROR(instance_->CheckUnsupportedFeatures());
  ZETASQL_RETURN_IF_ERROR(instance_->ValidateRestrictedPackages());

  return std::move(instance_);
}

absl::Status ProtoBundle::ValidateRestrictedPackages() {
  for (const auto& type : types_) {
    size_t spanner_pos = type.find("spanner.");
    size_t tech_spanner_pos = type.find("tech.spanner.");
    if (spanner_pos == 0 || tech_spanner_pos == 0) {
      std::string package_name =
          (spanner_pos == 0) ? "spanner" : "tech.spanner";
      return error::RestrictedPackagesCantBeUsed(type, package_name);
    }
  }
  return absl::OkStatus();
}

absl::Status ProtoBundle::ValidateMessage(
    const google::protobuf::Descriptor* descriptor,
    absl::flat_hash_set<const google::protobuf::Descriptor*>& visited_descriptors) const {
  if (!visited_descriptors.insert(descriptor).second) {
    // If a descriptor is already visited, we don't need to validate it again.
    // Cyclic dependency should be ok. E.g. google.protobuf.Value refers to
    // itself. Spanner supports it, and emulator should too.
    return absl::OkStatus();
  }
  if (descriptor->extension_count() > 0) {
    return error::MessageExtensionsNotSupported(descriptor->full_name());
  }
  if (descriptor->full_name() == "proto2.bridge.MessageSet") {
    return error::MessageTypeNotSupported(descriptor->full_name());
  }
  if (descriptor->options().HasExtension(zetasql::use_field_defaults)) {
    return error::ExtensionNotSupported(
        (zetasql::use_field_defaults).number(), descriptor->full_name());
  }
  for (int i = 0; i < descriptor->field_count(); i++) {
    const google::protobuf::FieldDescriptor* field = descriptor->field(i);
    if (zetasql::ProtoType::HasFormatAnnotation(field)) {
      int field_number = field->options().HasExtension(zetasql::format)
                             ? (zetasql::format.number())
                             : (zetasql::type.number());
      return error::ExtensionNotSupported(field_number, field->full_name());
    }
    if (field->options().HasExtension(zetasql::use_defaults)) {
      return error::ExtensionNotSupported((zetasql::use_defaults).number(),
                                          field->full_name());
    }
    if (field->type() == google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
      ZETASQL_RETURN_IF_ERROR(
          ValidateMessage(field->message_type(), visited_descriptors));
    }
  }
  visited_descriptors.erase(descriptor);
  return absl::OkStatus();
}

absl::Status ProtoBundle::CheckUnsupportedFeatures() const {
  for (const auto& type : types_) {
    const google::protobuf::Descriptor* descriptor =
        descriptor_pool_->FindMessageTypeByName(type);
    // This would happen if the descriptor is type is enum.
    if (!descriptor) continue;
    absl::flat_hash_set<const google::protobuf::Descriptor*> visited_descriptors;
    ZETASQL_RETURN_IF_ERROR(ValidateMessage(descriptor, visited_descriptors));
  }
  return absl::OkStatus();
}

absl::StatusOr<const google::protobuf::Descriptor*> ProtoBundle::GetTypeDescriptor(
    absl::string_view type) const {
  if (!types_.contains(type)) {
    return error::ProtoTypeNotFound(type);
  }

  const google::protobuf::Descriptor* descriptor =
      descriptor_pool_->FindMessageTypeByName(type);
  if (descriptor == nullptr) {
    return error::ProtoTypeNotFound(type);
  }
  return descriptor;
}

absl::StatusOr<const google::protobuf::EnumDescriptor*>
ProtoBundle::GetEnumTypeDescriptor(absl::string_view type) const {
  if (!types_.contains(type)) {
    return error::ProtoEnumTypeNotFound(type);
  }

  const google::protobuf::EnumDescriptor* descriptor =
      descriptor_pool_->FindEnumTypeByName(type);
  if (descriptor == nullptr) {
    return error::ProtoEnumTypeNotFound(type);
  }
  return descriptor;
}

absl::Status ProtoBundle::Builder::CheckIfTypeExistsInUnfilteredDescriptors(
    const std::string type) {
  google::protobuf::FileDescriptorProto unused_file_descriptor;
  if (!unfiltered_protodb_->FindFileContainingSymbol(type,
                                                     &unused_file_descriptor)) {
    return absl::NotFoundError(
        absl::Substitute("Missing descriptor for `$0`", type));
  }
  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
