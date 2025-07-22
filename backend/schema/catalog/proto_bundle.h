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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PROTO_BUNDLE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PROTO_BUNDLE_H_

#include <memory>
#include <string>

#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/descriptor.h"
#include "google/protobuf/descriptor_database.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// Holds the proto bundle used by a Schema.
//
// Notes:
// 1. Every instance of the ProtoBundle builds the descriptor database
// from scratch using the latest version of the file descriptors available in
// the `proto_descriptor_bytes`. A consequence of this is that when an ALTER
// PROTO BUNDLE statement is encountered, all the proto descriptors are updated
// regardless of whether the types were included in the ALTER statement. This
// should be ok in the ideal emulator usage, as an emulator instance is expected
// to be short lived and may not see multiple versions of the proto descriptors
// in its lifetime.
// 2. To simplify the implementation, we're using the default
// SimpleDescriptorDatabase which stores the complete FileDescriptors of all the
// messages that were requested as part of CREATE / ALTER PROTO BUNDLE. This
// means we're holding the enum and message descriptors for all the other
// messages which co-exist along with the enum / message types of our interest.
// To emulate the production behavior, we artificially enforce the type checks
// by storing the list of inserted types separately and verifying that a type
// exists in this list before returning the descriptors.
class ProtoBundle {
 public:
  // Returns the list of types.
  const absl::btree_set<std::string>& types() const { return types_; }

  // Returns true if the proto bundle does not have any types initialized.
  bool empty() const { return types_.empty(); }

  // Returns the descriptor for messages.
  absl::StatusOr<const google::protobuf::Descriptor*> GetTypeDescriptor(
      absl::string_view type) const;

  // Returns the descriptor for enums.
  absl::StatusOr<const google::protobuf::EnumDescriptor*> GetEnumTypeDescriptor(
      absl::string_view type) const;

  // Creates an empty ProtoBundle to be used in Schemas which do not use
  // proto types.
  // This empty instance will be used to return "Type not found" errors whenever
  // someone tries to use a proto type without creating the proto bundle.
  static std::unique_ptr<const ProtoBundle> CreateEmpty() {
    auto proto_bundle = absl::WrapUnique(new ProtoBundle());
    proto_bundle->descriptor_pool_ =
        std::make_unique<google::protobuf::DescriptorPool>(proto_bundle->protodb_.get());
    return proto_bundle;
  }

  // Used to build a new ProtoBundle, either from scratch or from the types
  // defined in a previous proto bundle.
  // Example usage:
  //
  // ZETASQL_ASSIGN_OR_RETURN(auto builder, ProtoBundle::Builder::New(descriptors));
  // ZETASQL_RETURN_IF_ERROR(builder->InsertTypes("package.name.Type"));
  // ZETASQL_ASSIGN_OR_RETURN(auto proto_bundle, builder->Build());
  //
  // When there's a previous ProtoBundle, use that as a base to load the
  // types into the new ProtoBundle. Note that this only loads the type list
  // from the previous proto bundle and expects that the older type descriptors
  // still exist in the FileDescriptorSet passed via `proto_descriptor_bytes`.
  class Builder {
   public:
    // Creates a new ProtoBundle Builder.
    // `proto_descriptor_bytes` is the protoc generated, bytes representation of
    // the FileDescriptorSet containing all the proto files required to build
    // the proto types that will be used by a Schema. All the types that are
    // being inserted / updated via this Builder should have their corresponding
    // FileDescriptor within this set.
    // `prev_proto_bundle` is an optional argument to which an earlier
    // ProtoBundle should be passed. When this argument is not passed, it is
    // equivalent to calling "CREATE PROTO BUNDLE" (all older types are purged),
    // and when it is passed it is equivalent to calling "ALTER PROTO BUNDLE".
    static absl::StatusOr<std::unique_ptr<Builder>> New(
        absl::string_view proto_descriptor_bytes,
        const ProtoBundle* prev_proto_bundle = nullptr) {
      auto builder = absl::WrapUnique(new Builder());

      ZETASQL_RETURN_IF_ERROR(
          builder->ParseProtoDescriptorBytes(proto_descriptor_bytes));

      if (prev_proto_bundle != nullptr) {
        // Manually merge file descriptors from the previous bundle.
        absl::flat_hash_set<std::string> seen_files;
        for (const std::string& type_name : prev_proto_bundle->types()) {
          const google::protobuf::FileDescriptor* file_desc =
              prev_proto_bundle->descriptor_pool_->FindFileContainingSymbol(
                  type_name);

          if (file_desc && !seen_files.contains(file_desc->name())) {
            google::protobuf::FileDescriptorProto file_proto;
            file_desc->CopyTo(&file_proto);

            // Add the reconstructed FileDescriptorProto to existing
            // unfiltered_protodb_
            builder->unfiltered_protodb_->Add(file_proto);
            seen_files.insert(std::string(file_desc->name()));
          }
        }
        ZETASQL_RETURN_IF_ERROR(
            builder->LoadTypesFromPreviousProtoBundle(prev_proto_bundle));
      }
      return builder;
    }

    // Creates the ProtoBundle by loading only the relevant FileDescriptors
    // and initializing the DescriptorPool.
    absl::StatusOr<std::unique_ptr<const ProtoBundle>> Build();

    absl::Status InsertTypes(absl::Span<const std::string> types);

    absl::Status UpdateTypes(absl::Span<const std::string> types);

    absl::Status DeleteTypes(absl::Span<const std::string> types);

   private:
    Builder() : instance_(absl::WrapUnique(new ProtoBundle())) {}

    std::unique_ptr<ProtoBundle> instance_;

    // Stores all the FileDescriptors provided in the `proto_descriptor_bytes`.
    const std::unique_ptr<google::protobuf::SimpleDescriptorDatabase>
        unfiltered_protodb_ =
            std::make_unique<google::protobuf::SimpleDescriptorDatabase>();

    // Checks if the type exists in the bundle provided by the customer.
    absl::Status CheckIfTypeExistsInUnfilteredDescriptors(
        const std::string type);

    // Parses and loads the `unfiltered_proto_db_` with the FileDescriptors
    // provided in the `proto_descriptor_bytes`.
    absl::Status ParseProtoDescriptorBytes(
        absl::string_view proto_descriptor_bytes);

    // Loads the type list from a previous proto bundle into the new
    // instance's type list. It expects that all the previous types exist in the
    // new bundle and will throw an error if they're missing (refer to note#1 on
    // the ProtoBundle class definition for additional details).
    absl::Status LoadTypesFromPreviousProtoBundle(
        const ProtoBundle* proto_bundle);
  };

 private:
  ProtoBundle() = default;
  ProtoBundle& operator=(const ProtoBundle&) = delete;
  // Validates if there are any types that are using unsupported type
  // annotation (googlesql.format, googlesql.use_defaults).
  absl::Status CheckUnsupportedFeatures() const;
  absl::Status ValidateMessage(const google::protobuf::Descriptor* descriptor,
                               absl::flat_hash_set<const google::protobuf::Descriptor*>&
                                   visited_descriptors) const;
  // Validates if there are any types that are using restricted spanner packages
  // spanner or tech.spanner.
  absl::Status ValidateRestrictedPackages();

  // Set of types as provided in the CREATE / ALTER PROTO BUNDLE DDL
  // statements. This is being maintained separately to return only the
  // descriptors for the types listed in the DDL statements. The descriptor
  // database used in the emulator holds the entire FileDescriptors which
  // include all the types defined in the proto files, but production spanner
  // only holds the descriptors that are included in the DDL.
  // We're using btree_set instead of flat_hash_set to maintain the order of
  // insertion (which shouldn't matter, but it is being used to test).
  absl::btree_set<std::string> types_;

  // Descriptor database constructed from the FileDescriptorSet provided along
  // with the DDL. Since this is built from the FileDescriptor for the
  // explicitly listed messages, it may hold other unrelated messages that are
  // co-located in the same proto file where the listed messages are defined.
  // Hence this should not be used directly for querying descriptors, please
  // use GetTypeDescriptor and GetEnumDescriptor defined above.
  const std::unique_ptr<google::protobuf::SimpleDescriptorDatabase> protodb_ =
      std::make_unique<google::protobuf::SimpleDescriptorDatabase>();

  // An empty descriptor pool which uses the above descriptor database to
  // query and construct the descriptors from the FileDescriptors. Do not use
  // this directly for querying proto descriptors for the same reasons listed
  // above for `protodb_`.
  std::unique_ptr<google::protobuf::DescriptorPool> descriptor_pool_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_SCHEMA_CATALOG_PROTO_BUNDLE_H_
