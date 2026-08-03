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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "google/longrunning/operations.pb.h"
#include "google/protobuf/empty.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "backend/database/database.h"
#include "common/errors.h"
#include "frontend/collections/operation_manager.h"
#include "frontend/common/uris.h"
#include "frontend/converters/time.h"
#include "frontend/entities/instance_partition.h"
#include "frontend/server/environment.h"
#include "frontend/server/handler.h"
#include "frontend/server/request_context.h"
#include "googlesql/base/status_macros.h"

namespace instance_api = ::google::spanner::admin::instance::v1;
namespace operations_api = ::google::longrunning;
namespace protobuf_api = ::google::protobuf;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

absl::flat_hash_map<std::string, std::vector<std::string>>
GetReferencingDatabases(ServerEnv* env, const std::string& instance_uri) {
  absl::flat_hash_map<std::string, std::vector<std::string>> partition_to_dbs;
  auto databases_or = env->database_manager()->ListDatabases(instance_uri);
  if (!databases_or.ok()) {
    return partition_to_dbs;
  }
  for (const auto& db : *databases_or) {
    if (db->backend() == nullptr) {
      continue;
    }
    const auto* schema = db->backend()->GetLatestSchema();
    if (schema == nullptr) {
      continue;
    }
    for (const auto* placement : schema->placements()) {
      if (placement->InstancePartition().has_value()) {
        std::string partition_val = placement->InstancePartition().value();
        std::string full_partition_uri;
        if (absl::StartsWith(partition_val, "projects/")) {
          full_partition_uri = partition_val;
        } else {
          full_partition_uri =
              MakeInstancePartitionUri(instance_uri, partition_val);
        }
        auto& dbs = partition_to_dbs[full_partition_uri];
        if (absl::c_find(dbs, db->database_uri()) == dbs.end()) {
          dbs.push_back(db->database_uri());
        }
      }
    }
  }
  return partition_to_dbs;
}

void PopulateReferencingDatabases(
    const absl::flat_hash_map<std::string, std::vector<std::string>>& dbs_map,
    instance_api::InstancePartition* proto) {
  auto it = dbs_map.find(proto->name());
  if (it != dbs_map.end()) {
    for (const auto& db_uri : it->second) {
      proto->add_referencing_databases(db_uri);
    }
  }
}

}  // namespace

// Lists all instance partitions in an instance.
absl::Status ListInstancePartitions(
    RequestContext* ctx,
    const instance_api::ListInstancePartitionsRequest* request,
    instance_api::ListInstancePartitionsResponse* response) {
  absl::string_view project_id, instance_id;
  GOOGLESQL_RETURN_IF_ERROR(
      ParseInstanceUri(request->parent(), &project_id, &instance_id));
  GOOGLESQL_RETURN_IF_ERROR(
      ctx->env()->instance_manager()->GetInstance(request->parent()).status());

  if (!request->page_token().empty()) {
    absl::string_view p_id, i_id, part_id;
    GOOGLESQL_RETURN_IF_ERROR(ParseInstancePartitionUri(request->page_token(), &p_id,
                                              &i_id, &part_id));
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::vector<std::shared_ptr<InstancePartition>> partitions,
      ctx->env()->instance_partition_manager()->ListInstancePartitions(
          request->parent()));

  int32_t page_size = request->page_size();
  static const int32_t kMaxPageSize = 1000;
  if (page_size <= 0 || page_size > kMaxPageSize) {
    page_size = kMaxPageSize;
  }

  auto dbs_map = GetReferencingDatabases(ctx->env(), request->parent());

  for (const auto& partition : partitions) {
    if (response->instance_partitions_size() >= page_size) {
      response->set_next_page_token(partition->partition_uri());
      break;
    }
    if (partition->partition_uri() >= request->page_token()) {
      auto* proto = response->add_instance_partitions();
      partition->ToProto(proto);
      PopulateReferencingDatabases(dbs_map, proto);
    }
  }
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, ListInstancePartitions);

// Gets information about an instance partition.
absl::Status GetInstancePartition(
    RequestContext* ctx,
    const instance_api::GetInstancePartitionRequest* request,
    instance_api::InstancePartition* response) {
  absl::string_view project_id, instance_id, partition_id;
  GOOGLESQL_RETURN_IF_ERROR(ParseInstancePartitionUri(request->name(), &project_id,
                                            &instance_id, &partition_id));
  std::string instance_uri = MakeInstanceUri(project_id, instance_id);

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::shared_ptr<InstancePartition> partition,
      ctx->env()->instance_partition_manager()->GetInstancePartition(
          request->name()));
  partition->ToProto(response);
  auto dbs_map = GetReferencingDatabases(ctx->env(), instance_uri);
  PopulateReferencingDatabases(dbs_map, response);
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, GetInstancePartition);

// Creates an instance partition.
absl::Status CreateInstancePartition(
    RequestContext* ctx,
    const instance_api::CreateInstancePartitionRequest* request,
    operations_api::Operation* response) {
  absl::string_view project_id, instance_id;
  GOOGLESQL_RETURN_IF_ERROR(
      ParseInstanceUri(request->parent(), &project_id, &instance_id));
  GOOGLESQL_RETURN_IF_ERROR(
      ctx->env()->instance_manager()->GetInstance(request->parent()).status());
  std::string partition_uri = MakeInstancePartitionUri(
      request->parent(), request->instance_partition_id());
  if (!request->instance_partition().name().empty() &&
      request->instance_partition().name() != partition_uri) {
    return error::InstancePartitionNameMismatch(
        request->instance_partition().name());
  }

  GOOGLESQL_RETURN_IF_ERROR(
      ValidateInstancePartitionId(request->instance_partition_id()));

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::shared_ptr<InstancePartition> partition,
      ctx->env()->instance_partition_manager()->CreateInstancePartition(
          partition_uri, request->instance_partition()));

  GOOGLESQL_ASSIGN_OR_RETURN(std::shared_ptr<Operation> operation,
                   ctx->env()->operation_manager()->CreateOperation(
                       partition_uri, OperationManager::kAutoGeneratedId));

  instance_api::InstancePartition partition_pb;
  partition->ToProto(&partition_pb);
  instance_api::CreateInstancePartitionMetadata metadata_pb;
  *metadata_pb.mutable_instance_partition() = partition_pb;

  GOOGLESQL_ASSIGN_OR_RETURN(*metadata_pb.mutable_start_time(),
                   TimestampToProto(ctx->env()->clock()->Now()));
  GOOGLESQL_ASSIGN_OR_RETURN(*metadata_pb.mutable_end_time(),
                   TimestampToProto(ctx->env()->clock()->Now()));
  operation->SetMetadata(metadata_pb);
  operation->SetResponse(partition_pb);
  operation->ToProto(response);

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, CreateInstancePartition);

// Updates an instance partition.
absl::Status UpdateInstancePartition(
    RequestContext* ctx,
    const instance_api::UpdateInstancePartitionRequest* request,
    operations_api::Operation* response) {
  return error::InstancePartitionUpdatesNotSupported();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, UpdateInstancePartition);

// Deletes an instance partition.
absl::Status DeleteInstancePartition(
    RequestContext* ctx,
    const instance_api::DeleteInstancePartitionRequest* request,
    protobuf_api::Empty* response) {
  absl::string_view project_id, instance_id, partition_id;
  GOOGLESQL_RETURN_IF_ERROR(ParseInstancePartitionUri(request->name(), &project_id,
                                            &instance_id, &partition_id));
  std::string instance_uri = MakeInstanceUri(project_id, instance_id);

  GOOGLESQL_RETURN_IF_ERROR(ctx->env()
                      ->instance_partition_manager()
                      ->GetInstancePartition(request->name())
                      .status());

  auto dbs_map = GetReferencingDatabases(ctx->env(), instance_uri);
  auto it = dbs_map.find(request->name());
  if (it != dbs_map.end() && !it->second.empty()) {
    return error::InstancePartitionReferencedByDatabase(request->name());
  }

  ctx->env()->instance_partition_manager()->DeleteInstancePartition(
      request->name());
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, DeleteInstancePartition);

// Lists operations on instance partitions.
absl::Status ListInstancePartitionOperations(
    RequestContext* ctx,
    const instance_api::ListInstancePartitionOperationsRequest* request,
    instance_api::ListInstancePartitionOperationsResponse* response) {
  absl::string_view project_id, instance_id;
  GOOGLESQL_RETURN_IF_ERROR(
      ParseInstanceUri(request->parent(), &project_id, &instance_id));
  GOOGLESQL_RETURN_IF_ERROR(
      ctx->env()->instance_manager()->GetInstance(request->parent()).status());

  std::string prefix = absl::StrCat(request->parent(), "/instancePartitions/");
  GOOGLESQL_ASSIGN_OR_RETURN(std::vector<std::shared_ptr<Operation>> operations,
                   ctx->env()->operation_manager()->ListOperations(prefix));
  for (const auto& op : operations) {
    op->ToProto(response->add_operations());
  }
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, ListInstancePartitionOperations);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
