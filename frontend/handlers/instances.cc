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

#include "google/longrunning/operations.pb.h"
#include "google/protobuf/empty.pb.h"
#include "google/spanner/admin/instance/v1/spanner_instance_admin.pb.h"
#include "common/errors.h"
#include "common/limits.h"
#include "frontend/collections/operation_manager.h"
#include "frontend/common/labels.h"
#include "frontend/common/uris.h"
#include "frontend/converters/time.h"
#include "frontend/entities/instance.h"
#include "frontend/entities/operation.h"
#include "frontend/server/handler.h"
#include "re2/re2.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace instance_api = ::google::spanner::admin::instance::v1;
namespace operations_api = ::google::longrunning;
namespace protobuf_api = ::google::protobuf;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

const absl::string_view kEmulatorInstanceConfig = "emulator-config";

// Hard-coded instance config for emulator.
instance_api::InstanceConfig GetEmulatorInstanceConfig(
    const absl::string_view project_id) {
  instance_api::InstanceConfig config;
  config.set_name(absl::StrCat("projects/", project_id, "/instanceConfigs/",
                               kEmulatorInstanceConfig));
  config.set_display_name("Emulator Instance Config");
  return config;
}

}  // namespace

// Lists the supported instance configurations for a given project.
absl::Status ListInstanceConfigs(
    RequestContext* ctx,
    const instance_api::ListInstanceConfigsRequest* request,
    instance_api::ListInstanceConfigsResponse* response) {
  absl::string_view project_id;
  ZETASQL_RETURN_IF_ERROR(ParseProjectUri(request->parent(), &project_id));
  response->add_instance_configs();
  *response->mutable_instance_configs(0) =
      GetEmulatorInstanceConfig(project_id);
  // Pagination is not supported.
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, ListInstanceConfigs);

// Gets information about a particular instance configuration.
absl::Status GetInstanceConfig(
    RequestContext* ctx, const instance_api::GetInstanceConfigRequest* request,
    instance_api::InstanceConfig* response) {
  absl::string_view project_id, instance_config_id;
  ZETASQL_RETURN_IF_ERROR(ParseInstanceConfigUri(request->name(), &project_id,
                                         &instance_config_id));
  if (instance_config_id != kEmulatorInstanceConfig) {
    return error::InstanceConfigNotFound(instance_config_id);
  }
  *response = GetEmulatorInstanceConfig(project_id);
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, GetInstanceConfig);

// Lists all instances in a project.
absl::Status ListInstances(RequestContext* ctx,
                           const instance_api::ListInstancesRequest* request,
                           instance_api::ListInstancesResponse* response) {
  // Validate that the ListInstances request is for a valid project.
  absl::string_view project_id;
  ZETASQL_RETURN_IF_ERROR(ParseProjectUri(request->parent(), &project_id));

  // Validate that the page_token provided is a valid instance_uri.
  if (!request->page_token().empty()) {
    absl::string_view project_id, instance_id;
    ZETASQL_RETURN_IF_ERROR(
        ParseInstanceUri(request->page_token(), &project_id, &instance_id));
  }

  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::shared_ptr<Instance>> instances,
      ctx->env()->instance_manager()->ListInstances(request->parent()));

  int32_t page_size = request->page_size();
  static const int32_t kMaxPageSize = 1000;
  if (page_size <= 0 || page_size > kMaxPageSize) {
    page_size = kMaxPageSize;
  }

  // Instances returned from instance manager are sorted by instance_uri and
  // thus we use instance uri of first instance in next page as next_page_token.
  for (const auto& instance : instances) {
    if (response->instances_size() >= page_size) {
      response->set_next_page_token(instance->instance_uri());
      break;
    }
    if (instance->instance_uri() >= request->page_token()) {
      instance->ToProto(response->add_instances());
    }
  }
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, ListInstances);

// Gets information about a particular instance.
absl::Status GetInstance(RequestContext* ctx,
                         const instance_api::GetInstanceRequest* request,
                         instance_api::Instance* response) {
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Instance> instance,
                   GetInstance(ctx, request->name()));
  instance->ToProto(response);
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, GetInstance);

// Creates an instance.
absl::Status CreateInstance(RequestContext* ctx,
                            const instance_api::CreateInstanceRequest* request,
                            operations_api::Operation* response) {
  // Verify that the instance creation request is valid.
  absl::string_view project_id;
  ZETASQL_RETURN_IF_ERROR(ParseProjectUri(request->parent(), &project_id));
  std::string instance_uri =
      MakeInstanceUri(project_id, request->instance_id());
  if (!request->instance().name().empty() &&
      request->instance().name() != instance_uri) {
    return error::InstanceNameMismatch(request->instance().name());
  }

  // Validate instance name.
  ZETASQL_RETURN_IF_ERROR(ValidateInstanceId(request->instance_id()));

  // Validate labels.
  ZETASQL_RETURN_IF_ERROR(ValidateLabels(request->instance().labels()));

  // Create the instance.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Instance> instance,
                   ctx->env()->instance_manager()->CreateInstance(
                       instance_uri, request->instance()));

  // Create an operation tracking this instance creation.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Operation> operation,
                   ctx->env()->operation_manager()->CreateOperation(
                       instance_uri, OperationManager::kAutoGeneratedId));

  // Fill in the metadata for the longrunning operation.
  instance_api::Instance instance_pb;
  instance->ToProto(&instance_pb);
  instance_api::CreateInstanceMetadata metadata_pb;
  *metadata_pb.mutable_instance() = instance_pb;

  // Update the start time of the LRO.
  ZETASQL_ASSIGN_OR_RETURN(*metadata_pb.mutable_start_time(),
                   TimestampToProto(ctx->env()->clock()->Now()));
  operation->SetMetadata(metadata_pb);

  // Convert to proto before setting the response so that the returned
  // longrunning operation is in !done state. Caller needs to poll the
  // longrunning operation at least once to make sure the instance is created.
  // This behavior follows the prod behavior more closely.
  // TODO: In integration tests, the client library currently
  // has a random delay with a long range before it polls the operations api.
  // This makes testing unnecessarily slow. Remove this when the issue is fixed.
  //  operation->ToProto(response);

  // Update the endtime after the LRO is conceptually completed.
  ZETASQL_ASSIGN_OR_RETURN(*metadata_pb.mutable_end_time(),
                   TimestampToProto(ctx->env()->clock()->Now()));
  operation->SetMetadata(metadata_pb);
  operation->SetResponse(instance_pb);

  // TODO: See discussion above, remove when the issue is fixed.
  operation->ToProto(response);

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, CreateInstance);

// Updates an instance.
absl::Status UpdateInstance(RequestContext* ctx,
                            const instance_api::UpdateInstanceRequest* request,
                            operations_api::Operation* response) {
  return error::InstanceUpdatesNotSupported();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, UpdateInstance);

// Deletes an instance. Returns OK even if the instance is not found.
absl::Status DeleteInstance(RequestContext* ctx,
                            const instance_api::DeleteInstanceRequest* request,
                            protobuf_api::Empty* response) {
  absl::string_view project_id, instance_id;
  ZETASQL_RETURN_IF_ERROR(ParseInstanceUri(request->name(), &project_id, &instance_id));

  // Clean up resources associated with the instance.
  ZETASQL_ASSIGN_OR_RETURN(
      std::vector<std::shared_ptr<Database>> databases,
      ctx->env()->database_manager()->ListDatabases(request->name()));
  for (const auto& database : databases) {
    ZETASQL_ASSIGN_OR_RETURN(
        std::vector<std::shared_ptr<Session>> sessions,
        ctx->env()->session_manager()->ListSessions(database->database_uri()));
    for (const auto& session : sessions) {
      ZETASQL_RETURN_IF_ERROR(
          ctx->env()->session_manager()->DeleteSession(session->session_uri()));
    }
    ZETASQL_RETURN_IF_ERROR(ctx->env()->database_manager()->DeleteDatabase(
        database->database_uri()));
  }

  // Clean up the instance.
  ctx->env()->instance_manager()->DeleteInstance(request->name());
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(InstanceAdmin, DeleteInstance);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
