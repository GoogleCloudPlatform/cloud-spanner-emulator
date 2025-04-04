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

#include <memory>

#include "google/protobuf/struct.pb.h"
#include "google/protobuf/timestamp.pb.h"
#include "google/spanner/v1/query_plan.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/time/time.h"
#include "backend/access/write.h"
#include "backend/schema/catalog/schema.h"
#include "frontend/common/protos.h"
#include "frontend/converters/mutations.h"
#include "frontend/converters/time.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
#include "frontend/proto/partition_token.pb.h"
#include "frontend/server/handler.h"
#include "frontend/server/request_context.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

// Helper function to set the status in the response.
void SetResponseStatus(spanner_api::BatchWriteResponse* response,
                       const absl::Status& status) {
  *response->mutable_status() = StatusToProto(status);
}

// Helper function to process a single mutation group within a transaction.
absl::Status ProcessMutationGroup(
    const backend::Schema* schema,
    const spanner_api::BatchWriteRequest::MutationGroup& mutation_group,
    Transaction* txn, spanner_api::BatchWriteResponse* response) {
  backend::Mutation backend_mutation;
  absl::Status status =
      MutationFromProto(*schema, mutation_group.mutations(), &backend_mutation);
  if (!status.ok()) {
    SetResponseStatus(response, status);
    txn->MaybeInvalidate(status);
    return status;
  }

  status = txn->Write(backend_mutation);
  if (!status.ok()) {
    SetResponseStatus(response, status);
    txn->MaybeInvalidate(status);
    return status;
  }

  status = txn->Commit();
  if (!status.ok()) {
    SetResponseStatus(response, status);
    txn->MaybeInvalidate(status);
    return status;
  }

  absl::StatusOr<absl::Time> commit_time = txn->GetCommitTimestamp();
  if (!commit_time.ok()) {
    SetResponseStatus(response, commit_time.status());
    return commit_time.status();
  }

  absl::StatusOr<google::protobuf::Timestamp> commit_time_proto =
      TimestampToProto(*commit_time);
  if (!commit_time_proto.ok()) {
    SetResponseStatus(response, commit_time_proto.status());
    return commit_time_proto.status();
  }

  *response->mutable_commit_timestamp() = *commit_time_proto;
  return absl::OkStatus();
}
}  // namespace

absl::Status BatchWrite(RequestContext* ctx,
                        const spanner_api::BatchWriteRequest* request,
                        ServerStream<spanner_api::BatchWriteResponse>* stream) {
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  for (int i = 0; i < request->mutation_groups_size(); ++i) {
    const spanner_api::BatchWriteRequest::MutationGroup& mutation_group =
        request->mutation_groups(i);
    spanner_api::BatchWriteResponse response;
    response.add_indexes(i);

    spanner_api::TransactionOptions txn_options;
    txn_options.mutable_read_write();
    absl::StatusOr<std::shared_ptr<Transaction>> single_use_transaction =
        session->CreateSingleUseTransaction(txn_options);
    if (!single_use_transaction.ok()) {
      SetResponseStatus(&response, single_use_transaction.status());
      stream->Send(response);
      continue;
    }

    std::shared_ptr<Transaction> txn = *single_use_transaction;

    // Wrap all operations on this transaction so they are atomic.
    absl::Status txn_status =
        txn->GuardedCall(Transaction::OpType::kCommit, [&]() -> absl::Status {
          return ProcessMutationGroup(txn->schema(), mutation_group, txn.get(),
                                      &response);
        });

    stream->Send(response);
  }
  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, BatchWrite);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
