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

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/keys.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "google/spanner/v1/type.pb.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "backend/query/query_engine.h"
#include "common/config.h"
#include "common/errors.h"
#include "frontend/converters/partition.h"
#include "frontend/converters/query.h"
#include "frontend/converters/reads.h"
#include "frontend/entities/session.h"
#include "frontend/proto/partition_token.pb.h"
#include "frontend/server/handler.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace spanner_api = ::google::spanner::v1;

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace {

absl::Status ValidateTransactionSelectorForPartitionRead(
    const spanner_api::TransactionSelector& selector) {
  // PartitionRead and PartitionQuery only support read only snapshot
  // transactions. Read/write and single use transactions are not supported.
  switch (selector.selector_case()) {
    case spanner_api::TransactionSelector::SelectorCase::kBegin: {
      if (!selector.begin().has_read_only()) {
        return error::PartitionReadNeedsReadOnlyTxn();
      }
      return absl::OkStatus();
    }
    case spanner_api::TransactionSelector::SelectorCase::kId: {
      return absl::OkStatus();
    }
    case spanner_api::TransactionSelector::SelectorCase::kSingleUse:
      return error::PartitionReadDoesNotSupportSingleUseTransaction();
    case spanner_api::TransactionSelector::SELECTOR_NOT_SET:
      return error::MissingRequiredFieldError("TransactionSelector.selector");
  }
}

absl::Status ValidatePartitionOptions(
    const spanner_api::PartitionOptions& partition_options) {
  if (partition_options.partition_size_bytes() < 0) {
    return error::InvalidBytesPerBatch("partition_options");
  }
  if (partition_options.max_partitions() < 0) {
    return error::InvalidMaxPartitionCount("partition_options");
  }
  return absl::OkStatus();
}

// Create a partition token for the given partition read request and partitioned
// key set.
zetasql_base::StatusOr<PartitionToken> CreatePartitionTokenForRead(
    const google::spanner::v1::PartitionReadRequest& request,
    const backend::TransactionID& txn_id,
    const google::spanner::v1::KeySet& partitioned_key_set) {
  PartitionToken partition_token;
  *partition_token.mutable_session() = request.session();
  *partition_token.mutable_transaction_id() = std::to_string(txn_id);

  auto read_params = partition_token.mutable_read_params();
  *read_params->mutable_table() = request.table();
  *read_params->mutable_index() = request.index();
  *read_params->mutable_key_set() = request.key_set();
  *read_params->mutable_columns() = request.columns();

  *partition_token.mutable_partitioned_key_set() = partitioned_key_set;
  return partition_token;
}

// Create a partition token for the given partition query request.
zetasql_base::StatusOr<PartitionToken> CreatePartitionTokenForQuery(
    const google::spanner::v1::PartitionQueryRequest& request,
    const backend::TransactionID& txn_id, bool empty_partition) {
  if (request.sql().empty()) {
    return error::MissingRequiredFieldError("sql");
  }
  PartitionToken partition_token;
  *partition_token.mutable_session() = request.session();
  *partition_token.mutable_transaction_id() = std::to_string(txn_id);

  auto query_params = partition_token.mutable_query_params();
  *query_params->mutable_sql() = request.sql();
  *query_params->mutable_params() = request.params();
  *query_params->mutable_param_types() = request.param_types();

  partition_token.set_empty_query_partition(empty_partition);
  return partition_token;
}

}  //  namespace

// Creates a set of partition tokens for executing parallel read operations.
absl::Status PartitionRead(RequestContext* ctx,
                           const spanner_api::PartitionReadRequest* request,
                           spanner_api::PartitionResponse* response) {
  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(
      ValidateTransactionSelectorForPartitionRead(request->transaction()));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));
  if (!txn->IsReadOnly()) {
    return error::PartitionReadNeedsReadOnlyTxn();
  }

  if (request->has_partition_options()) {
    ZETASQL_RETURN_IF_ERROR(ValidatePartitionOptions(request->partition_options()));
  }

  if (ShouldReturnTransaction(request->transaction())) {
    ZETASQL_ASSIGN_OR_RETURN(*response->mutable_transaction(), txn->ToProto());
  }

  // Add two partitions to result set, with first partition being empty.
  ZETASQL_ASSIGN_OR_RETURN(
      auto empty_partition_token,
      CreatePartitionTokenForRead(*request, txn->id(), spanner_api::KeySet()));
  spanner_api::Partition empty_partition;
  ZETASQL_ASSIGN_OR_RETURN(*empty_partition.mutable_partition_token(),
                   PartitionTokenToString(empty_partition_token));

  // Second partition contains full result set for requested key_set.
  ZETASQL_ASSIGN_OR_RETURN(
      auto full_partition_token,
      CreatePartitionTokenForRead(*request, txn->id(), request->key_set()));
  spanner_api::Partition full_partition;
  ZETASQL_ASSIGN_OR_RETURN(*full_partition.mutable_partition_token(),
                   PartitionTokenToString(full_partition_token));

  *response->mutable_partitions()->Add() = empty_partition;
  *response->mutable_partitions()->Add() = full_partition;

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, PartitionRead);

// Creates a set of partition tokens for executing parallel query operations.
absl::Status PartitionQuery(RequestContext* ctx,
                            const spanner_api::PartitionQueryRequest* request,
                            spanner_api::PartitionResponse* response) {
  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(
      ValidateTransactionSelectorForPartitionRead(request->transaction()));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));
  if (!txn->IsReadOnly()) {
    return error::PartitionReadNeedsReadOnlyTxn();
  }

  if (request->has_partition_options()) {
    ZETASQL_RETURN_IF_ERROR(ValidatePartitionOptions(request->partition_options()));
  }

  if (ShouldReturnTransaction(request->transaction())) {
    ZETASQL_ASSIGN_OR_RETURN(*response->mutable_transaction(), txn->ToProto());
  }

  // check query is partitionable.
  ZETASQL_ASSIGN_OR_RETURN(
      backend::Query query,
      QueryFromProto(request->sql(), request->params(), request->param_types(),
                     txn->query_engine()->type_factory()));
  ZETASQL_RETURN_IF_ERROR(txn->query_engine()->IsPartitionable(
      query,
      backend::QueryContext{
          .schema = txn->schema(), .reader = nullptr, .writer = nullptr}));

  // Add two partitions to result set, with first partition being empty.
  ZETASQL_ASSIGN_OR_RETURN(auto empty_partition_token,
                   CreatePartitionTokenForQuery(*request, txn->id(),
                                                /*empty_partition =*/true));
  spanner_api::Partition empty_partition;
  ZETASQL_ASSIGN_OR_RETURN(*empty_partition.mutable_partition_token(),
                   PartitionTokenToString(empty_partition_token));

  // Second partition contains full result set for requested query.
  ZETASQL_ASSIGN_OR_RETURN(auto full_partition_token,
                   CreatePartitionTokenForQuery(*request, txn->id(),
                                                /*empty_partition =*/false));
  spanner_api::Partition full_partition;
  ZETASQL_ASSIGN_OR_RETURN(*full_partition.mutable_partition_token(),
                   PartitionTokenToString(full_partition_token));

  *response->mutable_partitions()->Add() = empty_partition;
  *response->mutable_partitions()->Add() = full_partition;

  return absl::OkStatus();
}
REGISTER_GRPC_HANDLER(Spanner, PartitionQuery);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
