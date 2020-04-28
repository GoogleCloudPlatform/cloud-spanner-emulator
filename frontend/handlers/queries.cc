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
#include <utility>

#include "google/protobuf/struct.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "backend/access/read.h"
#include "backend/query/query_engine.h"
#include "common/errors.h"
#include "frontend/common/protos.h"
#include "frontend/converters/partition.h"
#include "frontend/converters/query.h"
#include "frontend/converters/reads.h"
#include "frontend/converters/types.h"
#include "frontend/converters/values.h"
#include "frontend/entities/session.h"
#include "frontend/entities/transaction.h"
#include "frontend/proto/partition_token.pb.h"
#include "frontend/server/handler.h"
#include "frontend/server/request_context.h"
#include "zetasql/base/status.h"
#include "zetasql/base/status_macros.h"
#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

zetasql_base::Status ValidateTransactionSelectorForQuery(
    const spanner_api::TransactionSelector& selector, bool is_dml) {
  if (selector.selector_case() ==
          spanner_api::TransactionSelector::SelectorCase::kSingleUse &&
      selector.single_use().mode_case() != v1::TransactionOptions::kReadOnly) {
    return error::InvalidModeForReadOnlySingleUseTransaction();
  }
  if (is_dml) {
    if (selector.begin().mode_case() == v1::TransactionOptions::kReadOnly) {
      // ReadWrite and PartitionedDML transactions are currently allowed.
      return error::DmlRequiresReadWriteTransaction("ReadOnly");
    }
    if (selector.selector_case() ==
        spanner_api::TransactionSelector::SelectorCase::kSingleUse) {
      return error::DmlDoesNotSupportSingleUseTransaction();
    }
  }
  return zetasql_base::OkStatus();
}

zetasql_base::Status ValidatePartitionToken(
    const PartitionToken& partition_token,
    const spanner_api::ExecuteSqlRequest* request) {
  if (request->query_mode() != v1::ExecuteSqlRequest::NORMAL) {
    return error::InvalidPartitionedQueryMode();
  }
  if (partition_token.session() != request->session()) {
    return error::ReadFromDifferentSession();
  }
  if (request->transaction().selector_case() != v1::TransactionSelector::kId ||
      partition_token.transaction_id() != request->transaction().id()) {
    return error::ReadFromDifferentTransaction();
  }

  if (!partition_token.has_query_params()) {
    return error::ReadFromDifferentParameters();
  }
  auto query_params = partition_token.query_params();

  if (query_params.sql() != request->sql()) {
    return error::ReadFromDifferentParameters();
  }

  if (query_params.params().SerializeAsString() !=
      request->params().SerializeAsString()) {
    return error::ReadFromDifferentParameters();
  }

  if (query_params.param_types_size() != request->param_types_size()) {
    return error::ReadFromDifferentParameters();
  }
  for (const auto& param_type : query_params.param_types()) {
    if (!request->param_types().contains(param_type.first) ||
        param_type.second.GetTypeName() !=
            request->param_types().at(param_type.first).GetTypeName()) {
      return error::ReadFromDifferentParameters();
    }
  }

  return zetasql_base::OkStatus();
}

bool IsDmlResult(const backend::QueryResult& result) {
  return result.rows == nullptr;
}

void AddQueryStatsFromQueryResult(const backend::QueryResult& result,
                                  google::protobuf::Struct* stats) {
  (*stats->mutable_fields())["rows_returned"].set_string_value(
      absl::StrCat(result.num_output_rows));
  (*stats->mutable_fields())["elapsed_time"].set_string_value(
      absl::FormatDuration(result.elapsed_time));
}

zetasql_base::StatusOr<backend::QueryResult> ExecuteQuery(
    const spanner_api::ExecuteBatchDmlRequest_Statement& statement,
    std::shared_ptr<Transaction> txn) {
  ZETASQL_ASSIGN_OR_RETURN(const backend::Query query,
                   QueryFromProto(statement.sql(), statement.params(),
                                  statement.param_types(),
                                  txn->query_engine()->type_factory()));
  return txn->ExecuteSql(query);
}

}  //  namespace

// Executes a SQL statement, returning all results in a single reply.
zetasql_base::Status ExecuteSql(RequestContext* ctx,
                        const spanner_api::ExecuteSqlRequest* request,
                        spanner_api::ResultSet* response) {
  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(ValidateTransactionSelectorForQuery(
      request->transaction(), backend::IsDMLQuery(request->sql())));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  // Wrap all operations on this transaction so they are atomic .
  return txn->GuardedCall([&]() -> zetasql_base::Status {
    // Cannot query after commit or rollback.
    if (txn->IsCommitted() || txn->IsRolledback()) {
      return error::CannotReadOrQueryAfterCommitOrRollback();
    }

    ZETASQL_ASSIGN_OR_RETURN(const backend::Query query,
                     QueryFromProto(request->sql(), request->params(),
                                    request->param_types(),
                                    txn->query_engine()->type_factory()));
    ZETASQL_ASSIGN_OR_RETURN(backend::QueryResult result, txn->ExecuteSql(query));

    // Populate transaction metadata.
    if (ShouldReturnTransaction(request->transaction())) {
      ZETASQL_ASSIGN_OR_RETURN(*response->mutable_metadata()->mutable_transaction(),
                       txn->ToProto());
    }

    if (IsDmlResult(result)) {
      response->mutable_stats()->set_row_count_exact(result.modified_row_count);
      // Set empty row type.
      response->mutable_metadata()->mutable_row_type();
    } else {
      ZETASQL_RETURN_IF_ERROR(RowCursorToResultSetProto(result.rows.get(),
                                                /*limit=*/0, response));
    }

    if (!request->partition_token().empty()) {
      ZETASQL_ASSIGN_OR_RETURN(auto partition_token,
                       PartitionTokenFromString(request->partition_token()));
      ZETASQL_RETURN_IF_ERROR(ValidatePartitionToken(partition_token, request));
      if (partition_token.empty_query_partition()) {
        response->clear_rows();
      }
    }

    // Add basic stats for PROFILE mode. We do this to interoperate with REPL
    // applications written for Cloud Spanner. The profile will not contain
    // statistics for plan nodes.
    if (request->query_mode() == spanner_api::ExecuteSqlRequest::PROFILE) {
      AddQueryStatsFromQueryResult(
          result, response->mutable_stats()->mutable_query_stats());
    }

    // Reject requests for PLAN mode. The emulator uses ZetaSQL reference
    // implementation which performs an unoptimized execution of the SQL query.
    // The plan chosen by ZetaSQL will have no relation to those generated by
    // Cloud Spanner, so we reject the PLAN mode completely.
    if (request->query_mode() == spanner_api::ExecuteSqlRequest::PLAN) {
      return error::EmulatorDoesNotSupportQueryPlans();
    }

    return zetasql_base::OkStatus();
  });
}
REGISTER_GRPC_HANDLER(Spanner, ExecuteSql);

// Executes a SQL statement, returning all results as a stream.
//
// resume_tokens is not supported in the emulator. This implementation does not
// limit the size of the response and therefore, chunked_value will always be
// false.
zetasql_base::Status ExecuteStreamingSql(
    RequestContext* ctx, const spanner_api::ExecuteSqlRequest* request,
    ServerStream<spanner_api::PartialResultSet>* stream) {
  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(ValidateTransactionSelectorForQuery(
      request->transaction(), backend::IsDMLQuery(request->sql())));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  // Wrap all operations on this transaction so they are atomic .
  return txn->GuardedCall([&]() -> zetasql_base::Status {
    // Cannot query after commit or rollback.
    if (txn->IsCommitted() || txn->IsRolledback()) {
      return error::CannotReadOrQueryAfterCommitOrRollback();
    }

    ZETASQL_ASSIGN_OR_RETURN(const backend::Query query,
                     QueryFromProto(request->sql(), request->params(),
                                    request->param_types(),
                                    txn->query_engine()->type_factory()));
    ZETASQL_ASSIGN_OR_RETURN(backend::QueryResult result, txn->ExecuteSql(query));

    std::vector<spanner_api::PartialResultSet> responses;
    if (IsDmlResult(result)) {
      responses.emplace_back();
      responses.back().mutable_stats()->set_row_count_exact(
          result.modified_row_count);
      // Set empty row type.
      responses.back().mutable_metadata()->mutable_row_type();
    } else {
      ZETASQL_ASSIGN_OR_RETURN(responses, RowCursorToPartialResultSetProtos(
                                      result.rows.get(), /*limit=*/0));
    }

    if (!request->partition_token().empty()) {
      ZETASQL_ASSIGN_OR_RETURN(auto partition_token,
                       PartitionTokenFromString(request->partition_token()));
      ZETASQL_RETURN_IF_ERROR(ValidatePartitionToken(partition_token, request));
      if (partition_token.empty_query_partition()) {
        // Clear all partial responses except the first one. Return only
        // metadata in the first partial response.
        responses.resize(1);
        responses.front().clear_values();
        responses.front().clear_chunked_value();
      }
    }

    // Populate transaction metadata.
    if (ShouldReturnTransaction(request->transaction())) {
      ZETASQL_ASSIGN_OR_RETURN(
          *responses.front().mutable_metadata()->mutable_transaction(),
          txn->ToProto());
    }

    // Add basic stats for PROFILE mode. We do this to interoperate with REPL
    // applications written for Cloud Spanner. The profile will not contain
    // statistics for plan nodes.
    if (request->query_mode() == spanner_api::ExecuteSqlRequest::PROFILE) {
      AddQueryStatsFromQueryResult(
          result, responses.front().mutable_stats()->mutable_query_stats());
    }

    // Reject requests for PLAN mode. The emulator uses ZetaSQL reference
    // implementation which performs an unoptimized execution of the SQL query.
    // The plan chosen by ZetaSQL will have no relation to those generated by
    // Cloud Spanner, so we reject the PLAN mode completely.
    if (request->query_mode() == spanner_api::ExecuteSqlRequest::PLAN) {
      return error::EmulatorDoesNotSupportQueryPlans();
    }

    // Send results back to client.
    for (const auto& response : responses) {
      stream->Send(response);
    }
    return zetasql_base::OkStatus();
  });
}
REGISTER_GRPC_HANDLER(Spanner, ExecuteStreamingSql);

// Executes a batch of DML statements.
zetasql_base::Status ExecuteBatchDml(RequestContext* ctx,
                             const spanner_api::ExecuteBatchDmlRequest* request,
                             spanner_api::ExecuteBatchDmlResponse* response) {
  // Verify the request has DML statement(s).
  if (request->statements().empty()) {
    return error::InvalidBatchDmlRequest();
  }

  // Take shared ownerships of session and transaction so that they will keep
  // valid throughout this function.
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Session> session,
                   GetSession(ctx, request->session()));

  // Get underlying transaction.
  ZETASQL_RETURN_IF_ERROR(ValidateTransactionSelectorForQuery(request->transaction(),
                                                      /*is_dml=*/true));
  ZETASQL_ASSIGN_OR_RETURN(std::shared_ptr<Transaction> txn,
                   session->FindOrInitTransaction(request->transaction()));

  // Wrap all operations on this transaction so they are atomic.
  return txn->GuardedCall([&]() -> zetasql_base::Status {
    // Cannot query after commit or rollback.
    if (txn->IsCommitted() || txn->IsRolledback()) {
      return error::CannotReadOrQueryAfterCommitOrRollback();
    }

    for (int index = 0; index < request->statements_size(); ++index) {
      const auto& statement = request->statements(index);
      if (!backend::IsDMLQuery(statement.sql())) {
        *response->mutable_status() =
            StatusToProto(error::ExecuteBatchDmlOnlySupportsDmlStatements(
                index, statement.sql()));
        return zetasql_base::OkStatus();
      }

      auto maybe_result = ExecuteQuery(statement, txn);
      if (!maybe_result.ok() &&
          maybe_result.status().code() != zetasql_base::StatusCode::kAborted) {
        *response->mutable_status() = StatusToProto(maybe_result.status());
        return zetasql_base::OkStatus();
      } else if (maybe_result.status().code() == zetasql_base::StatusCode::kAborted) {
        return maybe_result.status();
      }
      auto& result = maybe_result.value();
      spanner_api::ResultSet* result_set = response->add_result_sets();
      result_set->mutable_stats()->set_row_count_exact(
          result.modified_row_count);
      result_set->mutable_metadata()->mutable_row_type();

      // Populate transaction metadata.
      if (ShouldReturnTransaction(request->transaction())) {
        ZETASQL_ASSIGN_OR_RETURN(*result_set->mutable_metadata()->mutable_transaction(),
                         txn->ToProto());
      }
    }

    return zetasql_base::OkStatus();
  });
}
REGISTER_GRPC_HANDLER(Spanner, ExecuteBatchDml);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
