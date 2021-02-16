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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_TRANSACTIONS_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_TRANSACTIONS_H_

#include <memory>

#include "google/protobuf/empty.pb.h"
#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "absl/status/status.h"
#include "zetasql/base/statusor.h"
#include "absl/time/time.h"
#include "absl/types/optional.h"
#include "absl/types/variant.h"
#include "backend/common/ids.h"
#include "backend/query/query_engine.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "frontend/entities/database.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

// Transaction represents a database transaction within the frontend.
//
// This class provides frontend-level functionality (e.g. URI, proto conversion,
// replay protection, invalidation) and wraps the backend transaction classes
// which actually implement the core transaction functionality. We do this to
// keep the backend transaction a completely separate module from the frontend
// and isolate it from gRPC API details.
class Transaction {
 public:
  enum Type {
    kReadWrite,
    kReadOnly,
    kPartitionedDml,
  };

  enum Usage {
    kSingleUse,
    kMultiUse,
  };

  // These operation types correspond to the handler that was invoked. For
  // ExecuteSql handlers, the operation can be either kSql(read-only) or
  // kDml(read-write).
  enum class OpType {
    kRead,
    kCommit,
    kSql,
    kDml,
    kRollback,
  };

  // Indicates the DML error handling mode. This is used to differentiate
  // between requests, replays, and registration errors.
  enum class DMLErrorHandlingMode {
    kDmlRequest,
    kDmlReplay,
    kDmlRegistrationError,
  };

  // RequestReplayState tracks the state for DML operations. DML requests can be
  // replayed. If a replayed request is detected, the previously saved state and
  // outcome will be used.
  struct RequestReplayState {
    // Indicates the final status of the request. Used for replaying the status.
    // For BatchDml requests, this is not used since the status will be
    // saved/returned in the response.
    absl::Status status;

    // The hash of the request that generated this state. This is used to detect
    // request replays with a reused sequence number but with a different
    // request.
    int64_t request_hash;

    // "outcome" can be one of two types:
    //   a) ResultSet - Used by ExecuteSql and ExecuteStreamingSql handler.
    //   b) ExecuteBatchDmlResponse - Used by ExecuteBatchDml handler.
    absl::variant<spanner_api::ResultSet, spanner_api::ExecuteBatchDmlResponse>
        outcome;
  };

  Transaction(absl::variant<std::unique_ptr<backend::ReadWriteTransaction>,
                            std::unique_ptr<backend::ReadOnlyTransaction>>
                  backend_transaction,
              const backend::QueryEngine* query_engine,
              const spanner_api::TransactionOptions& options,
              const Usage& usage);

  // Mark the transaction as closed. This indicates that the transaction is no
  // longer valid in the context of its owning session.  For example, prior
  // transactions are closed once a new transaction is started.
  void Close() ABSL_LOCKS_EXCLUDED(mu_);

  // Converts this transaction to its proto representation.
  zetasql_base::StatusOr<google::spanner::v1::Transaction> ToProto();

  // Returns the schema from the backend transaction.
  const backend::Schema* schema() const;

  // Returns the engine used to execute queries against the database.
  const backend::QueryEngine* query_engine() { return query_engine_; }

  // Returns the TransactionID from the backend transaction.
  backend::TransactionID id() const;

  // Calls Read using the backend transaction.
  absl::Status Read(const backend::ReadArg& read_arg,
                    std::unique_ptr<backend::RowCursor>* cursor);

  // Calls ExecuteSql using the backend transaction and query engine.
  zetasql_base::StatusOr<backend::QueryResult> ExecuteSql(const backend::Query& query);

  // Calls Write using the backend transaction.
  absl::Status Write(const backend::Mutation& mutation);

  // Calls Commit using the backend transaction.
  absl::Status Commit();

  // Calls Rollback using the backend transaction.
  absl::Status Rollback();

  // Returns the read timestamp from the backend transaction.
  zetasql_base::StatusOr<absl::Time> GetReadTimestamp() const;

  // Returns the commit timestamp from the backend transaction.
  zetasql_base::StatusOr<absl::Time> GetCommitTimestamp() const;

  bool IsClosed() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns true if the current transaction has already been committed.
  // For ReadOnlyTransaction, always returns false.
  bool IsCommitted() const;

  // Returns true if the current transaction has already been rolledback.
  // For ReadOnlyTransaction, always returns false.
  bool IsRolledback() const;

  // Returns true if the current transaction has been aborted due to
  // non-recoverable errors. For ReadOnlyTransaction, always returns false.
  bool IsInvalid() const;

  // Returns true if the current transaction has been aborted.
  // For ReadOnlyTransaction & PartitionedDmlTransaction, always returns false.
  bool IsAborted() const;

  // Returns true if the current transaction is a ReadOnlyTransaction.
  bool IsReadOnly() const { return type_ == kReadOnly; }

  // Returns true if the current transaction is a ReadWriteTransaction.
  bool IsReadWrite() const { return type_ == kReadWrite; }

  // Returns true if the current transaction is a PartitionedDmlTransaction.
  bool IsPartitionedDml() const { return type_ == kPartitionedDml; }

  // All transaction methods should be called inside GuardedCall.
  absl::Status GuardedCall(OpType op, const std::function<absl::Status()>& fn)
      ABSL_LOCKS_EXCLUDED(mu_);

  backend::ReadWriteTransaction* read_write() const {
    return std::get<0>(transaction_).get();
  }

  backend::ReadOnlyTransaction* read_only() const {
    return std::get<1>(transaction_).get();
  }

  // Returns the current transaction status.
  absl::Status Status() const;

  // If status is a constraint error, it invalidates the transaction and sets
  // the transaction status.
  void MaybeInvalidate(const absl::Status& status);

  // Registers a new RequestReplayState using the given sequence number. If a
  // state already exists for a given sequence number with a different request
  // hash, an error will be returned in the Status field of RequestReplayState.
  // If a state already exists for a given sequence number with a matching
  // request hash, the replay state will be returned. If this is a new sequence
  // number, a new replay state will be registered and nullopt will be returned.
  absl::optional<RequestReplayState> LookupOrRegisterDmlRequest(
      int64_t seqno, int64_t request_hash, const std::string& sql_statement);

  // Sets the replay outcome for the current DML request if it completed
  // successfully.
  void SetDmlReplayOutcome(absl::variant<spanner_api::ResultSet,
                                         spanner_api::ExecuteBatchDmlResponse>
                               outcome);

  // Returns the DML request type.
  DMLErrorHandlingMode DMLErrorType() const;

  // Disallow copy and assignment.
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

 private:
  // Sets the status within the DML RequestReplayState for the currently
  // executing DML. If the current request failed due to a registration error
  // (sequence out of order or request hash mismatch), this will be a no-op.
  // When a DML replay request is received this saved status will be replayed
  // instead of repeating the execution of that operation.
  void SetDmlRequestReplayStatus(const absl::Status& status);

  // Invalidates the backend transaction. This should only ever be invoked due
  // to non-recoverable errors.
  absl::Status Invalidate();

  // Returns true if the transaction is in the given state.
  bool HasState(const backend::ReadWriteTransaction::State& state) const;

  // The underlying backend transaction.
  absl::variant<std::unique_ptr<backend::ReadWriteTransaction>,
                std::unique_ptr<backend::ReadOnlyTransaction>>
      transaction_;

  // The query engine for executing queries.
  const backend::QueryEngine* query_engine_;

  // True if this transaction should not be reused. In such a case, proto
  // representation of this transaction will not return transaction id.
  bool is_single_use_;

  // True if proto representation of this transaction should return the
  // timestamp at which reads are be performed by the backend transaction.
  bool return_read_timestamp_;

  // Usage type (single-use or multi-use) for this transaction.
  const Usage usage_type_;

  // Transaction type that determines read, write and partition use.
  const Type type_;

  // Options for the transaction from the original rpc request.
  const spanner_api::TransactionOptions options_;

  // Mutex to guard state below.
  mutable absl::Mutex mu_;

  // True if this transaction has been invalidated.
  bool closed_ ABSL_GUARDED_BY(mu_) = false;

  // Previous outcome of the transaction.
  absl::Status status_ ABSL_GUARDED_BY(mu_);

  // DML sequence number of the current request.
  int64_t current_dml_seqno_ ABSL_GUARDED_BY(mu_);

  // The type of DML request.
  DMLErrorHandlingMode dml_error_mode_;

  // DML sequence request map.
  std::map<int64_t, RequestReplayState> dml_requests_ ABSL_GUARDED_BY(mu_);
};

// Return true if the given transaction selector requires the transaction to be
// returned in the response.
bool ShouldReturnTransaction(
    const google::spanner::v1::TransactionSelector& selector);

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_TRANSACTIONS_H_
