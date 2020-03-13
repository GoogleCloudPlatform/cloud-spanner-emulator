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
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/query/query_engine.h"
#include "backend/schema/catalog/schema.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/clock.h"
#include "frontend/entities/database.h"
#include "zetasql/base/status.h"
#include "zetasql/base/statusor.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

// Transaction represents a database transaction within the frontend.
//
// This class provides frontend-level functionality (e.g. URI, proto conversion,
// replay protection, invalidation) and wraps the backend transaction classes
// which actually implement the core transaction functionality. We do this to
// keep the backend transaction a completely separate module from the frontend
// and isolate it from gRPC API details.
class Transaction {
 public:
  Transaction(absl::variant<std::unique_ptr<backend::ReadWriteTransaction>,
                            std::unique_ptr<backend::ReadOnlyTransaction>>
                  backend_transaction,
              const backend::QueryEngine* query_engine);

  // Mark the transaction as closed.  This indicates that the transaction is no
  // longer valid in the context of its owning session.  For example, prior
  // transactions are closed once a new transaction is started.
  void Close() ABSL_LOCKS_EXCLUDED(mu_);

  // Fills transaction metadata for this transaction.
  zetasql_base::Status MaybeFillTransactionMetadata(
      const google::spanner::v1::TransactionSelector& selector,
      google::spanner::v1::ResultSetMetadata* metadata);

  // Returns the schema from the backend transaction.
  const backend::Schema* schema() const;

  // Returns the engine used to execute queries against the database.
  const backend::QueryEngine* query_engine() { return query_engine_; }

  // Returns the TransactionID from the backend transaction.
  backend::TransactionID id() const;

  // Calls Read using the backend transaction.
  zetasql_base::Status Read(const backend::ReadArg& read_arg,
                    std::unique_ptr<backend::RowCursor>* cursor);

  // Calls ExecuteSql using the backend transaction and query engine.
  zetasql_base::StatusOr<backend::QueryResult> ExecuteSql(const backend::Query& query);

  // Calls Write using the backend transaction.
  zetasql_base::Status Write(const backend::Mutation& mutation);

  // Calls Commit using the backend transaction.
  zetasql_base::Status Commit();

  // Calls Rollback using the backend transaction.
  zetasql_base::Status Rollback();

  // Returns the read timestamp from the backend transaction.
  zetasql_base::StatusOr<absl::Time> GetReadTimestamp() const;

  // Returns the commit timestamp from the backend transaction.
  zetasql_base::StatusOr<absl::Time> GetCommitTimestamp();

  bool IsClosed() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns true if the current transaction has already been committed.
  // For ReadOnlyTransaction, always returns false.
  bool IsCommitted() const;

  // Returns true if the current transaction has already been rolledback.
  // For ReadOnlyTransaction, always returns false.
  bool IsRolledback() const;

  // Returns true if the current transaction is a ReadOnlyTransaction.
  bool IsReadOnly() const;

  // All transaction methods should be called inside GuardedCall so that the
  // transaction can be rolled back on any errors.
  zetasql_base::Status GuardedCall(const std::function<zetasql_base::Status()>& fn)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Disallow copy and assignment.
  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

 private:
  // Returns true if the transaction is in the given state.
  bool HasState(const backend::ReadWriteTransaction::State& state) const;

  // The underlying backend transaction.
  absl::variant<std::unique_ptr<backend::ReadWriteTransaction>,
                std::unique_ptr<backend::ReadOnlyTransaction>>
      transaction_;

  // The query engine for executing queries.
  const backend::QueryEngine* query_engine_;

  // Mutex to guard state below.
  mutable absl::Mutex mu_;

  // True if this transaction has been invalidated.
  bool closed_ ABSL_GUARDED_BY(mu_) = false;

  // Previous outcome of the transaction.
  zetasql_base::Status status_ ABSL_GUARDED_BY(mu_);
};

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_FRONTEND_ENTITIES_TRANSACTIONS_H_
