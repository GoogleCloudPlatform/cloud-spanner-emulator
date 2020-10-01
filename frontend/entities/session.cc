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

#include "frontend/entities/session.h"

#include <memory>

#include "google/spanner/v1/spanner.pb.h"
#include "google/spanner/v1/transaction.pb.h"
#include "zetasql/base/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "backend/common/ids.h"
#include "backend/transaction/options.h"
#include "backend/transaction/read_only_transaction.h"
#include "backend/transaction/read_write_transaction.h"
#include "common/errors.h"
#include "common/limits.h"
#include "frontend/common/protos.h"
#include "frontend/converters/reads.h"
#include "frontend/converters/time.h"
#include "frontend/entities/transaction.h"
#include "absl/status/status.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace frontend {

namespace spanner_api = ::google::spanner::v1;

namespace {

// Validate that read only options specify a concurrency mode that can be used
// with multi use transactions.
absl::Status ValidateReadOptionsForMultiUseTransaction(
    const spanner_api::TransactionOptions::ReadOnly& read_options) {
  using ReadOnly = spanner_api::TransactionOptions::ReadOnly;
  switch (read_options.timestamp_bound_case()) {
    case ReadOnly::kMinReadTimestamp:
      return error::InvalidReadOptionForMultiUseTransaction(
          "min_read_timestamp");
    case ReadOnly::kMaxStaleness:
      return error::InvalidReadOptionForMultiUseTransaction("max_staleness");
    default:
      return absl::OkStatus();
  }
}

absl::Status ValidateMultiUseTransactionOptions(
    const spanner_api::TransactionOptions& options) {
  switch (options.mode_case()) {
    case v1::TransactionOptions::kReadOnly:
      return ValidateReadOptionsForMultiUseTransaction(options.read_only());
    case v1::TransactionOptions::kReadWrite:
    case v1::TransactionOptions::kPartitionedDml:
      return absl::OkStatus();
    case v1::TransactionOptions::MODE_NOT_SET:
      return error::MissingRequiredFieldError("TransactionOptions.mode");
  }
}

absl::Status ValidateSingleUseTransactionOptions(
    const spanner_api::TransactionOptions& options) {
  switch (options.mode_case()) {
    case v1::TransactionOptions::kReadOnly:
    case v1::TransactionOptions::kReadWrite:
      return absl::OkStatus();
    case v1::TransactionOptions::kPartitionedDml:
      return error::DmlDoesNotSupportSingleUseTransaction();
    case v1::TransactionOptions::MODE_NOT_SET:
      return error::MissingRequiredFieldError("TransactionOptions.mode");
  }
}

}  // namespace

absl::Status Session::ToProto(spanner_api::Session* session,
                              bool include_labels) {
  absl::ReaderMutexLock lock(&mu_);
  session->set_name(session_uri_);
  if (include_labels) {
    session->mutable_labels()->insert(labels_.begin(), labels_.end());
  }
  ZETASQL_ASSIGN_OR_RETURN(*session->mutable_create_time(),
                   TimestampToProto(create_time_));
  ZETASQL_ASSIGN_OR_RETURN(*session->mutable_approximate_last_use_time(),
                   TimestampToProto(approximate_last_use_time_));
  return absl::OkStatus();
}

backend::RetryState Session::MakeRetryState(
    const spanner_api::TransactionOptions& options, bool is_single_use_txn) {
  mu_.AssertHeld();
  backend::RetryState retry_state;
  // Client libraries start a new transaction on the same session after
  // encountering an ABORT exception. Documentation:
  // https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions#retrying-aborted-transactions
  // Find if there was an ABORT status returned on the previous read-write
  // transaction for this session. Re-use the properties of backend read
  // write transaction to maintain transaction priority and abort retry
  // counts.
  if (options.has_read_write() && !is_single_use_txn &&
      active_transaction_ != nullptr && active_transaction_->IsReadWrite() &&
      active_transaction_->IsAborted()) {
    retry_state = active_transaction_->read_write()->retry_state();
  }
  return retry_state;
}

zetasql_base::StatusOr<std::shared_ptr<Transaction>> Session::CreateMultiUseTransaction(
    const spanner_api::TransactionOptions& options,
    const TransactionActivation& activation) {
  ZETASQL_RETURN_IF_ERROR(ValidateMultiUseTransactionOptions(options));

  absl::MutexLock lock(&mu_);
  // Move-convert unique pointer returned by CreateTransaction to shared pointer
  // since session will also hold a reference to multi-use transaction object
  // for future uses.
  ZETASQL_ASSIGN_OR_RETURN(
      std::shared_ptr<Transaction> txn,
      CreateTransaction(options, Transaction::Usage::kMultiUse,
                        MakeRetryState(options, /*is_single_use_txn=*/false)));

  // Insert shared transaction object into transaction map.
  transaction_map_.emplace(txn->id(), txn);

  // Clear older transactions if too many transactions are tracked by session.
  while (transaction_map_.size() > limits::kMaxTransactionsPerSession) {
    transaction_map_.begin()->second->Close();
    transaction_map_.erase(transaction_map_.begin());
  }

  if (activation == TransactionActivation::kInitializeAndActivate) {
    // Assign this as the current active transaction.
    active_transaction_ = txn;

    // Remove transactions that came before this one.
    auto it = transaction_map_.find(txn->id());
    for (auto prev_txn = transaction_map_.begin(); prev_txn != it; ++prev_txn) {
      prev_txn->second->Close();
    }
    transaction_map_.erase(transaction_map_.begin(), it);
  }
  return txn;
}

zetasql_base::StatusOr<std::unique_ptr<Transaction>>
Session::CreateSingleUseTransaction(
    const spanner_api::TransactionOptions& options) {
  ZETASQL_RETURN_IF_ERROR(ValidateSingleUseTransactionOptions(options));

  absl::MutexLock lock(&mu_);
  return CreateTransaction(options, Transaction::Usage::kSingleUse,
                           MakeRetryState(options, /*is_single_use_txn=*/true));
}

zetasql_base::StatusOr<std::unique_ptr<Transaction>> Session::CreateTransaction(
    const spanner_api::TransactionOptions& options,
    const Transaction::Usage& usage, const backend::RetryState& retry_state) {
  switch (options.mode_case()) {
    case v1::TransactionOptions::kReadOnly:
      return CreateReadOnly(options, usage);
    case v1::TransactionOptions::kReadWrite:
    case v1::TransactionOptions::kPartitionedDml:
      return CreateReadWrite(options, usage, retry_state);
    default:
      return error::Internal(
          "Unexpected TransactionOptions.mode for create transaction.");
  }
}

zetasql_base::StatusOr<std::unique_ptr<Transaction>> Session::CreateReadOnly(
    const spanner_api::TransactionOptions& options,
    const Transaction::Usage& usage) {
  // Populate read options.
  ZETASQL_ASSIGN_OR_RETURN(backend::ReadOnlyOptions read_only_options,
                   ReadOnlyOptionsFromProto(options.read_only()));

  // Create a new backend read only transaction.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<backend::ReadOnlyTransaction> read_only_transaction,
      database_->backend()->CreateReadOnlyTransaction(read_only_options));
  return std::make_unique<Transaction>(std::move(read_only_transaction),
                                       database_->backend()->query_engine(),
                                       options, usage);
}

zetasql_base::StatusOr<std::unique_ptr<Transaction>> Session::CreateReadWrite(
    const spanner_api::TransactionOptions& options,
    const Transaction::Usage& usage, const backend::RetryState& retry_state) {
  // Create a new backend read write transaction.
  ZETASQL_ASSIGN_OR_RETURN(
      std::unique_ptr<backend::ReadWriteTransaction> read_write_transaction,
      database_->backend()->CreateReadWriteTransaction(
          backend::ReadWriteOptions(), retry_state));

  return std::make_unique<Transaction>(std::move(read_write_transaction),
                                       database_->backend()->query_engine(),
                                       options, usage);
}

zetasql_base::StatusOr<std::shared_ptr<Transaction>> Session::FindAndUseTransaction(
    const std::string& bytes) {
  const backend::TransactionID& id = TransactionIDFromProto(bytes);
  absl::MutexLock lock(&mu_);
  if (id == backend::kInvalidTransactionID) {
    return error::InvalidTransactionID(backend::kInvalidTransactionID);
  }
  if (id < min_valid_id_) {
    return error::InvalidTransactionID(min_valid_id_);
  }
  min_valid_id_ = std::max(id, min_valid_id_);

  auto it = transaction_map_.find(id);
  if (it == transaction_map_.end()) {
    return error::TransactionNotFound(id);
  }

  if (it->second->IsClosed()) {
    return error::TransactionClosed(id);
  }
  active_transaction_ = it->second;

  // Remove transactions that came before this one.
  for (auto prev_txn = transaction_map_.begin(); prev_txn != it; ++prev_txn) {
    prev_txn->second->Close();
  }
  transaction_map_.erase(transaction_map_.begin(), it);
  return active_transaction_;
}

zetasql_base::StatusOr<std::shared_ptr<Transaction>> Session::FindOrInitTransaction(
    const spanner_api::TransactionSelector& selector) {
  std::shared_ptr<Transaction> txn;
  switch (selector.selector_case()) {
    case spanner_api::TransactionSelector::SelectorCase::kBegin: {
      ZETASQL_ASSIGN_OR_RETURN(txn, CreateMultiUseTransaction(
                                selector.begin(),
                                TransactionActivation::kInitializeAndActivate));
      break;
    }
    case spanner_api::TransactionSelector::SelectorCase::kId: {
      ZETASQL_ASSIGN_OR_RETURN(txn, FindAndUseTransaction(selector.id()));
      break;
    }
    case spanner_api::TransactionSelector::SelectorCase::kSingleUse: {
      ZETASQL_ASSIGN_OR_RETURN(txn, CreateSingleUseTransaction(selector.single_use()));
      break;
    }
    default:
      // If no transaction selector is provided, the default is a
      // temporary read-only transaction with strong concurrency.
      spanner_api::TransactionOptions options;
      options.mutable_read_only()->set_strong(true);
      options.mutable_read_only()->set_return_read_timestamp(false);
      ZETASQL_ASSIGN_OR_RETURN(txn, CreateSingleUseTransaction(options));
  }
  return txn;
}

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
