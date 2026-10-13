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

#include "backend/query/queue/queryable_queue_tvfs.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "googlesql/public/catalog.h"
#include "googlesql/public/evaluator_table_iterator.h"
#include "googlesql/public/function_signature.h"
#include "googlesql/public/table_valued_function.h"
#include "googlesql/public/types/array_type.h"
#include "googlesql/public/types/type.h"
#include "googlesql/public/value.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/queue.h"
#include "backend/schema/catalog/table.h"
#include "common/constants.h"
#include "common/errors.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

constexpr int64_t kDefaultReceiveMaxBatchSize = 10;
constexpr int64_t kMaxReceiveBatchSizeLimit = 1000;
constexpr int64_t kMinReceivePriority = 0;
constexpr int64_t kMaxReceivePriority = 3;
constexpr absl::Duration kDefaultMockLeaseDuration = absl::Seconds(10);
constexpr absl::Duration kDefaultReceiveMaxDuration = absl::Seconds(1);
constexpr absl::Duration kMaxReceiveDurationLimit = absl::Minutes(59);

bool IsDeliverTimeColumn(absl::string_view col_name) {
  return absl::EqualsIgnoreCase(col_name, kQueueDeliverTime) ||
         absl::EqualsIgnoreCase(col_name, kQueuePgDeliverTime);
}

bool IsSystemGeneratedReceiveColumn(absl::string_view col_name) {
  return IsDeliverTimeColumn(col_name) ||
         absl::EqualsIgnoreCase(col_name, kQueueLeaseExpirationTimestamp) ||
         absl::EqualsIgnoreCase(col_name, kQueuePgLeaseExpirationTimestamp) ||
         absl::EqualsIgnoreCase(col_name, kQueueReceiverLeaseToken) ||
         absl::EqualsIgnoreCase(col_name, kQueuePgReceiverLeaseToken) ||
         absl::EqualsIgnoreCase(col_name, kQueueLastBatchMessage) ||
         absl::EqualsIgnoreCase(col_name, kQueuePgLastBatchMessage);
}

class QueueReceiveTableIterator : public googlesql::EvaluatorTableIterator {
 public:
  QueueReceiveTableIterator(
      std::unique_ptr<googlesql::EvaluatorTableIterator> backing_iterator,
      const std::vector<googlesql::TVFSchemaColumn>& output_columns,
      std::vector<int> output_to_backing_idx, int deliver_time_backing_slot,
      std::vector<int> pk_backing_slots, int64_t max_batch_size,
      QueueReceiveMetadata* metadata)
      : backing_iterator_(std::move(backing_iterator)),
        output_columns_(output_columns),
        output_to_backing_idx_(std::move(output_to_backing_idx)),
        deliver_time_backing_slot_(deliver_time_backing_slot),
        pk_backing_slots_(std::move(pk_backing_slots)),
        max_batch_size_(max_batch_size),
        metadata_(metadata) {}

  int NumColumns() const override { return output_columns_.size(); }
  std::string GetColumnName(int i) const override {
    return output_columns_[i].name;
  }
  const googlesql::Type* GetColumnType(int i) const override {
    return output_columns_[i].type;
  }
  const googlesql::Value& GetValue(int i) const override {
    return batch_rows_[current_batch_idx_][i];
  }

  bool NextRow() override {
    if (current_batch_idx_ < 0) {
      absl::Time now = absl::Now();
      while (batch_rows_.size() < max_batch_size_ &&
             backing_iterator_->NextRow()) {
        if (deliver_time_backing_slot_ >= 0) {
          const googlesql::Value& dt_val =
              backing_iterator_->GetValue(deliver_time_backing_slot_);
          if (!dt_val.is_null() && dt_val.ToTime() > now) {
            continue;
          }
        }
        if (metadata_ != nullptr) {
          std::string row_key_str;
          for (int slot : pk_backing_slots_) {
            absl::StrAppend(&row_key_str,
                            backing_iterator_->GetValue(slot).FullDebugString(),
                            "|");
          }
          if (deliver_time_backing_slot_ >= 0) {
            absl::StrAppend(
                &row_key_str,
                backing_iterator_->GetValue(deliver_time_backing_slot_)
                    .FullDebugString());
          }
          auto [it, inserted] = metadata_->leased_row_keys.try_emplace(
              std::move(row_key_str), now);
          if (!inserted) {
            if (now < it->second + kDefaultMockLeaseDuration) {
              continue;
            }
            it->second = now;
          }
        }

        const int token_id = metadata_ != nullptr
                                 ? ++metadata_->lease_token_counter
                                 : ++token_counter_;
        std::vector<googlesql::Value> row(output_columns_.size());
        for (int i = 0; i < output_columns_.size(); ++i) {
          const std::string& name = output_columns_[i].name;
          if (IsDeliverTimeColumn(name)) {
            if (deliver_time_backing_slot_ >= 0) {
              const googlesql::Value& dt_val =
                  backing_iterator_->GetValue(deliver_time_backing_slot_);
              row[i] =
                  dt_val.is_null() ? googlesql::values::Timestamp(now) : dt_val;
            } else {
              row[i] = googlesql::values::Timestamp(now);
            }
          } else if (output_to_backing_idx_[i] >= 0) {
            row[i] = backing_iterator_->GetValue(output_to_backing_idx_[i]);
          } else if (absl::EqualsIgnoreCase(name,
                                            kQueueLeaseExpirationTimestamp) ||
                     absl::EqualsIgnoreCase(name,
                                            kQueuePgLeaseExpirationTimestamp)) {
            row[i] =
                googlesql::values::Timestamp(now + kDefaultMockLeaseDuration);
          } else if (absl::EqualsIgnoreCase(name, kQueueReceiverLeaseToken) ||
                     absl::EqualsIgnoreCase(name, kQueuePgReceiverLeaseToken)) {
            row[i] = googlesql::values::String(
                absl::StrCat("mock-lease-token-", token_id));
          } else if (absl::EqualsIgnoreCase(name, kQueueLastBatchMessage) ||
                     absl::EqualsIgnoreCase(name, kQueuePgLastBatchMessage)) {
            row[i] = googlesql::values::Bool(false);
          }
        }
        batch_rows_.push_back(std::move(row));
      }
      status_ = backing_iterator_->Status();
      if (!batch_rows_.empty()) {
        for (int i = 0; i < output_columns_.size(); ++i) {
          const std::string& name = output_columns_[i].name;
          if (absl::EqualsIgnoreCase(name, kQueueLastBatchMessage) ||
              absl::EqualsIgnoreCase(name, kQueuePgLastBatchMessage)) {
            batch_rows_.back()[i] = googlesql::values::Bool(true);
          }
        }
      }
    }

    if (++current_batch_idx_ >= batch_rows_.size()) {
      return false;
    }
    return true;
  }

  absl::Status Status() const override { return status_; }
  absl::Status Cancel() override { return backing_iterator_->Cancel(); }

 private:
  std::unique_ptr<googlesql::EvaluatorTableIterator> backing_iterator_;
  const std::vector<googlesql::TVFSchemaColumn> output_columns_;
  const std::vector<int> output_to_backing_idx_;
  const int deliver_time_backing_slot_;
  const std::vector<int> pk_backing_slots_;
  const int64_t max_batch_size_;
  QueueReceiveMetadata* const metadata_;
  std::vector<std::vector<googlesql::Value>> batch_rows_;
  int current_batch_idx_ = -1;
  absl::Status status_;
  int token_counter_ = 0;
};

class QueueRenewLeaseTableIterator : public googlesql::EvaluatorTableIterator {
 public:
  QueueRenewLeaseTableIterator(
      const std::vector<googlesql::Value>& tokens,
      const std::vector<googlesql::TVFSchemaColumn>& output_columns)
      : tokens_(tokens),
        output_columns_(output_columns),
        current_row_values_(output_columns_.size()) {}

  int NumColumns() const override { return output_columns_.size(); }
  std::string GetColumnName(int i) const override {
    return output_columns_[i].name;
  }
  const googlesql::Type* GetColumnType(int i) const override {
    return output_columns_[i].type;
  }
  const googlesql::Value& GetValue(int i) const override {
    return current_row_values_[i];
  }

  bool NextRow() override {
    if (++current_idx_ >= tokens_.size()) {
      return false;
    }

    const googlesql::Value& old_token = tokens_[current_idx_];
    absl::Time now = absl::Now();
    googlesql::Value new_token = googlesql::values::String(
        absl::StrCat(old_token.string_value(), "-renewed"));

    for (int i = 0; i < output_columns_.size(); ++i) {
      const std::string& name = output_columns_[i].name;
      if (absl::EqualsIgnoreCase(name, kQueueRenewLeaseOldLeaseToken) ||
          absl::EqualsIgnoreCase(name, kQueuePgRenewLeaseOldLeaseToken)) {
        current_row_values_[i] = old_token;
      } else if (absl::EqualsIgnoreCase(name, kQueueRenewLeaseNewLeaseToken) ||
                 absl::EqualsIgnoreCase(name,
                                        kQueuePgRenewLeaseNewLeaseToken)) {
        current_row_values_[i] = new_token;
      } else if (absl::EqualsIgnoreCase(name, kQueueLeaseExpirationTimestamp) ||
                 absl::EqualsIgnoreCase(name,
                                        kQueuePgLeaseExpirationTimestamp)) {
        current_row_values_[i] =
            googlesql::values::Timestamp(now + absl::Seconds(10));
      }
    }
    return true;
  }

  absl::Status Status() const override { return absl::OkStatus(); }
  absl::Status Cancel() override { return absl::OkStatus(); }

 private:
  const std::vector<googlesql::Value> tokens_;
  const std::vector<googlesql::TVFSchemaColumn> output_columns_;
  int current_idx_ = -1;
  std::vector<googlesql::Value> current_row_values_;
};

}  // namespace

absl::StatusOr<std::unique_ptr<QueryableQueueReceiveTvf>>
QueryableQueueReceiveTvf::Create(const Queue* queue,
                                 googlesql::Catalog* catalog,
                                 googlesql::TypeFactory* type_factory,
                                 bool is_pg,
                                 std::vector<std::string> tvf_name_path) {
  std::vector<googlesql::FunctionArgumentType> args;
  std::vector<googlesql::TVFSchemaColumn> output_columns;

  const std::string deliver_time_col_name =
      is_pg ? kQueuePgDeliverTime : kQueueDeliverTime;
  bool has_deliver_time = false;
  // Add user columns to output columns
  for (const Column* col : queue->columns()) {
    output_columns.emplace_back(col->Name(), col->GetType());
    if (IsDeliverTimeColumn(col->Name())) {
      has_deliver_time = true;
    }
  }

  // Add system columns to output columns
  if (!has_deliver_time) {
    output_columns.emplace_back(deliver_time_col_name,
                                type_factory->get_timestamp());
  }
  output_columns.emplace_back(
      is_pg ? kQueuePgLeaseExpirationTimestamp : kQueueLeaseExpirationTimestamp,
      type_factory->get_timestamp());
  output_columns.emplace_back(
      is_pg ? kQueuePgReceiverLeaseToken : kQueueReceiverLeaseToken,
      type_factory->get_string());
  output_columns.emplace_back(
      is_pg ? kQueuePgLastBatchMessage : kQueueLastBatchMessage,
      type_factory->get_bool());

  // Input arguments: max_batch_size (INT64), priority (INT64),
  // max_duration (STRING)
  auto add_optional_arg = [&](absl::string_view name,
                              const googlesql::Type* type) {
    auto arg_option =
        googlesql::FunctionArgumentTypeOptions()
            .set_argument_name(name, googlesql::kPositionalOrNamed)
            .set_cardinality(googlesql::FunctionArgumentType::OPTIONAL)
            .set_default(googlesql::values::Null(type));
    args.emplace_back(type, arg_option);
  };
  add_optional_arg("max_batch_size", type_factory->get_int64());
  add_optional_arg("priority", type_factory->get_int64());
  add_optional_arg("max_duration", type_factory->get_string());

  googlesql::TVFRelation result_schema(output_columns);
  const auto result_type = googlesql::FunctionArgumentType::RelationWithSchema(
      result_schema, /*extra_relation_input_columns_allowed=*/false);
  const auto signature = googlesql::FunctionSignature(result_type, args,
                                                      /*context_ptr=*/nullptr);
  if (tvf_name_path.empty()) {
    std::string prefix = is_pg ? absl::AsciiStrToLower(kQueueReceiverTvfPrefix)
                               : kQueueReceiverTvfPrefix;
    tvf_name_path = {absl::StrCat(prefix, queue->Name())};
  }
  return std::make_unique<QueryableQueueReceiveTvf>(
      tvf_name_path, signature, result_schema, queue, catalog);
}

absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
QueryableQueueReceiveTvf::CreateEvaluator(
    std::vector<TvfEvaluatorArg> input_arguments,
    const std::vector<googlesql::TVFSchemaColumn>& output_columns,
    const googlesql::FunctionSignature* function_call_signature) const {
  int64_t max_batch_size = kDefaultReceiveMaxBatchSize;
  if (!input_arguments.empty() && input_arguments[0].value.has_value() &&
      !input_arguments[0].value->is_null()) {
    int64_t val = input_arguments[0].value->int64_value();
    if (val <= 0 || val > kMaxReceiveBatchSizeLimit) {
      return error::QueueTvfInvalidMaxBatchSize(val, kMaxReceiveBatchSizeLimit);
    }
    max_batch_size = val;
  }
  if (input_arguments.size() > 1 && input_arguments[1].value.has_value() &&
      !input_arguments[1].value->is_null()) {
    int64_t priority = input_arguments[1].value->int64_value();
    if (priority < kMinReceivePriority || priority > kMaxReceivePriority) {
      return error::QueueTvfInvalidPriority();
    }
  }
  absl::Duration max_duration = kDefaultReceiveMaxDuration;
  if (input_arguments.size() > 2 && input_arguments[2].value.has_value() &&
      !input_arguments[2].value->is_null()) {
    if (!absl::ParseDuration(input_arguments[2].value->string_value(),
                             &max_duration) ||
        max_duration <= absl::ZeroDuration() ||
        max_duration > kMaxReceiveDurationLimit) {
      return error::QueueTvfInvalidMaxDurationFormat();
    }
  }
  if (metadata_ != nullptr) {
    metadata_->is_queue_receive_query = true;
    metadata_->max_duration = max_duration;
  }

  const Table* backing_table = queue_->backing_table();
  GOOGLESQL_RET_CHECK_NE(backing_table, nullptr);
  GOOGLESQL_RET_CHECK_NE(catalog_, nullptr);

  const googlesql::Table* gsql_backing_table = nullptr;
  GOOGLESQL_RETURN_IF_ERROR(
      catalog_->FindTable({backing_table->Name()}, &gsql_backing_table, {}));
  GOOGLESQL_RET_CHECK_NE(gsql_backing_table, nullptr);

  std::vector<int> backing_column_idxs;
  std::vector<int> output_to_backing_idx(output_columns.size(), -1);

  int deliver_time_backing_slot = -1;
  for (int j = 0; j < gsql_backing_table->NumColumns(); ++j) {
    if (IsDeliverTimeColumn(gsql_backing_table->GetColumn(j)->Name())) {
      deliver_time_backing_slot = backing_column_idxs.size();
      backing_column_idxs.push_back(j);
      break;
    }
  }

  for (int i = 0; i < output_columns.size(); ++i) {
    const std::string& col_name = output_columns[i].name;
    if (IsSystemGeneratedReceiveColumn(col_name)) {
      continue;
    }
    int backing_idx = -1;
    for (int j = 0; j < gsql_backing_table->NumColumns(); ++j) {
      if (absl::EqualsIgnoreCase(gsql_backing_table->GetColumn(j)->Name(),
                                 col_name)) {
        backing_idx = j;
        break;
      }
    }
    GOOGLESQL_RET_CHECK_NE(backing_idx, -1)
        << "Column not found in backing table: " << col_name;
    output_to_backing_idx[i] = backing_column_idxs.size();
    backing_column_idxs.push_back(backing_idx);
  }

  std::vector<int> pk_backing_slots;
  if (metadata_ != nullptr) {
    for (const KeyColumn* pk_col : backing_table->primary_key()) {
      int backing_idx = -1;
      for (int j = 0; j < gsql_backing_table->NumColumns(); ++j) {
        if (absl::EqualsIgnoreCase(gsql_backing_table->GetColumn(j)->Name(),
                                   pk_col->column()->Name())) {
          backing_idx = j;
          break;
        }
      }
      if (backing_idx >= 0) {
        pk_backing_slots.push_back(backing_column_idxs.size());
        backing_column_idxs.push_back(backing_idx);
      }
    }
  }

  GOOGLESQL_ASSIGN_OR_RETURN(
      std::unique_ptr<googlesql::EvaluatorTableIterator> backing_iterator,
      gsql_backing_table->CreateEvaluatorTableIterator(backing_column_idxs));

  return std::make_unique<QueueReceiveTableIterator>(
      std::move(backing_iterator), output_columns,
      std::move(output_to_backing_idx), deliver_time_backing_slot,
      std::move(pk_backing_slots), max_batch_size, metadata_);
}

absl::StatusOr<std::unique_ptr<QueryableQueueRenewLeaseTvf>>
QueryableQueueRenewLeaseTvf::Create(const Queue* queue,
                                    googlesql::TypeFactory* type_factory,
                                    bool is_pg,
                                    std::vector<std::string> tvf_name_path) {
  std::vector<googlesql::FunctionArgumentType> args;
  std::vector<googlesql::TVFSchemaColumn> output_columns;

  output_columns.emplace_back(
      is_pg ? kQueuePgRenewLeaseOldLeaseToken : kQueueRenewLeaseOldLeaseToken,
      type_factory->get_string());
  output_columns.emplace_back(
      is_pg ? kQueuePgRenewLeaseNewLeaseToken : kQueueRenewLeaseNewLeaseToken,
      type_factory->get_string());
  output_columns.emplace_back(
      is_pg ? kQueuePgLeaseExpirationTimestamp : kQueueLeaseExpirationTimestamp,
      type_factory->get_timestamp());

  GOOGLESQL_ASSIGN_OR_RETURN(const googlesql::ArrayType* lease_tokens_type,
                   type_factory->MakeArrayType(type_factory->get_string()));

  // lease_tokens is required.
  auto arg_option = googlesql::FunctionArgumentTypeOptions().set_argument_name(
      kQueueRenewLeaseTokens, googlesql::kPositionalOrNamed);
  args.emplace_back(lease_tokens_type, arg_option);

  googlesql::TVFRelation result_schema(output_columns);
  const auto result_type = googlesql::FunctionArgumentType::RelationWithSchema(
      result_schema, /*extra_relation_input_columns_allowed=*/false);
  const auto signature = googlesql::FunctionSignature(result_type, args,
                                                      /*context_ptr=*/nullptr);
  if (tvf_name_path.empty()) {
    std::string prefix = is_pg
                             ? absl::AsciiStrToLower(kQueueRenewLeaseTvfPrefix)
                             : kQueueRenewLeaseTvfPrefix;
    tvf_name_path = {absl::StrCat(prefix, queue->Name())};
  }
  return std::make_unique<QueryableQueueRenewLeaseTvf>(tvf_name_path, signature,
                                                       result_schema, queue);
}

absl::StatusOr<std::unique_ptr<googlesql::EvaluatorTableIterator>>
QueryableQueueRenewLeaseTvf::CreateEvaluator(
    std::vector<TvfEvaluatorArg> input_arguments,
    const std::vector<googlesql::TVFSchemaColumn>& output_columns,
    const googlesql::FunctionSignature* function_call_signature) const {
  GOOGLESQL_RET_CHECK_GE(input_arguments.size(), 1);
  GOOGLESQL_RET_CHECK(input_arguments[0].value.has_value());
  const googlesql::Value& tokens_value = *input_arguments[0].value;

  GOOGLESQL_RET_CHECK(tokens_value.type()->IsArray());

  std::vector<googlesql::Value> tokens;
  if (!tokens_value.is_null()) {
    GOOGLESQL_ASSIGN_OR_RETURN(googlesql::Value::ListView view,
                     tokens_value.elements_view());
    for (const googlesql::Value& token : view) {
      if (token.is_null() || token.string_value().empty()) {
        return error::InvalidQueueLeaseToken();
      }
      tokens.push_back(token);
    }
  }

  return std::make_unique<QueueRenewLeaseTableIterator>(tokens, output_columns);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
