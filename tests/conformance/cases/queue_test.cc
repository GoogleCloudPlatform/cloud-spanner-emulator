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
#include <optional>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "google/spanner/v1/commit_response.pb.h"
#include "google/spanner/v1/mutation.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "google/cloud/spanner/timestamp.h"
#include "tests/conformance/common/database_test_base.h"
#include "grpcpp/client_context.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using ::absl::StatusCode;
using ::google::spanner::admin::database::v1::DatabaseDialect;
using ::googlesql_base::testing::StatusIs;

class QueueTest : public DatabaseTest,
                  public ::testing::WithParamInterface<DatabaseDialect> {
 public:
  void SetUp() override {
    dialect_ = GetParam();
    DatabaseTest::SetUp();
  }

  absl::Status SetUpDatabase() override {
    if (GetParam() == DatabaseDialect::POSTGRESQL) {
      GOOGLESQL_RETURN_IF_ERROR(SetSchema({
          R"sql(
            CREATE TABLE parent_table (
              parent_id bigint NOT NULL,
              PRIMARY KEY (parent_id)
            )
          )sql",
          R"sql(
            CREATE QUEUE test_queue (
              id bigint NOT NULL,
              payload bytea NOT NULL,
              PRIMARY KEY (id)
            ) WITH (
              receive_mode = 'PULL'
            )
          )sql",
          R"sql(
            CREATE QUEUE child_queue (
              parent_id bigint NOT NULL,
              msg_id bigint NOT NULL,
              commit_ts spanner.commit_timestamp NOT NULL,
              payload varchar NOT NULL,
              PRIMARY KEY (parent_id, msg_id, commit_ts)
            ) INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
              TTL INTERVAL '1 day' ON commit_ts
          )sql",
      }));
    } else {
      GOOGLESQL_RETURN_IF_ERROR(SetSchema({
          R"sql(
            CREATE TABLE ParentTable (
              ParentId INT64 NOT NULL,
            ) PRIMARY KEY (ParentId)
          )sql",
          R"sql(
            CREATE QUEUE TestQueue (
              Id INT64 NOT NULL,
              Payload BYTES(MAX) NOT NULL,
            ) PRIMARY KEY (Id),
              OPTIONS (receive_mode = 'PULL')
          )sql",
          R"sql(
            CREATE QUEUE ChildQueue (
              ParentId INT64 NOT NULL,
              MsgId INT64 NOT NULL,
              CommitTs TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
              Payload STRING(MAX) NOT NULL,
            ) PRIMARY KEY (ParentId, MsgId, CommitTs),
              INTERLEAVE IN PARENT ParentTable ON DELETE CASCADE,
              ROW DELETION POLICY (OLDER_THAN(CommitTs, INTERVAL 1 DAY))
          )sql",
      }));
    }

    grpc::ClientContext context;
    spanner_api::CreateSessionRequest request;
    request.set_database(database()->FullName());
    spanner_api::Session response;
    GOOGLESQL_RETURN_IF_ERROR(raw_client()->CreateSession(&context, request, &response));
    session_name_ = response.name();
    return absl::OkStatus();
  }

 protected:
  absl::Status Send(absl::string_view queue, int64_t id,
                    absl::string_view payload_bytes,
                    std::optional<absl::Time> deliver_time = std::nullopt) {
    spanner_api::CommitRequest request;
    request.set_session(session_name_);
    request.mutable_single_use_transaction()->mutable_read_write();
    auto* send = request.add_mutations()->mutable_send();
    send->set_queue(queue);
    send->mutable_key()->add_values()->set_string_value(absl::StrCat(id));
    send->mutable_payload()->set_string_value(
        absl::Base64Escape(payload_bytes));
    if (deliver_time.has_value()) {
      send->mutable_deliver_time()->set_seconds(
          absl::ToUnixSeconds(*deliver_time));
      send->mutable_deliver_time()->set_nanos(static_cast<int32_t>(
          (*deliver_time -
           absl::FromUnixSeconds(absl::ToUnixSeconds(*deliver_time))) /
          absl::Nanoseconds(1)));
    }
    spanner_api::CommitResponse response;
    grpc::ClientContext context;
    return raw_client()->Commit(&context, request, &response);
  }

  absl::Status Ack(absl::string_view queue, int64_t id,
                   bool ignore_not_found = false) {
    spanner_api::CommitRequest request;
    request.set_session(session_name_);
    request.mutable_single_use_transaction()->mutable_read_write();
    auto* ack = request.add_mutations()->mutable_ack();
    ack->set_queue(queue);
    ack->mutable_key()->add_values()->set_string_value(absl::StrCat(id));
    ack->set_ignore_not_found(ignore_not_found);
    spanner_api::CommitResponse response;
    grpc::ClientContext context;
    return raw_client()->Commit(&context, request, &response);
  }

  std::string session_name_;
};

INSTANTIATE_TEST_SUITE_P(
    PerDialectQueueTests, QueueTest,
    testing::Values(DatabaseDialect::GOOGLE_STANDARD_SQL,
                    DatabaseDialect::POSTGRESQL),
    [](const testing::TestParamInfo<QueueTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(QueueTest, AlterAndDropQueueDdlLifecycle) {
  if (GetParam() == DatabaseDialect::POSTGRESQL) {
    GOOGLESQL_EXPECT_OK(UpdateSchema({
        "ALTER QUEUE test_queue SET OPTIONS (disable_delivery = true)",
    }));

    // Dropping a parent table with an interleaved queue must fail.
    EXPECT_THAT(UpdateSchema({"DROP TABLE parent_table"}),
                StatusIs(StatusCode::kFailedPrecondition));

    // After dropping the interleaved queue, dropping the parent table succeeds.
    GOOGLESQL_EXPECT_OK(UpdateSchema({
        "DROP QUEUE child_queue",
        "DROP TABLE parent_table",
    }));
  } else {
    GOOGLESQL_EXPECT_OK(UpdateSchema({
        "ALTER QUEUE ChildQueue SET ON DELETE NO ACTION",
        "ALTER QUEUE ChildQueue REPLACE ROW DELETION POLICY "
        "(OLDER_THAN(CommitTs, INTERVAL 2 DAY))",
        "ALTER QUEUE ChildQueue DROP ROW DELETION POLICY",
        "ALTER QUEUE ChildQueue ADD ROW DELETION POLICY "
        "(OLDER_THAN(CommitTs, INTERVAL 3 DAY))",
        "ALTER QUEUE TestQueue SET OPTIONS (disable_delivery = true)",
    }));

    // Dropping a parent table with an interleaved queue must fail.
    EXPECT_THAT(UpdateSchema({"DROP TABLE ParentTable"}),
                StatusIs(StatusCode::kFailedPrecondition));

    // After dropping the interleaved queue, dropping the parent table succeeds.
    GOOGLESQL_EXPECT_OK(UpdateSchema({
        "DROP QUEUE ChildQueue",
        "DROP TABLE ParentTable",
    }));
  }
}

TEST_P(QueueTest, InsertSelectAndDeleteUsingDml) {
  if (GetParam() == DatabaseDialect::POSTGRESQL) {
    GOOGLESQL_ASSERT_OK(CommitDml({
        SqlStatement("INSERT INTO test_queue (id, payload) VALUES "
                     "(1, 'msg-1'::bytea), (2, 'msg-2'::bytea)"),
    }));

    EXPECT_THAT(
        Query("SELECT id, payload, deliver_time IS NOT NULL FROM test_queue "
              "WHERE \"DeliverTime\" IS NOT NULL ORDER BY id"),
        IsOkAndHoldsRows({
            {1, Bytes(std::string("msg-1")), true},
            {2, Bytes(std::string("msg-2")), true},
        }));

    // Verify Read API accepts both snake_case deliver_time and DeliverTime.
    GOOGLESQL_EXPECT_OK(
        Read("test_queue", {"id", "payload", "deliver_time"}, KeySet::All())
            .status());
    GOOGLESQL_EXPECT_OK(
        Read("test_queue", {"id", "payload", "DeliverTime"}, KeySet::All())
            .status());

    GOOGLESQL_ASSERT_OK(CommitDml({
        SqlStatement("DELETE FROM test_queue WHERE id = 1"),
    }));

    EXPECT_THAT(Query("SELECT id, payload FROM test_queue ORDER BY id"),
                IsOkAndHoldsRows({
                    {2, Bytes(std::string("msg-2"))},
                }));
  } else {
    GOOGLESQL_ASSERT_OK(CommitDml({
        SqlStatement("INSERT INTO TestQueue (Id, Payload) VALUES "
                     "(1, b'msg-1'), (2, b'msg-2')"),
    }));

    EXPECT_THAT(
        Query("SELECT Id, Payload, DeliverTime IS NOT NULL FROM TestQueue "
              "ORDER BY Id"),
        IsOkAndHoldsRows({
            {1, Bytes(std::string("msg-1")), true},
            {2, Bytes(std::string("msg-2")), true},
        }));

    GOOGLESQL_EXPECT_OK(Read("TestQueue", {"Id", "Payload", "DeliverTime"}, KeySet::All())
                  .status());

    GOOGLESQL_ASSERT_OK(CommitDml({
        SqlStatement("DELETE FROM TestQueue WHERE Id = 1"),
    }));

    EXPECT_THAT(Query("SELECT Id, Payload FROM TestQueue ORDER BY Id"),
                IsOkAndHoldsRows({
                    {2, Bytes(std::string("msg-2"))},
                }));
  }
}

TEST_P(QueueTest, SendAndAckMutations) {
  const std::string queue_name =
      GetParam() == DatabaseDialect::POSTGRESQL ? "test_queue" : "TestQueue";
  const std::string id_col =
      GetParam() == DatabaseDialect::POSTGRESQL ? "id" : "Id";
  const std::string payload_col =
      GetParam() == DatabaseDialect::POSTGRESQL ? "payload" : "Payload";

  GOOGLESQL_ASSERT_OK(Send(queue_name, 10, "alpha"));
  GOOGLESQL_ASSERT_OK(Send(queue_name, 20, "beta"));

  EXPECT_THAT(Query("SELECT " + id_col + ", " + payload_col + " FROM " +
                    queue_name + " ORDER BY " + id_col),
              IsOkAndHoldsRows({
                  {10, Bytes(std::string("alpha"))},
                  {20, Bytes(std::string("beta"))},
              }));

  // Acking an existing message succeeds.
  GOOGLESQL_ASSERT_OK(Ack(queue_name, 10));
  EXPECT_THAT(Query("SELECT " + id_col + ", " + payload_col + " FROM " +
                    queue_name + " ORDER BY " + id_col),
              IsOkAndHoldsRows({
                  {20, Bytes(std::string("beta"))},
              }));

  // Acking a non-existent message fails with NOT_FOUND by default.
  EXPECT_THAT(Ack(queue_name, 10), StatusIs(StatusCode::kNotFound));

  // Acking a non-existent message with ignore_not_found = true succeeds.
  GOOGLESQL_EXPECT_OK(Ack(queue_name, 10, /*ignore_not_found=*/true));
}

TEST_P(QueueTest, CannotUseInsertUpdateDeleteOnQueue) {
  const std::string queue_name =
      GetParam() == DatabaseDialect::POSTGRESQL ? "test_queue" : "TestQueue";
  const std::string id_col =
      GetParam() == DatabaseDialect::POSTGRESQL ? "id" : "Id";
  const std::string payload_col =
      GetParam() == DatabaseDialect::POSTGRESQL ? "payload" : "Payload";

  EXPECT_THAT(Insert(queue_name, {id_col, payload_col}, {1, Bytes("payload")}),
              StatusIs(StatusCode::kNotFound));

  GOOGLESQL_ASSERT_OK(Send(queue_name, 1, "payload"));

  EXPECT_THAT(Update(queue_name, {id_col, payload_col}, {1, Bytes("updated")}),
              StatusIs(StatusCode::kNotFound));
  EXPECT_THAT(
      InsertOrUpdate(queue_name, {id_col, payload_col}, {1, Bytes("updated")}),
      StatusIs(StatusCode::kNotFound));
  EXPECT_THAT(Replace(queue_name, {id_col, payload_col}, {1, Bytes("updated")}),
              StatusIs(StatusCode::kNotFound));
  EXPECT_THAT(Delete(queue_name, Key(1)), StatusIs(StatusCode::kNotFound));

  // DML UPDATE on a queue must fail with INVALID_ARGUMENT.
  if (GetParam() == DatabaseDialect::POSTGRESQL) {
    EXPECT_THAT(
        CommitDml({SqlStatement(
            "UPDATE test_queue SET payload = 'updated'::bytea WHERE id = 1")}),
        StatusIs(StatusCode::kInvalidArgument));
  } else {
    EXPECT_THAT(CommitDml({SqlStatement(
                    "UPDATE TestQueue SET Payload = b'updated' WHERE Id = 1")}),
                StatusIs(StatusCode::kInvalidArgument));
  }
}

TEST_P(QueueTest, ReceiveAndRenewLeaseTvfs) {
  const std::string queue_name =
      GetParam() == DatabaseDialect::POSTGRESQL ? "test_queue" : "TestQueue";
  GOOGLESQL_ASSERT_OK(Send(queue_name, 1, "task-1"));
  GOOGLESQL_ASSERT_OK(Send(queue_name, 2, "task-2"));
  // Future-dated message must be filtered out by RECEIVE_<queue>.
  GOOGLESQL_ASSERT_OK(Send(queue_name, 3, "future-task", absl::Now() + absl::Hours(1)));

  std::string receive_sql = GetParam() == DatabaseDialect::POSTGRESQL ? R"sql(
        SELECT
          id,
          payload,
          deliver_time,
          spanner_lease_expiration_timestamp,
          spanner_lease_token,
          spanner_last_batch_message
        FROM spanner.receive_test_queue(10, 1, '1s')
      )sql"
                                                                      : R"sql(
        SELECT
          Id,
          Payload,
          DeliverTime,
          SpannerLeaseExpirationTimestamp,
          SpannerLeaseToken,
          SpannerLastBatchMessage
        FROM RECEIVE_TestQueue(max_batch_size => 10, max_duration => '1s')
      )sql";

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> received_rows, Query(receive_sql));

  ASSERT_EQ(received_rows.size(), 2);
  EXPECT_EQ(received_rows[0].values()[0], Value(int64_t{1}));
  EXPECT_EQ(received_rows[0].values()[1], Value(Bytes(std::string("task-1"))));
  ASSERT_TRUE(
      received_rows[0].values()[2].get<cloud::spanner::Timestamp>().ok());
  cloud::spanner::Timestamp deliver_time_0 =
      received_rows[0].values()[2].get<cloud::spanner::Timestamp>().value();
  ASSERT_TRUE(
      received_rows[0].values()[3].get<cloud::spanner::Timestamp>().ok());
  cloud::spanner::Timestamp lease_exp_0 =
      received_rows[0].values()[3].get<cloud::spanner::Timestamp>().value();
  EXPECT_GT(lease_exp_0, deliver_time_0);
  ASSERT_TRUE(received_rows[0].values()[4].get<std::string>().ok());
  std::string token_0 = received_rows[0].values()[4].get<std::string>().value();
  EXPECT_FALSE(token_0.empty());
  EXPECT_EQ(received_rows[0].values()[5], Value(false));

  EXPECT_EQ(received_rows[1].values()[0], Value(int64_t{2}));
  EXPECT_EQ(received_rows[1].values()[1], Value(Bytes(std::string("task-2"))));
  ASSERT_TRUE(
      received_rows[1].values()[2].get<cloud::spanner::Timestamp>().ok());
  cloud::spanner::Timestamp deliver_time_1 =
      received_rows[1].values()[2].get<cloud::spanner::Timestamp>().value();
  ASSERT_TRUE(
      received_rows[1].values()[3].get<cloud::spanner::Timestamp>().ok());
  cloud::spanner::Timestamp lease_exp_1 =
      received_rows[1].values()[3].get<cloud::spanner::Timestamp>().value();
  EXPECT_GT(lease_exp_1, deliver_time_1);
  ASSERT_TRUE(received_rows[1].values()[4].get<std::string>().ok());
  std::string token_1 = received_rows[1].values()[4].get<std::string>().value();
  EXPECT_FALSE(token_1.empty());
  EXPECT_EQ(received_rows[1].values()[5], Value(true));

  std::string renew_sql =
      GetParam() == DatabaseDialect::POSTGRESQL
          ? absl::StrCat(
                "SELECT spanner_old_lease_token, "
                "spanner_new_lease_token, "
                "spanner_lease_expiration_timestamp "
                "FROM spanner.renewlease_test_queue(ARRAY['",
                token_0, "'])")
          : absl::StrCat(
                "SELECT SpannerOldLeaseToken, SpannerNewLeaseToken, "
                "SpannerLeaseExpirationTimestamp "
                "FROM RENEWLEASE_TestQueue(lease_tokens => ['",
                token_0, "'])");

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<ValueRow> renew_rows, Query(renew_sql));
  ASSERT_EQ(renew_rows.size(), 1);
  EXPECT_EQ(renew_rows[0].values()[0], Value(token_0));
  ASSERT_TRUE(renew_rows[0].values()[1].get<std::string>().ok());
  std::string new_token = renew_rows[0].values()[1].get<std::string>().value();
  EXPECT_FALSE(new_token.empty());
  EXPECT_NE(new_token, token_0);
  EXPECT_TRUE(renew_rows[0].values()[2].get<cloud::spanner::Timestamp>().ok());

  // Verify QueueTVFRule rejects combining Queue TVFs with ORDER BY / WHERE.
  EXPECT_THAT(
      Query(GetParam() == DatabaseDialect::POSTGRESQL
                ? "SELECT id FROM spanner.receive_test_queue(NULL, NULL, NULL) "
                  "ORDER BY id"
                : "SELECT Id FROM RECEIVE_TestQueue() ORDER BY Id"),
      StatusIs(testing::AnyOf(StatusCode::kInvalidArgument,
                              StatusCode::kUnimplemented)));

  // Acknowledge messages 1 and 2 via Ack mutation, send message 4, and verify
  // RECEIVE returns only message 4 (and still filters out future message 3).
  GOOGLESQL_ASSERT_OK(Ack(queue_name, 1));
  GOOGLESQL_ASSERT_OK(Ack(queue_name, 2));
  GOOGLESQL_ASSERT_OK(Send(queue_name, 4, "task-4"));
  EXPECT_THAT(
      Query(GetParam() == DatabaseDialect::POSTGRESQL
                ? "SELECT id, payload FROM spanner.receive_test_queue(10, 1, "
                  "'1s')"
                : "SELECT Id, Payload FROM RECEIVE_TestQueue(max_batch_size => "
                  "10, max_duration => '1s')"),
      IsOkAndHoldsRows({
          {4, Bytes(std::string("task-4"))},
      }));

  // Acknowledge message 4, send message 5 with a near-future deliver_time
  // (300ms from now), and verify that long-running RECEIVE_ waits and yields
  // message 5 once its deliver_time is reached within max_duration ('1s').
  GOOGLESQL_ASSERT_OK(Ack(queue_name, 4));
  GOOGLESQL_ASSERT_OK(Send(queue_name, 5, "delayed-task",
                 absl::Now() + absl::Milliseconds(300)));
  EXPECT_THAT(
      Query(GetParam() == DatabaseDialect::POSTGRESQL
                ? "SELECT id, payload, spanner_last_batch_message FROM "
                  "spanner.receive_test_queue(10, 1, '1s')"
                : "SELECT Id, Payload, SpannerLastBatchMessage FROM "
                  "RECEIVE_TestQueue(max_batch_size => 10, max_duration => "
                  "'1s')"),
      IsOkAndHoldsRows({
          {5, Bytes(std::string("delayed-task")), true},
      }));
}

}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
