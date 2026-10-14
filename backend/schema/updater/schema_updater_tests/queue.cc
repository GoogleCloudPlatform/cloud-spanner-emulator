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

#include "backend/schema/catalog/queue.h"

#include <memory>
#include <string>
#include <vector>

#include "google/spanner/admin/database/v1/common.pb.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "googlesql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/schema.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/printer/print_ddl.h"
#include "backend/schema/updater/schema_updater_tests/base.h"
#include "common/errors.h"
#include "tests/common/proto_matchers.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace test {

using database_api::DatabaseDialect::GOOGLE_STANDARD_SQL;
using database_api::DatabaseDialect::POSTGRESQL;
using ::google::spanner::emulator::test::EqualsProto;

class QueueSchemaUpdaterTest : public SchemaUpdaterTest {
 public:
  QueueSchemaUpdaterTest() = default;
};

INSTANTIATE_TEST_SUITE_P(
    QueueSchemaUpdaterPerDialectTests, QueueSchemaUpdaterTest,
    testing::Values(GOOGLE_STANDARD_SQL, POSTGRESQL),
    [](const testing::TestParamInfo<QueueSchemaUpdaterTest::ParamType>& info) {
      return database_api::DatabaseDialect_Name(info.param);
    });

TEST_P(QueueSchemaUpdaterTest, CreateQueue_Basic) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id)
    )"}));

  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->Name(), "myqueue");

  // Case-sensitive lookup should succeed with exact case.
  const Queue* queue_case_sensitive = schema->FindQueueCaseSensitive("myqueue");
  ASSERT_NE(queue_case_sensitive, nullptr);
  EXPECT_EQ(queue_case_sensitive->Name(), "myqueue");

  // Case-sensitive lookup should fail with different case.
  const Queue* queue_wrong_case = schema->FindQueueCaseSensitive("MYQUEUE");
  EXPECT_EQ(queue_wrong_case, nullptr);

  const Column* id_col = queue->FindColumn("id");
  ASSERT_NE(id_col, nullptr);
  EXPECT_TRUE(id_col->GetType()->IsInt64());
  EXPECT_FALSE(id_col->is_nullable());

  const Column* payload_col = queue->FindColumn("Payload");
  ASSERT_NE(payload_col, nullptr);
  EXPECT_TRUE(payload_col->GetType()->IsBytes());
  EXPECT_FALSE(payload_col->is_nullable());

  EXPECT_EQ(queue->primary_key().size(), 1);
  EXPECT_EQ(queue->primary_key()[0]->column()->Name(), "id");

  if (GetParam() == GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        schema->Dump(), EqualsProto(R"pb(
          statement {
            create_queue {
              queue_name: "myqueue"
              column { column_name: "id" type: INT64 not_null: true }
              column { column_name: "Payload" type: BYTES not_null: true }
              primary_key { key_name: "id" }
            }
          }
        )pb"));
  }
}

TEST_P(QueueSchemaUpdaterTest, CreateQueue_WithInterleave) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE TABLE parent_table (
        parent_id INT64 NOT NULL,
      ) PRIMARY KEY (parent_id)
    )",
                                                                           R"(
      CREATE QUEUE myqueue (
        parent_id INT64 NOT NULL,
        queue_id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (parent_id, queue_id),
      INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    )"}));

  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->parent()->Name(), "parent_table");
  EXPECT_EQ(queue->on_delete_action(), Table::OnDeleteAction::kCascade);

  const Table* parent = schema->FindTable("parent_table");
  ASSERT_NE(parent, nullptr);
  EXPECT_EQ(parent->child_queues().size(), 1);
  EXPECT_EQ(parent->child_queues()[0], queue);

  if (GetParam() == GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        schema->Dump(), EqualsProto(R"pb(
          statement {
            create_table {
              table_name: "parent_table"
              column { column_name: "parent_id" type: INT64 not_null: true }
              primary_key { key_name: "parent_id" }
            }
          }
          statement {
            create_queue {
              queue_name: "myqueue"
              column { column_name: "parent_id" type: INT64 not_null: true }
              column { column_name: "queue_id" type: INT64 not_null: true }
              column { column_name: "Payload" type: BYTES not_null: true }
              primary_key { key_name: "parent_id" }
              primary_key { key_name: "queue_id" }
              interleave_clause {
                table_name: "parent_table"
                type: IN_PARENT
                on_delete: CASCADE
              }
            }
          }
        )pb"));
  }
}

TEST_P(QueueSchemaUpdaterTest, CreateQueue_WithOptions) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id),
      OPTIONS (
        receive_mode = 'PULL'
      )
    )"}));

  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  ASSERT_EQ(queue->options().size(), 1);
  EXPECT_EQ(queue->options()[0].option_name(), "receive_mode");
  EXPECT_EQ(queue->options()[0].string_value(), "PULL");

  if (GetParam() == GOOGLE_STANDARD_SQL) {
    EXPECT_THAT(
        schema->Dump(), EqualsProto(R"pb(
          statement {
            create_queue {
              queue_name: "myqueue"
              column { column_name: "id" type: INT64 not_null: true }
              column { column_name: "Payload" type: BYTES not_null: true }
              primary_key { key_name: "id" }
              set_options { option_name: "receive_mode" string_value: "PULL" }
            }
          }
        )pb"));
  }
}

TEST_P(QueueSchemaUpdaterTest, CreateQueue_WithColumnLength) {
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP() << "Queues are not supported in PostgreSQL";
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(100) NOT NULL
      ) PRIMARY KEY (id)
    )"}));

  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  const Column* payload_col = queue->FindColumn("Payload");
  ASSERT_NE(payload_col, nullptr);
  EXPECT_EQ(payload_col->declared_max_length(), 100);

  const Table* backing_table = queue->backing_table();
  ASSERT_NE(backing_table, nullptr);
  const Column* backing_payload_col = backing_table->FindColumn("Payload");
  ASSERT_NE(backing_payload_col, nullptr);
  EXPECT_EQ(backing_payload_col->declared_max_length(), 100);
}

TEST_P(QueueSchemaUpdaterTest, CreateQueue_InterleavedInMissingTable_Fails) {
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP() << "Queues are not supported in PostgreSQL";
  }

  EXPECT_THAT(CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id),
      INTERLEAVE IN PARENT non_existent_table
    )"}),
              StatusIs(error::TableNotFound("non_existent_table")));
}

TEST_P(QueueSchemaUpdaterTest, DropQueue_Basic) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id)
    )"}));

  ASSERT_NE(schema->FindQueue("myqueue"), nullptr);

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      DROP QUEUE myqueue
    )"}));

  EXPECT_EQ(schema->FindQueue("myqueue"), nullptr);
}

TEST_P(QueueSchemaUpdaterTest, DropTable_WithInterleavedQueue_Fails) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE TABLE parent_table (
        parent_id INT64 NOT NULL,
      ) PRIMARY KEY (parent_id)
    )",
                                                                           R"(
      CREATE QUEUE myqueue (
        parent_id INT64 NOT NULL,
        queue_id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (parent_id, queue_id),
      INTERLEAVE IN PARENT parent_table
    )"}));

  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      DROP TABLE parent_table
    )"}),
              StatusIs(error::DropTableWithInterleavedQueues("parent_table",
                                                             "myqueue")));
}

TEST_P(QueueSchemaUpdaterTest, QueueBackingTable) {
  if (GetParam() == POSTGRESQL) {
    GTEST_SKIP() << "Queues are not supported in PostgreSQL";
  }

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE TABLE parent_table (
        parent_id INT64 NOT NULL,
      ) PRIMARY KEY (parent_id)
    )",
                                                                           R"(
      CREATE QUEUE myqueue (
        parent_id INT64 NOT NULL,
        queue_id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (parent_id, queue_id),
      INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    )"}));

  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);

  // Verify FindTable does not return the queue's backing table.
  EXPECT_EQ(schema->FindTable("myqueue"), nullptr);
  const Table* backing_table = queue->backing_table();
  ASSERT_NE(backing_table, nullptr);
  EXPECT_EQ(backing_table->Name(), "myqueue");

  // Verify table DDL statements do not find the queue as a table.
  EXPECT_THAT(UpdateSchema(schema.get(), {"DROP TABLE myqueue"}),
              StatusIs(error::TableNotFound("myqueue")));
  EXPECT_THAT(
      UpdateSchema(schema.get(), {"ALTER TABLE myqueue ADD COLUMN c INT64"}),
      StatusIs(error::TableNotFound("myqueue")));

  // Verify backing table is NOT public.
  EXPECT_FALSE(backing_table->is_public());
  EXPECT_EQ(backing_table->owner_queue(), queue);

  // Verify columns.
  const Column* parent_id_col = backing_table->FindColumn("parent_id");
  ASSERT_NE(parent_id_col, nullptr);
  EXPECT_TRUE(parent_id_col->GetType()->IsInt64());
  EXPECT_EQ(parent_id_col->table(), backing_table);

  const Column* queue_id_col = backing_table->FindColumn("queue_id");
  ASSERT_NE(queue_id_col, nullptr);
  EXPECT_TRUE(queue_id_col->GetType()->IsInt64());
  EXPECT_EQ(queue_id_col->table(), backing_table);

  const Column* payload_col = backing_table->FindColumn("Payload");
  ASSERT_NE(payload_col, nullptr);
  EXPECT_TRUE(payload_col->GetType()->IsBytes());
  EXPECT_EQ(payload_col->table(), backing_table);

  // Verify PKs.
  ASSERT_EQ(backing_table->primary_key().size(), 2);
  EXPECT_EQ(backing_table->primary_key()[0]->column()->Name(), "parent_id");
  EXPECT_EQ(backing_table->primary_key()[1]->column()->Name(), "queue_id");

  // Verify interleave.
  const Table* parent = schema->FindTable("parent_table");
  ASSERT_NE(parent, nullptr);
  EXPECT_EQ(backing_table->parent(), parent);
  EXPECT_EQ(backing_table->on_delete_action(), Table::OnDeleteAction::kCascade);
  EXPECT_THAT(parent->children(), testing::Contains(backing_table));

  // Dropping the queue also drops its backing table from the schema graph and
  // parent table's children, allowing the parent table to be dropped and the
  // name to be reused.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      DROP QUEUE myqueue
    )"}));
  EXPECT_EQ(schema->FindQueue("myqueue"), nullptr);
  parent = schema->FindTable("parent_table");
  ASSERT_NE(parent, nullptr);
  EXPECT_THAT(parent->children(), testing::IsEmpty());
  EXPECT_THAT(parent->child_queues(), testing::IsEmpty());
  for (const SchemaNode* node : schema->GetSchemaGraph()->GetSchemaNodes()) {
    if (const Table* t = node->As<const Table>()) {
      EXPECT_EQ(t->owner_queue(), nullptr);
    }
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      DROP TABLE parent_table
    )",
                                                           R"(
      CREATE TABLE myqueue (
        id INT64 NOT NULL,
      ) PRIMARY KEY (id)
    )"}));
  EXPECT_NE(schema->FindTable("myqueue"), nullptr);
}

TEST_P(QueueSchemaUpdaterTest, DropNonExistentQueue_Fails) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id)
    )"}));

  absl::Status status =
      UpdateSchema(schema.get(), {"DROP QUEUE non_existent_queue"}).status();
  EXPECT_EQ(status.code(), absl::StatusCode::kNotFound);
  EXPECT_THAT(status.message(),
              testing::HasSubstr("Queue not found: non_existent_queue"));
}

TEST_P(QueueSchemaUpdaterTest, DropQueueIfExists_NonExistentQueue_Succeeds) {
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id)
    )"}));

  GOOGLESQL_EXPECT_OK(
      UpdateSchema(schema.get(), {"DROP QUEUE IF EXISTS non_existent_queue"}));
}

TEST_P(QueueSchemaUpdaterTest, CreateQueue_WithDescendingKey) {
  if (GetParam() != GOOGLE_STANDARD_SQL) {
    return;
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id DESC)
    )"}));

  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->primary_key().size(), 1);
  EXPECT_TRUE(queue->primary_key()[0]->is_descending());
}

TEST_P(QueueSchemaUpdaterTest, AlterQueue_SetOptions) {
  if (GetParam() != GOOGLE_STANDARD_SQL) {
    return;
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id),
      OPTIONS (receive_mode = 'PULL')
    )"}));

  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET OPTIONS (
        disable_delivery = true,
        disable_send = false
      )
    )"}));
  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->options().size(), 3);

  // Clear an option with NULL.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET OPTIONS (disable_delivery = NULL)
    )"}));
  queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->options().size(), 2);

  // Invalid receive_mode fails.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET OPTIONS (receive_mode = 'PUSH')
    )"}),
              StatusIs(error::InvalidQueueReceiveMode("myqueue", "PUSH")));

  // Non-existent queue fails.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER QUEUE non_existent_queue SET OPTIONS (disable_delivery = true)
    )"}),
              StatusIs(error::QueueNotFound("non_existent_queue")));
}

TEST_P(QueueSchemaUpdaterTest, AlterQueue_RowDeletionPolicy) {
  if (GetParam() != GOOGLE_STANDARD_SQL) {
    return;
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        CreateTime TIMESTAMP NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id, CreateTime)
    )"}));

  // Add row deletion policy.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue ADD ROW DELETION POLICY (
        OLDER_THAN(CreateTime, INTERVAL 2 DAY)
      )
    )"}));
  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  ASSERT_TRUE(queue->row_deletion_policy().has_value());
  EXPECT_EQ(queue->row_deletion_policy()->column_name(), "CreateTime");
  EXPECT_EQ(queue->row_deletion_policy()->older_than().count(), 2);

  // Adding existing row deletion policy fails.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue ADD ROW DELETION POLICY (
        OLDER_THAN(CreateTime, INTERVAL 3 DAY)
      )
    )"}),
      StatusIs(error::RowDeletionPolicyAlreadyExists("CreateTime", "myqueue")));

  // Replace row deletion policy.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue REPLACE ROW DELETION POLICY (
        OLDER_THAN(CreateTime, INTERVAL 7 DAY)
      )
    )"}));
  queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  ASSERT_TRUE(queue->row_deletion_policy().has_value());
  EXPECT_EQ(queue->row_deletion_policy()->older_than().count(), 7);

  // Drop row deletion policy.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue DROP ROW DELETION POLICY
    )"}));
  queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_FALSE(queue->row_deletion_policy().has_value());

  // Dropping non-existent row deletion policy fails.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue DROP ROW DELETION POLICY
    )"}),
              StatusIs(error::RowDeletionPolicyDoesNotExist("myqueue")));

  // Row deletion policy on non-existent column fails.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue ADD ROW DELETION POLICY (
        OLDER_THAN(non_existent_col, INTERVAL 2 DAY)
      )
    )"}),
              StatusIs(error::RowDeletionPolicyOnColumnDoesNotExist(
                  "non_existent_col", "myqueue")));
}

TEST_P(QueueSchemaUpdaterTest, AlterQueue_Interleave) {
  if (GetParam() != GOOGLE_STANDARD_SQL) {
    return;
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE TABLE parent_table (
        parent_id INT64 NOT NULL,
      ) PRIMARY KEY (parent_id)
    )",
                                                                           R"(
      CREATE QUEUE myqueue (
        parent_id INT64 NOT NULL,
        queue_id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (parent_id, queue_id),
      INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
    )"}));

  // Alter interleave on delete action via SET INTERLEAVE.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET INTERLEAVE IN PARENT parent_table ON DELETE CASCADE
    )"}));
  const Queue* queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->on_delete_action(), Table::OnDeleteAction::kCascade);

  // Alter interleave on delete action via SET ON DELETE.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET ON DELETE NO ACTION
    )"}));
  queue = schema->FindQueue("myqueue");
  ASSERT_NE(queue, nullptr);
  EXPECT_EQ(queue->on_delete_action(), Table::OnDeleteAction::kNoAction);

  // Altering to different parent table fails.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET INTERLEAVE IN PARENT other_table ON DELETE CASCADE
    )"}),
              StatusIs(error::ChangeInterleavingTableNotAllowed("myqueue")));

  // Setting ON DELETE on non-interleaved queue fails.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      CREATE QUEUE standalone_queue (
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id)
    )"}));
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER QUEUE standalone_queue SET ON DELETE CASCADE
    )"}),
      StatusIs(error::SetOnDeleteWithoutInterleaving("standalone_queue")));
}

TEST_P(QueueSchemaUpdaterTest, CreateQueue_DuplicateColumn_Fails) {
  EXPECT_THAT(CreateSchema({R"(
      CREATE QUEUE myqueue (
        id INT64 NOT NULL,
        id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (id)
    )"}),
              StatusIs(error::DuplicateColumnName("myqueue.id")));
}

TEST_P(QueueSchemaUpdaterTest, InterleaveQueue_WithParentRowDeletionPolicy) {
  if (GetParam() != GOOGLE_STANDARD_SQL) {
    return;
  }
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
      CREATE TABLE parent_table (
        parent_id INT64 NOT NULL,
        CreateTime TIMESTAMP NOT NULL,
      ) PRIMARY KEY (parent_id)
    )",
                                                                           R"(
      CREATE QUEUE myqueue (
        parent_id INT64 NOT NULL,
        queue_id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (parent_id, queue_id),
      INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
    )"}));

  // Adding row deletion policy on parent_table fails when child queue has
  // ON DELETE NO ACTION.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      ALTER TABLE parent_table ADD ROW DELETION POLICY (
        OLDER_THAN(CreateTime, INTERVAL 2 DAY)
      )
    )"}),
              StatusIs(error::RowDeletionPolicyHasChildWithOnDeleteNoAction(
                  "parent_table", "myqueue")));

  // Updating child queue to ON DELETE CASCADE allows parent row deletion
  // policy.
  GOOGLESQL_ASSERT_OK_AND_ASSIGN(schema, UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET ON DELETE CASCADE
    )",
                                                           R"(
      ALTER TABLE parent_table ADD ROW DELETION POLICY (
        OLDER_THAN(CreateTime, INTERVAL 2 DAY)
      )
    )"}));

  // Altering child queue back to ON DELETE NO ACTION fails when parent has
  // row deletion policy.
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET ON DELETE NO ACTION
    )"}),
      StatusIs(error::RowDeletionPolicyOnAncestors("myqueue", "parent_table")));
  EXPECT_THAT(
      UpdateSchema(schema.get(), {R"(
      ALTER QUEUE myqueue SET INTERLEAVE IN PARENT parent_table ON DELETE NO ACTION
    )"}),
      StatusIs(error::RowDeletionPolicyOnAncestors("myqueue", "parent_table")));

  // Creating a new child queue without ON DELETE CASCADE fails when parent has
  // row deletion policy.
  EXPECT_THAT(UpdateSchema(schema.get(), {R"(
      CREATE QUEUE myqueue2 (
        parent_id INT64 NOT NULL,
        queue_id INT64 NOT NULL,
        Payload BYTES(MAX) NOT NULL
      ) PRIMARY KEY (parent_id, queue_id),
      INTERLEAVE IN PARENT parent_table
    )"}),
              StatusIs(error::RowDeletionPolicyOnAncestors("myqueue2",
                                                           "parent_table")));
}

TEST_P(QueueSchemaUpdaterTest, PrintDDLStatements_Queue) {
  if (GetParam() == GOOGLE_STANDARD_SQL) {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
        CREATE TABLE parent_table (
          parent_id INT64 NOT NULL
        ) PRIMARY KEY (parent_id)
      )",
                                                                             R"(
        CREATE QUEUE myqueue (
          parent_id INT64 NOT NULL,
          queue_id INT64 NOT NULL,
          CreateTime TIMESTAMP NOT NULL,
          Payload BYTES(MAX) NOT NULL
        ) PRIMARY KEY (parent_id, queue_id, CreateTime),
        INTERLEAVE IN PARENT parent_table ON DELETE CASCADE,
        ROW DELETION POLICY (OLDER_THAN(CreateTime, INTERVAL 7 DAY)),
        OPTIONS (
          receive_mode = 'PULL'
        )
      )"}));

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> ddl_statements,
                         PrintDDLStatements(schema.get()));
    EXPECT_THAT(
        ddl_statements,
        testing::ElementsAre(
            "CREATE TABLE parent_table (\n"
            "  parent_id INT64 NOT NULL,\n"
            ") PRIMARY KEY(parent_id)",
            "CREATE QUEUE myqueue (\n"
            "  parent_id INT64 NOT NULL,\n"
            "  queue_id INT64 NOT NULL,\n"
            "  CreateTime TIMESTAMP NOT NULL,\n"
            "  Payload BYTES(MAX) NOT NULL,\n"
            ") PRIMARY KEY(parent_id, queue_id, CreateTime),\n"
            "  INTERLEAVE IN PARENT parent_table ON DELETE CASCADE, "
            "ROW DELETION POLICY (OLDER_THAN(CreateTime, INTERVAL 7 DAY)), "
            "OPTIONS ( receive_mode = 'PULL' )"));
  } else {
    GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::unique_ptr<const Schema> schema, CreateSchema({R"(
        CREATE TABLE parent_table (
          parent_id INT64 NOT NULL
        ) PRIMARY KEY (parent_id)
      )",
                                                                             R"(
        CREATE QUEUE myqueue (
          parent_id INT64 NOT NULL,
          queue_id INT64 NOT NULL,
          Payload BYTES(MAX) NOT NULL
        ) PRIMARY KEY (parent_id, queue_id),
        INTERLEAVE IN PARENT parent_table ON DELETE CASCADE,
        OPTIONS (
          receive_mode = 'PULL'
        )
      )"}));

    GOOGLESQL_ASSERT_OK_AND_ASSIGN(std::vector<std::string> ddl_statements,
                         PrintDDLStatements(schema.get()));
    EXPECT_THAT(
        ddl_statements,
        testing::ElementsAre(
            "CREATE TABLE parent_table (\n"
            "  parent_id bigint NOT NULL,\n"
            "  PRIMARY KEY(parent_id)\n"
            ")",
            "CREATE QUEUE myqueue (\n"
            "  parent_id bigint NOT NULL,\n"
            "  queue_id bigint NOT NULL,\n"
            "  \"Payload\" bytea NOT NULL,\n"
            "  PRIMARY KEY(parent_id, queue_id)\n"
            ") INTERLEAVE IN PARENT parent_table ON DELETE CASCADE WITH (\n"
            "  receive_mode = 'PULL'\n"
            ")"));
  }
}

}  // namespace test
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
