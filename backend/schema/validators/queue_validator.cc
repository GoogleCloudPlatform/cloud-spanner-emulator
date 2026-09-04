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

#include "backend/schema/validators/queue_validator.h"

#include <string>

#include "googlesql/public/type.h"
#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/datamodel/types.h"
#include "backend/schema/catalog/column.h"
#include "backend/schema/catalog/table.h"
#include "backend/schema/updater/global_schema_names.h"
#include "backend/schema/updater/schema_validation_context.h"
#include "common/errors.h"
#include "common/limits.h"
#include "googlesql/base/ret_check.h"
#include "googlesql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {

absl::Status CheckKeyPartCompatibility(const Queue* interleaved_queue,
                                       const KeyColumn* parent_key,
                                       const KeyColumn* child_key) {
  const std::string object_type = "Queue";
  const std::string object_name = interleaved_queue->Name();
  const Column* parent_key_col = parent_key->column();
  const Column* child_key_col = child_key->column();

  if (child_key_col->Name() != parent_key_col->Name()) {
    return error::MustReferenceParentKeyColumn(object_type, object_name,
                                               parent_key_col->Name());
  }

  if (child_key->is_descending() != parent_key->is_descending()) {
    return error::IncorrectParentKeyOrder(
        object_type, object_name, parent_key_col->Name(),
        child_key->is_descending() ? "ASC" : "DESC");
  }

  if (!child_key_col->GetType()->Equals(parent_key_col->GetType())) {
    return error::IncorrectParentKeyType(object_type, object_name,
                                         parent_key_col->Name(),
                                         ToString(child_key_col->GetType()),
                                         ToString(parent_key_col->GetType()));
  }

  if (child_key_col->declared_max_length() !=
      parent_key_col->declared_max_length()) {
    auto column_length = [](const Column* column) {
      return column->declared_max_length().has_value()
                 ? absl::StrCat(column->declared_max_length().value())
                 : "MAX";
    };
    return error::IncorrectParentKeyLength(
        object_type, object_name, parent_key_col->Name(),
        column_length(child_key_col), column_length(parent_key_col));
  }

  if (child_key_col->is_nullable() != parent_key_col->is_nullable()) {
    return error::IncorrectParentKeyNullability(
        object_type, object_name, parent_key_col->Name(),
        parent_key_col->is_nullable() ? "nullable" : "not null",
        child_key_col->is_nullable() ? "nullable" : "not null");
  }

  return absl::OkStatus();
}

absl::Status CheckInterleaveDepthLimit(const Queue* queue) {
  int depth = 1;
  const Table* to_test = queue->parent();
  while (to_test) {
    to_test = to_test->parent();
    ++depth;
    if (depth > limits::kMaxInterleavingDepth) {
      return error::DeepNesting("Queue", queue->Name(),
                                limits::kMaxInterleavingDepth);
    }
  }
  return absl::OkStatus();
}

}  // namespace

absl::Status QueueValidator::Validate(const Queue* queue,
                                      SchemaValidationContext* context) {
  GOOGLESQL_RET_CHECK(!queue->Name().empty());
  GOOGLESQL_RET_CHECK(!queue->id().empty());

  GOOGLESQL_RETURN_IF_ERROR(
      GlobalSchemaNames::ValidateSchemaName("Queue", queue->Name()));

  // Validate that all columns are unique.
  CaseInsensitiveStringSet unique_columns;
  for (const Column* column : queue->columns()) {
    GOOGLESQL_RET_CHECK_NE(column, nullptr);
    std::string column_name = column->Name();
    if (!unique_columns.insert(column_name).second) {
      return error::DuplicateColumnName(column->FullName());
    }
  }

  if (queue->columns().size() > limits::kMaxColumnsPerTable) {
    return error::TooManyColumns("Queue", queue->Name(),
                                 limits::kMaxColumnsPerTable);
  }

  // Validate that all key columns are unique.
  CaseInsensitiveStringSet unique_keys;
  for (const KeyColumn* key_column : queue->primary_key()) {
    GOOGLESQL_RET_CHECK_NE(key_column, nullptr);
    const Column* column = key_column->column();
    GOOGLESQL_RET_CHECK_NE(column, nullptr);
    const Column* queue_column = queue->FindColumn(column->Name());
    GOOGLESQL_RET_CHECK_EQ(queue_column, column);
    if (!unique_keys.insert(column->Name()).second) {
      return error::MultipleRefsToKeyColumn("Queue", queue->Name(),
                                            column->Name());
    }
  }

  if (queue->primary_key().empty()) {
    return error::QueueWithoutPrimaryKeys(queue->Name());
  }

  if (queue->primary_key().size() > limits::kMaxKeyColumns) {
    return error::TooManyKeys("Queue", queue->Name(),
                              queue->primary_key().size(),
                              limits::kMaxKeyColumns);
  }

  // Check interleave compatibility.
  if (!queue->parent()) {
    if (queue->has_on_delete_action()) {
      return error::SetOnDeleteWithoutInterleaving(queue->Name());
    }
  } else {
    GOOGLESQL_RET_CHECK(queue->parent()->is_public());
    absl::Span<const KeyColumn* const> parent_pk =
        queue->parent()->primary_key();
    for (int i = 0; i < parent_pk.size(); ++i) {
      if (i >= queue->primary_key().size()) {
        return error::MustReferenceParentKeyColumn(
            "Queue", queue->Name(), parent_pk[i]->column()->Name());
      }
      GOOGLESQL_RETURN_IF_ERROR(CheckKeyPartCompatibility(queue, parent_pk[i],
                                                queue->primary_key()[i]));
    }
    GOOGLESQL_RETURN_IF_ERROR(CheckInterleaveDepthLimit(queue));

    if (queue->columns().empty()) {
      return error::NoColumnsTable("Queue", queue->Name());
    }
  }

  // Check Payload column.
  const Column* payload = queue->FindColumn("Payload");
  if (payload == nullptr) {
    return error::QueueMissingPayloadColumn(queue->Name());
  }
  if (payload->is_nullable()) {
    return error::QueueWithNullablePayloadColumn(queue->Name());
  }
  const googlesql::Type* payload_type = payload->GetType();
  if (!payload_type->IsBytes() && !payload_type->IsProto() &&
      !payload_type->IsString() && !payload_type->IsJson()) {
    return error::QueuePayloadWithWrongType(queue->Name());
  }

  // Check that all columns other than Payload are primary key columns,
  // and that Payload is not part of the primary key.
  if (unique_keys.contains("Payload")) {
    return error::QueueWithExtraColumns(queue->Name());
  }
  for (const Column* column : queue->columns()) {
    if (column == payload) {
      continue;
    }
    if (!unique_keys.contains(column->Name())) {
      return error::QueueWithExtraColumns(queue->Name());
    }
  }

  // Check queue options.
  for (const ddl::SetOption& option : queue->options()) {
    if (option.option_name() == "receive_mode") {
      if (option.has_string_value() &&
          !absl::EqualsIgnoreCase(option.string_value(), "PULL")) {
        return error::InvalidQueueReceiveMode(queue->Name(),
                                              option.string_value());
      }
    }
  }

  return absl::OkStatus();
}

absl::Status QueueValidator::ValidateUpdate(const Queue* queue,
                                            const Queue* old_queue,
                                            SchemaValidationContext* context) {
  if (queue->is_deleted()) {
    context->global_names()->RemoveName(queue->Name());
    return absl::OkStatus();
  }

  GOOGLESQL_RET_CHECK_EQ(queue->id(), old_queue->id());

  // Check additional constraints on new columns.
  for (const Column* column : queue->columns()) {
    if (old_queue->FindColumn(column->Name()) != nullptr) {
      continue;
    }
    if (!column->is_nullable() && !column->is_generated() &&
        !column->has_default_value()) {
      return error::AddingNotNullColumn(queue->Name(), column->Name());
    }
  }

  // Cannot drop key columns, change their order or nullability.
  GOOGLESQL_RET_CHECK_EQ(queue->primary_key().size(), old_queue->primary_key().size());
  for (int i = 0; i < queue->primary_key().size(); ++i) {
    if (queue->primary_key()[i]->is_deleted()) {
      return error::InvalidDropKeyColumn(
          queue->primary_key()[i]->column()->Name(), queue->Name());
    }
    GOOGLESQL_RET_CHECK_EQ(queue->primary_key()[i]->is_descending(),
                 old_queue->primary_key()[i]->is_descending());
    if (queue->primary_key()[i]->column()->is_nullable() !=
        old_queue->primary_key()[i]->column()->is_nullable()) {
      std::string reason = absl::Substitute(
          "from $0 to $1",
          old_queue->primary_key()[i]->column()->is_nullable() ? "NULL"
                                                               : "NOT NULL",
          queue->primary_key()[i]->column()->is_nullable() ? "NULL"
                                                           : "NOT NULL");
      return error::CannotChangeKeyColumn(
          absl::StrCat(queue->Name(), ".",
                       queue->primary_key()[i]->column()->Name()),
          reason);
    }
  }

  return absl::OkStatus();
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
