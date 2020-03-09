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

#include "backend/actions/ops.h"

#include "zetasql/public/value.h"
#include "absl/strings/str_cat.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

namespace {
std::string DebugString(const InsertOp& op) {
  std::string result = "InsertOp:\n";
  absl::StrAppend(&result, "Table: ", op.table->Name(), "\n");
  absl::StrAppend(&result, "Key: ", op.key.DebugString(), "\n");
  absl::StrAppend(&result, "Columns: ", "\n{ ");
  for (int i = 0; i < op.columns.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(&result, ", ");
    }
    absl::StrAppend(&result, op.columns[i]->Name());
  }
  absl::StrAppend(&result, " }\n ");
  absl::StrAppend(&result, "Values: ", "\n{");
  for (int i = 0; i < op.values.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(&result, ", ");
    }
    absl::StrAppend(&result, op.values[i].DebugString());
  }
  absl::StrAppend(&result, " }\n ");

  return result;
}

std::string DebugString(const UpdateOp& op) {
  std::string result = "UpdateOp:\n";
  absl::StrAppend(&result, "Table: ", op.table->Name(), "\n");
  absl::StrAppend(&result, "Key: ", op.key.DebugString(), "\n");
  absl::StrAppend(&result, "Columns: ", "\n{ ");
  for (int i = 0; i < op.columns.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(&result, ", ");
    }
    absl::StrAppend(&result, op.columns[i]->Name());
  }
  absl::StrAppend(&result, " }\n ");
  absl::StrAppend(&result, "Values: ", "\n{");
  for (int i = 0; i < op.values.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(&result, ", ");
    }
    absl::StrAppend(&result, op.values[i].DebugString());
  }
  absl::StrAppend(&result, " }\n ");

  return result;
}

std::string DebugString(const DeleteOp& op) {
  std::string result = "DeleteOp:\n";
  absl::StrAppend(&result, "Table: ", op.table->Name(), "\n");
  absl::StrAppend(&result, "Key: ", op.key.DebugString(), "\n");
  return result;
}

}  // namespace

std::ostream& operator<<(std::ostream& out, const WriteOp& op) {
  std::visit(overloaded{
                 [&](const InsertOp& op) { out << DebugString(op); },
                 [&](const UpdateOp& op) { out << DebugString(op); },
                 [&](const DeleteOp& op) { out << DebugString(op); },
             },
             op);
  return out;
}

std::ostream& operator<<(std::ostream& out, const InsertOp& op) {
  out << DebugString(op);
  return out;
}

std::ostream& operator<<(std::ostream& out, const UpdateOp& op) {
  out << DebugString(op);
  return out;
}

std::ostream& operator<<(std::ostream& out, const DeleteOp& op) {
  out << DebugString(op);
  return out;
}

bool operator==(const InsertOp& op1, const InsertOp& op2) {
  return op1.table == op2.table && op1.key == op2.key &&
         op1.columns == op2.columns && op1.values == op2.values;
}

bool operator==(const UpdateOp& op1, const UpdateOp& op2) {
  return op1.table == op2.table && op1.key == op2.key &&
         op1.columns == op2.columns && op1.values == op2.values;
}

bool operator==(const DeleteOp& op1, const DeleteOp& op2) {
  return op1.table == op2.table && op1.key == op2.key;
}

struct TableVisitor {
  template <typename OpT>
  const Table* operator()(const OpT& op) const {
    return op.table;
  }
};

const Table* TableOf(const WriteOp& op) {
  return absl::visit(TableVisitor(), op);
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
