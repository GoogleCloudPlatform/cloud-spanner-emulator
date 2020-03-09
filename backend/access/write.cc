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

#include "backend/access/write.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

void Mutation::AddWriteOp(MutationOpType type, const std::string& table,
                          std::vector<std::string> columns,
                          std::vector<ValueList> values) {
  ops_.emplace_back(
      MutationOp(type, table, std::move(columns), std::move(values)));
}

void Mutation::AddDeleteOp(const std::string& table, const KeySet& key_set) {
  ops_.emplace_back(MutationOp(MutationOpType::kDelete, table, key_set));
}

std::string MutationOp::DebugString() const {
  std::stringstream out;
  out << (*this);
  return out.str();
}

std::ostream& operator<<(std::ostream& out, const MutationOp& op) {
  switch (op.type) {
    case MutationOpType::kInsert:
      out << "Type   : kInsert";
      break;
    case MutationOpType::kUpdate:
      out << "Type   : kUpdate";
      break;
    case MutationOpType::kInsertOrUpdate:
      out << "Type   : kInsertOrUpdate";
      break;
    case MutationOpType::kReplace:
      out << "Type   : kReplace";
      break;
    case MutationOpType::kDelete:
      out << "Type   : kDelete";
      break;
  }

  out << "\n";
  out << "Table  : '" << op.table << "'\n";

  if (op.type == MutationOpType::kDelete) {
    out << "KeySet : " << op.key_set << "\n";
    return out;
  }

  out << "Columns: [";
  for (int i = 0; i < op.columns.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << "'" << op.columns[i] << "'";
  }
  out << "]\n";

  out << "Rows   :\n";
  for (int i = 0; i < op.rows.size(); ++i) {
    out << "- {";
    for (int j = 0; j < op.rows[i].size(); ++j) {
      if (j > 0) {
        out << ", ";
      }
      out << op.rows[i][j];
    }
    out << "}\n";
  }

  return out;
}

std::ostream& operator<<(std::ostream& out, const Mutation& mut) {
  const std::vector<MutationOp>& ops = mut.ops();

  for (int i = 0; i < ops.size(); ++i) {
    const MutationOp& op = ops[i];
    out << "#" << i << " Op:\n";
    out << op;
    if (ops.size() > 1) {
      out << "\n";
    }
  }

  return out;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
