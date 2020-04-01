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

#include "backend/access/read.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

std::ostream& operator<<(std::ostream& out, const ReadArg& arg) {
  out << "Table  : '" << arg.table << "'\n";
  if (!arg.index.empty()) {
    out << "Index  : '" << arg.index << "'\n";
  }
  out << "KeySet : " << arg.key_set << "\n";
  out << "Columns: [";
  for (int i = 0; i < arg.columns.size(); ++i) {
    if (i > 0) {
      out << " ,";
    }
    out << "'" << arg.columns[i] << "'";
  }
  out << "]\n";

  return out;
}

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
