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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_UNIQUE_INDEX_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_UNIQUE_INDEX_H_

#include "backend/actions/action.h"
#include "backend/actions/context.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/index.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// UniqueIndexVerifier triggers on mutations to an index data table.
//
// Delete operation to an index do not need any verification. Update operation
// to a table are converted to Insert operation on the Index, therefore no
// verification is required for an Update operation to an index. Insert
// operation verifies if there are no duplicate index keys within a unique index
// table.
class UniqueIndexVerifier : public Verifier {
 public:
  explicit UniqueIndexVerifier(const Index* index);

 private:
  absl::Status Verify(const ActionContext* ctx,
                      const InsertOp& op) const override;

  // Index to verify unique entries.
  const Index* index_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_UNIQUE_INDEX_H_
