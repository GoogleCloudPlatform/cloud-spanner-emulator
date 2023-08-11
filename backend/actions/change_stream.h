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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHANGE_STREAM_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_ACTIONS_CHANGE_STREAM_H_

#include <cstdint>
#include <memory>

#include "backend/actions/action.h"
#include "backend/actions/ops.h"
#include "backend/schema/catalog/table.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// ChangeStreamEffector triggers on mutations to a table tracked by change
// streams.
//
// Insert, Update & Delete operations to a row in a table tracked by change
// streams are added as operations to a change stream data table. The following
// operations are applied to the change stream data table based on operations to
// the tracked table:
class ChangeStreamEffector : public Effector {
 public:
  explicit ChangeStreamEffector(const ChangeStream* change_stream);

 private:
  absl::Status Effect(const ActionContext* ctx,
                      const InsertOp& op) const override;
  absl::Status Effect(const ActionContext* ctx,
                      const UpdateOp& op) const override;
  absl::Status Effect(const ActionContext* ctx,
                      const DeleteOp& op) const override;

  const ChangeStream* change_stream_;
};
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_COMMON_CHANGE_STREAM_H_
