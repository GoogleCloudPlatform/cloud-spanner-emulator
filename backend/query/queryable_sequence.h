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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_SEQUENCE_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_SEQUENCE_H_

#include <memory>
#include <optional>

#include "zetasql/public/catalog.h"
#include "absl/status/statusor.h"
#include "backend/schema/catalog/sequence.h"
#include "absl/status/status.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {

// A wrapper over Sequence class which implements zetasql::Sequence.
// QueryableSequence has a reference to the backend schema Sequence.
class QueryableSequence : public zetasql::Sequence {
 public:
  QueryableSequence(const backend::Sequence* backend_sequence);

  std::string Name() const override { return wrapped_sequence_->Name(); }

  // FullName is used in debugging so it's OK to not include full path here.
  std::string FullName() const override { return Name(); }

  const backend::Sequence* wrapped_sequence() const {
    return wrapped_sequence_;
  }

 private:
  // The underlying Sequence object which backes the QueryableSequence.
  const backend::Sequence* wrapped_sequence_;
};

}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_QUERYABLE_SEQUENCE_H_
