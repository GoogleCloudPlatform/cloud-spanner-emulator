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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_REMOTE_UDF_REMOTE_UDF_EVALUATOR_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_REMOTE_UDF_REMOTE_UDF_EVALUATOR_H_

#include <cstdint>
#include <string>

#include "googlesql/public/function.h"
#include "googlesql/public/json_value.h"
#include "googlesql/public/type.h"
#include "googlesql/public/value.h"
#include "absl/flags/declare.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"

ABSL_DECLARE_FLAG(std::string, remote_functions_host_port);

namespace google::spanner::emulator::backend {

class RemoteUdfEvaluator {
 public:
  // Evaluates a remote function call using HTTP call to a server pointed by
  // remote_functions_host_port flag.
  //
  // Arguments:
  //   endpoint: The endpoint of the remote function.
  //             E.g.
  //             "https://us-east1-PROJECT_ID.cloudfunctions.net/remote_add"
  //   schema_object_name: The name of the schema object.
  //             E.g. "RemoteAdd"
  //   language_options: The language options to use for JSON serialization.
  //   args: The arguments to pass to the remote function.
  //
  // Returns:
  //   The result of the remote function call as a JSON value.
  static absl::StatusOr<googlesql::JSONValue> EvaluateRemoteFunction(
      absl::string_view endpoint, absl::string_view schema_object_name,
      absl::Span<const googlesql::Value> args);

  // Pseudo-random function evaluator for remote functions.
  //
  // Calculates hash of the arguments and returns it as a value of the given
  // type.
  //
  // Arguments:
  //   args: The arguments to pass to the remote function.
  //   type: The type of the return value.
  //
  // Returns:
  //   The result of the remote function call as a value of the given type.
  static absl::StatusOr<googlesql::Value> EvaluatePseudoRandomRemoteFunction(
      absl::Span<const googlesql::Value> args, const googlesql::Type* type);

  // Builds a function evaluator for a remote function.
  //
  // Depending on the remote_functions_host_port flag, it will build a function
  // evaluator that calls the remote function or a pseudo-random function
  // evaluator.
  //
  // Arguments:
  //   endpoint: The endpoint of the remote function.
  //             E.g.
  //             "https://us-east1-PROJECT_ID.cloudfunctions.net/remote_add"
  //   schema_object_name: The name of the schema object.
  //             E.g. "RemoteAdd"
  //   return_type: The type of the return value.
  //
  // Returns:
  //   A function evaluator for the remote function.
  static googlesql::FunctionEvaluator BuildEvaluator(
      std::string endpoint, std::string schema_object_name,
      const googlesql::Type* return_type);
};

// A class that encapsulates the protocol for remote UDFs.
class RemoteUdfProtocol {
 public:
  // Converts a JSON value received from a remote UDF to a GSQL value.
  static absl::StatusOr<googlesql::Value> ToValue(
      googlesql::JSONValueConstRef json, const googlesql::Type* type);

  // Converts a value to a JSON value compatible with remote UDF protocol.
  static absl::StatusOr<googlesql::JSONValue> ToJson(
      const googlesql::Value& value);

  // Calculates a fingerprint of a list of values.
  // Used by pseudo-random function evaluator.
  static absl::StatusOr<uint64_t> Fingerprint(
      absl::Span<const googlesql::Value> values);

  // Calculates a fingerprint of a value.
  // Used by pseudo-random function evaluator.
  static absl::StatusOr<uint64_t> Fingerprint(const googlesql::Value& value);

  // Calculates a fingerprint of a JSON value.
  // Used by pseudo-random function evaluator.
  static absl::StatusOr<uint64_t> Fingerprint(
      const googlesql::JSONValueConstRef& json);

  // Converts a fingerprint to a value of the given type.
  // Used by pseudo-random function evaluator.
  static absl::StatusOr<googlesql::Value> ToValue(uint64_t fingerprint,
                                                  const googlesql::Type* type);
};

}  // namespace google::spanner::emulator::backend

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_REMOTE_UDF_REMOTE_UDF_EVALUATOR_H_
