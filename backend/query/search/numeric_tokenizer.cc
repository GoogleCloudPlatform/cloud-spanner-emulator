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

#include "backend/query/search/numeric_tokenizer.h"

#include <cstdint>
#include <functional>
#include <limits>
#include <string>

#include "zetasql/public/value.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "backend/common/case.h"
#include "backend/query/search/tokenizer.h"
#include "common/errors.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

namespace {

static constexpr char kDefaultComparisonType[] = "all";
static constexpr char kDefaultAlgorithm[] = "auto";
static constexpr int64_t kDefaultGranularity = 1;
static constexpr int64_t kDefaultTreeBase = 2;
static constexpr int64_t kDefaultPrecision = 15;

// Indexes of the parameters
constexpr int kComparisonType = 1;
constexpr int kAlgorithm = 2;
constexpr int kMin = 3;
constexpr int kMax = 4;
constexpr int kGranularity = 5;
constexpr int kTreeBase = 6;
constexpr int kPrecision = 7;

zetasql::TypeKind GetMinMaxTypeKind(absl::Span<const zetasql::Value> args) {
  zetasql::TypeKind type_kind = zetasql::TypeKind::TYPE_UINT64;
  if (args.size() > kMin && !args[kMin].is_null()) {
    type_kind = args[kMin].type()->kind();
  } else if (args.size() > kMax && !args[kMax].is_null()) {
    type_kind = args[kMax].type()->kind();
  } else if (args.size() > kGranularity && !args[kGranularity].is_null()) {
    type_kind = args[kGranularity].type()->kind();
  }

  return type_kind;
}

template <typename T>
absl::Status ValidateMinMaxAndGranularity(
    absl::Span<const zetasql::Value> args,
    std::function<T(const zetasql::Value)> get_value) {
  T min_value = std::numeric_limits<T>::lowest();
  if (args.size() > kMin && !args[kMin].is_null()) {
    min_value = get_value(args[kMin]);
  }

  T max_value = std::numeric_limits<T>::max();
  if (args.size() > kMax && !args[kMax].is_null()) {
    max_value = get_value(args[kMax]);
  }

  T granularity = kDefaultGranularity;
  if (args.size() > kGranularity && !args[kGranularity].is_null()) {
    granularity = get_value(args[kGranularity]);
  }

  if (!std::isfinite(min_value)) {
    return error::NumericIndexingVariableMustBeFinite(
        "min", std::to_string(min_value));
  }
  if (!std::isfinite(max_value)) {
    return error::NumericIndexingVariableMustBeFinite(
        "max", std::to_string(max_value));
  }
  if (min_value >= max_value) {
    return error::NumericIndexingMinMustBeLessThanMax(
        "TOKENIZE_NUMBER", std::to_string(min_value),
        std::to_string(max_value));
  }
  if (granularity <= 0 || !std::isfinite(granularity)) {
    return error::NumericIndexingGranularityMustBeFiniteAndPositive(
        std::to_string(granularity));
  }

  if (static_cast<long double>(min_value) + granularity >
      static_cast<long double>(max_value)) {
    return error::NumericIndexingGranularityMustBeLessThanDiffBetweenMinAndMax(
        std::to_string(granularity), std::to_string(min_value),
        std::to_string(max_value));
  }

  return absl::OkStatus();
}

absl::Status ValidateTokenizeNumberOptionalParameters(
    absl::Span<const zetasql::Value> args) {
  std::string comparison_type = kDefaultComparisonType;
  CaseInsensitiveStringSet valid_comparison_types = {"all", "range",
                                                     "equality"};
  if (args.size() > kComparisonType && !args[kComparisonType].is_null()) {
    comparison_type = args[kComparisonType].string_value();
    if (valid_comparison_types.find(comparison_type) ==
        valid_comparison_types.end()) {
      return error::NumericIndexingUnsupportedComparisonType("TOKENIZE_NUMBER",
                                                             comparison_type);
    }
  }

  int64_t precision = GetIntParameterValue(args, kPrecision, kDefaultPrecision);
  if (precision < 1 || precision > 15) {
    return error::NumericIndexingPrecisionNotInRange(std::to_string(precision));
  }

  std::string algorithm = kDefaultAlgorithm;
  CaseInsensitiveStringSet valid_algorithm = {"auto", "logtree", "prefixtree",
                                              "floatingpoint"};
  if (args.size() > kAlgorithm && !args[kAlgorithm].is_null()) {
    algorithm = args[kAlgorithm].string_value();
    if (valid_algorithm.find(algorithm) == valid_algorithm.end()) {
      return error::NumericIndexingUnsupportedAlgorithm("TOKENIZE_NUMBER",
                                                        algorithm);
    }
  }

  if (!absl::EqualsIgnoreCase(algorithm, "floatingpoint")) {
    int64_t tree_base = GetIntParameterValue(args, kTreeBase, kDefaultTreeBase);
    if (tree_base < 2 || tree_base > 10) {
      return error::NumericIndexingTreeBaseNotInRange(
          std::to_string(tree_base));
    }

    // Assume min, max and granularity are of the same type, otherwise the
    // function signature validation should already failed.
    zetasql::TypeKind type_kind = GetMinMaxTypeKind(args);
    if (type_kind == zetasql::TypeKind::TYPE_FLOAT ||
        type_kind == zetasql::TypeKind::TYPE_DOUBLE) {
      return ValidateMinMaxAndGranularity<double>(
          args, [](const zetasql::Value& value) -> double {
            return value.double_value();
          });
    } else {
      return ValidateMinMaxAndGranularity<int64_t>(
          args, [](const zetasql::Value& value) -> int64_t {
            return value.int64_value();
          });
    }
  } else {
    const zetasql::Type* type = args[0].type();
    if (type->IsArray()) {
      type = type->AsArray()->element_type();
    }

    if (!type->IsDouble()) {
      return error::FpAlgorithmOnlySupportedOnFloats();
    }
  }

  return absl::OkStatus();
}

}  // namespace
// The tokenlist generated by TOKENIZE_NUMBER function does not have a
// correspondent SEARCH function. So only add the tokenize functio name into the
// tokenlist to block it being used by SEARCH or SEARCH_SUBSTRING function.
absl::StatusOr<zetasql::Value> NumericTokenizer::Tokenize(
    absl::Span<const zetasql::Value> args) {
  ZETASQL_RETURN_IF_ERROR(ValidateTokenizeNumberOptionalParameters(args));

  return TokenListFromStrings({std::string(kNumericTokenizer)});
}

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
