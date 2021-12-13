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

#include "backend/query/feature_filter/gsql_supported_functions.h"

namespace google::spanner::emulator::backend {

const absl::flat_hash_set<absl::string_view>* SupportedZetaSQLFunctions() {
  static const auto* supported_functions =
      new absl::flat_hash_set<absl::string_view>{
          // clang-format off
        "abs",
        "acos",
        "acosh",
        "$add",
        "$and",
        "any_value",
        "array_agg",
        "$array_at_offset",
        "$array_at_ordinal",
        "$safe_array_at_offset",
        "$safe_array_at_ordinal",
        "array_concat",
        "array_concat_agg",
        "array_is_distinct",
        "array_length",
        "array_reverse",
        "array_to_string",
        "asin",
        "asinh",
        "atan",
        "atan2",
        "atanh",
        "avg",
        "bit_and",
        "bit_count",
        "bit_or",
        "bit_xor",
        "$between",
        "$bitwise_and",
        "$bitwise_not",
        "$bitwise_or",
        "$bitwise_xor",
        "byte_length",
        "$case_no_value",
        "$case_with_value",
        "ceil",
        "char_length",
        "coalesce",
        "concat",
        "$concat_op",
        "cos",
        "cosh",
        "count",
        "$count_star",
        "countif",
        "current_date",
        "current_timestamp",
        "date",
        "date_add",
        "date_diff",
        "date_sub",
        "date_trunc",
        "date_from_unix_date",
        "div",
        "$divide",
        "safe_divide",
        "safe_add",
        "safe_subtract",
        "safe_multiply",
        "safe_negate",
        "ends_with",
        "$equal",
        "error",
        "exp",
        "$extract",
        "$extract_date",
        "farm_fingerprint",
        "floor",
        "format",
        "format_date",
        "format_timestamp",
        "from_hex",
        "to_hex",
        "generate_array",
        "generate_date_array",
        "$greater",
        "$greater_or_equal",
        "greatest",
        "ieee_divide",
        "if",
        "ifnull",
        "$in",
        "$in_array",
        "is_inf",
        "is_nan",
        "$is_false",
        "$is_null",
        "$is_true",
        "least",
        "$bitwise_left_shift",
        "length",
        "$less",
        "$less_or_equal",
        "$like",
        "ln",
        "log",
        "log10",
        "logical_and",
        "logical_or",
        "lower",
        "ltrim",
        "lpad",
        "rpad",
        "repeat",
        "reverse",
        "max",
        "$make_array",
        "md5",
        "min",
        "mod",
        "$multiply",
        "$unary_minus",
        "net.ip_from_string",
        "net.safe_ip_from_string",
        "net.ip_to_string",
        "net.ip_net_mask",
        "net.ip_trunc",
        "net.ipv4_from_int64",
        "net.ipv4_to_int64",
        "net.host",
        "net.public_suffix",
        "net.reg_domain",
        "$not",
        "$not_equal",
        "nullif",
        "$or",
        "parse_date",
        "parse_timestamp",
        "pow",
        "regexp_contains",
        "regexp_extract",
        "regexp_extract_all",
        "regexp_replace",
        "normalize",
        "normalize_and_casefold",
        "replace",
        "from_base64",
        "to_base64",
        "from_base32",
        "to_base32",
        "safe_convert_bytes_to_string",
        "code_points_to_bytes",
        "code_points_to_string",
        "to_code_points",
        "json_query",
        "json_value",
        "$bitwise_right_shift",
        "round",
        "rtrim",
        "sha1",
        "sha256",
        "sha512",
        "sign",
        "sin",
        "sinh",
        "split",
        "sqrt",
        "starts_with",
        "stddev",
        "stddev_samp",
        "string",
        "string_agg",
        "strpos",
        "substr",
        "$subtract",
        "sum",
        "tan",
        "tanh",
        "timestamp_add",
        "timestamp_diff",
        "timestamp_micros",
        "timestamp_millis",
        "timestamp_seconds",
        "timestamp_sub",
        "timestamp_trunc",
        "timestamp",
        "trim",
        "trunc",
        "unix_date",
        "unix_micros",
        "unix_millis",
        "unix_seconds",
        "upper",
        "variance",
        "var_samp",
          // clang-format on
      };
  return supported_functions;
}

const absl::flat_hash_set<absl::string_view>* SupportedJsonFunctions() {
  static const auto* supported_functions =
      new absl::flat_hash_set<absl::string_view>{
          // clang-format off
          "parse_json",
          "to_json",
          "to_json_string",
          "json_query",
          "json_value",
          "json_query_array",
          "json_value_array",
          "$subscript"
          // clang-format on
      };
  return supported_functions;
}

bool IsSupportedZetaSQLFunction(const zetasql::Function& function) {
  return SupportedZetaSQLFunctions()->contains(
      function.FullName(/*include_group=*/false));
}

bool IsSupportedJsonFunction(const zetasql::Function& function) {
  return SupportedJsonFunctions()->contains(
      function.FullName(/*include_group=*/false));
}

}  // namespace google::spanner::emulator::backend
