//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#ifndef DATATYPES_COMMON_NUMERIC_CORE_H_
#define DATATYPES_COMMON_NUMERIC_CORE_H_

#include <string>

#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "third_party/spanner_pg/errors/error_catalog.h"

namespace postgres_translator::spangres::datatypes::common {

// Max significant digits in the whole and fractional parts of the numeric value
inline constexpr uint32_t kMaxPGNumericWholeDigits = 131072;
inline constexpr uint16_t kMaxPGNumericFractionalDigits = 16383;
// Numeric constant values
inline constexpr char kPGNumericPositiveInfinity[] = "Infinity";
inline constexpr char kPGNumericNegativeInfinity[] = "-Infinity";
inline constexpr char kPGNumericNaN[] = "NaN";
// JSONB Numerics have to be parsed into a long double. Long double has a max
// value of 1.1897..e+4932L and a minvalue of 3.362..e-4932. Since it is desired
// to give the representable range in number of whole digits, 4932 is the max
// number of whole digits that can be fully represented.
inline constexpr uint32_t kMaxPGJSONBNumericWholeDigits = 4932;
inline constexpr uint16_t kMaxPGJSONBNumericFractionalDigits =
    kMaxPGNumericFractionalDigits;

enum class NumericErrorCode {
  WHOLE_COMPONENT_TOO_LARGE,
};

inline absl::Status Error(NumericErrorCode error_code,
                          absl::string_view numeric_str,
                          absl::string_view invalid_char = "") {
  switch (error_code) {
    case NumericErrorCode::WHOLE_COMPONENT_TOO_LARGE:
      return ::spangres::data_exception::NumericWholeComponentTooLarge(
          numeric_str);
  }
}

inline absl::string_view MaxNumericString() {
  static const std::string max_numeric =
      absl::StrCat(std::string(kMaxPGNumericWholeDigits, '9'), ".",
                   std::string(kMaxPGNumericFractionalDigits, '9'));
  return max_numeric;
}

inline absl::string_view MinNumericString() {
  static const std::string min_numeric = absl::StrCat("-", MaxNumericString());
  return min_numeric;
}

}  // namespace postgres_translator::spangres::datatypes::common

#endif  // DATATYPES_COMMON_NUMERIC_CORE_H_
