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

#ifndef UTIL_INTEGRAL_HELPERS_H_
#define UTIL_INTEGRAL_HELPERS_H_

#include <limits>

#include "absl/numeric/int128.h"
#include "absl/status/status.h"

template <typename T>
static void Check64BitIntOrSmaller() {
  static_assert(sizeof(T) <= 8,
                "SafeIntOps can not operate on types larger than 64 bits");
  static_assert(std::numeric_limits<T>::is_integer,
                "SafeIntOps can not operate on non-integer types");
}

template <typename T>
static absl::int128 ConvertToInt128(T from) {
  if (from < 0) {
    return absl::MakeInt128(0xFFFFFFFFFFFFFFFF, from);
  } else {
    return absl::MakeInt128(0, from);
  }
}

// Returns the sum of the given numbers if it fits in an int32_t. Returns an out
// of range error if there is an overflow or an underflow.
template <typename T, typename U, typename V>
absl::Status SafeAdd(T lhs, U rhs, V* result) {
  Check64BitIntOrSmaller<T>();
  Check64BitIntOrSmaller<U>();
  Check64BitIntOrSmaller<V>();

  absl::int128 big_lhs = ConvertToInt128(lhs);
  absl::int128 big_rhs = ConvertToInt128(rhs);
  absl::int128 unsafe_result = big_lhs + big_rhs;

  if (unsafe_result < std::numeric_limits<V>::min()) {
    return absl::OutOfRangeError("underflow");
  }
  if (unsafe_result > std::numeric_limits<V>::max()) {
    return absl::OutOfRangeError("overflow");
  }

  *result = static_cast<V>(unsafe_result);
  return absl::OkStatus();
}

// Returns the subtraction of the given numbers if it fits in an int32_t. Returns
// an out of range error if there is an underflow or an overflow.
template <typename T, typename U, typename V>
absl::Status SafeSubtract(T lhs, U rhs, V* result) {
  Check64BitIntOrSmaller<T>();
  Check64BitIntOrSmaller<U>();
  Check64BitIntOrSmaller<V>();

  absl::int128 big_lhs = ConvertToInt128(lhs);
  absl::int128 big_rhs = ConvertToInt128(rhs);
  absl::int128 unsafe_result = big_lhs - big_rhs;

  if (unsafe_result < std::numeric_limits<V>::min()) {
    return absl::OutOfRangeError("underflow");
  }
  if (unsafe_result > std::numeric_limits<V>::max()) {
    return absl::OutOfRangeError("overflow");
  }

  *result = static_cast<V>(unsafe_result);
  return absl::OkStatus();
}

#endif  // UTIL_INTEGRAL_HELPERS_H_
