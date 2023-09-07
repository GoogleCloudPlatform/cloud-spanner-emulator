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

#ifndef UTIL_INTEGRAL_TYPES_H_
#define UTIL_INTEGRAL_TYPES_H_

#include <stdint.h>  // not cstdint; this file is used from C

#define DEFINE_INT(x) int##x
#define DEFINE_UINT(x) uint##x

// Signed integer types with width of exactly 8, 16, 32, or 64 bits
// respectively, for use when exact sizes are required.
typedef int8_t DEFINE_INT(8);
typedef int16_t DEFINE_INT(16);
typedef int32_t DEFINE_INT(32);
typedef int64_t DEFINE_INT(64);

// Unsigned integer types with width of exactly 8, 16, 32, or 64 bits
// respectively, for use when exact sizes are required.
typedef uint8_t DEFINE_UINT(8);
typedef uint16_t DEFINE_UINT(16);
typedef uint32_t DEFINE_UINT(32);
typedef uint64_t DEFINE_UINT(64);

#endif  // UTIL_INTEGRAL_TYPES_H_
