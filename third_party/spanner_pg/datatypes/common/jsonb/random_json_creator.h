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

#ifndef DATATYPES_COMMON_JSONB_RANDOM_JSON_CREATOR_H_
#define DATATYPES_COMMON_JSONB_RANDOM_JSON_CREATOR_H_

#include <array>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "absl/strings/string_view.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {

static constexpr std::array<absl::string_view, 8> kValidSpecialCharacters = {
    "\u0022", "\u005C", "\u002F", "f", "n", "r", "t", "b"};
static constexpr char kValidDigits[11] = "0123456789";
static constexpr int kValidCharactersLength = 63421;

// Random JSON creator. The created JSON adheres to the "ECMA-404 The JSON
// Data Interchange Standard".
class RandomJsonCreator {
 public:
  struct Options {
    // maximum allowed length of a string.
    int max_string_length = 100;
    // maximum height of the JSON tree.
    int max_depth = 1000;
    // maximum allowed number of digits before the decimal point.
    int max_whole_number_digits = 1000;
    // maximum allowed number of digts after the decimal point.
    int max_fractional_number_digits = 1000;
    // maximum number of root level key value pairs in the json.
    int max_object_length = 50;
    // maximum number of root level values in an array.
    int max_array_length = 50;
    // maximum length of whitespaces.
    int max_whitespace_length = 50;
    // change for choosing a special character during random string creation.
    double special_characters_max_percent = 0.1;
    // change for choosing a negative number.
    double negative_number_max_percent = 0.5;
    // change for choosing a fractional number.
    double fractional_number_max_percent = 0.5;
  };

  RandomJsonCreator();
  explicit RandomJsonCreator(const Options& options);

  std::string GetRandomJson();

  // For testing only
  int64_t GetSeed() const { return seed_; }

 private:
  // An object structure is represented as a pair of curly bracket tokens
  // surrounding zero or more name/value pairs. A name is a string. A single
  // colon token follows each name, separating the name from the value. A
  // single comma token separates a value from a following name. The JSON
  // syntax does not impose any restrictions on the strings used as names,
  // does not require that name strings be unique, and does not assign any
  // significance to the ordering of name/value pairs.
  std::string CreateRandomObject();

  // An array structure is a pair of square bracket tokens surrounding zero or
  // more values. The values are separated by commas. The JSON syntax does not
  // define any specific meaning to the ordering of the values.
  std::string CreateRandomArray();

  // A JSON value can be an object, array, number, string, true, false or
  // null.
  std::string CreateRandomValue();

  // A number is a sequence of decimal digits with no superfluous leading
  // zero. It may have a preceding minus sign. It may have a fractional part
  // prefixed by a decimal point. It may have an exponent, prefixed by
  // e or E and optionally + or –. Numeric values that cannot be represented
  // as sequences of digits such as Infinity and NaN are not permitted.
  std::string CreateRandomNumber();

  // A string is represented as a pair of quotation marks surrounding a
  // sequence of Unicode code points. All code points may be placed within the
  // quotation marks except for the code points that must be escaped:
  // quotation mark, reverse solidus, and the control characters.
  std::string CreateRandomString();

  // Insignificant whitespace is allowed before or after any token. Whitespace
  // is not allowed within any token, except that space is allowed in strings.
  // The set of tokens includes six structural tokens, strings, numbers, and
  // three literal name tokens. Structural tokens are left square bracket,
  // left curly bracket, right square bracket, right curly bracket, colon and
  // comma. The literal tokens are true, false and null.
  std::string CreateRandomWhitespaces();

  void AppendRandomDigitsOfLengthN(std::string& string, uint32_t length);

  absl::string_view GetRandomPrintableCh();
  absl::string_view GetRandomSpecialCh();
  std::vector<char> GetRandomDigits();
  static std::array<std::string, kValidCharactersLength>*
  PopulateValidCharacters();
  void Init();

  const Options options_;
  const int64_t seed_;
  std::mt19937 rng_;
  std::uniform_int_distribution<> dist_valid_characters_;
  std::uniform_int_distribution<> dist_valid_special_characters_;
  std::uniform_int_distribution<> dist_whitespace_characters_;
  std::uniform_int_distribution<> dist_digits_;
  std::uniform_int_distribution<> dist_string_length_;
  std::uniform_int_distribution<> dist_whole_number_length_;
  std::uniform_int_distribution<> dist_fractional_number_length_;
  std::uniform_int_distribution<> dist_array_length_;
  std::uniform_int_distribution<> dist_object_length_;
  std::uniform_int_distribution<> dist_whitespace_characters_length_;
  std::uniform_real_distribution<> dist_uniform_;
  std::uniform_int_distribution<int64_t> dist_int_;
  std::uniform_int_distribution<uint64_t> dist_uint_;
  std::uniform_real_distribution<double> dist_double_;

  static std::array<std::string, kValidCharactersLength>* valid_characters_;
  int current_depth_;
};
}  // namespace postgres_translator::spangres::datatypes::common::jsonb

#endif  // DATATYPES_COMMON_JSONB_RANDOM_JSON_CREATOR_H_
