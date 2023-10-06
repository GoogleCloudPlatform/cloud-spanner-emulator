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

#include "third_party/spanner_pg/datatypes/common/jsonb/random_json_creator.h"

#include <array>
#include <cfloat>
#include <chrono>
#include <codecvt>
#include <limits>
#include <locale>
#include <random>
#include <string>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace postgres_translator::spangres::datatypes::common::jsonb {

using absl::StrAppend;

std::array<std::string, kValidCharactersLength>*
    RandomJsonCreator::valid_characters_ = PopulateValidCharacters();

RandomJsonCreator::RandomJsonCreator(const Options& options)
    : options_(options),
      seed_(
          std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      rng_(seed_),
      dist_valid_characters_(0, valid_characters_->size() - 1),
      dist_valid_special_characters_(0, kValidSpecialCharacters.size() - 1),
      dist_digits_(0, strlen(kValidDigits) - 1),
      dist_string_length_(0, options_.max_string_length),
      dist_whole_number_length_(0, options_.max_whole_number_digits),
      dist_fractional_number_length_(0, options_.max_fractional_number_digits),
      dist_array_length_(0, options_.max_array_length),
      dist_object_length_(0, options_.max_object_length),
      dist_whitespace_characters_length_(0, options_.max_whitespace_length),
      dist_uniform_(0, 1),
      dist_int_(std::numeric_limits<int64_t>::min(),
                std::numeric_limits<int64_t>::max()),
      dist_uint_(std::numeric_limits<uint64_t>::min(),
                 std::numeric_limits<uint64_t>::max()),
      dist_double_(DBL_MIN, DBL_MAX),
      current_depth_(0) {}

RandomJsonCreator::RandomJsonCreator() : RandomJsonCreator(Options()) {}

std::string RandomJsonCreator::GetRandomJson() { return CreateRandomObject(); }

// object = left-curly-bracket [ member *( , member ) ]  right-curly-bracket
// member = string : value
std::string RandomJsonCreator::CreateRandomObject() {
  ++current_depth_;
  std::string randomObject;
  StrAppend(&randomObject, CreateRandomWhitespaces(), "{",
            CreateRandomWhitespaces());
  auto length = dist_object_length_(rng_);
  int count = 0;
  for (; count < length - 1; ++count) {
    StrAppend(&randomObject, CreateRandomString(), CreateRandomWhitespaces(),
              ":", CreateRandomWhitespaces(), CreateRandomValue(),
              CreateRandomWhitespaces(), ",", CreateRandomWhitespaces());
  }
  if (count == length - 1) {
    StrAppend(&randomObject, CreateRandomString(), CreateRandomWhitespaces(),
              ":", CreateRandomWhitespaces(), CreateRandomValue());
  }
  StrAppend(&randomObject, CreateRandomWhitespaces(), "}",
            CreateRandomWhitespaces());
  return randomObject;
}

// array = left-square-bracket [ value *( , value ) ] right-square-bracket
std::string RandomJsonCreator::CreateRandomArray() {
  ++current_depth_;
  std::string randomArray;
  StrAppend(&randomArray, CreateRandomWhitespaces(), "[",
            CreateRandomWhitespaces());
  auto length = dist_array_length_(rng_);
  int count = 0;
  for (; count < length - 1; ++count) {
    StrAppend(&randomArray, CreateRandomValue(), CreateRandomWhitespaces(), ",",
              CreateRandomWhitespaces());
  }
  if (count == length - 1) {
    StrAppend(&randomArray, CreateRandomValue());
  }
  StrAppend(&randomArray, CreateRandomWhitespaces(), "]",
            CreateRandomWhitespaces());
  return randomArray;
}

std::string RandomJsonCreator::CreateRandomValue() {
  auto option_percent = dist_uniform_(rng_);
  if (current_depth_ < options_.max_depth && option_percent < 1.0 / 12) {
    return CreateRandomObject();
  } else if (current_depth_ < options_.max_depth && option_percent < 1.0 / 6) {
    return CreateRandomArray();
  } else if (option_percent < 2.0 / 6) {
    return CreateRandomNumber();
  } else if (option_percent < 3.0 / 6) {
    return CreateRandomString();
  } else if (option_percent < 4.0 / 6) {
    return "true";
  } else if (option_percent < 5.0 / 6) {
    return "false";
  } else {
    return "null";
  }
}

void RandomJsonCreator::AppendRandomDigitsOfLengthN(std::string& string,
                                                    uint32_t length) {
  for (int i = 0; i < length; ++i) {
    auto index = dist_digits_(rng_);
    string.push_back(kValidDigits[index]);
  }
}

std::string RandomJsonCreator::CreateRandomNumber() {
  std::string random_number;
  std::bernoulli_distribution d(options_.negative_number_max_percent);
  bool is_positive = d(rng_);

  if (is_positive) {
    static std::bernoulli_distribution plus_sign_distribution(0.1);
    bool add_plus_sign = plus_sign_distribution(rng_);
    random_number += add_plus_sign ? "+" : "";
  } else {
    random_number += "-";
  }

  AppendRandomDigitsOfLengthN(random_number, dist_whole_number_length_(rng_));
  static std::bernoulli_distribution fractional_number_distribution(
      options_.fractional_number_max_percent);
  bool is_fractional = fractional_number_distribution(rng_);
  if (is_fractional) {
    random_number.push_back('.');
    AppendRandomDigitsOfLengthN(random_number, dist_whole_number_length_(rng_));
  }

  return random_number;
}

std::string RandomJsonCreator::CreateRandomString() {
  std::string random_string = "\"";
  auto length = dist_string_length_(rng_);
  for (int i = 0; i < length; ++i) {
    auto special_characters_percent = dist_uniform_(rng_);
    if (special_characters_percent < options_.special_characters_max_percent) {
      StrAppend(&random_string, "\\");
      StrAppend(&random_string, GetRandomSpecialCh());
    } else {
      StrAppend(&random_string, GetRandomPrintableCh());
    }
  }
  StrAppend(&random_string, "\"");
  return random_string;
}

std::string RandomJsonCreator::CreateRandomWhitespaces() {
  std::string random_whitespaces;
  auto length = dist_whitespace_characters_length_(rng_);
  for (int i = 0; i < length; ++i) {
    StrAppend(&random_whitespaces, " ");
  }
  return random_whitespaces;
}

absl::string_view RandomJsonCreator::GetRandomPrintableCh() {
  auto index = dist_valid_characters_(rng_);
  return (*valid_characters_)[index];
}

absl::string_view RandomJsonCreator::GetRandomSpecialCh() {
  auto index = dist_valid_special_characters_(rng_);
  return kValidSpecialCharacters[index];
}

// This contains Unicode codepoints in the range 0000 to FFFF with few
// exceptions. Codepoints in the range D800 to DFFF are skipped as they are
// invalid in C++. Control codepoints 0000 to 001F, 007F to 009F are skipped as
// they are not allowed in the JSON specification defined in "ECMA-404 The JSON
// Data Interchange Standard". 0022 and 005C are skipped here as they need an
// escape character prefix.
std::array<std::string, kValidCharactersLength>*
RandomJsonCreator::PopulateValidCharacters() {
  auto valid_characters = new std::array<std::string, kValidCharactersLength>();
  std::wstring_convert<std::codecvt_utf8<char32_t>, char32_t> int_to_utf8;
  int index = 0;
  for (int i = 0; i < 65536; ++i) {
    // skip 0000 to 001F
    if (i <= 31) {
      continue;
    }
    // skip D800 to DFFF
    if (i >= 55296 && i <= 57343) {
      continue;
    }
    // skip 007F to 009F
    if (i >= 127 && i <= 159) {
      continue;
    }
    // skip `"` and `\`
    if (i == 34 || i == 92) {
      continue;
    }
    (*valid_characters)[index++] = int_to_utf8.to_bytes(i);
  }
  return valid_characters;
}

}  // namespace postgres_translator::spangres::datatypes::common::jsonb
