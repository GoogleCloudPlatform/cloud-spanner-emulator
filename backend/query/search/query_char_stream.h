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

#ifndef THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_CHAR_STREAM_H_
#define THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_CHAR_STREAM_H_

#include "absl/strings/string_view.h"
#include "backend/query/search/CharStream.h"
#include "backend/query/search/query_token_base.h"

namespace google {
namespace spanner {
namespace emulator {
namespace backend {
namespace query {
namespace search {

class SearchQueryCharStream : public CharStream {
  // Stream class used by generated search parser to read the input query.
 public:
  SearchQueryCharStream(const SearchQueryCharStream& other) = delete;
  SearchQueryCharStream& operator=(const SearchQueryCharStream& ohter) = delete;

  // text: the input query string
  explicit SearchQueryCharStream(absl::string_view text)
      : CharStream(text.data(), text.size(), 1 /* start line */,
                   1 /* start column */,
                   text.size() + 1 /* buffer size -- to assure one buffer */) {}

  // Mark the beginning of a token for future process.
  int token_begin() const { return tokenBegin; }
};

}  // namespace search
}  // namespace query
}  // namespace backend
}  // namespace emulator
}  // namespace spanner
}  // namespace google

#endif  // THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_SEARCH_QUERY_CHAR_STREAM_H_
