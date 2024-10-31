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

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "tests/conformance/common/database_test_base.h"
#include "zetasql/base/status_macros.h"

namespace google {
namespace spanner {
namespace emulator {
namespace test {

namespace {

using testing::ContainsRegex;
using testing::HasSubstr;
using testing::Matcher;
using zetasql_base::testing::StatusIs;

class SearchTest : public DatabaseTest {
 public:
  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(
        SetSchema({// All TOKENLIST columns need to be defined as HIDDEN to be
                   // in conformance with Cloud Spanner.
                   R"sql(
          CREATE TABLE Albums (
            AlbumId INT64 NOT NULL,
            UserId INT64 NOT NULL,
            ReleaseTimestamp INT64 NOT NULL,
            Uid INT64 NOT NULL,
            Name STRING(MAX),
            Name_Tokens TOKENLIST AS (TOKEN(Name)) HIDDEN,
            Name_Ngrams_Tokens TOKENLIST AS (TOKENIZE_NGRAMS(Name, ngram_size_max=>4, ngram_size_min=>2)) HIDDEN,
            Name_Exists BOOL AS (Name IS NOT NULL AND Name!=""),
            Name_Exists_Tokens TOKENLIST AS (TOKENIZE_BOOL(Name_Exists)) HIDDEN,
            Tracks ARRAY<STRING(MAX)>,
            Tracks_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Tracks)) STORED HIDDEN,
            Tracks_Substring_Tokens TOKENLIST AS (TOKENIZE_SUBSTRING(Tracks)) STORED HIDDEN,
            Summary STRING(MAX),
            Summary_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Summary)) HIDDEN,
            Summary_SubStr_Tokens TOKENLIST AS (TOKENIZE_SUBSTRING(Summary, ngram_size_max=>4, ngram_size_min=>3)) STORED HIDDEN,
            Summary_SubStr_Relative_Tokens TOKENLIST AS (TOKENIZE_SUBSTRING(Summary, relative_search_types=>["word_prefix"])) STORED HIDDEN,
            Summary_SubStr_Relative_All_Tokens TOKENLIST AS (TOKENIZE_SUBSTRING(Summary, relative_search_types=>["all"], ngram_size_max=>5, ngram_size_min=>3)) STORED HIDDEN,
            Summary2 STRING(MAX),
            Summary2_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Summary2)) HIDDEN,
            Summary2_SubStr_Tokens TOKENLIST AS (TOKENIZE_SUBSTRING(Summary2, ngram_size_min=>3, ngram_size_max=>4, language_tag=>"ar-ar")) STORED HIDDEN,
            Summaries_Tokens TOKENLIST AS (TOKENLIST_CONCAT([Summary_Tokens, Summary2_Tokens])) STORED HIDDEN,
            Summaries_SubStr_Tokens TOKENLIST AS (TOKENLIST_CONCAT([Summary_SubStr_Tokens, Summary2_SubStr_Tokens])) HIDDEN,
            Length INT64,
            Length_Tokens TOKENLIST AS (TOKENIZE_NUMBER(Length, min=>0, max=>0x40000000)) STORED HIDDEN,
          ) PRIMARY KEY(AlbumId)
        )sql"}));
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"sql(
          CREATE SEARCH INDEX AlbumIndex
          ON Albums(Name_Tokens,
                    Name_Ngrams_Tokens,
                    Tracks_Tokens,
                    Tracks_Substring_Tokens,
                    Summary_Tokens,
                    Summary_SubStr_Tokens,
                    Summary_SubStr_Relative_Tokens,
                    Summary_SubStr_Relative_All_Tokens,
                    Summary2_Tokens,
                    Summary2_SubStr_Tokens,
                    Summaries_Tokens,
                    Summaries_SubStr_Tokens,
                    Length_Tokens)
          STORING(Length)
          PARTITION BY UserId
          ORDER BY ReleaseTimestamp
          OPTIONS (
            sort_order_sharding = true,
            disable_automatic_uid_column=true)
        )sql",
    }));

    return PopulateDatabase();
  }

 protected:
  absl::Status PopulateDatabase() {
    std::vector<std::string> one_track = {"track1 track2"};
    std::vector<std::string> two_tracks = {"track1", "track2"};
    std::vector<std::string> single_track = {"track1"};
    ZETASQL_RETURN_IF_ERROR(
        MultiInsert(
            "Albums",
            {"AlbumId", "UserId", "ReleaseTimestamp", "Uid", "Name", "Tracks",
             "Summary", "Summary2"},
            {{0, 1, 0, 0, "name1", one_track, "", ""},
             {1, 1, 11, 0, "name1", one_track, "global top 50 song", "foo bar"},
             {2, 1, 9, 0, "name1", two_tracks, "global top 100 song", ""},
             {3, 2, 3, 0, "name2", two_tracks, "US top 50 song", "bar baz"},
             {4, 1, 2, 0, "name1", two_tracks, "Song at top 1000 in US", "foo"},
             {5, 2, 13, 0, "name1 name2", one_track, "global and US top song",
              ""},
             {6, 1, 5, 0, "name3 name2 name1", two_tracks, "popular song", ""},
             {7, 1, 8, 0, "name1", one_track, "US top 1000 song", ""},
             {8, 2, 4, 1, "name1", two_tracks, "global top 1000 song", ""},
             {9, 1, 7, 2, "name1", two_tracks, "", ""},
             {10, 1, 6, 3, "name1 name2", one_track, "", ""},
             {11, 2, 14, 0, "", std::vector<std::string>{}, "", ""},
             {12, 1, 19, 0, " ", single_track, "", ""},
             {13, 1, 17, 0, "  ", std::vector<std::string>{}, "", ""},
             {16, 1, 20, 0, "classic rock", std::vector<std::string>{}, "", ""},
             {17, 1, 21, 0, "electronic rock", std::vector<std::string>{}, "",
              ""}})
            .status());

    // Insert a row with NULL name
    ZETASQL_RETURN_IF_ERROR(
        Insert("Albums",
               {"AlbumId", "UserId", "ReleaseTimestamp", "Uid", "Summary"},
               {14, 2, 9, 0, ""})
            .status());

    // Insert a row with NULL summary
    return Insert("Albums",
                  {"AlbumId", "UserId", "ReleaseTimestamp", "Uid", "Name"},
                  {15, 1, 12, 0, ""})
        .status();
  }
};

TEST_F(SearchTest, CreateSearchIndexWithUniformSharding) {
  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
          CREATE SEARCH INDEX AlbumIndexUniformSharding
          ON Albums(Name_Tokens,
                    Tracks_Tokens,
                    Summary_Tokens,
                    Length_Tokens)
          STORING(Length)
          PARTITION BY UserId
          ORDER BY ReleaseTimestamp
          OPTIONS (
            sort_order_sharding = false)
        )sql",
  }));

  ZETASQL_EXPECT_OK(SetSchema({
      R"sql(
          CREATE SEARCH INDEX AlbumIndexDefaultUniformSharding
          ON Albums(Name_Tokens,
                    Tracks_Tokens,
                    Summary_Tokens,
                    Length_Tokens)
          STORING(Length)
          PARTITION BY UserId
          ORDER BY ReleaseTimestamp
        )sql",
  }));
}

TEST_F(SearchTest, TokenWrongArguments) {
  EXPECT_THAT(UpdateSchema({
                  R"sql(
          ALTER TABLE Albums
          ADD COLUMN Name_Token2 TOKENLIST
          AS (TOKEN(Name, 15, true)) STORED HIDDEN)sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "SPANNER:TOKEN")));
}

TEST_F(SearchTest, TokenizeNumberWrongArguments) {
  EXPECT_THAT(UpdateSchema({
                  R"sql(
          ALTER TABLE Albums
          ADD COLUMN Length_Token2 TOKENLIST
              AS (TOKENIZE_NUMBER(
                   Length,
                  comparison_type=>"ALL",
                  algorithm=>"auto",
                  min=>-1000,
                  max=>1000,
                  granularity=>1,
                  tree_base=>2,
                  precision=>7,
                  "extra-arg")) STORED HIDDEN)sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "SPANNER:TOKENIZE_NUMBER")));
}

TEST_F(SearchTest, TokenizeNumberNotAcceptPositionalOptionalArguments) {
  EXPECT_THAT(UpdateSchema({
                  R"sql(
          ALTER TABLE Albums
          ADD COLUMN Length_Token2 TOKENLIST
              AS (TOKENIZE_NUMBER(
                   Length,
                  "ALL")) STORED HIDDEN)sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "SPANNER:TOKENIZE_NUMBER")));
}

TEST_F(SearchTest, TokenizeNumberWithOptionalArguments) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums
          ADD COLUMN Length_Tokens2 TOKENLIST
              AS (TOKENIZE_NUMBER(
                  Length,
                  comparison_type=>"ALL",
                  algorithm=>"auto",
                  min=>-1000,
                  max=>1000,
                  granularity=>1,
                  tree_base=>2,
                  precision=>7)) STORED HIDDEN)sql"}));
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
      ALTER TABLE Albums DROP COLUMN Length_Tokens2)sql"}));
}

TEST_F(SearchTest, TokenizeNumberOptionalArgumentsNoOrder) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums
          ADD COLUMN Length_Tokens3 TOKENLIST
              AS (TOKENIZE_NUMBER(
                  Length,
                  min=>-1000,
                  max=>1000,
                  comparison_type=>"ALL",
                  algorithm=>"auto",
                  granularity=>1,
                  tree_base=>2,
                  precision=>7)) STORED HIDDEN)sql"}));
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
      ALTER TABLE Albums DROP COLUMN Length_Tokens3)sql"}));
}

TEST_F(SearchTest, TokenizeBoolWrongArguments) {
  EXPECT_THAT(UpdateSchema({
                  R"sql(
          ALTER TABLE Albums
          ADD COLUMN Name_Exists_Token2 TOKENLIST
              AS (TOKENIZE_BOOL(Name_Exists, 15)) STORED HIDDEN)sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("function SPANNER:TOKENIZE_BOOL")));
}

TEST_F(SearchTest, TokenizeFullTextSupportHtml) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums
          ADD COLUMN Summary_Tokens2 TOKENLIST
              AS (TOKENIZE_FULLTEXT(
                    Summary,
                    language_tag=>"en-us",
                    content_type=>"text/html")) STORED HIDDEN)sql"}));
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums DROP COLUMN Summary_Tokens2)sql"}));
}

TEST_F(SearchTest, TokenizeFullTextSupportsPlainText) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums
          ADD COLUMN Summary_Tokens2 TOKENLIST
              AS (TOKENIZE_FULLTEXT(
                    Summary,
                    language_tag=>"en-us",
                    content_type=>"text/plain")) STORED HIDDEN)sql"}));
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums DROP COLUMN Summary_Tokens2)sql"}));
}

TEST_F(SearchTest, TokenizeFullTextWrongArguments) {
  EXPECT_THAT(UpdateSchema({
                  R"sql(
          ALTER TABLE Albums ADD COLUMN Name_Token2 TOKENLIST
              AS (TOKENIZE_FULLTEXT(Summary,
                                    "en-us",
                                    "text/html",
                                    true)) STORED HIDDEN)sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "SPANNER:TOKENIZE_FULLTEXT")));
}

TEST_F(SearchTest, SearchFunctionSupportOptionalArguments) {
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "top", enhance_query=>true)
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"));
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens,
                       "top",
                       enhance_query=>true,
                       language_tag=>"en-us")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"));
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens,
                       "top",
                      enhance_query=>false,
                      language_tag=>"en-us")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"));
}

TEST_F(SearchTest, SearchFunctionWrongArguments) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens)
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("function SPANNER:SEARCH")));
}

TEST_F(SearchTest, TokenizeSubstringSupportHtml) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums
          ADD COLUMN Summary_SubStr_Tokens2 TOKENLIST
              AS (TOKENIZE_SUBSTRING(Summary,
                  ngram_size_max=>3,
                  ngram_size_min=>2,
                  content_type=>"text/html")) STORED HIDDEN)sql"}));
}

TEST_F(SearchTest, TokenizeSubstringSupportsPlainText) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
          ALTER TABLE Albums
          ADD COLUMN Summary_SubStr_Tokens2 TOKENLIST
              AS (TOKENIZE_SUBSTRING(Summary,
                  ngram_size_max=>3,
                  ngram_size_min=>2,
                  content_type=>"text/plain")) STORED HIDDEN)sql"}));
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
            ALTER TABLE Albums DROP COLUMN Summary_SubStr_Tokens2)sql"}));
}

TEST_F(SearchTest, TokenizeSubstringWrongArguments) {
  EXPECT_THAT(UpdateSchema({
                  R"sql(
          ALTER TABLE Albums
          ADD COLUMN Summary_SubStr_Tokens2 TOKENLIST
              AS (TOKENIZE_SUBSTRING(
                  Summary,
                  3,
                  2,
                  true,
                  "text/html",
                  true)) STORED HIDDEN)sql"}),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("No matching signature for function "
                                 "SPANNER:TOKENIZE_SUBSTRING")));
}

TEST_F(SearchTest, SearchSubStringRelativeSearch) {
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_Tokens, "son",
                                 relative_search_type=>"word_prefix")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"));
}

TEST_F(SearchTest, SearchSubStringRelativeSearchNotSupported) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, "son",
                                 relative_search_type=>"word_prefix")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, SearchSubstringWrongArguments) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_Tokens,
                                  "son",
                                  relative_search_type=>"word_prefix",
                                  language_tag=>"en-us",
                                  true)
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("function SPANNER:SEARCH_SUBSTRING")));
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens)
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("function SPANNER:SEARCH_SUBSTRING")));
}

TEST_F(SearchTest, UnableToSearchOnExactToken) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Name_Tokens, 'name1')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, UnableToSearchOnNumericToken) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Length_Tokens, 'name1')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, UnableToSearchSubstringOnNumericToken) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Length_Tokens, 'name1')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// failed comformance
TEST_F(SearchTest, UnableToSearchOnMixedConcatToken) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(
                  TOKENLIST_CONCAT([Summary_Tokens, Summary_SubStr_Tokens]),
                  'foo')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

// failed comformance
TEST_F(SearchTest, UnableToSearchSubstringOnMixedConcatToken) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(
                  TOKENLIST_CONCAT([Summary_SubStr_Tokens, Summary_Tokens]),
                  'foo')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, BasicSearch) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(SearchTest, BasicSearchOnTokenlistConcat) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summaries_Tokens, "global foo")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}}));
}

TEST_F(SearchTest, BasicSearchConnectedPhraseOnTokenlistConcat) {
  // Phrase will not match across tokenlists.
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summaries_Tokens, "global-foo")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, BasicSearchAnd) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global US")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

TEST_F(SearchTest, BasicSearchOr) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global | US")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {7}}));
}

TEST_F(SearchTest, BasicSearchNot) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "-global")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows(
                  {{0}, {4}, {6}, {7}, {9}, {10}, {12}, {13}, {16}, {17}}));
}

TEST_F(SearchTest, BasicSearchAround) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global AROUND(2) song")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(SearchTest, BasicSearchConnectedPhrase) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "top-song")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

TEST_F(SearchTest, BasicSearchQuotedPhrase) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "\"global and US\"")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

TEST_F(SearchTest, CompositeSearch) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "top 1000 global | US")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{4}, {7}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global | US 50")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global top-song")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "top-1000 -global")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{4}, {7}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "\"top 1000\" -US")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{8}}));
}

// If there are multiple AROUND distances, use default 5
// In row 5 of the table, 'global' is 3 away from top and 4 away from song,
// matches neither of the AROUND condition but since there are two distances
// provided, the code falls back to default distance, and return row 5 as a
// match.
TEST_F(SearchTest, MultipleAroundDistanceFallBackToDefault) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "global AROUND(1) top AROUND(2) song")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(SearchTest, SearchIsCaseInsensitive) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "GLObal us song")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

TEST_F(SearchTest, SearchPhraseNotAcrossArrayBoundary) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Tracks_Tokens, "\"track1 track2\"")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

TEST_F(SearchTest, SearchAroundNotAcrossArrayBoundary) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Tracks_Tokens, "track1 AROUND(2) track2")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

TEST_F(SearchTest, EmptyQueryAlwaysReturnFalse) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              // Row 15 has NULL summary so it does not show up in the result
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, NullQueryAlwaysReturnFalse) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, CAST(NULL AS STRING))
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, NullTokenListAlwaysReturnFalse) {
  // Trying to perform search on row 16 (AlbumId = 15), where Summary is null.
  EXPECT_THAT(Query(
                  R"sql(
          SELECT Name
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "US")
            AND UserId = 1
            AND AlbumId > 13
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT Name
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, CAST(NULL AS STRING))
            AND UserId = 1
            AND AlbumId > 13
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, BasicSearchSubstring) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'son')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}, {7}}));
}

TEST_F(SearchTest, BasicSearchSubstringWithTokenlistConcat) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summaries_SubStr_Tokens, 'son')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}, {7}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summaries_SubStr_Tokens, 'foo')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {4}}));
}

TEST_F(SearchTest, BasicSearchSubstringSubstringInMiddle) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'loba')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(SearchTest, BasicSearchSubstringMultiSubstrings) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'loba 100')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{2}}));
}

TEST_F(SearchTest, SearchSubstringWithMatchingPartition) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'son')
            AND UserId = 1
            AND AlbumId > 3
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{4}, {6}}));
}

TEST_F(SearchTest, SearchSubstringWithUnMatchingPartition) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'loba')
            AND UserId = 1
            AND AlbumId > 8
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchSubstringWithWordPrefix) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'son',
                                 relative_search_type=>"word_prefix")
            AND UserId = 1
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'ong',
                                 relative_search_type=>"word_prefix")
            AND UserId = 1
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchSubstringWithWordSuffix) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'son',
                                 relative_search_type=>"word_suffix")
            AND UserId = 1
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'ong',
                                 relative_search_type=>"word_suffix")
            AND UserId = 1
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}}));
}

TEST_F(SearchTest, SearchSubstringWithValuePrefix) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'son',
                                 relative_search_type=>"value_prefix")
            AND UserId = 1
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{4}}));
}

TEST_F(SearchTest, SearchSubstringWithValueSuffix) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'ong',
                                 relative_search_type=>"value_suffix")
            AND UserId = 1
            AND AlbumId < 7
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {6}}));
}

TEST_F(SearchTest, SearchSubstringWithPhrase) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'US to',
                                 relative_search_type=>"phrase")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{7}}));

  // Even though the length of 'us' is less than ngrams_size_min, when
  // considering the leading and trailing space ' us ', it's still a match.
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'us',
                                 relative_search_type=>"phrase")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{4}, {7}}));

  // There's no phrase/word matches "to".
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Relative_All_Tokens,
                                 'to',
                                 relative_search_type=>"phrase")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchTooShortSubstring) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'ba')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'bal us')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchEmptySubstringAlwaysReturnEmpty) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, '')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchNullSubstringAlwaysReturnEmpty) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, CAST(NULL AS STRING))
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchNormalizedEmptySubstring) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, '*^& %- // ')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, BasicSearchNgrams) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_NGRAMS(Name_Ngrams_Tokens, 'rock')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{16}, {17}}));
}

TEST_F(SearchTest, FuzzySearchNgrams) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_NGRAMS(Name_Ngrams_Tokens, 'electronci')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{17}}));
}

TEST_F(SearchTest, FuzzySearchNgramsOnDiffLargerThanMinNgramsReturnsEmpty) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_NGRAMS(Name_Ngrams_Tokens, 'elcetrnoci', min_ngrams=>5)
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchEmptyNgramsAlwaysReturnEmpty) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_NGRAMS(Name_Ngrams_Tokens, '')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchNullNgramsAlwaysReturnEmpty) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_NGRAMS(Name_Ngrams_Tokens, CAST(NULL AS STRING))
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchNormalizedEmptyNgrams) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_NGRAMS(Name_Ngrams_Tokens, '*^& %- // ')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({}));
}

TEST_F(SearchTest, SearchSupportsEnhanceQueryHint) {
  EXPECT_THAT(Query(
                  R"sql(
          @{require_enhance_query=true, enhance_query_timeout_ms=300}
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(Summary_Tokens, "GLObal us song")
            AND UserId = 2
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{5}}));
}

// The logic of SNIPPET, SCORE, and SCORE_NGRAMS functions are different between
// the Cloud Spanner and the Emulator. It is expected that the Cloud Spanner and
// the Emulator return different results for those functions. Thus we only check
// if the call is successful.
TEST_F(SearchTest, BasicScore) {
  ZETASQL_EXPECT_OK(Query(R"sql(
      SELECT SCORE(Summary_Tokens, 'top')
      FROM Albums
      WHERE UserId = 1 AND SEARCH(Summary_Tokens, 'top'))sql"));
}

TEST_F(SearchTest, ScoreFunctionSupportOptionalArguments) {
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE(Summary_Tokens, "top", enhance_query=>true) >= 1
            AND UserId = 1
            AND SEARCH(Summary_Tokens, "top")
          ORDER BY AlbumId ASC)sql"));
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE(Summary_Tokens,
                      "top",
                      enhance_query=>false,
                      language_tag=>"en-us") >= 1
            AND UserId = 1
            AND SEARCH(Summary_Tokens, "top")
          ORDER BY AlbumId ASC)sql"));
}

TEST_F(SearchTest, ScoreFunctionWrongArguments) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE(Summary_Tokens, "top", "en-us") >= 1
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, BasicScoreNgrams) {
  ZETASQL_EXPECT_OK(Query(R"sql(
      SELECT SCORE_NGRAMS(Tracks_Substring_Tokens, "top")
      FROM Albums
      WHERE UserId = 1 AND SEARCH(Summary_Tokens, "top"))sql"));
}

TEST_F(SearchTest, ScoreNgramsFunctionSupportOptionalArguments) {
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE_NGRAMS(Tracks_Substring_Tokens, "top") >= 0.25
            AND UserId = 1
            AND SEARCH(Summary_Tokens, "top")
          ORDER BY AlbumId ASC)sql"));
  ZETASQL_EXPECT_OK(Query(
      R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE_NGRAMS(Tracks_Substring_Tokens,
                      "top",
                      algorithm=>"trigrams",
                      language_tag=>"en-us") >= 0.25
            AND UserId = 1
            AND SEARCH(Summary_Tokens, "top")
          ORDER BY AlbumId ASC)sql"));
}

TEST_F(SearchTest, ScoreNgramsFunctionWrongArguments) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE_NGRAMS(Tracks_Substring_Tokens, "trigrams", "en-us") >= 0.25
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, ScoreNgramsFunctionWrongTokenListType) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SCORE_NGRAMS(Tracks_Tokens, "top") >= 0.25
            AND UserId = 1
            AND SEARCH(Summary_Tokens, "top")
          ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, BasicSnippet) {
  ZETASQL_EXPECT_OK(Query(R"sql(
      SELECT SNIPPET(Summary, 'top')
      FROM Albums
      WHERE UserId = 1)sql"));
}

TEST_F(SearchTest, InvalidMaxSnippets) {
  EXPECT_THAT(Query(R"sql(
      SELECT SNIPPET(Summary, 'top', max_snippets=>2147483648)
      FROM Albums
      WHERE UserId = 1)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Query(R"sql(
      SELECT SNIPPET(Summary, 'top', max_snippets=>-1)
      FROM Albums
      WHERE UserId = 1)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, InvalidMaxSnippetWidth) {
  EXPECT_THAT(Query(R"sql(
      SELECT SNIPPET(Summary, 'top', max_snippet_width=>2147483648)
      FROM Albums
      WHERE UserId = 1)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));

  EXPECT_THAT(Query(R"sql(
      SELECT SNIPPET(Summary, 'top', max_snippet_width=>-1)
      FROM Albums
      WHERE UserId = 1)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, SnippetNamedArgumentShuffledOrder) {
  ZETASQL_EXPECT_OK(Query(R"sql(
      SELECT SNIPPET(language_tag=>'',
                     query=>'foo',
                     max_snippet_width=>10,
                     value=>Summary,
                     enhance_query=>false,
                     max_snippets=>4)
      FROM Albums
      WHERE UserId = 1)sql"));
}

TEST_F(SearchTest, WrongSnippetContentType) {
  EXPECT_THAT(
      Query(R"sql(
      SELECT SNIPPET(Summary,
                     'foo',
                     enhance_query=>false,
                     language_tag=>'',
                     max_snippet_width=>10,
                     max_snippets=>4,
                     content_type=>'unsupported')
      FROM Albums
      WHERE UserId = 1)sql"),
      StatusIs(
          absl::StatusCode::kInvalidArgument,
          HasSubstr(
              "Invalid use of SNIPPET: content_type. \'unsupported\' is not a "
              "supported content type.")));
}

TEST_F(SearchTest, SnippetContentTypeCaseInsensitive) {
  ZETASQL_EXPECT_OK(Query(R"sql(
      SELECT SNIPPET(Summary,
                     'foo',
                     enhance_query=>false,
                     language_tag=>'',
                     max_snippet_width=>10,
                     max_snippets=>4,
                     content_type=>'text/HTML')
      FROM Albums
      WHERE UserId = 1)sql"));
}

TEST_F(SearchTest, SearchWithExistingSearchIndexHint) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
      CREATE SEARCH INDEX Summary_idx
      ON Albums(Summary_Tokens, Summary_SubStr_Tokens))sql"}));
  EXPECT_THAT(Query(
                  R"sql(
      SELECT AlbumId
      FROM Albums@{force_index=Summary_idx}
      WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'son')
        AND UserId = 1
      ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}, {7}}));
}

TEST_F(SearchTest, SearchWithComplicatedSearchIndexHint) {
  ZETASQL_EXPECT_OK(UpdateSchema({
      R"sql(
        CREATE SEARCH INDEX Summary_idx
        ON Albums(Summary_Tokens, Summary_SubStr_Tokens)
        STORING(Length)
        PARTITION BY UserId
        ORDER BY ReleaseTimestamp
        OPTIONS (sort_order_sharding = true, disable_automatic_uid_column=true)
      )sql"}));
  EXPECT_THAT(Query(
                  R"sql(
      SELECT AlbumId
      FROM Albums@{force_index=Summary_idx}
      WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'loba')
        AND UserId = 1
      ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}}));
}

TEST_F(SearchTest, TokenlistConcatInSearch) {
  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH(TOKENLIST_CONCAT([Summary_Tokens, Summary2_Tokens]), "global foo")
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}}));

  EXPECT_THAT(Query(
                  R"sql(
          SELECT AlbumId
          FROM Albums@{force_index=AlbumIndex}
          WHERE SEARCH_SUBSTRING(TOKENLIST_CONCAT([Summary_SubStr_Tokens, Summary2_SubStr_Tokens]), 'son')
            AND UserId = 1
          ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}, {7}}));
}

// The emulator's query_validator does not have the table name when fail the
// hint value check. Thus just try to match a portion of the error message.
TEST_F(SearchTest, SearchWithNonExistingSearchIndexFails) {
  EXPECT_THAT(Query(
                  R"sql(
      SELECT AlbumId
      FROM Albums@{force_index=Summary_idx}
      WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'son')
        AND UserId = 1
      ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("index called Summary_idx")));
}

// The emulator's query_validator does not have the table name when fail the
// hint value check. Thus just try to match a portion of the error message.
TEST_F(SearchTest, SearchFailAfterDropSearchIndex) {
  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
      CREATE SEARCH INDEX Summary_idx
      ON Albums(Summary_Tokens, Summary_SubStr_Tokens))sql"}));
  EXPECT_THAT(Query(R"sql(
      SELECT AlbumId
      FROM Albums@{force_index=Summary_idx}
      WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'son')
        AND UserId = 1
      ORDER BY AlbumId ASC)sql"),
              IsOkAndHoldsRows({{1}, {2}, {4}, {6}, {7}}));

  ZETASQL_EXPECT_OK(UpdateSchema({R"sql(
      DROP SEARCH INDEX Summary_idx)sql"}));
  EXPECT_THAT(Query(R"sql(
      SELECT AlbumId
      FROM Albums@{force_index=Summary_idx}
      WHERE SEARCH_SUBSTRING(Summary_SubStr_Tokens, 'son')
        AND UserId = 1
      ORDER BY AlbumId ASC)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument,
                       HasSubstr("index called Summary_idx")));
}

TEST_F(SearchTest, ProjectTokenlistFailColRef) {
  EXPECT_THAT(Query(R"sql(
      SELECT AlbumId, Length_Tokens
      FROM Albums)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, ProjectTokenlistFailFunctionCall) {
  EXPECT_THAT(Query(R"sql(
      SELECT TOKEN("name1") AS col1
      FROM Albums)sql"),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchTest, ProjectAll) {
  ZETASQL_EXPECT_OK(Query(R"sql(
      SELECT *
      FROM Albums
      WHERE AlbumId = 1)sql"));
}

TEST_F(SearchTest, ArrayIncludesSupported) {
  EXPECT_THAT(Query(R"sql(
    SELECT a.AlbumId
    FROM Albums@{force_index=AlbumIndex} a
    WHERE ARRAY_INCLUDES(a.Tracks, "track2")
      AND a.UserId = 1
    ORDER BY a.AlbumId
  )sql"),
              IsOkAndHoldsRows({{2}, {4}, {6}, {9}}));

  EXPECT_THAT(Query(R"sql(
    SELECT a.AlbumId
    FROM Albums@{force_index=AlbumIndex} a
    WHERE ARRAY_INCLUDES_ANY(a.Tracks, ["track1", "track2"])
      AND a.UserId = 1
    ORDER BY a.AlbumId
  )sql"),
              IsOkAndHoldsRows({{2}, {4}, {6}, {9}, {12}}));

  EXPECT_THAT(Query(R"sql(
    SELECT a.AlbumId
    FROM Albums@{force_index=AlbumIndex} a
    WHERE ARRAY_INCLUDES_ALL(a.Tracks, ["track1", "track2"])
      AND a.UserId = 1
    ORDER BY a.AlbumId
  )sql"),
              IsOkAndHoldsRows({{2}, {4}, {6}, {9}}));
}

TEST_F(SearchTest, ArrayIncludesNullOrEmpty) {
  EXPECT_THAT(Query(R"sql(
    SELECT a.AlbumId
    FROM Albums@{force_index=AlbumIndex} a
    WHERE ARRAY_INCLUDES(a.Tracks, NULL)
      AND a.UserId = 1
    ORDER BY a.AlbumId
  )sql"),
              IsOkAndHoldsRows({}));

  EXPECT_THAT(Query(R"sql(
    SELECT a.AlbumId
    FROM Albums@{force_index=AlbumIndex} a
    WHERE ARRAY_INCLUDES_ANY(a.Tracks, NULL)
      AND a.UserId = 1
    ORDER BY a.AlbumId
  )sql"),
              IsOkAndHoldsRows({}));

  EXPECT_THAT(Query(R"sql(
    SELECT a.AlbumId
    FROM Albums@{force_index=AlbumIndex} a
    WHERE ARRAY_INCLUDES_ANY(a.Tracks, [])
      AND a.UserId = 1
    ORDER BY a.AlbumId
  )sql"),
              IsOkAndHoldsRows({}));
}

static constexpr char kSearchQueryFormat[] = R"sql(
      SELECT a.Name
      FROM SqlSearchCountries$0 a
      WHERE $1)sql";

static constexpr char kSearchIndexHint[] =
    "@{force_index=SqlSearchCountriesIndex}";

static constexpr char kSearchPredicate[] = "SEARCH(a.Name_Tokens, 'foo')";
static constexpr char kNonSearchPrediate[] = "CountryId = 1";

static constexpr char kAllowSearchInTransactionHint[] =
    R"sql(@{allow_search_indexes_in_transaction=true})sql";

struct SearchInTransactionQuery {
  bool has_index_hint = false;
  bool has_search = false;
  bool read_only = false;
  bool with_allow_search_in_transaction_hint = false;
  bool expect_error = false;
};

class SearchInTransactionTest
    : public DatabaseTest,
      public testing::WithParamInterface<SearchInTransactionQuery> {
 public:
  static std::vector<SearchInTransactionQuery> GetTransactionTests() {
    return {
        {.has_index_hint = false, .has_search = true, .read_only = true},
        {.has_index_hint = true, .has_search = true, .read_only = true},
        {.has_index_hint = false, .has_search = true, .expect_error = true},
        {.has_index_hint = false,
         .has_search = true,
         .with_allow_search_in_transaction_hint = true},
        {.has_index_hint = true, .has_search = true, .expect_error = true},
        {.has_index_hint = true,
         .has_search = true,
         .with_allow_search_in_transaction_hint = true},
        {.has_index_hint = true, .has_search = false, .read_only = true},
        {.has_index_hint = true, .has_search = false, .expect_error = true},
        {.has_index_hint = true,
         .has_search = false,
         .with_allow_search_in_transaction_hint = true},
    };
  }

  absl::Status SetUpDatabase() override {
    ZETASQL_RETURN_IF_ERROR(SetSchema({
        R"sql(
        CREATE TABLE SqlSearchCountries (
          CountryId INT64 NOT NULL,
          Name STRING(MAX),
          Population INT64,
          Name_Tokens TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) STORED HIDDEN,
          Name_Substring_Tokens TOKENLIST AS (TOKENIZE_SUBSTRING(Name)) STORED HIDDEN,
          Population_Tokens TOKENLIST AS (TOKENIZE_NUMBER(Population)) STORED HIDDEN,
          ) PRIMARY KEY(CountryId)
        )sql"}));
    return SetSchema({
        R"sql(
          CREATE SEARCH INDEX SqlSearchCountriesIndex ON
          SqlSearchCountries(Name_Tokens,
                             Name_Substring_Tokens,
                             Population_Tokens)
          OPTIONS (sort_order_sharding = true)
        )sql",
    });
  }
};

TEST_P(SearchInTransactionTest, SearchInTransactionalQuery) {
  const SearchInTransactionQuery& test = GetParam();
  auto txn = test.read_only ? Transaction(Transaction::ReadOnlyOptions())
                            : Transaction(Transaction::ReadWriteOptions());
  std::string query = absl::Substitute(
      kSearchQueryFormat, test.has_index_hint ? kSearchIndexHint : "",
      test.has_search ? kSearchPredicate : kNonSearchPrediate);
  query = test.with_allow_search_in_transaction_hint
              ? absl::StrCat(kAllowSearchInTransactionHint, query)
              : query;
  auto query_result = QueryTransaction(txn, query);
  if (test.expect_error) {
    EXPECT_THAT(query_result, StatusIs(absl::StatusCode::kInvalidArgument));
  } else {
    ZETASQL_EXPECT_OK(query_result);
  }
}

INSTANTIATE_TEST_SUITE_P(
    SearchInTransactionalQuery, SearchInTransactionTest,
    testing::ValuesIn(SearchInTransactionTest::GetTransactionTests()));

TEST_P(SearchInTransactionTest, SearchInBatchQuery) {
  const SearchInTransactionQuery& test = GetParam();
  Transaction txn{Transaction::ReadOnlyOptions{}};
  std::string query = absl::Substitute(
      kSearchQueryFormat, test.has_index_hint ? kSearchIndexHint : "",
      test.has_search ? kSearchPredicate : kNonSearchPrediate);
  query = test.with_allow_search_in_transaction_hint
              ? absl::StrCat(kAllowSearchInTransactionHint, query)
              : query;
  auto query_result = PartitionQuery(txn, query);
  EXPECT_THAT(query_result, StatusIs(absl::StatusCode::kInvalidArgument));
}

INSTANTIATE_TEST_SUITE_P(
    SearchInBatchQuery, SearchInTransactionTest,
    testing::ValuesIn(SearchInTransactionTest::GetTransactionTests()));

static constexpr char kQueryWithoutSearch[] = R"sql(
      SELECT a.Name
      FROM SqlSearchCountries a
      WHERE name = 'foo')sql";

TEST_F(SearchInTransactionTest, NonSearchQuerySucceedInTransaction) {
  auto txn = Transaction(Transaction::ReadWriteOptions());
  ZETASQL_EXPECT_OK(QueryTransaction(txn, kQueryWithoutSearch));

  txn = Transaction(Transaction::ReadOnlyOptions());
  ZETASQL_EXPECT_OK(PartitionQuery(txn, kQueryWithoutSearch));
}

TEST_F(SearchInTransactionTest, SearchNotSupportedInPartitionedDML) {
  EXPECT_THAT(ExecutePartitionedDml(
                  SqlStatement("UPDATE SqlSearchCountries SET Name = NULL "
                               "WHERE SEARCH_SUBSTRING(Name_Tokens, 'foo')")),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

TEST_F(SearchInTransactionTest, SearchNotSupportedInBatchDML) {
  EXPECT_THAT(CommitBatchDml(
                  {SqlStatement("UPDATE SqlSearchCountries SET Name = NULL "
                                "WHERE SEARCH_SUBSTRING(Name_Tokens, 'foo')")}),
              StatusIs(absl::StatusCode::kInvalidArgument));
}

struct TokenizeNumberParametersTestCase {
  std::string query;
  Matcher<const std::string&> expected_error;
};

class TokenizeNumberParametersTest
    : public DatabaseTest,
      public testing::WithParamInterface<TokenizeNumberParametersTestCase> {
 public:
  absl::Status SetUpDatabase() override { return absl::OkStatus(); }

  static std::vector<TokenizeNumberParametersTestCase>
  GetTokenizeNumberErrorTests() {
    return {
        {.query = "SELECT TOKENIZE_NUMBER(999, comparison_type=>'compare')",
         .expected_error =
             HasSubstr("TOKENIZE_NUMBER: Unsupported comparison type: "
                       "'compare'")},
        {.query = "SELECT TOKENIZE_NUMBER(999, algorithm=>'linear')",
         .expected_error = ContainsRegex(
             "TOKENIZE_NUMBER: Algorithm is not supported: 'linear'; "
             "supported algorithms are: 'auto', 'logtree', 'prefixtree', "
             "'floatingpoint' \\(floatingpoint is supported only for floating "
             "point types\\)")},
        {.query = "SELECT TOKENIZE_NUMBER(999, tree_base=>11)",
         .expected_error =
             HasSubstr("TOKENIZE_NUMBER: tree_base must be in the range "
                       "[2, 10], got: 11")},
        {.query = "SELECT TOKENIZE_NUMBER(2e255, min=>CAST('inf' AS FLOAT64))",
         .expected_error =
             HasSubstr("TOKENIZE_NUMBER: min must be finite, got: inf")},
        {.query = "SELECT TOKENIZE_NUMBER(2e255, max=>CAST('nan' AS FLOAT64))",
         .expected_error =
             HasSubstr("TOKENIZE_NUMBER: max must be finite, got: nan")},
        {.query = "SELECT TOKENIZE_NUMBER(2, min=>5, max=>-1)",
         .expected_error = HasSubstr(
             "TOKENIZE_NUMBER: min must be less than max, got: 5 and -1")},
        {.query = "SELECT TOKENIZE_NUMBER(2e255, granularity=>CAST('inf' AS "
                  "FLOAT64))",
         .expected_error =
             HasSubstr("TOKENIZE_NUMBER: granularity must be finite and "
                       "positive, got: inf")},
        {.query = "SELECT TOKENIZE_NUMBER(5, min=>1, max=>10, granularity=>11)",
         .expected_error = HasSubstr(
             "TOKENIZE_NUMBER: granularity (11) must be less than or equal "
             "to the difference between min and max (1, 10)")},
        {.query = "SELECT TOKENIZE_NUMBER(2.567, min=>-1.0, max=>5.0, "
                  "algorithm=>'floatingpoint', precision=>16)",
         .expected_error = ContainsRegex("TOKENIZE_NUMBER: precision must be "
                                         "in the range \\[1, 15\\], got: 16")},
        {.query = "SELECT TOKENIZE_NUMBER(2, min=>-1, max=>5, "
                  "algorithm=>'floatingpoint');",
         .expected_error =
             ContainsRegex("TOKENIZE_NUMBER: algorithm 'floatingpoint' can "
                           "only be used to tokenize floating point values")},
        {.query = "SELECT TOKENIZE_NUMBER(0, algorithm=>'logtree', "
                  "precision=>-1)",
         .expected_error = ContainsRegex("TOKENIZE_NUMBER: precision must be "
                                         "in the range \\[1, 15\\], got: -1")}};
  }
};

TEST_P(TokenizeNumberParametersTest, TokenizeNumberErrorTest) {
  const TokenizeNumberParametersTestCase& test = GetParam();
  EXPECT_THAT(Query(test.query), StatusIs(absl::StatusCode::kInvalidArgument,
                                          test.expected_error));
}

INSTANTIATE_TEST_SUITE_P(
    TokenizeNumberErrorTest, TokenizeNumberParametersTest,
    testing::ValuesIn(
        TokenizeNumberParametersTest::GetTokenizeNumberErrorTests()));
}  // namespace

}  // namespace test
}  // namespace emulator
}  // namespace spanner
}  // namespace google
