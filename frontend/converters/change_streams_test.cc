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

#include "frontend/converters/change_streams.h"

#include <optional>
#include <string>
#include <vector>

#include "google/spanner/v1/result_set.pb.h"
#include "google/spanner/v1/spanner.pb.h"
#include "zetasql/public/types/type.h"
#include "zetasql/public/types/type_factory.h"
#include "zetasql/public/value.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "zetasql/base/testing/status_matchers.h"
#include "tests/common/proto_matchers.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "frontend/converters/chunking.h"
#include "tests/common/change_streams.h"
#include "tests/common/chunking.h"
#include "tests/common/row_cursor.h"
namespace google {
namespace spanner {
namespace emulator {
namespace frontend {
namespace {

using ::google::spanner::emulator::test::TestRowCursor;
using ::google::spanner::v1::PartialResultSet;
using zetasql::types::BoolType;
using zetasql::types::Int64Type;
using zetasql::types::JsonArrayType;
using zetasql::types::StringArrayType;
using zetasql::types::StringType;
using zetasql::types::TimestampType;
using zetasql::values::Bool;
using zetasql::values::Int64;
using zetasql::values::Json;
using zetasql::values::String;
using zetasql::values::Timestamp;
using testing::ElementsAre;
using test::EqualsProto;
using test::proto::Partially;

class ChangeStreamResultConverterTest : public testing::Test {
 protected:
  void SetUp() override {
    now_ = absl::Now();
    one_min_from_now_ = absl::Now() + absl::Minutes(1);
    now_str_ = test::EncodeTimestampString(now_);
    one_min_from_now_str_ = test::EncodeTimestampString(one_min_from_now_);
  }
  google::spanner::v1::ResultSet ConvertPartialResultSetToResultSet(
      PartialResultSet& partial_result) {
    google::spanner::v1::ResultSet result_pb;
    for (const auto& val : partial_result.values()) {
      auto* row_pb = result_pb.add_rows();
      *row_pb->add_values() = val;
    }
    return result_pb;
  }

  absl::Time now_;
  absl::Time one_min_from_now_;
  std::string now_str_;
  std::string one_min_from_now_str_;
};
TEST_F(ChangeStreamResultConverterTest,
       PopulateFixedOutputColumnTypeMetadataForFirstResponse) {
  TestRowCursor cursor({"start_time", "partition_token", "parents"},
                       {TimestampType(), StringType(), StringArrayType()},
                       {{Timestamp(now_), String("token1"),
                         zetasql::Value::EmptyArray(StringArrayType())}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,

                       ConvertPartitionTableRowCursorToStruct(
                           &cursor, now_, /*expect_metadata=*/true));
  EXPECT_THAT(
      results,
      ElementsAre(Partially(EqualsProto(
          R"pb(metadata {
                 row_type {
                   fields {
                     name: "ChangeRecord"
                     type {
                       code: ARRAY
                       array_element_type {
                         code: STRUCT
                         struct_type {
                           fields {
                             name: "data_change_record"
                             type {
                               code: ARRAY
                               array_element_type {
                                 code: STRUCT
                                 struct_type {
                                   fields {
                                     name: "commit_timestamp"
                                     type { code: TIMESTAMP }
                                   }
                                   fields {
                                     name: "record_sequence"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "server_transaction_id"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "is_last_record_in_transaction_in_partition"
                                     type { code: BOOL }
                                   }
                                   fields {
                                     name: "table_name"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "column_types"
                                     type {
                                       code: ARRAY
                                       array_element_type {
                                         code: STRUCT
                                         struct_type {
                                           fields {
                                             name: "name"
                                             type { code: STRING }
                                           }
                                           fields {
                                             name: "type"
                                             type { code: JSON }
                                           }
                                           fields {
                                             name: "is_primary_key"
                                             type { code: BOOL }
                                           }
                                           fields {
                                             name: "ordinal_position"
                                             type { code: INT64 }
                                           }
                                         }
                                       }
                                     }
                                   }
                                   fields {
                                     name: "mods"
                                     type {
                                       code: ARRAY
                                       array_element_type {
                                         code: STRUCT
                                         struct_type {
                                           fields {
                                             name: "keys"
                                             type { code: JSON }
                                           }
                                           fields {
                                             name: "new_values"
                                             type { code: JSON }
                                           }
                                           fields {
                                             name: "old_values"
                                             type { code: JSON }
                                           }
                                         }
                                       }
                                     }
                                   }
                                   fields {
                                     name: "mod_type"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "value_capture_type"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "number_of_records_in_transaction"
                                     type { code: INT64 }
                                   }
                                   fields {
                                     name: "number_of_partitions_in_transaction"
                                     type { code: INT64 }
                                   }
                                   fields {
                                     name: "transaction_tag"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "is_system_transaction"
                                     type { code: BOOL }
                                   }
                                 }
                               }
                             }
                           }
                           fields {
                             name: "heartbeat_record"
                             type {
                               code: ARRAY
                               array_element_type {
                                 code: STRUCT
                                 struct_type {
                                   fields {
                                     name: "timestamp"
                                     type { code: TIMESTAMP }
                                   }
                                 }
                               }
                             }
                           }
                           fields {
                             name: "child_partitions_record"
                             type {
                               code: ARRAY
                               array_element_type {
                                 code: STRUCT
                                 struct_type {
                                   fields {
                                     name: "start_timestamp"
                                     type { code: TIMESTAMP }
                                   }
                                   fields {
                                     name: "record_sequence"
                                     type { code: STRING }
                                   }
                                   fields {
                                     name: "child_partitions"
                                     type {
                                       code: ARRAY
                                       array_element_type {
                                         code: STRUCT
                                         struct_type {
                                           fields {
                                             name: "token"
                                             type { code: STRING }
                                           }
                                           fields {
                                             name: "parent_partition_tokens"
                                             type {
                                               code: ARRAY
                                               array_element_type {
                                                 code: STRING
                                               }
                                             }
                                           }
                                         }
                                       }
                                     }
                                   }
                                 }
                               }
                             }
                           }
                         }
                       }
                     }
                   }
                 }
               })pb"))));
}

TEST_F(ChangeStreamResultConverterTest,
       ConvertInitialPartitionTableRowCursorToMultipleChangeRecordsResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(StringArrayType(), {String("parent1")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val2,
      zetasql::Value::MakeArray(StringArrayType(), {String("parent2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val},
       {Timestamp(now_), String("token2"), parents_array_val2}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToStruct(
                           &cursor, /*initial_start_time=*/one_min_from_now_,
                           /*expect_metadata=*/true));
  EXPECT_THAT(results,
              ElementsAre(Partially(EqualsProto(absl::Substitute(
                  R"pb(
                    values {
                      list_value {
                        values {
                          list_value {
                            values { list_value {} }
                            values { list_value {} }
                            values {
                              list_value {
                                values {
                                  list_value {
                                    values { string_value: "$0" }
                                    values { string_value: "00000000" }
                                    values {
                                      list_value {
                                        values {
                                          list_value {
                                            values { string_value: "token1" }
                                            values { list_value {} }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    values {
                      list_value {
                        values {
                          list_value {
                            values { list_value {} }
                            values { list_value {} }
                            values {
                              list_value {
                                values {
                                  list_value {
                                    values { string_value: "$0" }
                                    values { string_value: "00000001" }
                                    values {
                                      list_value {
                                        values {
                                          list_value {
                                            values { string_value: "token2" }
                                            values { list_value {} }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  )pb",
                  one_min_from_now_str_)))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 2);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(ChangeStreamResultConverterTest,
       ConvertMoveEventPartitionTableRowCursorToChangeRecordResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(StringArrayType(), {String("move_token1")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToStruct(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*expect_metadata=*/false));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(
            values {
              list_value {
                values {
                  list_value {
                    values { list_value {} }
                    values { list_value {} }
                    values {
                      list_value {
                        values {
                          list_value {
                            values { string_value: "$0" }
                            values { string_value: "00000000" }
                            values {
                              list_value {
                                values {
                                  list_value {
                                    values { string_value: "token1" }
                                    values {
                                      list_value {
                                        values { string_value: "move_token1" }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            resume_token: "$1"
          )pb",
          now_str_, kChangeStreamDummyResumeToken))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(ChangeStreamResultConverterTest,
       ConvertMergeEventPartitionTableRowCursorToChangeRecordResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(
          StringArrayType(), {String("merge_token1"), String("merge_token2")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("token1"), parents_array_val}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToStruct(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*expect_metadata=*/false));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(
            values {
              list_value {
                values {
                  list_value {
                    values { list_value {} }
                    values { list_value {} }
                    values {
                      list_value {
                        values {
                          list_value {
                            values { string_value: "$0" }
                            values { string_value: "00000000" }
                            values {
                              list_value {
                                values {
                                  list_value {
                                    values { string_value: "token1" }
                                    values {
                                      list_value {
                                        values { string_value: "merge_token1" }
                                        values { string_value: "merge_token2" }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            resume_token: "$1"
          )pb",
          now_str_, kChangeStreamDummyResumeToken))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(ChangeStreamResultConverterTest,
       ConvertSplitEventPartitionTableRowCursorToChangeRecordResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto parents_array_val,
      zetasql::Value::MakeArray(StringArrayType(), {String("parent_token")}));
  TestRowCursor cursor(
      {"start_time", "partition_token", "parents"},
      {TimestampType(), StringType(), StringArrayType()},
      {{Timestamp(now_), String("split_token1"), parents_array_val},
       {Timestamp(now_), String("split_token2"), parents_array_val}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results,
                       ConvertPartitionTableRowCursorToStruct(
                           &cursor, /*initial_start_time=*/std::nullopt,
                           /*need_metadata=*/false));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(
            values {
              list_value {
                values {
                  list_value {
                    values { list_value {} }
                    values { list_value {} }
                    values {
                      list_value {
                        values {
                          list_value {
                            values { string_value: "$0" }
                            values { string_value: "00000000" }
                            values {
                              list_value {
                                values {
                                  list_value {
                                    values { string_value: "split_token1" }
                                    values {
                                      list_value {
                                        values { string_value: "parent_token" }
                                      }
                                    }
                                  }
                                }
                                values {
                                  list_value {
                                    values { string_value: "split_token2" }
                                    values {
                                      list_value {
                                        values { string_value: "parent_token" }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            resume_token: "$1"
          )pb",
          now_str_, kChangeStreamDummyResumeToken))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 1);
  ASSERT_EQ(
      change_recods.child_partition_records[0].child_partitions.values_size(),
      2);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(ChangeStreamResultConverterTest,
       HearbeatTimestampToChangeRecordResultSetProto) {
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      results,
      google::spanner::emulator::frontend::ConvertHeartbeatTimestampToStruct(
          now_));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(values {
                 list_value {
                   values {
                     list_value {
                       values { list_value {} }
                       values {
                         list_value {
                           values {
                             list_value { values { string_value: "$0" } }
                           }
                         }
                       }
                       values { list_value {} }
                     }
                   }
                 }
               }
               resume_token: "$1"
          )pb",
          now_str_, kChangeStreamDummyResumeToken))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 1);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(ChangeStreamResultConverterTest, ParseChunkedPartialResultSetProto) {
  const size_t kChunkSize = 40;
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      results,
      google::spanner::emulator::frontend::ConvertHeartbeatTimestampToStruct(
          now_));
  auto result_set = ConvertPartialResultSetToResultSet(results[0]);
  ZETASQL_ASSERT_OK_AND_ASSIGN(std::vector<PartialResultSet> chunked_results,
                       ChunkResultSet(result_set, kChunkSize));
  EXPECT_EQ(chunked_results.size(), 2);
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto chunked_result_set,
      backend::test::MergePartialResultSets(results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      test::ChangeStreamRecords change_recods,
      test::GetChangeStreamRecordsFromResultSet(chunked_result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 1);
  ASSERT_EQ(change_recods.data_change_records.size(), 0);
}

TEST_F(ChangeStreamResultConverterTest,
       ConvertNewValuesDataTableRowCursorToChangeRecordResultSet) {
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_name_arr_val,
      zetasql::Value::MakeArray(StringArrayType(),
                                  {String("IsPrimaryUser"), String("UserId")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_type_arr_val,
      zetasql::Value::MakeArray(
          StringArrayType(),
          {String("{\"code\":\"BOOL\"}"), String("{\"code\":\"STRING\"}")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_is_primary_key_arr_val,
      zetasql::Value::MakeArray(zetasql::types::BoolArrayType(),
                                  {Bool(false), Bool(true)}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto col_types_ordinal_position_arr_val,
      zetasql::Value::MakeArray(zetasql::types::Int64ArrayType(),
                                  {Int64(1), Int64(2)}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto mods_keys,
      zetasql::Value::MakeArray(StringArrayType(),
                                  {String("{\"UserId\": \"User2\"}"),
                                   String("{\"UserId\": \"User2\"}")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(
      auto mods_new_values,
      zetasql::Value::MakeArray(
          StringArrayType(),
          {String("{\"IsPrimaryUser\": true,\"UserId\": \"User2\"}"),
           String("{\"IsPrimaryUser\": false}")}));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto mods_old_values,
                       zetasql::Value::MakeArray(
                           StringArrayType(), {String("{}"), String("{}")}));
  TestRowCursor cursor(
      {"partition_token", "commit_timestamp", "server_transaction_id",
       "record_sequence", "is_last_record_in_transaction_in_partition",
       "table_name", "column_types_name", "column_types_type",
       "column_type_is_primary_key", "column_types_ordinal_position",
       "mods_keys", "mods_new_values", "mods_old_values", "mod_type",
       "value_capture_type", "number_of_records_in_transaction",
       "number_of_partitions_in_transaction", "transaction_tag",
       "is_system_transaction"},
      {StringType(), TimestampType(), StringType(), StringType(), BoolType(),
       StringType(), StringType(), StringType(), BoolType(), Int64Type(),
       StringType(), StringType(), StringType(), StringType(), StringType(),
       Int64Type(), Int64Type(), StringType(), BoolType()},
      {{String("test_token"), Timestamp(now_), String("test_id"),
        String("00000001"), Bool(false), String("test_table"),
        col_types_name_arr_val, col_types_type_arr_val,
        col_types_is_primary_key_arr_val, col_types_ordinal_position_arr_val,
        mods_keys, mods_new_values, mods_old_values, String("UPDATE"),
        String("NEW_VALUES"), Int64(3), Int64(2), String("test_tag"),
        Bool(false)}});
  std::vector<PartialResultSet> results;
  ZETASQL_ASSERT_OK_AND_ASSIGN(results, ConvertDataTableRowCursorToStruct(&cursor));
  EXPECT_THAT(
      results,
      ElementsAre(EqualsProto(absl::Substitute(
          R"pb(values {
                 list_value {
                   values {
                     list_value {
                       values {
                         list_value {
                           values {
                             list_value {
                               values { string_value: "$0" }
                               values { string_value: "00000001" }
                               values { string_value: "test_id" }
                               values { bool_value: false }
                               values { string_value: "test_table" }
                               values {
                                 list_value {
                                   values {
                                     list_value {
                                       values { string_value: "IsPrimaryUser" }
                                       values {
                                         string_value: "{\"code\":\"BOOL\"}"
                                       }
                                       values { bool_value: false }
                                       values { string_value: "1" }
                                     }
                                   }
                                   values {
                                     list_value {
                                       values { string_value: "UserId" }
                                       values {
                                         string_value: "{\"code\":\"STRING\"}"
                                       }
                                       values { bool_value: true }
                                       values { string_value: "2" }
                                     }
                                   }
                                 }
                               }
                               values {
                                 list_value {
                                   values {
                                     list_value {
                                       values {
                                         string_value: "{\"UserId\":\"User2\"}"
                                       }
                                       values {
                                         string_value: "{\"IsPrimaryUser\":true,\"UserId\":\"User2\"}"
                                       }
                                       values { string_value: "{}" }
                                     }
                                   }
                                   values {
                                     list_value {
                                       values {
                                         string_value: "{\"UserId\":\"User2\"}"
                                       }
                                       values {
                                         string_value: "{\"IsPrimaryUser\":false}"
                                       }
                                       values { string_value: "{}" }
                                     }
                                   }
                                 }
                               }
                               values { string_value: "UPDATE" }
                               values { string_value: "NEW_VALUES" }
                               values { string_value: "3" }
                               values { string_value: "2" }
                               values { string_value: "test_tag" }
                               values { bool_value: false }
                             }
                           }
                         }
                       }
                       values { list_value {} }
                       values { list_value {} }
                     }
                   }
                 }
               }
               resume_token: "$1"
          )pb",
          now_str_, kChangeStreamDummyResumeToken))));
  ZETASQL_ASSERT_OK_AND_ASSIGN(auto result_set, backend::test::MergePartialResultSets(
                                            results, /*columns_per_row=*/1));
  ZETASQL_ASSERT_OK_AND_ASSIGN(test::ChangeStreamRecords change_recods,
                       test::GetChangeStreamRecordsFromResultSet(result_set));
  ASSERT_EQ(change_recods.child_partition_records.size(), 0);
  ASSERT_EQ(change_recods.heartbeat_records.size(), 0);
  ASSERT_EQ(change_recods.data_change_records.size(), 1);
}

}  // namespace

}  // namespace frontend
}  // namespace emulator
}  // namespace spanner
}  // namespace google
