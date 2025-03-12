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

#include <iostream>
#include <string>

#include "absl/flags/parse.h"
#include "zetasql/base/logging.h"
#include "google/spanner/admin/database/v1/common.pb.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "riegeli/bytes/cfile_reader.h"
#include "riegeli/csv/csv_reader.h"
#include "riegeli/csv/csv_record.h"

using ::google::spanner::admin::database::v1::DatabaseDialect;
using ::riegeli::CsvReader;
using ::riegeli::CsvReaderBase;
using ::riegeli::CsvRecord;
using ::riegeli::CFileReader;

static constexpr char kCsvSeparator = ',';

std::string GetMetadataFileByDialect(const DatabaseDialect& dialect,
                                     absl::string_view gsql_file,
                                     absl::string_view pg_file) {
  std::string metadata_file;
  switch (dialect) {
    case DatabaseDialect::DATABASE_DIALECT_UNSPECIFIED:
    case DatabaseDialect::GOOGLE_STANDARD_SQL:
      metadata_file = gsql_file;
      break;
    case DatabaseDialect::POSTGRESQL:
      metadata_file = pg_file;
      break;
    default:
      // Should never get here.
      break;
  }

  constexpr absl::string_view kInfoSchemaColumnsMetadataPath =
      "backend/query/";
  return absl::StrCat(kInfoSchemaColumnsMetadataPath, metadata_file);
}

// This is used to create c++ code which are used to populate information
// schema. It reads information schema data from csv files and
// populates a header file with a static vector.

std::string PopulateInfoSchemaColumnsMetadata(const DatabaseDialect& dialect) {
  std::string prefix = "";
  if (dialect == DatabaseDialect::POSTGRESQL) {
    prefix = "PG";
  }
  std::string metadata_code = absl::Substitute(
      R"(inline const std::vector<ColumnsMetaEntry>& $0ColumnsMetadata() {
  // clang-format off
  static const zetasql_base::NoDestructor<std::vector<ColumnsMetaEntry>>
      kColumnsMetadata({
)",
      prefix);

  constexpr absl::string_view kGSQLInfoSchemaColumnsMetadata =
      "info_schema_columns_metadata.csv";
  constexpr absl::string_view kPGInfoSchemaColumnsMetadata =
      "pg_info_schema_columns_metadata.csv";
  std::string metadata_file = GetMetadataFileByDialect(
      dialect, kGSQLInfoSchemaColumnsMetadata, kPGInfoSchemaColumnsMetadata);
  CsvReaderBase::Options options;
  options.set_field_separator(kCsvSeparator);
  options.set_required_header(
      {"table_name", "column_name", "is_nullable", "spanner_type"});
  options.set_comment('#');
  CFileReader file_reader = CFileReader(metadata_file);
  CsvReader csv_reader(&file_reader, options);
  ZETASQL_VLOG(csv_reader.status().ok())
      << "Error reading csv file:" << csv_reader.status();
  absl::StrAppend(&metadata_code, "  // NOLINTBEGIN(whitespace/line_length)\n");
  for (CsvRecord record; csv_reader.ReadRecord(record);) {
    std::string table_name = absl::StrCat("\"", record["table_name"], "\"");
    std::string column_name = absl::StrCat("\"", record["column_name"], "\"");
    std::string is_nullable = absl::StrCat("\"", record["is_nullable"], "\"");
    std::string spanner_type = absl::StrCat("\"", record["spanner_type"], "\"");
    absl::StrAppend(&metadata_code, "    {", table_name, ", ", column_name,
                    ", ", is_nullable, ", ", spanner_type, "},\n");
  }
  absl::StrAppend(&metadata_code, R"(  });
  // NOLINTEND(whitespace/line_length)
  // clang-format on
    return *kColumnsMetadata;
}

)");
  ZETASQL_VLOG(csv_reader.Close()) << csv_reader.status();
  return metadata_code;
}

std::string PopulateInfoSchemaColumnsMetadataForIndex(
    const DatabaseDialect& dialect) {
  std::string prefix = "";
  if (dialect == DatabaseDialect::POSTGRESQL) {
    prefix = "PG";
  }
  std::string metadata_for_index_code = absl::Substitute(
      R"(inline const std::vector<IndexColumnsMetaEntry>& $0IndexColumnsMetadata() {
  // clang-format off
  static const zetasql_base::NoDestructor<std::vector<IndexColumnsMetaEntry>>
      kColumnsMetadataForIndex({
)",
      prefix);

  constexpr absl::string_view kGSQLInfoSchemaColumnsMetadataForIndex =
      "info_schema_columns_metadata_for_index.csv";
  constexpr absl::string_view kPGInfoSchemaColumnsMetadataForIndex =
      "pg_info_schema_columns_metadata_for_index.csv";
  std::string metadata_file =
      GetMetadataFileByDialect(dialect, kGSQLInfoSchemaColumnsMetadataForIndex,
                               kPGInfoSchemaColumnsMetadataForIndex);
  CsvReaderBase::Options options;
  options.set_field_separator(kCsvSeparator);
  options.set_required_header({"table_name", "column_name", "is_nullable",
                               "column_ordering", "spanner_type",
                               "ordinal_position"});
  options.set_comment('#');
  CFileReader file_reader = CFileReader(metadata_file);
  CsvReader csv_reader(&file_reader, options);
  ZETASQL_VLOG(csv_reader.status().ok())
      << "Error reading csv file:" << csv_reader.status();
  absl::StrAppend(&metadata_for_index_code,
                  "  // NOLINTBEGIN(whitespace/line_length)\n");

  for (CsvRecord record; csv_reader.ReadRecord(record);) {
    std::string table_name = absl::StrCat("\"", record["table_name"], "\"");
    std::string column_name = absl::StrCat("\"", record["column_name"], "\"");
    std::string is_nullable = absl::StrCat("\"", record["is_nullable"], "\"");
    std::string column_ordering =
        absl::StrCat("\"", record["column_ordering"], "\"");
    std::string spanner_type = absl::StrCat("\"", record["spanner_type"], "\"");
    std::string ordinal_position = record["ordinal_position"];

    absl::StrAppend(&metadata_for_index_code, "    {", table_name, ", ",
                    column_name, ", ", is_nullable, ", ", column_ordering, ", ",
                    spanner_type, ", ", ordinal_position, "},\n");
  }
  absl::StrAppend(&metadata_for_index_code, R"(  });
  // NOLINTEND(whitespace/line_length)
  // clang-format on
    return *kColumnsMetadataForIndex;
}

)");

  ZETASQL_VLOG(csv_reader.Close()) << csv_reader.status();
  return metadata_for_index_code;
}

std::string PopulateSpannerSysColumnsMetadata() {
  std::string metadata_code =
      R"(struct SpannerSysColumnsMetaEntry {
  const char* table_name;
  const char* column_name;
  const char* is_nullable;
  const char* spanner_type;
  int primary_key_ordinal;
};

inline const std::vector<SpannerSysColumnsMetaEntry>&
SpannerSysColumnsMetadata() {
  // clang-format off
  static const zetasql_base::NoDestructor<std::vector<SpannerSysColumnsMetaEntry>>
      kSpannerSysColumnsMetadata({
)";

  // clang-format off
  constexpr absl::string_view kSpannerSysColumnsMetadata =
      "backend/query/spanner_sys_columns_metadata.csv"; // NOLINT
  // clang-format on
  CsvReaderBase::Options options;
  options.set_field_separator(kCsvSeparator);
  options.set_required_header({"table_name", "column_name", "is_nullable",
                               "spanner_type", "ordinal_position"});
  CFileReader file_reader = CFileReader(kSpannerSysColumnsMetadata);
  CsvReader csv_reader(&file_reader, options);
  ZETASQL_VLOG(csv_reader.status().ok())
      << "Error reading csv file:" << csv_reader.status();
  absl::StrAppend(&metadata_code, "  // NOLINTBEGIN(whitespace/line_length)\n");
  for (CsvRecord record; csv_reader.ReadRecord(record);) {
    std::string table_name = absl::StrCat("\"", record["table_name"], "\"");
    std::string column_name = absl::StrCat("\"", record["column_name"], "\"");
    std::string is_nullable = absl::StrCat("\"", record["is_nullable"], "\"");
    std::string spanner_type = absl::StrCat("\"", record["spanner_type"], "\"");
    std::string ordinal_position = record["ordinal_position"];
    absl::StrAppend(&metadata_code, "    {", table_name, ", ", column_name,
                    ", ", is_nullable, ", ", spanner_type, ", ",
                    ordinal_position, "},\n");
  }
  absl::StrAppend(&metadata_code, R"(  });
  // NOLINTEND(whitespace/line_length)
  // clang-format on
    return *kSpannerSysColumnsMetadata;
}

)");
  ZETASQL_VLOG(csv_reader.Close()) << csv_reader.status();
  return metadata_code;
}

std::string PopulatePGCatalogColumnsMetadata() {
  std::string metadata_code =
      R"(inline const std::vector<ColumnsMetaEntry>& PGCatalogColumnsMetadata() {
  // clang-format off
  static const zetasql_base::NoDestructor<std::vector<ColumnsMetaEntry>>
      kPGCatalogColumnsMetadata({
)";

  // clang-format off
  constexpr absl::string_view kPGCatalogColumnsMetadata =
      "backend/query/pg_catalog_columns_metadata.csv"; // NOLINT
  // clang-format on
  CsvReaderBase::Options options;
  options.set_field_separator(kCsvSeparator);
  options.set_required_header(
      {"table_name", "column_name", "is_nullable", "spanner_type"});
  CFileReader file_reader = CFileReader(kPGCatalogColumnsMetadata);
  CsvReader csv_reader(&file_reader, options);
  ZETASQL_VLOG(csv_reader.status().ok())  // crash ok
      << "Error reading csv file:" << csv_reader.status();
  absl::StrAppend(&metadata_code, "  // NOLINTBEGIN(whitespace/line_length)\n");
  for (CsvRecord record; csv_reader.ReadRecord(record);) {
    std::string table_name = absl::StrCat("\"", record["table_name"], "\"");
    std::string column_name = absl::StrCat("\"", record["column_name"], "\"");
    std::string is_nullable = absl::StrCat("\"", record["is_nullable"], "\"");
    std::string spanner_type = absl::StrCat("\"", record["spanner_type"], "\"");
    absl::StrAppend(&metadata_code, "    {", table_name, ", ", column_name,
                    ", ", is_nullable, ", ", spanner_type, "},\n");
  }
  absl::StrAppend(&metadata_code, R"(  });
  // NOLINTEND(whitespace/line_length)
  // clang-format on
    return *kPGCatalogColumnsMetadata;
}

)");
  ZETASQL_VLOG(csv_reader.Close()) << csv_reader.status();  // crash ok
  return metadata_code;
}

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  constexpr absl::string_view kTemplate =
      R"(#ifndef $0
#define $0

#include <string>
#include <vector>

#include "zetasql/base/no_destructor.h"

// WARNING -  DO NOT EDIT
// AUTOGENERATED FILE USING BUILD RULE:
// populate_info_schema_columns_metadata

namespace google::spanner::emulator::backend {

struct ColumnsMetaEntry {
  const char* table_name;
  const char* column_name;
  const char* is_nullable;
  const char* spanner_type;
};

struct IndexColumnsMetaEntry {
  const char* table_name;
  const char* column_name;
  const char* is_nullable;
  const char* column_ordering;
  const char* spanner_type;
  int primary_key_ordinal = 0;
};

$1
$2
$3
$4
$5
$6
}  // namespace google::spanner::emulator::backend

#endif  // $0

)";

  std::cout << absl::Substitute(
      kTemplate,
      "THIRD_PARTY_CLOUD_SPANNER_EMULATOR_BACKEND_QUERY_INFO_SCHEMA_",
      PopulateInfoSchemaColumnsMetadata(DatabaseDialect::GOOGLE_STANDARD_SQL),
      PopulateInfoSchemaColumnsMetadataForIndex(
          DatabaseDialect::GOOGLE_STANDARD_SQL),
      PopulateInfoSchemaColumnsMetadata(DatabaseDialect::POSTGRESQL),
      PopulateInfoSchemaColumnsMetadataForIndex(DatabaseDialect::POSTGRESQL),
      PopulatePGCatalogColumnsMetadata(),
      PopulateSpannerSysColumnsMetadata());
  return 0;
}
