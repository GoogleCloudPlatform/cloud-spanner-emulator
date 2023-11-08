# Cloud Spanner PostgreSQL Interface for the Cloud Spanner Emulator

The Cloud Spanner Emulator provides application developers with a
locally-running, _emulated_ instance of Cloud Spanner to enable local
development and testing.

The PostgresSQL Interface enables applications using the PostgreSQL dialect of Cloud Spanner to use the emulator for local unit testing.

## Quickstart

The PostgreSQL Interface is available through all the forms that the Cloud
Spanner Emulator can be invoked through. Please refer to [Cloud Spanner Emulator
Quickstart](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator#quickstart)
for instructions.

PostgreSQL dialect databases can be created and interacted with in the same
manner as in the production Cloud Spanner service.

## Technical Details

The PostgreSQL Interface for the Cloud Spanner Emulator parses PostgreSQL DDL
statements, queries and DML statements with the open-source PostgreSQL
implementation to produce a PostgreSQL parse tree. The parse tree is then
translated to Cloud Spanner DDL statements and
[ZetaSQL](https://github.com/google/zetasql) ASTs, respectively.


```
googlesql::AnalyzerOptions analyzer_options = /* init analyzer options */;
googlesql::TypeFactory type_factory = /* init type factory */;
google::spanner::emulator::backend::Catalog catalog{/* init catalog */};
std::string query = "select 1";

std::unique_ptr<const googlesql::AnalyzerOutput> analyzer_output;
if (dialect == database_api::DatabaseDialect::POSTGRESQL) {
  analyzer_options.CreateDefaultArenasIfNotSet();
  ASSIGN_OR_RETURN(
      analyzer_output,
      ParseAndAnalyzePostgreSQL(query, &catalog, type_factory));
} else {
  RETURN_IF_ERROR(googlesql::AnalyzeStatement(query, analyzer_options,
                  catalog, type_factory, &analyzer_output));
}

/* Execute query using the resolved GoogleSQL AST in the analyzer_output. */
```

The Cloud Spanner Emulator then executes DDL statements, queries and DML
statements using the ZetaSQL reference implementation. Please refer to the
[Cloud Spanner Emulator Technical
Details](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator#technical-details)
for further information.

**We make no guarantees that Cloud Spanner supports any specific version of PostgreSQL.**
## Features and Limitations

The supported features and limitations for the PostgreSQL dialect are mostly
consistent with the [Cloud Spanner Emulator Features and
Limitations](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator#features-and-limitations).
The following additional limitations currently apply:

- The catalog name in information schema tables in production Cloud Spanner
  PostgreSQL databases contain the database name. But in the emulator, the
  database name is not readily available at the point at which the information
  schema tables are populated so any columns with the catalog name will be
  empty.

- Casting from PG.JSONB to bool, double, bigint and varchar types is
  currently unsupported.

- Dataflow templates

- Any limitations listed in [The PostgreSQL language in
  Spanner](https://cloud.google.com/spanner/docs/reference/postgresql/overview)
  documentation.

## Frequently Asked Questions (FAQ)

Please refer to the [Cloud Spanner Emulator Frequently Asked Questions
(FAQ)](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator#frequently-asked-questions-faq).

## Contribute

We are currently not accepting external code contributions to this project.

## Issues

Please file bugs and feature requests using
[GitHub's issue tracker](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/new)
or using the existing Cloud Spanner [support channels](https://cloud.google.com/spanner/docs/getting-support).

## Security

For information on reporting security vulnerabilities, see
[SECURITY.md](https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/blob/master/SECURITY.md).

## License

[PostgreSQL License](LICENSE)
