// Apache Arrow ADBC's canonical C++ validation suite, adapted to the
// BigQuery ADBC driver and loaded through the ADBC driver manager.

#include <arrow-adbc/adbc.h>
#include <gtest/gtest.h>

#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

namespace {

constexpr std::string_view kDatasetPrefix = "bigquery_adbc_validation_";

std::string EnvOr(const char* name, const char* fallback = "") {
  const char* value = std::getenv(name);
  return value == nullptr ? std::string(fallback) : std::string(value);
}

std::string QuoteIdentifier(std::string_view name) {
  std::string quoted("`");
  for (char character : name) {
    if (character == '`') quoted.push_back('`');
    quoted.push_back(character);
  }
  quoted.push_back('`');
  return quoted;
}

bool IsDatasetId(std::string_view name) {
  if (name.empty() || name.size() > 1024) return false;
  for (char raw_character : name) {
    const auto character = static_cast<unsigned char>(raw_character);
    if (!std::isalnum(character) && character != '_') return false;
  }
  return true;
}

std::string QualifiedTable(std::string_view name, std::string_view db_schema) {
  const std::string project = EnvOr("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT");
  const std::string schema = db_schema.empty() ? EnvOr("ADBC_BIGQUERY_DATASET")
                                               : std::string(db_schema);
  return QuoteIdentifier(project + "." + schema + "." + std::string(name));
}

AdbcStatusCode RunIgnoringResult(struct AdbcConnection* connection,
                                 const std::string& sql,
                                 struct AdbcError* error) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));
  RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, sql.c_str(), error));
  RAISE_ADBC(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
  return AdbcStatementRelease(&statement.value, error);
}

std::string CreateDatasetSql(std::string_view project,
                             std::string_view dataset) {
  return "CREATE SCHEMA " +
         QuoteIdentifier(std::string(project) + "." + std::string(dataset)) +
         " OPTIONS(default_table_expiration_days=1, "
         "labels=[(\"adbc_validation\", \"true\"), "
         "(\"bigquery_adbc_driver\", \"rust\")])";
}

class BigQueryQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const std::string library = EnvOr("ADBC_BIGQUERY_LIBRARY");
    const std::string project =
        EnvOr("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT");
    const std::string dataset = EnvOr("ADBC_BIGQUERY_DATASET");
    RAISE_ADBC(
        AdbcDatabaseSetOption(database, "driver", library.c_str(), error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "entrypoint",
                                     "AdbcDriverBigqueryInit", error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "bigquery.project_id",
                                     project.c_str(), error));
    RAISE_ADBC(AdbcDatabaseSetOption(database, "bigquery.dataset_id",
                                     dataset.c_str(), error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection,
                           const std::string& name,
                           struct AdbcError* error) const override {
    return RunIgnoringResult(
        connection, "DROP TABLE IF EXISTS " + QualifiedTable(name, ""), error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection,
                           const std::string& name,
                           const std::string& db_schema,
                           struct AdbcError* error) const override {
    return RunIgnoringResult(
        connection, "DROP TABLE IF EXISTS " + QualifiedTable(name, db_schema),
        error);
  }

  AdbcStatusCode EnsureDbSchema(struct AdbcConnection*, const std::string& name,
                                struct AdbcError*) const override {
    // The runner creates this dataset without IF NOT EXISTS and owns it for the
    // whole invocation.
    return name == EnvOr("ADBC_BIGQUERY_SECONDARY_DATASET")
               ? ADBC_STATUS_OK
               : ADBC_STATUS_INVALID_ARGUMENT;
  }

  std::optional<std::string> PrimaryKeyTableDdl(
      std::string_view name) const override {
    return "CREATE TABLE " + QualifiedTable(name, "") +
           " (id INT64, PRIMARY KEY (id) NOT ENFORCED)";
  }

  std::optional<std::string> PrimaryKeyIngestTableDdl(
      std::string_view) const override {
    return std::nullopt;
  }

  std::optional<std::string> CompositePrimaryKeyTableDdl(
      std::string_view name) const override {
    return "CREATE TABLE " + QualifiedTable(name, "") +
           " (id_primary_col1 INT64, id_primary_col2 INT64, "
           "PRIMARY KEY (id_primary_col1, id_primary_col2) NOT ENFORCED)";
  }

  std::optional<std::string> ForeignKeyChildTableDdl(
      std::string_view child_name, std::string_view parent_name_1,
      std::string_view parent_name_2) const override {
    return "CREATE TABLE " + QualifiedTable(child_name, "") +
           " (id_child_col1 INT64, id_child_col2 INT64, id_child_col3 INT64, "
           "PRIMARY KEY (id_child_col1) NOT ENFORCED, "
           "FOREIGN KEY (id_child_col3) REFERENCES " +
           QualifiedTable(parent_name_1, "") +
           " (id) NOT ENFORCED, "
           "FOREIGN KEY (id_child_col1, id_child_col2) REFERENCES " +
           QualifiedTable(parent_name_2, "") +
           " (id_primary_col1, id_primary_col2) NOT ENFORCED)";
  }

  std::string BindParameter(int) const override { return "?"; }

  std::string QuoteIdentifier(std::string_view name) const override {
    return ::QuoteIdentifier(name);
  }

  std::string RewriteSql(std::string_view query_id,
                         std::string default_sql) const override {
    struct Rewrite {
      std::string_view expected_default;
      std::string_view sql;
    };
    static const std::unordered_map<std::string_view, Rewrite> kRewrites = {
        {"StatementTest::TestSqlQueryFloats::cast-1.5-as-float",
         {"SELECT CAST(1.5 AS FLOAT)", "SELECT CAST(1.5 AS FLOAT64)"}},
        {"StatementTest::TestSqlSchemaFloats::cast-1.5-as-float",
         {"SELECT CAST(1.5 AS FLOAT)", "SELECT CAST(1.5 AS FLOAT64)"}},
        {"StatementTest::TestSqlIngestInterval::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC"}},
        {"StatementTest::TestSqlIngestMultipleConnections::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC NULLS LAST",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC"}},
        {"StatementTest::TestSqlIngestAppend::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest`",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` DESC"}},
        {"StatementTest::TestSqlIngestSample::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY int64s ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY int64s ASC"}},
        {"StatementTest::TestSqlPrepareSelectParams::select-params",
         {"SELECT ?, ?", "SELECT CAST(? AS INT64), CAST(? AS STRING)"}},
        {"StatementTest::TestSqlPrepareUpdate::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `int64s` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `int64s` ASC"}},
        {"StatementTest::TestSqlPrepareUpdateStream::select-bulk-ingest",
         {"SELECT * FROM `bulk_ingest` ORDER BY `ints` ASC NULLS FIRST",
          "SELECT * FROM `bulk_ingest` ORDER BY `ints` ASC"}},
        {"StatementTest::TestSqlBind::create-table-bindtest",
         {"CREATE TABLE bindtest (col1 INTEGER, col2 TEXT)",
          "CREATE TABLE bindtest (col1 INT64, col2 STRING)"}},
        {"StatementTest::TestSqlBind::select-bindtest",
         {"SELECT * FROM bindtest ORDER BY col1 ASC NULLS FIRST",
          "SELECT * FROM bindtest ORDER BY col1 ASC"}},
        {"StatementTest::TestSqlQueryEmpty::create-table-queryempty",
         {"CREATE TABLE queryempty (FOO INT)",
          "CREATE TABLE queryempty (FOO INT64)"}},
        {"StatementTest::TestSqlQueryInsertRollback::create-table-rollbacktest",
         {"CREATE TABLE `rollbacktest` (a INT)",
          "CREATE TEMP TABLE `rollbacktest` (a INT64)"}},
        {"StatementTest::TestSqlQueryRowsAffectedDelete::create-table-delete-"
         "test",
         {"CREATE TABLE `delete_test` (foo INT)",
          "CREATE TABLE `delete_test` (foo INT64)"}},
        {"StatementTest::TestSqlQueryRowsAffectedDeleteStream::create-table-"
         "delete-test",
         {"CREATE TABLE `delete_test` (foo INT)",
          "CREATE TABLE `delete_test` (foo INT64)"}},
    };

    constexpr std::string_view kIngestTypePrefix =
        "StatementTest::TestSqlIngestType::select-bulk-ingest::";
    if (query_id.starts_with(kIngestTypePrefix)) {
      EXPECT_EQ(default_sql,
                "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC NULLS FIRST")
          << "upstream default SQL for " << query_id << " changed";
      if (query_id.substr(kIngestTypePrefix.size()) == "list") {
        return "SELECT * FROM `bulk_ingest` ORDER BY `col`[SAFE_OFFSET(0)] ASC";
      }
      return "SELECT * FROM `bulk_ingest` ORDER BY `col` ASC";
    }

    if (query_id ==
        "StatementTest::TestSqlIngestTemporalType::select-bulk-ingest") {
      constexpr std::string_view kPrefix = "SELECT * FROM `bulk_ingest_";
      constexpr std::string_view kSuffix = "` ORDER BY `col` ASC NULLS FIRST";
      if (!default_sql.starts_with(kPrefix) ||
          !default_sql.ends_with(kSuffix)) {
        ADD_FAILURE() << "upstream default SQL for " << query_id
                      << " changed: " << default_sql;
        return default_sql;
      }
      default_sql.erase(default_sql.size() -
                        std::string_view(" NULLS FIRST").size());
      return default_sql;
    }

    const auto rewrite = kRewrites.find(query_id);
    if (rewrite == kRewrites.end()) return default_sql;
    EXPECT_EQ(default_sql, rewrite->second.expected_default)
        << "upstream default SQL for " << query_id << " changed";
    return std::string(rewrite->second.sql);
  }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_HALF_FLOAT:
      case NANOARROW_TYPE_FLOAT:
        return NANOARROW_TYPE_DOUBLE;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
        return NANOARROW_TYPE_BINARY;
      case NANOARROW_TYPE_INTERVAL_MONTHS:
      case NANOARROW_TYPE_INTERVAL_DAY_TIME:
        return NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
      case NANOARROW_TYPE_DURATION:
        return NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
      case NANOARROW_TYPE_TIMESTAMP:
        return NANOARROW_TYPE_TIMESTAMP;
      default:
        return ingest_type;
    }
  }

  int64_t IngestTemporalValueMultiplier(
      ArrowType type, enum ArrowTimeUnit unit) const override {
    if (unit == NANOARROW_TIME_UNIT_NANO &&
        (type == NANOARROW_TYPE_DURATION || type == NANOARROW_TYPE_TIMESTAMP)) {
      return 1'000;
    }
    return 1;
  }

  bool ingest_null_lists_as_empty() const override { return true; }

  bool supports_bulk_ingest(const char* mode) const override {
    return std::strcmp(mode, ADBC_INGEST_OPTION_MODE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_REPLACE) == 0;
  }

  bool supports_bulk_ingest_catalog() const override { return true; }
  bool supports_bulk_ingest_db_schema() const override { return true; }
  bool supports_cancel() const override { return true; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_execute_schema() const override { return true; }
  bool supports_partitioned_data() const override { return true; }
  bool supports_transactions() const override { return true; }
  // The canonical Transactions case requires CREATE TABLE to roll back.
  // BigQuery rejects permanent-table DDL in transactions, and a temporary
  // table survives rollback, so neither can satisfy that assertion.
  bool ddl_implicit_commit_txn() const override { return true; }
  bool supports_get_sql_info() const override { return true; }
  bool supports_get_objects() const override { return true; }
  bool supports_metadata_current_catalog() const override { return true; }
  bool supports_metadata_current_db_schema() const override { return true; }
  bool supports_dynamic_parameter_binding() const override { return true; }
  bool supports_rows_affected() const override { return true; }
  bool supports_statistics() const override { return true; }
  bool supports_error_on_incompatible_schema() const override { return true; }
  bool supports_ingest_view_types() const override { return true; }

  std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(
      uint32_t info_code) const override {
    switch (info_code) {
      case ADBC_INFO_DRIVER_NAME:
        return std::string("bigquery-adbc");
      case ADBC_INFO_DRIVER_VERSION:
        return std::string(BIGQUERY_DRIVER_VERSION);
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        return static_cast<int64_t>(ADBC_VERSION_1_1_0);
      case ADBC_INFO_VENDOR_NAME:
        return std::string("Google BigQuery");
      case ADBC_INFO_VENDOR_VERSION:
        return std::string(BIGQUERY_VENDOR_VERSION);
      default:
        return std::nullopt;
    }
  }

  std::string catalog() const override {
    return EnvOr("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT");
  }

  std::string db_schema() const override {
    return EnvOr("ADBC_BIGQUERY_DATASET");
  }

  std::string secondary_db_schema() const override {
    return EnvOr("ADBC_BIGQUERY_SECONDARY_DATASET");
  }
};

class BigQueryDatabaseTest : public ::testing::Test,
                             public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override {
    return &quirks_;
  }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 private:
  BigQueryQuirks quirks_;
};
ADBCV_TEST_DATABASE(BigQueryDatabaseTest)

class BigQueryConnectionTest : public ::testing::Test,
                               public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override {
    return &quirks_;
  }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 private:
  BigQueryQuirks quirks_;
};
ADBCV_TEST_CONNECTION(BigQueryConnectionTest)

class BigQueryStatementTest : public ::testing::Test,
                              public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override {
    return &quirks_;
  }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  // BigQuery has only signed INT64; the driver rejects a UInt64 ingest schema
  // with NOT_IMPLEMENTED. Hiding the base method opts out of one canonical
  // case, as upstream's own drivers do.
  void TestSqlIngestUInt64() { GTEST_SKIP() << "BigQuery has no UInt64 type"; }

 protected:
  void ValidateIngestedTemporalData(struct ArrowArrayView* values,
                                    ArrowType type, enum ArrowTimeUnit unit,
                                    const char*) override {
    const int64_t multiplier =
        quirks_.IngestTemporalValueMultiplier(type, unit);

    if (type == NANOARROW_TYPE_DURATION) {
      int64_t nanoseconds_per_unit = 1;
      switch (unit) {
        case NANOARROW_TIME_UNIT_SECOND:
          nanoseconds_per_unit = 1'000'000'000;
          break;
        case NANOARROW_TIME_UNIT_MILLI:
          nanoseconds_per_unit = 1'000'000;
          break;
        case NANOARROW_TIME_UNIT_MICRO:
          nanoseconds_per_unit = 1'000;
          break;
        case NANOARROW_TIME_UNIT_NANO:
          break;
      }
      struct ArrowInterval negative;
      struct ArrowInterval zero;
      struct ArrowInterval positive;
      ArrowIntervalInit(&negative, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
      ArrowIntervalInit(&zero, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
      ArrowIntervalInit(&positive, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
      negative.ns = -42 * multiplier * nanoseconds_per_unit;
      positive.ns = 42 * multiplier * nanoseconds_per_unit;
      ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareArray<ArrowInterval*>(
          values, {std::nullopt, &negative, &zero, &positive}));
      return;
    }

    ASSERT_EQ(type, NANOARROW_TYPE_TIMESTAMP);
    int64_t microseconds_per_unit = 1;
    switch (unit) {
      case NANOARROW_TIME_UNIT_SECOND:
        microseconds_per_unit = 1'000'000;
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        microseconds_per_unit = 1'000;
        break;
      case NANOARROW_TIME_UNIT_MICRO:
        break;
      case NANOARROW_TIME_UNIT_NANO:
        ASSERT_EQ(multiplier % 1'000, 0);
        microseconds_per_unit = 0;
        break;
    }
    const int64_t expected = unit == NANOARROW_TIME_UNIT_NANO
                                 ? 42 * (multiplier / 1'000)
                                 : 42 * multiplier * microseconds_per_unit;
    ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareArray<int64_t>(
        values, {std::nullopt, -expected, 0, expected}));
  }

 private:
  BigQueryQuirks quirks_;
};
ADBCV_TEST_STATEMENT(BigQueryStatementTest)

std::string ErrorMessage(const struct AdbcError& error) {
  return error.message == nullptr ? std::string("no error detail")
                                  : std::string(error.message);
}

void ReleaseError(struct AdbcError* error) {
  if (error->release != nullptr) error->release(error);
  std::memset(error, 0, sizeof(*error));
}

bool ConfigureDatabase(struct AdbcDatabase* database, struct AdbcError* error,
                       bool with_dataset) {
  const std::string library = EnvOr("ADBC_BIGQUERY_LIBRARY");
  const std::string project =
      EnvOr("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT");
  const std::string dataset = EnvOr("ADBC_BIGQUERY_DATASET");
  if (library.empty() || project.empty() || (with_dataset && dataset.empty())) {
    std::cerr << "missing ADBC_BIGQUERY_LIBRARY, "
                 "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT, or "
                 "ADBC_BIGQUERY_DATASET environment variable\n";
    return false;
  }
  if (AdbcDatabaseNew(database, error) != ADBC_STATUS_OK ||
      AdbcDatabaseSetOption(database, "driver", library.c_str(), error) !=
          ADBC_STATUS_OK ||
      AdbcDatabaseSetOption(database, "entrypoint", "AdbcDriverBigqueryInit",
                            error) != ADBC_STATUS_OK ||
      AdbcDatabaseSetOption(database, "bigquery.project_id", project.c_str(),
                            error) != ADBC_STATUS_OK ||
      (with_dataset &&
       AdbcDatabaseSetOption(database, "bigquery.dataset_id", dataset.c_str(),
                             error) != ADBC_STATUS_OK) ||
      AdbcDatabaseInit(database, error) != ADBC_STATUS_OK) {
    std::cerr << "failed to initialize BigQuery driver: "
              << ErrorMessage(*error) << '\n';
    return false;
  }
  return true;
}

bool OpenAdminConnection(struct AdbcDatabase* database,
                         struct AdbcConnection* connection,
                         struct AdbcError* error) {
  if (!ConfigureDatabase(database, error, false)) return false;
  if (AdbcConnectionNew(connection, error) != ADBC_STATUS_OK ||
      AdbcConnectionInit(connection, database, error) != ADBC_STATUS_OK) {
    std::cerr << "failed to open BigQuery administration connection: "
              << ErrorMessage(*error) << '\n';
    return false;
  }
  return true;
}

bool ExecuteAdminSql(struct AdbcConnection* connection, const std::string& sql,
                     struct AdbcError* error) {
  struct AdbcStatement statement{};
  AdbcStatusCode status = AdbcStatementNew(connection, &statement, error);
  if (status == ADBC_STATUS_OK) {
    status = AdbcStatementSetSqlQuery(&statement, sql.c_str(), error);
  }
  if (status == ADBC_STATUS_OK) {
    status = AdbcStatementExecuteQuery(&statement, nullptr, nullptr, error);
  }
  const std::string failure =
      status == ADBC_STATUS_OK ? std::string() : ErrorMessage(*error);
  if (statement.private_data != nullptr) {
    struct AdbcError release_error{};
    const AdbcStatusCode release_status =
        AdbcStatementRelease(&statement, &release_error);
    if (status == ADBC_STATUS_OK && release_status != ADBC_STATUS_OK) {
      status = release_status;
    }
    ReleaseError(&release_error);
  }
  if (status != ADBC_STATUS_OK) {
    std::cerr << "BigQuery administration SQL failed: " << failure
              << "\nSQL: " << sql << '\n';
    return false;
  }
  return true;
}

bool DropOwnedDataset(struct AdbcConnection* connection,
                      std::string_view project, std::string_view dataset,
                      struct AdbcError* error) {
  if (!IsDatasetId(dataset) || !dataset.starts_with(kDatasetPrefix)) {
    std::cerr << "refusing to drop dataset with unexpected ownership marker: "
              << dataset << '\n';
    return false;
  }
  return ExecuteAdminSql(
      connection,
      "DROP SCHEMA IF EXISTS " +
          QuoteIdentifier(std::string(project) + "." + std::string(dataset)) +
          " CASCADE",
      error);
}

int AdminDataset(bool create) {
  const std::string project =
      EnvOr("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT");
  const std::string dataset = EnvOr("ADBC_BIGQUERY_DATASET");
  if (!IsDatasetId(dataset) || !dataset.starts_with(kDatasetPrefix)) {
    std::cerr << "unsafe validation dataset ID: " << dataset << '\n';
    return 2;
  }

  struct AdbcError error{};
  struct AdbcDatabase database{};
  struct AdbcConnection connection{};
  bool ok = OpenAdminConnection(&database, &connection, &error);
  if (ok && create) {
    ok = ExecuteAdminSql(&connection, CreateDatasetSql(project, dataset),
                         &error);
  } else if (ok) {
    ok = DropOwnedDataset(&connection, project, dataset, &error);
  }

  ReleaseError(&error);
  if (connection.private_data != nullptr) {
    struct AdbcError release_error{};
    if (AdbcConnectionRelease(&connection, &release_error) != ADBC_STATUS_OK)
      ok = false;
    ReleaseError(&release_error);
  }
  if (database.private_data != nullptr) {
    struct AdbcError release_error{};
    if (AdbcDatabaseRelease(&database, &release_error) != ADBC_STATUS_OK)
      ok = false;
    ReleaseError(&release_error);
  }
  return ok ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  if (EnvOr("BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT").empty()) {
    std::cerr << "BIGQUERY_ADBC_INTEGRATION_TEST_GCP_PROJECT must be set "
                 "to a non-empty project ID\n";
    return 2;
  }
  if (argc == 2 && std::string_view(argv[1]) == "--adbc-create-dataset") {
    return AdminDataset(true);
  }
  if (argc == 2 && std::string_view(argv[1]) == "--adbc-drop-dataset") {
    return AdminDataset(false);
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
