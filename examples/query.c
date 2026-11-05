// Runs a query through the ADBC driver manager and prints how many rows it returned.
//
// Build the driver and this example, then pass the driver's path, a Google Cloud project and a
// query. Credentials come from Application Default Credentials (ADC):
//
//   cargo rustc --release --locked --lib --crate-type cdylib
//   cc examples/query.c -o query -ladbc_driver_manager
//   ./query target/release/libadbc_driver_bigquery.so my-project "SELECT 1 AS n"

#include <arrow-adbc/adbc.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(call)                 \
  do {                              \
    if ((call) != ADBC_STATUS_OK) { \
      goto done;                    \
    }                               \
  } while (0)

int main(int argc, char** argv) {
  if (argc != 4) {
    fprintf(stderr, "usage: %s DRIVER_PATH PROJECT SQL\n", argv[0]);
    return EXIT_FAILURE;
  }

  char uri[256];
  int uri_length = snprintf(uri, sizeof(uri), "bigquery:///%s", argv[2]);
  if (uri_length < 0 || (size_t)uri_length >= sizeof(uri)) {
    fprintf(stderr, "project name is too long\n");
    return EXIT_FAILURE;
  }

  int exit_code = EXIT_FAILURE;
  struct AdbcError error = ADBC_ERROR_INIT;
  struct AdbcDatabase database = {0};
  struct AdbcConnection connection = {0};
  struct AdbcStatement statement = {0};
  struct ArrowArrayStream stream = {0};

  CHECK(AdbcDatabaseNew(&database, &error));
  CHECK(AdbcDatabaseSetOption(&database, "driver", argv[1], &error));
  CHECK(AdbcDatabaseSetOption(&database, "uri", uri, &error));
  CHECK(AdbcDatabaseInit(&database, &error));

  CHECK(AdbcConnectionNew(&connection, &error));
  CHECK(AdbcConnectionInit(&connection, &database, &error));

  CHECK(AdbcStatementNew(&connection, &statement, &error));
  CHECK(AdbcStatementSetSqlQuery(&statement, argv[3], &error));
  CHECK(AdbcStatementExecuteQuery(&statement, &stream, NULL, &error));

  int64_t rows = 0;
  for (;;) {
    struct ArrowArray batch = {0};
    if (stream.get_next(&stream, &batch) != 0) {
      fprintf(stderr, "%s\n", stream.get_last_error(&stream));
      goto done;
    }
    if (batch.release == NULL) {  // The end of the stream.
      break;
    }
    rows += batch.length;
    batch.release(&batch);
  }
  printf("%lld rows\n", (long long)rows);
  exit_code = EXIT_SUCCESS;

done:
  if (error.release != NULL) {
    fprintf(stderr, "%s\n", error.message);
    error.release(&error);
  }
  // Release in reverse order of creation, including after a failure.
  if (stream.release != NULL) {
    stream.release(&stream);
  }
  if (statement.private_data != NULL) {
    AdbcStatementRelease(&statement, NULL);
  }
  if (connection.private_data != NULL) {
    AdbcConnectionRelease(&connection, NULL);
  }
  if (database.private_data != NULL) {
    AdbcDatabaseRelease(&database, NULL);
  }
  return exit_code;
}
