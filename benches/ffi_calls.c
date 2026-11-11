// Times exported entry points called through the driver's vtable, without a driver manager or
// network access, to measure the C ABI layer's per-call overhead (see docs/PERFORMANCE.md).
//
//   cargo rustc --release --locked --lib --crate-type cdylib
//   cc -O2 benches/ffi_calls.c -o ffi_calls -ldl
//   ./ffi_calls target/release/libadbc_driver_bigquery.so setopt 200000
//
// Cases: setopt (StatementSetOption), getopt (StatementGetOption), progress
// (StatementGetOptionDouble of the lock-free progress key), newstmt (StatementNew and
// StatementRelease), tabletypes (ConnectionGetTableTypes, then reading and releasing its stream).

#include <arrow-adbc/adbc.h>

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CHECK(call)                                                       \
  do {                                                                    \
    if ((call) != ADBC_STATUS_OK) {                                       \
      fprintf(stderr, "%s failed: %s\n", #call,                          \
              error.message ? error.message : "(no message)");            \
      return EXIT_FAILURE;                                                \
    }                                                                     \
  } while (0)

static double seconds(void) {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return now.tv_sec + now.tv_nsec / 1e9;
}

int main(int argc, char** argv) {
  if (argc != 4) {
    fprintf(stderr, "usage: %s LIBRARY CASE ITERATIONS\n", argv[0]);
    return EXIT_FAILURE;
  }
  const char* name = argv[2];
  long iterations = atol(argv[3]);
  void* library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
  if (!library) {
    fprintf(stderr, "%s\n", dlerror());
    return EXIT_FAILURE;
  }
  AdbcDriverInitFunc init = (AdbcDriverInitFunc)dlsym(library, "AdbcDriverInit");
  if (!init) {
    fprintf(stderr, "%s\n", dlerror());
    return EXIT_FAILURE;
  }

  struct AdbcError error = ADBC_ERROR_INIT;
  struct AdbcDriver driver = {0};
  struct AdbcDatabase database = {0};
  struct AdbcConnection connection = {0};
  struct AdbcStatement statement = {0};
  CHECK(init(ADBC_VERSION_1_1_0, &driver, &error));
  CHECK(driver.DatabaseNew(&database, &error));
  // Anonymous access to an endpoint nothing listens on: no credentials or requests needed.
  CHECK(driver.DatabaseSetOption(&database, "uri", "bigquery:///a-project", &error));
  CHECK(driver.DatabaseSetOption(&database, "bigquery.auth_type", "anonymous", &error));
  CHECK(driver.DatabaseSetOption(&database, "bigquery.endpoint", "http://127.0.0.1:1", &error));
  CHECK(driver.DatabaseInit(&database, &error));
  CHECK(driver.ConnectionNew(&connection, &error));
  CHECK(driver.ConnectionInit(&connection, &database, &error));
  CHECK(driver.StatementNew(&connection, &statement, &error));
  CHECK(driver.StatementSetOption(&statement, "bigquery.query.priority", "BATCH", &error));

  double started = seconds();
  for (long i = 0; i < iterations; i++) {
    if (strcmp(name, "setopt") == 0) {
      CHECK(driver.StatementSetOption(&statement, "bigquery.query.priority", "BATCH", &error));
    } else if (strcmp(name, "getopt") == 0) {
      char value[16];
      size_t length = sizeof(value);
      CHECK(driver.StatementGetOption(&statement, "bigquery.query.priority", value, &length,
                                      &error));
    } else if (strcmp(name, "progress") == 0) {
      double value;
      CHECK(driver.StatementGetOptionDouble(&statement, ADBC_STATEMENT_OPTION_PROGRESS, &value, &error));
    } else if (strcmp(name, "newstmt") == 0) {
      struct AdbcStatement other = {0};
      CHECK(driver.StatementNew(&connection, &other, &error));
      CHECK(driver.StatementRelease(&other, &error));
    } else if (strcmp(name, "tabletypes") == 0) {
      struct ArrowArrayStream stream = {0};
      struct ArrowSchema schema;
      struct ArrowArray array;
      CHECK(driver.ConnectionGetTableTypes(&connection, &stream, &error));
      if (stream.get_schema(&stream, &schema) != 0) return EXIT_FAILURE;
      schema.release(&schema);
      for (;;) {
        if (stream.get_next(&stream, &array) != 0) return EXIT_FAILURE;
        if (!array.release) break;
        array.release(&array);
      }
      stream.release(&stream);
    } else {
      fprintf(stderr, "unknown case %s\n", name);
      return EXIT_FAILURE;
    }
  }
  double elapsed = seconds() - started;
  if (iterations > 0) printf("%s: %.1f ns per iteration\n", name, elapsed * 1e9 / iterations);

  CHECK(driver.StatementRelease(&statement, &error));
  CHECK(driver.ConnectionRelease(&connection, &error));
  CHECK(driver.DatabaseRelease(&database, &error));
  return EXIT_SUCCESS;
}
