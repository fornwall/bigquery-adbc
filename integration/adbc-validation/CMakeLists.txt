cmake_minimum_required(VERSION 3.20)
project(adbc_bigquery_validation LANGUAGES C CXX)

set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(BIGQUERY_VALIDATION_SANITIZE ""
    CACHE STRING "comma-separated sanitizers for the C++ validation process")
if(BIGQUERY_VALIDATION_SANITIZE)
  add_compile_options(-fsanitize=${BIGQUERY_VALIDATION_SANITIZE}
                      -fno-omit-frame-pointer -fno-sanitize-recover=all)
  add_link_options(-fsanitize=${BIGQUERY_VALIDATION_SANITIZE})
endif()

# Derive the Arrow ADBC revision from the Rust pins in ../../Cargo.toml so the
# harness validates the revision the driver builds against; a restated SHA
# drifts. Reconfigure when the manifest changes, and FORCE the cache value so a
# pin bump overrides a stale CMakeCache.txt.
set(BIGQUERY_CARGO_MANIFEST "${CMAKE_CURRENT_SOURCE_DIR}/../../Cargo.toml")
if(NOT EXISTS "${BIGQUERY_CARGO_MANIFEST}")
  message(FATAL_ERROR "cannot derive the Arrow ADBC revision: "
                      "${BIGQUERY_CARGO_MANIFEST} does not exist")
endif()
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
             "${BIGQUERY_CARGO_MANIFEST}")

file(STRINGS "${BIGQUERY_CARGO_MANIFEST}" BIGQUERY_ADBC_PIN_LINES
     REGEX "github\\.com/apache/arrow-adbc\"[^\"]*rev = \"[0-9a-f]+\"")
set(BIGQUERY_ADBC_REVISIONS "")
foreach(pin_line IN LISTS BIGQUERY_ADBC_PIN_LINES)
  string(REGEX MATCH "rev = \"([0-9a-f]+)\"" _unused "${pin_line}")
  list(APPEND BIGQUERY_ADBC_REVISIONS "${CMAKE_MATCH_1}")
endforeach()
if(NOT BIGQUERY_ADBC_REVISIONS)
  message(FATAL_ERROR "no apache/arrow-adbc git pin found in "
                      "${BIGQUERY_CARGO_MANIFEST}")
endif()
list(REMOVE_DUPLICATES BIGQUERY_ADBC_REVISIONS)
list(LENGTH BIGQUERY_ADBC_REVISIONS BIGQUERY_ADBC_REVISION_COUNT)
if(NOT BIGQUERY_ADBC_REVISION_COUNT EQUAL 1)
  string(REPLACE ";" ", " BIGQUERY_ADBC_REVISION_LIST
         "${BIGQUERY_ADBC_REVISIONS}")
  message(FATAL_ERROR
          "${BIGQUERY_CARGO_MANIFEST} pins apache/arrow-adbc at "
          "${BIGQUERY_ADBC_REVISION_COUNT} different revisions "
          "(${BIGQUERY_ADBC_REVISION_LIST}); the driver and this harness must "
          "build against one revision")
endif()
set(ARROW_ADBC_TAG "${BIGQUERY_ADBC_REVISIONS}"
    CACHE STRING "apache/arrow-adbc revision derived from Cargo.toml" FORCE)
message(STATUS "Arrow ADBC revision from Cargo.toml: ${ARROW_ADBC_TAG}")

# GetInfo reports the package version and the pinned BigQuery client library;
# derive the adapter's expectations from the same sources.
file(STRINGS "${BIGQUERY_CARGO_MANIFEST}" BIGQUERY_DRIVER_VERSION
     REGEX "^version = \"[^\"]+\"$" LIMIT_COUNT 1)
string(REGEX REPLACE "^version = \"([^\"]+)\"$" "\\1" BIGQUERY_DRIVER_VERSION
       "${BIGQUERY_DRIVER_VERSION}")
set(BIGQUERY_CARGO_LOCK "${CMAKE_CURRENT_SOURCE_DIR}/../../Cargo.lock")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
             "${BIGQUERY_CARGO_LOCK}")
file(READ "${BIGQUERY_CARGO_LOCK}" BIGQUERY_CARGO_LOCK_CONTENTS)
string(REGEX MATCHALL "\nname = \"google-cloud-bigquery\"\nversion = \"[^\"]+\""
       BIGQUERY_CLIENT_PINS "${BIGQUERY_CARGO_LOCK_CONTENTS}")
list(LENGTH BIGQUERY_CLIENT_PINS BIGQUERY_CLIENT_PIN_COUNT)
if(NOT BIGQUERY_DRIVER_VERSION OR NOT BIGQUERY_CLIENT_PIN_COUNT EQUAL 1)
  message(FATAL_ERROR "cannot derive the driver version from "
                      "${BIGQUERY_CARGO_MANIFEST} and one google-cloud-bigquery "
                      "version from ${BIGQUERY_CARGO_LOCK}")
endif()
string(REGEX REPLACE ".*version = \"([^\"]+)\"" "\\1" BIGQUERY_CLIENT_VERSION
       "${BIGQUERY_CLIENT_PINS}")

include(FetchContent)
find_package(Git REQUIRED)

# CMake reruns the patch step only when its command line changes, so the
# command carries the patch's hash.
set(BIGQUERY_VALIDATION_PATCH
    "${CMAKE_CURRENT_SOURCE_DIR}/patches/validation-quirks.patch")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
             "${BIGQUERY_VALIDATION_PATCH}")
file(SHA256 "${BIGQUERY_VALIDATION_PATCH}" BIGQUERY_VALIDATION_PATCH_SHA256)

set(ADBC_DRIVER_MANAGER ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_TESTS ON CACHE BOOL "" FORCE)
set(ADBC_WITH_VENDORED_FMT ON CACHE BOOL "" FORCE)
set(ADBC_WITH_VENDORED_NANOARROW ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_SHARED ON CACHE BOOL "" FORCE)
set(ADBC_BUILD_STATIC OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_FLIGHTSQL OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_POSTGRESQL OFF CACHE BOOL "" FORCE)
set(ADBC_DRIVER_SQLITE OFF CACHE BOOL "" FORCE)
set(ADBC_INTEGRATION_DUCKDB OFF CACHE BOOL "" FORCE)

# Use the GoogleTest that Arrow ADBC pins, never an installed one.
set(CMAKE_DISABLE_FIND_PACKAGE_GTest TRUE)

FetchContent_Declare(
  arrow_adbc
  GIT_REPOSITORY https://github.com/apache/arrow-adbc.git
  GIT_TAG ${ARROW_ADBC_TAG}
  # A pinned non-tip SHA requires full history; do not add GIT_SHALLOW here.
  SOURCE_SUBDIR c
  PATCH_COMMAND
    "${CMAKE_COMMAND}" "-DGIT_EXECUTABLE=${GIT_EXECUTABLE}"
    "-DSOURCE_DIR=<SOURCE_DIR>"
    "-DPATCH_FILE=${BIGQUERY_VALIDATION_PATCH}"
    "-DPATCH_SHA256=${BIGQUERY_VALIDATION_PATCH_SHA256}"
    -P "${CMAKE_CURRENT_SOURCE_DIR}/apply-patch.cmake")
FetchContent_MakeAvailable(arrow_adbc)

add_executable(bigquery_validation bigquery_validation.cc)
target_compile_definitions(
  bigquery_validation
  PRIVATE "BIGQUERY_DRIVER_VERSION=\"${BIGQUERY_DRIVER_VERSION}\""
          "BIGQUERY_VENDOR_VERSION=\"google-cloud-bigquery ${BIGQUERY_CLIENT_VERSION}\"")
target_link_libraries(bigquery_validation PRIVATE adbc_validation
                                                   adbc_driver_manager_shared
                                                   GTest::gtest)
target_include_directories(
  bigquery_validation SYSTEM
  PRIVATE "${arrow_adbc_SOURCE_DIR}/c" "${arrow_adbc_SOURCE_DIR}/c/include"
          "${arrow_adbc_SOURCE_DIR}/c/vendor" "${arrow_adbc_SOURCE_DIR}/c/driver")

if(MSVC)
  target_compile_options(bigquery_validation PRIVATE /W4 /WX)
else()
  target_compile_options(
    bigquery_validation PRIVATE -Wall -Wextra -Wpedantic -Wshadow -Wconversion
                                -Wsign-conversion -Werror)
endif()

include(GoogleTest)
enable_testing()
gtest_discover_tests(bigquery_validation)
