foreach(required_variable IN ITEMS GIT_EXECUTABLE SOURCE_DIR PATCH_FILE)
  if(NOT DEFINED ${required_variable} OR "${${required_variable}}" STREQUAL "")
    message(FATAL_ERROR "apply-patch.cmake requires ${required_variable}")
  endif()
endforeach()

# A changed patch reruns this step on a tree that still carries the previous
# one, so revert the copy recorded when that one was applied.
set(applied_patch "${SOURCE_DIR}/.git/bigquery-validation-applied.patch")
if(EXISTS "${applied_patch}")
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${applied_patch}"
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE applied_check_result
    OUTPUT_QUIET ERROR_QUIET)
  if(applied_check_result EQUAL 0)
    execute_process(
      COMMAND "${GIT_EXECUTABLE}" apply --reverse --whitespace=nowarn
              "${applied_patch}"
      WORKING_DIRECTORY "${SOURCE_DIR}"
      RESULT_VARIABLE revert_result
      ERROR_VARIABLE revert_error)
    if(NOT revert_result EQUAL 0)
      message(FATAL_ERROR "failed to revert ${applied_patch}:\n${revert_error}")
    endif()
  endif()
  file(REMOVE "${applied_patch}")
endif()

execute_process(
  COMMAND "${GIT_EXECUTABLE}" apply --check "${PATCH_FILE}"
  WORKING_DIRECTORY "${SOURCE_DIR}"
  RESULT_VARIABLE apply_check_result
  ERROR_VARIABLE apply_check_error)
if(apply_check_result EQUAL 0)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --whitespace=nowarn "${PATCH_FILE}"
    WORKING_DIRECTORY "${SOURCE_DIR}"
    RESULT_VARIABLE apply_result
    ERROR_VARIABLE apply_error)
  if(NOT apply_result EQUAL 0)
    message(FATAL_ERROR "failed to apply ${PATCH_FILE}:\n${apply_error}")
  endif()
  file(READ "${PATCH_FILE}" patch_contents)
  file(WRITE "${applied_patch}" "${patch_contents}")
  message(STATUS "Applied ${PATCH_FILE}")
  return()
endif()

execute_process(
  COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${PATCH_FILE}"
  WORKING_DIRECTORY "${SOURCE_DIR}"
  RESULT_VARIABLE reverse_check_result
  ERROR_VARIABLE reverse_check_error)
if(reverse_check_result EQUAL 0)
  file(READ "${PATCH_FILE}" patch_contents)
  file(WRITE "${applied_patch}" "${patch_contents}")
  message(STATUS "Patch already applied: ${PATCH_FILE}")
  return()
endif()

message(
  FATAL_ERROR
    "${PATCH_FILE} neither applies cleanly nor is already applied. If the "
    "tree carries an unrecorded earlier version, delete the build directory.\n"
    "Apply check:\n${apply_check_error}\n"
    "Reverse check:\n${reverse_check_error}")
