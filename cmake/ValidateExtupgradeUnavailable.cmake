if(NOT DEFINED SOURCE_DIR OR NOT DEFINED TEST_NAME OR NOT DEFINED EXTENSION_NAME)
  message(FATAL_ERROR "ValidateExtupgradeUnavailable.cmake requires SOURCE_DIR, TEST_NAME, and EXTENSION_NAME")
endif()

set(WATCH_PATHS "")
if(DEFINED WATCH_PATHS_SERIALIZED AND NOT WATCH_PATHS_SERIALIZED STREQUAL "")
  string(REPLACE "|" ";" WATCH_PATHS "${WATCH_PATHS_SERIALIZED}")
endif()

find_program(GIT_EXECUTABLE git)
if(NOT GIT_EXECUTABLE)
  message(STATUS
    "pg_validate_extupgrade is unavailable, so ${TEST_NAME} did not run. "
    "Git is also unavailable, so local upgrade-sensitive changes could not be inspected."
  )
  return()
endif()

execute_process(
  COMMAND "${GIT_EXECUTABLE}" -C "${SOURCE_DIR}" status --short --untracked-files=all -- ${WATCH_PATHS}
  RESULT_VARIABLE GIT_STATUS_RESULT
  OUTPUT_VARIABLE GIT_STATUS_OUTPUT
  ERROR_VARIABLE GIT_STATUS_ERROR
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_STRIP_TRAILING_WHITESPACE
)

if(NOT GIT_STATUS_RESULT EQUAL 0)
  message(FATAL_ERROR
    "pg_validate_extupgrade is unavailable, so ${TEST_NAME} did not run.\n"
    "The placeholder test also could not inspect upgrade-sensitive files.\n"
    "git status failed with:\n${GIT_STATUS_ERROR}\n"
    "Install pg_validate_extupgrade and rerun ctest:\n"
    "  cargo install --git https://github.com/rjuju/pg_validate_extupgrade pg_validate_extupgrade"
  )
endif()

if(NOT GIT_STATUS_OUTPUT STREQUAL "")
  message(FATAL_ERROR
    "pg_validate_extupgrade is unavailable, so ${TEST_NAME} did not run.\n"
    "Local changes touch upgrade-sensitive files for extension ${EXTENSION_NAME}:\n"
    "${GIT_STATUS_OUTPUT}\n"
    "Install pg_validate_extupgrade and rerun ctest:\n"
    "  cargo install --git https://github.com/rjuju/pg_validate_extupgrade pg_validate_extupgrade"
  )
endif()

message(STATUS
  "pg_validate_extupgrade is unavailable, so ${TEST_NAME} did not run. "
  "No local upgrade-sensitive changes were detected for extension ${EXTENSION_NAME}."
)
