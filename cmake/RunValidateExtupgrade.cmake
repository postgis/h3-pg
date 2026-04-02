foreach(required_var
    PG_VALIDATE_EXTUPGRADE
    POSTGRESQL_BINDIR
    TEMP_ROOT
    TEMP_PORT
    EXTNAME
    FROM_VERSION
    TO_VERSION
    DYNAMIC_LIBRARY_PATH)
  if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
    message(FATAL_ERROR "RunValidateExtupgrade.cmake requires ${required_var}")
  endif()
endforeach()

find_program(INITDB_EXECUTABLE initdb HINTS "${POSTGRESQL_BINDIR}" NO_DEFAULT_PATH)
find_program(PG_CTL_EXECUTABLE pg_ctl HINTS "${POSTGRESQL_BINDIR}" NO_DEFAULT_PATH)
find_program(PG_ISREADY_EXECUTABLE pg_isready HINTS "${POSTGRESQL_BINDIR}" NO_DEFAULT_PATH)

foreach(required_program INITDB_EXECUTABLE PG_CTL_EXECUTABLE PG_ISREADY_EXECUTABLE)
  if(NOT ${required_program})
    message(FATAL_ERROR "Could not find ${required_program} in ${POSTGRESQL_BINDIR}")
  endif()
endforeach()

set(DATA_DIR "${TEMP_ROOT}/data")
set(SOCKET_DIR "${TEMP_ROOT}/socket")
set(LOG_FILE "${TEMP_ROOT}/postgresql.log")
set(POSTGRESQL_CONF "${DATA_DIR}/postgresql.conf")

file(REMOVE_RECURSE "${TEMP_ROOT}")
file(MAKE_DIRECTORY "${TEMP_ROOT}")
file(MAKE_DIRECTORY "${SOCKET_DIR}")

execute_process(
  COMMAND "${INITDB_EXECUTABLE}" -D "${DATA_DIR}" -A trust --no-locale
  RESULT_VARIABLE INITDB_RESULT
  OUTPUT_VARIABLE INITDB_OUTPUT
  ERROR_VARIABLE INITDB_ERROR
)
if(NOT INITDB_RESULT EQUAL 0)
  message(FATAL_ERROR "initdb failed:\n${INITDB_OUTPUT}${INITDB_ERROR}")
endif()

file(APPEND "${POSTGRESQL_CONF}" "dynamic_library_path = '${DYNAMIC_LIBRARY_PATH}'\n")
if(DEFINED EXTENSION_CONTROL_PATH AND NOT "${EXTENSION_CONTROL_PATH}" STREQUAL "")
  file(APPEND "${POSTGRESQL_CONF}" "extension_control_path = '${EXTENSION_CONTROL_PATH}'\n")
endif()

execute_process(
  COMMAND "${PG_CTL_EXECUTABLE}" -D "${DATA_DIR}" -l "${LOG_FILE}"
          -o "-c listen_addresses='' -k ${SOCKET_DIR} -p ${TEMP_PORT}" start
  RESULT_VARIABLE START_RESULT
  OUTPUT_VARIABLE START_OUTPUT
  ERROR_VARIABLE START_ERROR
)
if(NOT START_RESULT EQUAL 0)
  message(FATAL_ERROR "pg_ctl start failed:\n${START_OUTPUT}${START_ERROR}")
endif()

set(CLUSTER_STARTED TRUE)
set(READY FALSE)
foreach(_attempt RANGE 1 30)
  execute_process(
    COMMAND "${PG_ISREADY_EXECUTABLE}" -h "${SOCKET_DIR}" -p "${TEMP_PORT}" -d postgres
    RESULT_VARIABLE READY_RESULT
    OUTPUT_QUIET
    ERROR_QUIET
  )
  if(READY_RESULT EQUAL 0)
    set(READY TRUE)
    break()
  endif()
  execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep 1)
endforeach()

if(NOT READY)
  execute_process(COMMAND "${PG_CTL_EXECUTABLE}" -D "${DATA_DIR}" stop -m fast
                  OUTPUT_QUIET ERROR_QUIET)
  file(READ "${LOG_FILE}" LOG_CONTENT)
  message(FATAL_ERROR "Temporary PostgreSQL cluster did not become ready:\n${LOG_CONTENT}")
endif()

execute_process(
  COMMAND "${PG_VALIDATE_EXTUPGRADE}"
          --host "${SOCKET_DIR}"
          --port "${TEMP_PORT}"
          --dbname postgres
          --extname "${EXTNAME}"
          --from "${FROM_VERSION}"
          --to "${TO_VERSION}"
  WORKING_DIRECTORY "${WORKING_DIRECTORY}"
  RESULT_VARIABLE EXTUPGRADE_RESULT
  OUTPUT_VARIABLE EXTUPGRADE_OUTPUT
  ERROR_VARIABLE EXTUPGRADE_ERROR
)

execute_process(
  COMMAND "${PG_CTL_EXECUTABLE}" -D "${DATA_DIR}" stop -m fast
  RESULT_VARIABLE STOP_RESULT
  OUTPUT_VARIABLE STOP_OUTPUT
  ERROR_VARIABLE STOP_ERROR
)

if(NOT STOP_RESULT EQUAL 0)
  message(FATAL_ERROR
    "pg_ctl stop failed:\n${STOP_OUTPUT}${STOP_ERROR}\n"
    "pg_validate_extupgrade output:\n${EXTUPGRADE_OUTPUT}${EXTUPGRADE_ERROR}")
endif()

if(NOT EXTUPGRADE_RESULT EQUAL 0)
  message(FATAL_ERROR "${EXTUPGRADE_OUTPUT}${EXTUPGRADE_ERROR}")
endif()
