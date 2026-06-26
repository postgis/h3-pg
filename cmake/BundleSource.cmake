foreach(required_var ARCHIVE_OUTPUT ARCHIVE_PREFIX H3_SOURCE_DIR META_JSON PROJECT_BINARY_DIR PROJECT_SOURCE_DIR)
  if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
    message(FATAL_ERROR "${required_var} is required")
  endif()
endforeach()

set(bundle_root "${PROJECT_BINARY_DIR}/source-bundle")
set(staging_dir "${bundle_root}/${ARCHIVE_PREFIX}")
set(archive_tar "${PROJECT_BINARY_DIR}/${ARCHIVE_PREFIX}.git.tar")

file(REMOVE_RECURSE "${bundle_root}" "${archive_tar}" "${ARCHIVE_OUTPUT}")
file(MAKE_DIRECTORY "${bundle_root}")

execute_process(
  COMMAND git archive --format tar --prefix=${ARCHIVE_PREFIX}/ -o "${archive_tar}" HEAD
  WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
  COMMAND_ERROR_IS_FATAL ANY
)
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E tar xf "${archive_tar}"
  WORKING_DIRECTORY "${bundle_root}"
  COMMAND_ERROR_IS_FATAL ANY
)

file(COPY "${META_JSON}" DESTINATION "${staging_dir}")
file(REMOVE_RECURSE "${staging_dir}/cmake/h3/upstream")
file(MAKE_DIRECTORY "${staging_dir}/cmake/h3/upstream")
file(COPY "${H3_SOURCE_DIR}/" DESTINATION "${staging_dir}/cmake/h3/upstream")

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E tar czf "${ARCHIVE_OUTPUT}" --format=gnutar "${ARCHIVE_PREFIX}"
  WORKING_DIRECTORY "${bundle_root}"
  COMMAND_ERROR_IS_FATAL ANY
)
