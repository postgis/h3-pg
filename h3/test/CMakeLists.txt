set(TESTS
  clustering
  deprecated
  edge
  extension
  hierarchy
  indexing
  inspection
  miscellaneous
  opclass_brin
  opclass_btree
  opclass_hash
  opclass_spgist
  regions
  traversal
  type
  vertex
)

if(PostgreSQL_REGRESS)
  if(WIN32)
    set(H3_REGRESS_PATH_SEP ";")
  else()
    set(H3_REGRESS_PATH_SEP ":")
  endif()

  if(CMAKE_CONFIGURATION_TYPES)
    set(H3_REGRESS_TEMP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/regress-postgresql-$<CONFIG>.conf")
    set(H3_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH
      "$<TARGET_FILE_DIR:postgresql_h3>${H3_REGRESS_PATH_SEP}$libdir")
  else()
    set(H3_REGRESS_TEMP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/regress-postgresql.conf")
    set(H3_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH
      "${CMAKE_BINARY_DIR}/h3${H3_REGRESS_PATH_SEP}$libdir")
  endif()

  # Force pg_regress temp clusters to load extensions from the build tree.
  set(H3_REGRESS_TEMP_CONFIG_CONTENT
    "dynamic_library_path = '${H3_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH}'\n")
  if(PostgreSQL_VERSION_MAJOR VERSION_GREATER_EQUAL "18")
    string(PREPEND H3_REGRESS_TEMP_CONFIG_CONTENT
      "extension_control_path = '${CMAKE_BINARY_DIR}/h3${H3_REGRESS_PATH_SEP}$system'\n")
  endif()
  if(CMAKE_CONFIGURATION_TYPES)
    file(GENERATE OUTPUT "${H3_REGRESS_TEMP_CONFIG}" CONTENT "${H3_REGRESS_TEMP_CONFIG_CONTENT}")
  else()
    file(WRITE "${H3_REGRESS_TEMP_CONFIG}" "${H3_REGRESS_TEMP_CONFIG_CONTENT}")
  endif()

  add_test(
    NAME h3_regress
    COMMAND ${PostgreSQL_REGRESS}
      --temp-instance=${CMAKE_BINARY_DIR}/tmp
      --temp-config=${H3_REGRESS_TEMP_CONFIG}
      --bindir=${PostgreSQL_BIN_DIR}
      --inputdir=${CMAKE_CURRENT_SOURCE_DIR}
      --outputdir=${CMAKE_CURRENT_BINARY_DIR}
      --load-extension h3
      ${TESTS}
  )
endif()

if(PostgreSQL_VALIDATE_EXTUPGRADE)
  add_test(
    NAME h3_validate_extupgrade
    COMMAND pg_validate_extupgrade
      --extname h3
      --from 0.1.0
      --to ${INSTALL_VERSION}
  )
endif()

# @TODO: Figure out how to inline on MacOS
set(H3_INLINED_TEST_ENABLED FALSE)
if(CMAKE_CONFIGURATION_TYPES)
  # Multi-config generators (e.g. Visual Studio) select the active config at
  # test time, so we can safely register this test and scope it to Release.
  set(H3_INLINED_TEST_ENABLED TRUE)
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
  # Single-config generators only have one active build type in the tree.
  # This check asserts Release-quality optimization, not debug behavior.
  set(H3_INLINED_TEST_ENABLED TRUE)
endif()

if(NOT APPLE AND H3_INLINED_TEST_ENABLED)
  add_test(
    NAME h3_inlined
    COMMAND sh -c "! objdump -D \"$<TARGET_FILE:postgresql_h3>\" | grep radsToDegs"
    CONFIGURATIONS Release
  )
endif()
