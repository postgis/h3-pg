set(TESTS
  deprecations
  postgis
  rasters
  tile_coverage
)

# Cross-extension @extschema:<name>@ placeholders are supported on PG16+.
# Keep the dedicated separate-schema regression there; older versions use
# compatibility SQL generation without cross-extension placeholders.
if(PostgreSQL_VERSION_MAJOR VERSION_GREATER_EQUAL "16")
  list(APPEND TESTS schemas)
endif()

if(PostgreSQL_REGRESS)
  if(WIN32)
    set(H3_POSTGIS_REGRESS_PATH_SEP ";")
  else()
    set(H3_POSTGIS_REGRESS_PATH_SEP ":")
  endif()

  if(CMAKE_CONFIGURATION_TYPES)
    set(H3_POSTGIS_REGRESS_TEMP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/regress-postgresql-$<CONFIG>.conf")
    set(H3_POSTGIS_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH
      "$<TARGET_FILE_DIR:postgresql_h3_postgis>${H3_POSTGIS_REGRESS_PATH_SEP}$<TARGET_FILE_DIR:postgresql_h3>${H3_POSTGIS_REGRESS_PATH_SEP}$libdir")
  else()
    set(H3_POSTGIS_REGRESS_TEMP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/regress-postgresql.conf")
    set(H3_POSTGIS_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH
      "${CMAKE_BINARY_DIR}/h3_postgis${H3_POSTGIS_REGRESS_PATH_SEP}${CMAKE_BINARY_DIR}/h3${H3_POSTGIS_REGRESS_PATH_SEP}$libdir")
  endif()

  # Force pg_regress temp clusters to load extensions from the build tree.
  set(H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT
    "dynamic_library_path = '${H3_POSTGIS_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH}'\n")
  if(PostgreSQL_VERSION_MAJOR VERSION_GREATER_EQUAL "18")
    string(PREPEND H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT
      "extension_control_path = '${CMAKE_BINARY_DIR}/share${H3_POSTGIS_REGRESS_PATH_SEP}$system'\n")
  endif()
  if(CMAKE_CONFIGURATION_TYPES)
    file(GENERATE OUTPUT "${H3_POSTGIS_REGRESS_TEMP_CONFIG}" CONTENT "${H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT}")
  else()
    file(WRITE "${H3_POSTGIS_REGRESS_TEMP_CONFIG}" "${H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT}")
  endif()

  add_test(
    NAME "h3_postgis_regress"
    COMMAND ${PostgreSQL_REGRESS}
      --temp-instance=${CMAKE_BINARY_DIR}/tmp
      --temp-config=${H3_POSTGIS_REGRESS_TEMP_CONFIG}
      --bindir=${PostgreSQL_BIN_DIR}
      --inputdir=${CMAKE_CURRENT_SOURCE_DIR}
      --outputdir=${CMAKE_CURRENT_BINARY_DIR}
      --load-extension h3
      --load-extension postgis
      --load-extension postgis_raster
      --load-extension h3_postgis
      ${TESTS}
  )

  if(PostgreSQL_VERSION_MAJOR VERSION_GREATER_EQUAL "18")
    set(H3_POSTGIS_EXTUPGRADE_EXTENSION_CONTROL_PATH
      "${CMAKE_BINARY_DIR}/share${H3_POSTGIS_REGRESS_PATH_SEP}$system")
  else()
    set(H3_POSTGIS_EXTUPGRADE_EXTENSION_CONTROL_PATH "")
  endif()
endif()

PostgreSQL_add_extupgrade_test(
  NAME h3_postgis_validate_extupgrade
  EXTNAME h3_postgis
  FROM_VERSION 4.0.0
  TO_VERSION ${INSTALL_VERSION}
  TEMP_ROOT ${CMAKE_CURRENT_BINARY_DIR}/extupgrade-h3_postgis
  TEMP_PORT 65442
  DYNAMIC_LIBRARY_PATH ${H3_POSTGIS_REGRESS_TEMP_CONFIG_DYNAMIC_LIBRARY_PATH}
  EXTENSION_CONTROL_PATH ${H3_POSTGIS_EXTUPGRADE_EXTENSION_CONTROL_PATH}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  WATCH_PATHS
    CMakeLists.txt
    cmake/AddPostgreSQLExtension.cmake
    h3_postgis/CMakeLists.txt
    h3_postgis/sql/install
    h3_postgis/sql/updates
)
