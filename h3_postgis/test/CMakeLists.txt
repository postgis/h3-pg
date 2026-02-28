set(TESTS
  deprecations
  postgis
  rasters
)

if(PostgreSQL_REGRESS)
  set(H3_POSTGIS_REGRESS_TEMP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/regress-postgresql.conf")
  # Force pg_regress temp clusters to load extensions from the build tree.
  set(H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT
    "dynamic_library_path = '${CMAKE_BINARY_DIR}/h3_postgis:${CMAKE_BINARY_DIR}/h3:$libdir'\n")
  if(PostgreSQL_VERSION_MAJOR VERSION_GREATER_EQUAL "18")
    string(PREPEND H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT
      "extension_control_path = '${CMAKE_BINARY_DIR}/h3_postgis:${CMAKE_BINARY_DIR}/h3:$system'\n")
  endif()
  file(WRITE "${H3_POSTGIS_REGRESS_TEMP_CONFIG}" "${H3_POSTGIS_REGRESS_TEMP_CONFIG_CONTENT}")

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
endif()

if(PostgreSQL_VALIDATE_EXTUPGRADE)
  add_test(
    NAME "h3_postgis_validate_extupgrade"
    COMMAND pg_validate_extupgrade
        --extname h3_postgis
        --from 4.0.0
        --to ${INSTALL_VERSION}
  )
endif()
