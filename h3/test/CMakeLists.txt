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
  set(H3_REGRESS_TEMP_CONFIG "${CMAKE_CURRENT_BINARY_DIR}/regress-postgresql.conf")
  # Force pg_regress temp clusters to load extensions from the build tree.
  set(H3_REGRESS_TEMP_CONFIG_CONTENT
    "dynamic_library_path = '${CMAKE_BINARY_DIR}/h3:$libdir'\n")
  if(PostgreSQL_VERSION_MAJOR VERSION_GREATER_EQUAL "18")
    string(PREPEND H3_REGRESS_TEMP_CONFIG_CONTENT
      "extension_control_path = '${CMAKE_BINARY_DIR}/h3:$system'\n")
  endif()
  file(WRITE "${H3_REGRESS_TEMP_CONFIG}" "${H3_REGRESS_TEMP_CONFIG_CONTENT}")

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
if(NOT APPLE)
  add_test(
    NAME h3_inlined
    COMMAND sh -c "! objdump -D ${PostgreSQL_PKG_LIBRARY_DIR}/h3.so | grep radsToDegs"
    CONFIGURATIONS Release
  )
endif()
