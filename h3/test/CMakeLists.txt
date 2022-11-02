set(TESTS
  extension
  hierarchy
  indexing
  inspection
  miscellaneous
  opclass_btree
  opclass_hash
  #postgis
  regions
  traversal
  type
  edge
)

if(BUILD_TESTING AND PostgreSQL_REGRESS)
  add_test(
    NAME h3_regress
    COMMAND ${PostgreSQL_REGRESS}
      --temp-instance=${CMAKE_BINARY_DIR}/tmp
      --bindir=${PostgreSQL_BIN_DIR}
      --inputdir=${CMAKE_CURRENT_SOURCE_DIR}
      --outputdir=${CMAKE_CURRENT_BINARY_DIR}
      --load-extension h3
      ${TESTS}
  )
endif()
