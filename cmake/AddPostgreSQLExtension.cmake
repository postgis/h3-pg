# Add pg_regress binary
find_program(PostgreSQL_REGRESS pg_regress
  HINTS
    "${PostgreSQL_PKG_LIBRARY_DIR}/pgxs/src/test/regress/"
    "${PostgreSQL_BIN_DIR}"
)

# Add pg_validate_extupgrade binary
find_program(PostgreSQL_VALIDATE_EXTUPGRADE pg_validate_extupgrade)

# Helper command to add extensions
function(PostgreSQL_add_extension LIBRARY_NAME)
  set(options RELOCATABLE)
  set(oneValueArgs NAME COMMENT COMPONENT)
  set(multiValueArgs REQUIRES SOURCES INSTALLS UPDATES)
  cmake_parse_arguments(EXTENSION "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  # Default extension name to same as library name
  if(NOT EXTENSION_NAME)
    set(EXTENSION_NAME ${LIBRARY_NAME})
  endif()

  # Allow extensions without sources
  if(EXTENSION_SOURCES)
    # Add extension as a dynamically linked library
    add_library(${LIBRARY_NAME} MODULE ${EXTENSION_SOURCES})

    # Link extension to PostgreSQL
    target_link_libraries(${LIBRARY_NAME} PostgreSQL::PostgreSQL)

    # Fix apple missing symbols
    if(APPLE)
      set_target_properties(${LIBRARY_NAME} PROPERTIES LINK_FLAGS ${PostgreSQL_LINK_FLAGS})
    endif()

    # Final touches on output file
    set_target_properties(${LIBRARY_NAME} PROPERTIES
      OUTPUT_NAME ${EXTENSION_NAME}
      INTERPROCEDURAL_OPTIMIZATION TRUE
      #C_VISIBILITY_PRESET hidden # @TODO: how to get this working?
      PREFIX "" # Avoid lib* prefix on output file
    )

    # Install .so/.dll to pkglib-dir
    install(
      TARGETS ${LIBRARY_NAME}
      LIBRARY DESTINATION "${PostgreSQL_PKG_LIBRARY_DIR}"
      COMPONENT ${EXTENSION_COMPONENT}
    )
  endif()

  # Generate .control file
  string(REPLACE ";" ", " EXTENSION_REQUIRES "${EXTENSION_REQUIRES}")
  configure_file(
    ${CMAKE_SOURCE_DIR}/cmake/control.in
    ${EXTENSION_NAME}.control
  )

  # Generate .sql install file
  set(EXTENSION_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/${EXTENSION_NAME}--${PROJECT_VERSION}.sql)
  foreach(file ${EXTENSION_INSTALLS})
    file(READ ${file} CONTENTS)
    file(APPEND ${EXTENSION_INSTALL} "${CONTENTS}")
  endforeach()

  # Install everything else into share-dir
  install(
    FILES
      ${CMAKE_CURRENT_BINARY_DIR}/${EXTENSION_NAME}.control
      ${EXTENSION_INSTALL}
      ${EXTENSION_UPDATES}
    DESTINATION "${PostgreSQL_SHARE_DIR}/extension"
    COMPONENT ${EXTENSION_COMPONENT}
  )
endfunction()
