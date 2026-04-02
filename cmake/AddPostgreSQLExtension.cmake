# Add pg_regress binary
find_program(PostgreSQL_REGRESS pg_regress
  HINTS
    "${PostgreSQL_PKG_LIBRARY_DIR}/pgxs/src/test/regress/"
    "${PostgreSQL_BIN_DIR}"
)

# Add pg_validate_extupgrade binary
find_program(PostgreSQL_VALIDATE_EXTUPGRADE pg_validate_extupgrade)

# Add pgindent binary
find_program(PostgreSQL_INDENT pgindent)
find_program(PostgreSQL_BSD_INDENT pg_bsd_indent
  HINTS "${PostgreSQL_BIN_DIR}"
)

# Keep source formatting opt-in for normal builds.
option(PostgreSQL_AUTOFORMAT "Run pgindent targets as build dependencies" OFF)

# Prefer PostgreSQL-provided defaults when available.
set(PostgreSQL_INDENT_ARGS "")
if(EXISTS "${PostgreSQL_SHARE_DIR}/typedefs.list")
  list(APPEND PostgreSQL_INDENT_ARGS "--typedefs=${PostgreSQL_SHARE_DIR}/typedefs.list")
endif()
if(PostgreSQL_BSD_INDENT)
  list(APPEND PostgreSQL_INDENT_ARGS "--indent=${PostgreSQL_BSD_INDENT}")
endif()

# Helper that emits and installs extension bitcode for PostgreSQL JIT.
# This intentionally follows PostgreSQL's `bitcode/<ext>/...` + `<ext>.index.bc`
# layout so the server can discover extension IR consistently.
function(PostgreSQL_add_extension_bitcode LIBRARY_NAME EXTENSION_NAME EXTENSION_COMPONENT)
  if(NOT PostgreSQL_WITH_LLVM)
    return()
  endif()
  if(NOT PostgreSQL_LLVM_CLANG_BIN OR NOT PostgreSQL_LLVM_LTO_BIN)
    message(STATUS "Skipping ${EXTENSION_NAME} bitcode: missing clang/llvm-lto tools")
    return()
  endif()

  get_target_property(EXTENSION_SOURCES ${LIBRARY_NAME} SOURCES)
  if(NOT EXTENSION_SOURCES)
    return()
  endif()

  set(EXTENSION_BITCODE_ROOT "${CMAKE_CURRENT_BINARY_DIR}/bitcode")
  set(EXTENSION_BITCODE_DIR "${EXTENSION_BITCODE_ROOT}/${EXTENSION_NAME}")
  set(EXTENSION_BITCODE_FILES "")
  set(EXTENSION_BITCODE_REL_FILES "")

  set(EXTENSION_BITCODE_INCLUDE_DIRS
    ${PostgreSQL_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
  )
  get_target_property(EXTENSION_TARGET_INCLUDE_DIRS ${LIBRARY_NAME} INCLUDE_DIRECTORIES)
  if(EXTENSION_TARGET_INCLUDE_DIRS)
    list(APPEND EXTENSION_BITCODE_INCLUDE_DIRS ${EXTENSION_TARGET_INCLUDE_DIRS})
  endif()

  # Pull include paths from linked targets (e.g. `h3`, shared helper objects).
  # This keeps bitcode compilation aligned with normal target compilation.
  get_target_property(EXTENSION_LINK_LIBRARIES ${LIBRARY_NAME} LINK_LIBRARIES)
  foreach(link_lib ${EXTENSION_LINK_LIBRARIES})
    if(TARGET ${link_lib})
      get_target_property(LINK_INTERFACE_INCLUDE_DIRS ${link_lib} INTERFACE_INCLUDE_DIRECTORIES)
      if(LINK_INTERFACE_INCLUDE_DIRS)
        list(APPEND EXTENSION_BITCODE_INCLUDE_DIRS ${LINK_INTERFACE_INCLUDE_DIRS})
      endif()
      get_target_property(LINK_INCLUDE_DIRS ${link_lib} INCLUDE_DIRECTORIES)
      if(LINK_INCLUDE_DIRS)
        list(APPEND EXTENSION_BITCODE_INCLUDE_DIRS ${LINK_INCLUDE_DIRS})
      endif()
    endif()
  endforeach()

  # Keep compatibility with older helper variable shape if available.
  if(DEFINED H3_INCLUDE_DIR)
    list(APPEND EXTENSION_BITCODE_INCLUDE_DIRS "${H3_INCLUDE_DIR}")
  endif()
  list(REMOVE_DUPLICATES EXTENSION_BITCODE_INCLUDE_DIRS)

  set(EXTENSION_BITCODE_INCLUDE_ARGS "")
  foreach(include_dir ${EXTENSION_BITCODE_INCLUDE_DIRS})
    list(APPEND EXTENSION_BITCODE_INCLUDE_ARGS "-I${include_dir}")
  endforeach()

  # Match extension compile-time feature toggles that affect exposed symbols.
  set(EXTENSION_BITCODE_DEFINE_ARGS
    "-DPOSTGRESQL_VERSION_MAJOR=${PostgreSQL_VERSION_MAJOR}"
  )
  set(EXTENSION_BITCODE_COMPILE_ARGS "")
  if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    list(APPEND EXTENSION_BITCODE_COMPILE_ARGS "-fno-semantic-interposition")
  endif()

  foreach(source_file ${EXTENSION_SOURCES})
    get_filename_component(SOURCE_ABS "${source_file}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
    file(RELATIVE_PATH SOURCE_REL "${CMAKE_CURRENT_SOURCE_DIR}" "${SOURCE_ABS}")
    set(BITCODE_REL_FILE "${SOURCE_REL}")
    string(REGEX REPLACE "\\.[^.]+$" ".bc" BITCODE_REL_FILE "${BITCODE_REL_FILE}")
    set(BITCODE_FILE "${EXTENSION_BITCODE_DIR}/${BITCODE_REL_FILE}")
    get_filename_component(BITCODE_FILE_DIR "${BITCODE_FILE}" DIRECTORY)

    add_custom_command(
      OUTPUT "${BITCODE_FILE}"
      COMMAND ${CMAKE_COMMAND} -E make_directory "${BITCODE_FILE_DIR}"
      COMMAND ${PostgreSQL_LLVM_CLANG_BIN}
        -emit-llvm
        -c
        -O2
        -flto=thin
        -fPIC
        ${EXTENSION_BITCODE_COMPILE_ARGS}
        ${EXTENSION_BITCODE_INCLUDE_ARGS}
        ${EXTENSION_BITCODE_DEFINE_ARGS}
        "${SOURCE_ABS}"
        -o "${BITCODE_FILE}"
      DEPENDS "${SOURCE_ABS}"
      COMMENT "Generating ${EXTENSION_NAME} bitcode for ${SOURCE_REL}"
      VERBATIM
    )
    list(APPEND EXTENSION_BITCODE_FILES "${BITCODE_FILE}")
    list(APPEND EXTENSION_BITCODE_REL_FILES "${EXTENSION_NAME}/${BITCODE_REL_FILE}")
  endforeach()

  set(EXTENSION_BITCODE_INDEX "${EXTENSION_BITCODE_ROOT}/${EXTENSION_NAME}.index.bc")
  add_custom_command(
    OUTPUT "${EXTENSION_BITCODE_INDEX}"
    COMMAND ${PostgreSQL_LLVM_LTO_BIN}
      -thinlto-action=thinlink
      -o "${EXTENSION_BITCODE_INDEX}"
      ${EXTENSION_BITCODE_REL_FILES}
    WORKING_DIRECTORY "${EXTENSION_BITCODE_ROOT}"
    DEPENDS ${EXTENSION_BITCODE_FILES}
    COMMENT "Linking ${EXTENSION_NAME} bitcode index for PostgreSQL JIT (ThinLTO)"
    VERBATIM
  )

  add_custom_target("${LIBRARY_NAME}_bitcode" ALL
    DEPENDS "${EXTENSION_BITCODE_INDEX}"
  )
  add_dependencies("${LIBRARY_NAME}_bitcode" "${LIBRARY_NAME}")

  # Install bitcode preserving relative paths used in the ThinLTO index.
  foreach(bitcode_file ${EXTENSION_BITCODE_FILES})
    file(RELATIVE_PATH BITCODE_REL_FILE "${EXTENSION_BITCODE_DIR}" "${bitcode_file}")
    get_filename_component(BITCODE_REL_DIR "${BITCODE_REL_FILE}" DIRECTORY)
    if(BITCODE_REL_DIR AND NOT BITCODE_REL_DIR STREQUAL ".")
      set(BITCODE_INSTALL_DEST "${PostgreSQL_PKG_LIBRARY_DIR}/bitcode/${EXTENSION_NAME}/${BITCODE_REL_DIR}")
    else()
      set(BITCODE_INSTALL_DEST "${PostgreSQL_PKG_LIBRARY_DIR}/bitcode/${EXTENSION_NAME}")
    endif()
    install(
      FILES "${bitcode_file}"
      DESTINATION "${BITCODE_INSTALL_DEST}"
      COMPONENT ${EXTENSION_COMPONENT}
    )
  endforeach()
  install(
    FILES "${EXTENSION_BITCODE_INDEX}"
    DESTINATION "${PostgreSQL_PKG_LIBRARY_DIR}/bitcode"
    COMPONENT ${EXTENSION_COMPONENT}
  )
endfunction()

# Helper command to add extensions
function(PostgreSQL_add_extension LIBRARY_NAME)
  set(options RELOCATABLE)
  set(oneValueArgs NAME COMMENT VERSION COMPONENT)
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
    target_link_libraries(${LIBRARY_NAME} PRIVATE PostgreSQL::PostgreSQL)

    if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
      # Allow cross-TU optimization of internal calls inside the extension DSO.
      target_compile_options(${LIBRARY_NAME} PRIVATE -fno-semantic-interposition)
    endif()

    # Handle macOS specifics
    if(APPLE)
      # Fix apple missing symbols
      set_target_properties(${LIBRARY_NAME} PROPERTIES LINK_FLAGS ${PostgreSQL_LINK_FLAGS})

      # Since Postgres 16, the shared library extension on macOS is `dylib`, not `so`.
      # Ref https://github.com/postgres/postgres/commit/b55f62abb2c2e07dfae99e19a2b3d7ca9e58dc1a
      if (${PostgreSQL_VERSION_MAJOR} VERSION_GREATER_EQUAL "16")
        set_target_properties (${LIBRARY_NAME} PROPERTIES SUFFIX ".dylib")
      endif()
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

  # Materialize build-tree extension metadata under a share-root layout that
  # PostgreSQL can discover through extension_control_path.
  string(REPLACE ";" ", " EXTENSION_REQUIRES "${EXTENSION_REQUIRES}")
  set(EXTENSION_BUILD_SHARE_DIR "${CMAKE_BINARY_DIR}/share")
  set(EXTENSION_BUILD_EXTENSION_DIR "${EXTENSION_BUILD_SHARE_DIR}/extension")
  file(MAKE_DIRECTORY "${EXTENSION_BUILD_EXTENSION_DIR}")
  configure_file(
    ${CMAKE_SOURCE_DIR}/cmake/control.in
    ${CMAKE_CURRENT_BINARY_DIR}/${EXTENSION_NAME}.control
    @ONLY
  )
  configure_file(
    ${CMAKE_SOURCE_DIR}/cmake/control.in
    ${EXTENSION_BUILD_EXTENSION_DIR}/${EXTENSION_NAME}.control
    @ONLY
  )
  configure_file(
    ${CMAKE_SOURCE_DIR}/cmake/control.in
    ${CMAKE_CURRENT_BINARY_DIR}/${EXTENSION_NAME}.control.install
    @ONLY
  )

  # Generate .sql install file
  set(EXTENSION_INSTALL ${CMAKE_CURRENT_BINARY_DIR}/${EXTENSION_NAME}--${EXTENSION_VERSION}.sql)
  file(WRITE "${EXTENSION_INSTALL}.in" "")
  foreach(file ${EXTENSION_INSTALLS})
    file(READ ${file} CONTENTS)
    # PostgreSQL 13-15 do not understand cross-extension placeholders
    # (`@extschema:<name>@`). Strip those qualifiers in generated SQL for
    # compatibility; PostgreSQL 16+ keeps the placeholder form.
    if(PostgreSQL_VERSION_MAJOR VERSION_LESS "16")
      string(REPLACE "@extschema:${EXTENSION_NAME}@." "" CONTENTS "${CONTENTS}")
      string(REPLACE "@extschema:h3@." "" CONTENTS "${CONTENTS}")
      string(REPLACE "@extschema:postgis@." "" CONTENTS "${CONTENTS}")
      string(REPLACE "@extschema:postgis_raster@." "" CONTENTS "${CONTENTS}")
    endif()
    file(APPEND "${EXTENSION_INSTALL}.in" "${CONTENTS}")
  endforeach()
  configure_file("${EXTENSION_INSTALL}.in" "${EXTENSION_INSTALL}" COPYONLY)
  configure_file(
    "${EXTENSION_INSTALL}"
    "${EXTENSION_BUILD_EXTENSION_DIR}/${EXTENSION_NAME}--${EXTENSION_VERSION}.sql"
    COPYONLY
  )

  # Apply the same compatibility preprocessing to update scripts.
  set(EXTENSION_UPDATES_PROCESSED "")
  foreach(file ${EXTENSION_UPDATES})
    get_filename_component(UPDATE_NAME "${file}" NAME)
    set(UPDATE_OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${UPDATE_NAME}")
    if(PostgreSQL_VERSION_MAJOR VERSION_LESS "16")
      file(READ "${file}" UPDATE_CONTENTS)
      string(REPLACE "@extschema:${EXTENSION_NAME}@." "" UPDATE_CONTENTS "${UPDATE_CONTENTS}")
      string(REPLACE "@extschema:h3@." "" UPDATE_CONTENTS "${UPDATE_CONTENTS}")
      string(REPLACE "@extschema:postgis@." "" UPDATE_CONTENTS "${UPDATE_CONTENTS}")
      string(REPLACE "@extschema:postgis_raster@." "" UPDATE_CONTENTS "${UPDATE_CONTENTS}")
      file(WRITE "${UPDATE_OUTPUT}" "${UPDATE_CONTENTS}")
    else()
      configure_file("${file}" "${UPDATE_OUTPUT}" COPYONLY)
    endif()
    configure_file(
      "${UPDATE_OUTPUT}"
      "${EXTENSION_BUILD_EXTENSION_DIR}/${UPDATE_NAME}"
      COPYONLY
    )
    list(APPEND EXTENSION_UPDATES_PROCESSED "${UPDATE_OUTPUT}")
  endforeach()

  # Install everything else into share-dir
  install(
    FILES ${CMAKE_CURRENT_BINARY_DIR}/${EXTENSION_NAME}.control.install
    DESTINATION "${PostgreSQL_SHARE_DIR}/extension"
    RENAME ${EXTENSION_NAME}.control
    COMPONENT ${EXTENSION_COMPONENT}
  )

  install(
    FILES
      ${EXTENSION_INSTALL}
      ${EXTENSION_UPDATES_PROCESSED}
    DESTINATION "${PostgreSQL_SHARE_DIR}/extension"
    COMPONENT ${EXTENSION_COMPONENT}
  )

  # Setup auto-format
  if(PostgreSQL_INDENT AND EXTENSION_SOURCES)
    add_custom_target("format_${EXTENSION_NAME}"
      COMMAND ${PostgreSQL_INDENT} ${PostgreSQL_INDENT_ARGS} ${EXTENSION_SOURCES}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "Formatting ${EXTENSION_NAME} sources"
    )
    if(PostgreSQL_AUTOFORMAT)
      add_dependencies(${LIBRARY_NAME} "format_${EXTENSION_NAME}")
    endif()
  endif()
endfunction()

# Register a pg_validate_extupgrade-backed test when available, or an explicit
# placeholder test that makes skipped upgrade validation visible in ctest.
function(PostgreSQL_add_extupgrade_test)
  set(options "")
  set(oneValueArgs
    NAME
    EXTNAME
    FROM_VERSION
    TO_VERSION
    TEMP_ROOT
    TEMP_PORT
    DYNAMIC_LIBRARY_PATH
    EXTENSION_CONTROL_PATH
    WORKING_DIRECTORY
  )
  set(multiValueArgs WATCH_PATHS)
  cmake_parse_arguments(EXTUPGRADE "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})

  if(NOT EXTUPGRADE_NAME OR NOT EXTUPGRADE_EXTNAME OR NOT EXTUPGRADE_FROM_VERSION OR NOT EXTUPGRADE_TO_VERSION)
    message(FATAL_ERROR "PostgreSQL_add_extupgrade_test requires NAME, EXTNAME, FROM_VERSION, and TO_VERSION")
  endif()

  if(PostgreSQL_VALIDATE_EXTUPGRADE)
    if(NOT EXTUPGRADE_TEMP_ROOT OR NOT EXTUPGRADE_TEMP_PORT OR NOT EXTUPGRADE_DYNAMIC_LIBRARY_PATH)
      message(FATAL_ERROR
        "PostgreSQL_add_extupgrade_test requires TEMP_ROOT, TEMP_PORT, and DYNAMIC_LIBRARY_PATH "
        "when pg_validate_extupgrade is available")
    endif()

    add_test(
      NAME ${EXTUPGRADE_NAME}
      COMMAND ${CMAKE_COMMAND}
        -DPG_VALIDATE_EXTUPGRADE=${PostgreSQL_VALIDATE_EXTUPGRADE}
        -DPOSTGRESQL_BINDIR=${PostgreSQL_BIN_DIR}
        -DTEMP_ROOT=${EXTUPGRADE_TEMP_ROOT}
        -DTEMP_PORT=${EXTUPGRADE_TEMP_PORT}
        -DEXTNAME=${EXTUPGRADE_EXTNAME}
        -DFROM_VERSION=${EXTUPGRADE_FROM_VERSION}
        -DTO_VERSION=${EXTUPGRADE_TO_VERSION}
        -DDYNAMIC_LIBRARY_PATH=${EXTUPGRADE_DYNAMIC_LIBRARY_PATH}
        -DEXTENSION_CONTROL_PATH=${EXTUPGRADE_EXTENSION_CONTROL_PATH}
        -DWORKING_DIRECTORY=${EXTUPGRADE_WORKING_DIRECTORY}
        -P ${CMAKE_SOURCE_DIR}/cmake/RunValidateExtupgrade.cmake
    )
    return()
  endif()

  string(JOIN "|" EXTUPGRADE_WATCH_PATHS_SERIALIZED ${EXTUPGRADE_WATCH_PATHS})
  add_test(
    NAME ${EXTUPGRADE_NAME}_unavailable
    COMMAND ${CMAKE_COMMAND}
      -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
      -DTEST_NAME=${EXTUPGRADE_NAME}
      -DEXTENSION_NAME=${EXTUPGRADE_EXTNAME}
      -DWATCH_PATHS_SERIALIZED=${EXTUPGRADE_WATCH_PATHS_SERIALIZED}
      -P ${CMAKE_SOURCE_DIR}/cmake/ValidateExtupgradeUnavailable.cmake
  )
endfunction()
