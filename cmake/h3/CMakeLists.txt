include(FetchContent)

# Skip core tooling we won't need for extension
set(ENABLE_COVERAGE   OFF)
set(BUILD_BENCHMARKS  OFF)
set(BUILD_FUZZERS     OFF)
set(BUILD_FILTERS     OFF)
set(BUILD_GENERATORS  OFF)
set(ENABLE_FORMAT     OFF)
set(ENABLE_LINTING    OFF)
set(ENABLE_DOCS       OFF)

set(H3_BUNDLED_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/upstream")
if(EXISTS "${H3_BUNDLED_SOURCE_DIR}/CMakeLists.txt")
  file(READ "${H3_BUNDLED_SOURCE_DIR}/VERSION" H3_BUNDLED_VERSION)
  string(STRIP "${H3_BUNDLED_VERSION}" H3_BUNDLED_VERSION)
  if(NOT H3_BUNDLED_VERSION VERSION_EQUAL H3_CORE_VERSION)
    message(FATAL_ERROR
      "Bundled H3 source version ${H3_BUNDLED_VERSION} does not match "
      "configured H3 core version ${H3_CORE_VERSION}"
    )
  endif()

  FetchContent_Declare(
    h3
    SOURCE_DIR "${H3_BUNDLED_SOURCE_DIR}"
  )
else()
  FetchContent_Declare(
    h3
    URL      https://github.com/uber/h3/archive/refs/tags/v${H3_CORE_VERSION}.tar.gz
    URL_HASH SHA256=${H3_CORE_SHA256}
  )
endif()
FetchContent_MakeAvailable(h3)
set(h3_SOURCE_DIR "${h3_SOURCE_DIR}" CACHE PATH "Vendored H3 source directory" FORCE)

set_target_properties(h3 PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION TRUE
  C_VISIBILITY_PRESET          hidden
)

if(CMAKE_C_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
  target_compile_options(h3 PRIVATE -fno-semantic-interposition)
endif()

# Export vendored H3 headers for downstream extension/tooling steps.
# In particular, LLVM bitcode generation for `h3`/`h3_postgis` needs this path.
set(H3_INCLUDE_DIR ${h3_BINARY_DIR}/src/h3lib/include CACHE INTERNAL "Vendored H3 include directory")

# h3_postgis also relies on a small set of released H3 internals for seam
# stitching. Recheck this path when bumping H3.
set(H3_INTERNAL_INCLUDE_DIR ${h3_SOURCE_DIR}/src/h3lib/include CACHE INTERNAL "Vendored H3 internal include directory")
