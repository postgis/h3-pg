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

FetchContent_Declare(
  h3
  URL      https://github.com/uber/h3/archive/refs/tags/v4.0.1.tar.gz
  URL_HASH SHA256=01891901707c6430caaea7e645ff5ac6980cae166094a6f924ded197e5774a19
)
FetchContent_MakeAvailable(h3)

set_target_properties(h3 PROPERTIES
  INTERPROCEDURAL_OPTIMIZATION TRUE
  C_VISIBILITY_PRESET          hidden
)

set(H3_INCLUDE_DIR ${h3_BINARY_DIR}/src/h3lib/include)
