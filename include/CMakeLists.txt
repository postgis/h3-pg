add_library(postgresql_h3_shared
  OBJECT error.c
)
target_link_libraries(postgresql_h3_shared
  PRIVATE PostgreSQL::PostgreSQL
)
target_include_directories(postgresql_h3_shared
  INTERFACE ./
)
