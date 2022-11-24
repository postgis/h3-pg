PostgreSQL_add_extension(postgresql_h3
  RELOCATABLE
  NAME h3
  COMMENT "H3 bindings for PostgreSQL"
  VERSION ${PROJECT_VERSION}
  COMPONENT ${PROJECT_NAME}
  SOURCES
    src/deprecated.c
    src/edge.c
    src/extension.c
    src/hierarchy.c
    src/indexing.c
    src/inspection.c
    src/miscellaneous.c
    src/opclass_btree.c
    src/opclass_hash.c
    src/operators.c
    src/regions.c
    src/traversal.c
    src/type.c
    src/vertex.c
  INSTALLS
    sql/install/00-type.sql
    sql/install/01-indexing.sql
    sql/install/02-inspection.sql
    sql/install/03-traversal.sql
    sql/install/04-hierarchy.sql
    sql/install/05-regions.sql
    sql/install/06-edge.sql
    sql/install/07-vertex.sql
    sql/install/08-miscellaneous.sql
    sql/install/10-operators.sql
    sql/install/11-opclass_btree.sql
    sql/install/12-opclass_hash.sql
    sql/install/13-opclass_brin.sql
    sql/install/20-casts.sql
    sql/install/30-extension.sql
    sql/install/99-deprecated.sql
  UPDATES
    sql/updates/h3--0.1.0.sql
    sql/updates/h3--0.1.0--0.2.0.sql
    sql/updates/h3--0.2.0--0.3.0.sql
    sql/updates/h3--0.3.0--0.3.1.sql
    sql/updates/h3--0.3.1--0.3.2.sql
    sql/updates/h3--0.3.2--0.4.0.sql
    sql/updates/h3--0.4.0--1.0.0.sql
    sql/updates/h3--1.0.0--1.0.1.sql
    sql/updates/h3--1.0.1--1.0.2.sql
    sql/updates/h3--1.0.2--1.0.3.sql
    sql/updates/h3--1.0.3--1.0.4.sql
    sql/updates/h3--1.0.4--1.0.5.sql
    sql/updates/h3--1.0.5--1.0.6.sql
    sql/updates/h3--1.0.6--3.4.0.sql
    sql/updates/h3--3.4.0--3.4.1.sql
    sql/updates/h3--3.4.1--3.5.0.sql
    sql/updates/h3--3.5.0--3.6.0.sql
    sql/updates/h3--3.6.0--3.6.1.sql
    sql/updates/h3--3.6.1--3.6.2.sql
    sql/updates/h3--3.6.2--3.6.3.sql
    sql/updates/h3--3.6.3--3.6.4.sql
    sql/updates/h3--3.6.4--3.6.5.sql
    sql/updates/h3--3.6.5--3.7.0.sql
    sql/updates/h3--3.7.0--3.7.1.sql
    sql/updates/h3--3.7.1--3.7.2.sql
    sql/updates/h3--3.7.2--4.0.0.sql
    sql/updates/h3--4.0.0--4.0.1.sql
    sql/updates/h3--4.0.1--4.0.2.sql
    sql/updates/h3--4.0.2--4.0.3.sql
    sql/updates/h3--4.0.3--unreleased.sql
)

# include
target_include_directories(postgresql_h3 PRIVATE
  ${CMAKE_BINARY_DIR}/include
  ${CMAKE_SOURCE_DIR}/include
)

# link to h3lib
target_link_libraries(postgresql_h3 h3)

if(BUILD_TESTING)
  add_subdirectory(test)
endif()
