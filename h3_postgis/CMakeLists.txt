PostgreSQL_add_extension(postgresql_h3_postgis
  RELOCATABLE
  NAME h3_postgis
  COMMENT "H3 PostGIS integration"
  VERSION ${PROJECT_VERSION}
  COMPONENT ${PROJECT_NAME}
  REQUIRES
    h3
    postgis
  INSTALLS
    sql/install/01-indexing.sql
    sql/install/03-traversal.sql
    sql/install/05-regions.sql
    sql/install/20-casts.sql
    sql/install/99-deprecated.sql
  UPDATES
    sql/updates/h3_postgis--4.0.0.sql
    sql/updates/h3_postgis--4.0.0--4.0.1.sql
    sql/updates/h3_postgis--4.0.1--4.0.2.sql
    sql/updates/h3_postgis--4.0.2--4.0.3.sql
    sql/updates/h3_postgis--4.0.3--unreleased.sql
)

if(BUILD_TESTING AND PostgreSQL_PostGIS_FOUND)
  add_subdirectory(test)
endif()
