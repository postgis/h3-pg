\pset tuples_only on

-- Recreate extensions in dedicated schemas.
DROP EXTENSION h3_postgis;
DROP EXTENSION postgis_raster;
DROP EXTENSION postgis;
DROP EXTENSION h3;

CREATE SCHEMA h3_s;
CREATE SCHEMA postgis_s;
CREATE SCHEMA postgis_raster_s;
CREATE SCHEMA h3pg_s;

CREATE EXTENSION h3 SCHEMA h3_s;
CREATE EXTENSION postgis SCHEMA postgis_s;

-- PostGIS constraint: postgis_raster must be in the same schema as postgis.
CREATE FUNCTION h3_test_postgis_raster_schema_rejected() RETURNS boolean LANGUAGE plpgsql AS
$$
BEGIN
    BEGIN
        EXECUTE 'CREATE EXTENSION postgis_raster SCHEMA postgis_raster_s';
        EXECUTE 'DROP EXTENSION postgis_raster';
        RETURN false;
    EXCEPTION WHEN OTHERS THEN
        RETURN true;
    END;
END;
$$;
SELECT h3_test_postgis_raster_schema_rejected();
DROP FUNCTION h3_test_postgis_raster_schema_rejected();

CREATE EXTENSION postgis_raster SCHEMA postgis_s;
CREATE EXTENSION h3_postgis SCHEMA h3pg_s;

CREATE TABLE h3pg_s.h3_schema_idx_test (
    h3cell h3_s.h3index
);
INSERT INTO h3pg_s.h3_schema_idx_test VALUES ('8a63a9a99047fff'::h3_s.h3index);

-- Force restricted lookup to mimic PostgreSQL 17+ maintenance operation context.
SET search_path = pg_catalog, pg_temp;

CREATE INDEX h3_schema_idx_test_gix
    ON h3pg_s.h3_schema_idx_test
    USING GIST (h3pg_s.h3_cell_to_geometry(h3cell));
DROP INDEX h3pg_s.h3_schema_idx_test_gix;

CREATE MATERIALIZED VIEW h3pg_s.h3_schema_mv_test AS
SELECT h3pg_s.h3_cell_to_geometry(h3cell) AS geom
FROM h3pg_s.h3_schema_idx_test;
REFRESH MATERIALIZED VIEW h3pg_s.h3_schema_mv_test;
DROP MATERIALIZED VIEW h3pg_s.h3_schema_mv_test;

SELECT h3pg_s.h3_latlng_to_cell(
    'POINT(55.6677199224442 12.592131261648213)'::postgis_s.geometry,
    10
)::text = '8a63a9a99047fff';

-- pg_dump/restore sets search_path='' and should still replay expression
-- indexes that call deprecated wrappers.
SELECT pg_catalog.set_config('search_path', '', false) = '';
CREATE TABLE h3pg_s.h3_schema_dump_restore_test (
    bar postgis_s.geometry(Point, 4326) NOT NULL
);
CREATE INDEX h3_schema_dump_restore_test_idx
    ON h3pg_s.h3_schema_dump_restore_test
    USING btree (h3pg_s.h3_lat_lng_to_cell(bar, 10));
DROP INDEX h3pg_s.h3_schema_dump_restore_test_idx;
DROP TABLE h3pg_s.h3_schema_dump_restore_test;

RESET search_path;
DROP TABLE h3pg_s.h3_schema_idx_test;
