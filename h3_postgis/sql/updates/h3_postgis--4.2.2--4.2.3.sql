/*
 * Copyright 2025 Zacharias Knudsen
 * Copyright 2026 Darafei Praliaskouski
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION h3_postgis UPDATE TO '4.2.3'" to load this file. \quit

CREATE OR REPLACE FUNCTION h3_get_resolution_from_tile_zoom(
    z integer,
    max_h3_resolution integer DEFAULT 15,
    min_h3_resolution integer DEFAULT 0,
    hex_edge_pixels integer DEFAULT 44,
    tile_size integer DEFAULT 512
) RETURNS integer
AS $$
DECLARE
    e0  CONSTANT numeric := @extschema:h3@.h3_get_hexagon_edge_length_avg(0,'m'); -- res-0 edge
    ln7 CONSTANT numeric := pg_catalog.LN(pg_catalog.SQRT(7.0));  -- = ln(√7)
    desired_edge numeric;
    r_est        integer;
BEGIN
    IF z < 0 THEN
        RAISE EXCEPTION 'Negative tile zoom levels are not supported';
    END IF;

    desired_edge := 40075016.6855785 / (tile_size * 2 ^ z) * hex_edge_pixels;

    r_est := pg_catalog.ROUND(pg_catalog.LN(e0 / desired_edge) / ln7);

    RETURN GREATEST(min_h3_resolution,
           LEAST(r_est, max_h3_resolution));
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_get_resolution_from_tile_zoom(integer, integer, integer, integer, integer)
IS 'Returns the optimal H3 resolution for a specified XYZ tile zoom level, based on hexagon size in pixels and resolution limits';

-- deprecations

CREATE OR REPLACE FUNCTION h3_latlng_to_cell(geometry, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_latlng_to_cell($1::point, $2); $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_latlng_to_cell(geometry, resolution integer)
IS 'Indexes the location at the specified resolution.';

CREATE OR REPLACE FUNCTION h3_latlng_to_cell(geography, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_latlng_to_cell(($1::@extschema:postgis@.geometry)::point, $2); $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_latlng_to_cell(geometry, resolution integer)
IS 'Indexes the location at the specified resolution.';

COMMENT ON FUNCTION
    h3_lat_lng_to_cell(geometry, resolution integer)
IS 'DEPRECATED: Use `h3_latlng_to_cell` instead..';

COMMENT ON FUNCTION
    h3_lat_lng_to_cell(geometry, resolution integer)
IS 'DEPRECATED: Use `h3_latlng_to_cell` instead..';

-- deprecations/indexing

CREATE OR REPLACE FUNCTION h3_cell_to_geometry(h3index) RETURNS geometry
  AS $$ SELECT @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_latlng($1)::@extschema:postgis@.geometry, 4326) $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

-- deprecations/traversal
CREATE OR REPLACE FUNCTION
    h3_grid_path_cells_recursive(origin h3index, destination h3index) RETURNS SETOF h3index
AS $$
DECLARE
    self_schema CONSTANT text := (
        SELECT extnamespace::regnamespace::text
        FROM pg_catalog.pg_extension
        WHERE extname = 'h3_postgis'
    );
    g1 @extschema:postgis@.geometry;
    g2 @extschema:postgis@.geometry;
    middle origin%TYPE;
BEGIN
    IF (SELECT
            origin OPERATOR(@extschema:h3@.<>) destination
            AND NOT @extschema:h3@.h3_are_neighbor_cells(origin, destination)
            AND ((base1 OPERATOR(@extschema:h3@.<>) base2 AND NOT @extschema:h3@.h3_are_neighbor_cells(base1, base2))
                OR ((@extschema:h3@.h3_is_pentagon(base1) OR @extschema:h3@.h3_is_pentagon(base2))
                    AND NOT (
                        @extschema:h3@.h3_get_icosahedron_faces(origin)
                        && @extschema:h3@.h3_get_icosahedron_faces(destination))))
        FROM (
            SELECT
                @extschema:h3@.h3_cell_to_parent(origin, 0) AS base1,
                @extschema:h3@.h3_cell_to_parent(destination, 0) AS base2) AS t)
    THEN
        SELECT
            @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_latlng(origin)::@extschema:postgis@.geometry, 4326),
            @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_latlng(destination)::@extschema:postgis@.geometry, 4326)
        INTO g1, g2
        ;

        SELECT
            @extschema:h3@.h3_latlng_to_cell(
                (
                    @extschema:postgis@.ST_Centroid(
                        @extschema:postgis@.ST_MakeLine(g1, g2)::@extschema:postgis@.geography
                    )::@extschema:postgis@.geometry
                )::point,
                @extschema:h3@.h3_get_resolution(origin)
            )
        INTO middle
        ;

        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT * FROM %I.h3_grid_path_cells_recursive($1, $2)',
            self_schema
        )
        USING origin, middle;

        RETURN QUERY EXECUTE pg_catalog.format(
            'SELECT * FROM %I.h3_grid_path_cells_recursive($1, $2)',
            self_schema
        )
        USING middle, destination;
    ELSE
        RETURN QUERY SELECT @extschema:h3@.h3_grid_path_cells(origin, destination);
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- deprecations/operators
DROP OPERATOR @ (geometry, integer);
CREATE OPERATOR @ (
    PROCEDURE = h3_latlng_to_cell,
    LEFTARG = geometry, RIGHTARG = integer
);
COMMENT ON OPERATOR @ (geometry, integer) IS
  'Index geometry at specified resolution.';

DROP OPERATOR @ (geography, integer);
CREATE OPERATOR @ (
    PROCEDURE = h3_latlng_to_cell,
    LEFTARG = geography, RIGHTARG = integer
);
COMMENT ON OPERATOR @ (geography, integer) IS
  'Index geography at specified resolution.';

-- depracations/rasters

CREATE OR REPLACE FUNCTION __h3_raster_polygon_centroid_cell(
    poly geometry,
    resolution integer)
RETURNS h3index
AS $$
    WITH centroid_cell AS (
        SELECT @extschema:h3@.h3_latlng_to_cell(
            (@extschema:postgis@.ST_Transform(@extschema:postgis@.ST_Centroid(poly), 4326))::point,
            resolution
        ) AS cell
    )
    SELECT CASE
        WHEN @extschema:h3@.h3_is_pentagon(cell) THEN (
            SELECT neighbor
            FROM @extschema:h3@.h3_grid_disk(cell) AS neighbor
            WHERE neighbor OPERATOR(@extschema:h3@.<>) cell
            LIMIT 1
        )
        ELSE
            cell
    END
    FROM centroid_cell;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION h3_raster_summary_centroids(
    rast raster,
    resolution integer,
    nband integer DEFAULT 1)
RETURNS TABLE (h3 h3index, stats h3_raster_summary_stats)
AS $$
    SELECT
        @extschema:h3@.h3_latlng_to_cell(
            (@extschema:postgis@.ST_Transform(geom, 4326))::point,
            resolution
        ) AS h3,
        ROW(
            pg_catalog.count(val),
            pg_catalog.sum(val),
            pg_catalog.avg(val),
            pg_catalog.stddev_pop(val),
            pg_catalog.min(val),
            pg_catalog.max(val)
        ) AS stats
    FROM @extschema:postgis_raster@.ST_PixelAsCentroids(rast, nband)
    GROUP BY 1;
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION __h3_raster_class_summary_centroids(
    rast raster,
    resolution integer,
    nband integer,
    pixel_area double precision)
RETURNS TABLE (h3 h3index, val integer, summary h3_raster_class_summary_item)
AS $$
    SELECT
        @extschema:h3@.h3_latlng_to_cell(
            (@extschema:postgis@.ST_Transform(geom, 4326))::point,
            resolution
        ) AS h3,
        val::integer AS val,
        ROW(
            val::integer,
            pg_catalog.count(*)::double precision,
            pg_catalog.count(*) * pixel_area
        ) AS summary
    FROM @extschema:postgis_raster@.ST_PixelAsCentroids(rast, nband)
    GROUP BY 1, 2;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
