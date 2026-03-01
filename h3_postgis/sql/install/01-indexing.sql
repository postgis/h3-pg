/*
 * Copyright 2024 Zacharias Knudsen
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

--| ## Input requirements
--|
--| `h3_postgis` functions interpret PostGIS coordinates as lon/lat degrees in SRID 4326.
--| They do not reproject.
--|
--| Practical checklist:
--|
--| - Coordinate bounds (lon/lat range): enable `h3.strict` (see GUCs)
--| - For polygon-to-cell functions: validate inputs with `ST_IsValid()`
--| - Invalid polygons: behavior is undefined and can produce unexpectedly large result sets
--| - If you repair inputs: `ST_MakeValid()` can return MULTIPOLYGON/GEOMETRYCOLLECTION, so keep polygonal parts
--|
--| ### Quick sanity checks
--| ```sql
--| -- Optional: reject out-of-range lon/lat early
--| SET h3.strict TO true;
--|
--| SELECT
--|   ST_SRID(geom)          AS srid,
--|   ST_GeometryType(geom)  AS type,
--|   ST_IsValid(geom)       AS is_valid
--| FROM my_polygons
--| LIMIT 10;
--| ```
--|
--| ### Repairing invalid geometries
--|
--| PostGIS supports optional `ST_MakeValid()` parameters (for example `method=structure`) on
--| newer versions. See the PostGIS docs for details.
--| ```sql
--| WITH prepared AS (
--|   SELECT ST_CollectionExtract(
--|       CASE
--|           WHEN ST_IsValid(geom) THEN geom
--|           ELSE ST_MakeValid(geom)
--|       END,
--|       3  -- polygonal components
--|   ) AS geom
--|   FROM my_polygons
--| )
--| SELECT h3_polygon_to_cells(geom, 7)
--| FROM prepared
--| WHERE NOT ST_IsEmpty(geom);
--| ```
--|
--| `ST_MakeValid()` is not a universal fix: it can change topology, and results can differ
--| across geometry models and projections. In particular, self-intersections are often
--| repaired into "bow-tie" style MULTIPOLYGON output. Review repaired geometries (and consider
--| inspecting `ST_IsValidReason()` during debugging).
--| See PostGIS docs: <https://postgis.net/docs/ST_MakeValid.html>

--| # PostGIS Indexing Functions
--|
--| PostgreSQL 17+ executes CREATE INDEX (and other maintenance operations)
--| with a restricted search_path. Use @extschema:*@ placeholders so wrapper
--| functions can always resolve cross-extension symbols safely.
--| Keep wrappers as plain SQL without STRICT to preserve SQL-function inlining.

--@ availability: 4.2.3
--@ refid: h3_latlng_to_cell_geometry
CREATE OR REPLACE FUNCTION h3_latlng_to_cell(@extschema:postgis@.geometry, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_latlng_to_cell($1::point, $2); $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_latlng_to_cell(geometry, resolution integer)
IS 'Indexes the location at the specified resolution.';

--@ availability: 4.2.3
--@ refid: h3_latlng_to_cell_geography
CREATE OR REPLACE FUNCTION h3_latlng_to_cell(@extschema:postgis@.geography, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_latlng_to_cell($1::@extschema:postgis@.geometry, $2); $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_latlng_to_cell(geometry, resolution integer)
IS 'Indexes the location at the specified resolution.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_geometry
CREATE OR REPLACE FUNCTION h3_cell_to_geometry(h3index) RETURNS @extschema:postgis@.geometry
  AS $$ SELECT @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_latlng($1)::@extschema:postgis@.geometry, 4326) $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_geometry(h3index)
IS 'Finds the centroid of the index.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_geography
CREATE OR REPLACE FUNCTION h3_cell_to_geography(h3index) RETURNS @extschema:postgis@.geography
  AS $$ SELECT @extschema@.h3_cell_to_geometry($1)::@extschema:postgis@.geography $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_geography(h3index)
IS 'Finds the centroid of the index.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_boundary_geometry
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geometry(h3index) RETURNS @extschema:postgis@.geometry
  AS $$ SELECT @extschema:h3@.h3_cell_to_boundary_wkb($1)::@extschema:postgis@.geometry $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_boundary_geometry(h3index)
IS 'Finds the boundary of the index.

Splits polygons when crossing 180th meridian.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_boundary_geography
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geography(h3index) RETURNS @extschema:postgis@.geography
  AS $$ SELECT @extschema:h3@.h3_cell_to_boundary_wkb($1)::@extschema:postgis@.geography $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_boundary_geography(h3index)
IS 'Finds the boundary of the index.

Splits polygons when crossing 180th meridian.';

--@ availability: 4.2.3
--@ refid: h3_get_resolution_from_tile_zoom
CREATE OR REPLACE FUNCTION h3_get_resolution_from_tile_zoom(
    z integer,
    max_h3_resolution integer DEFAULT 15,
    min_h3_resolution integer DEFAULT 0,
    hex_edge_pixels integer DEFAULT 44,
    tile_size integer DEFAULT 512
) RETURNS integer
AS $$
DECLARE
    e0  CONSTANT numeric := h3_get_hexagon_edge_length_avg(0,'m'); -- res-0 edge
    ln7 CONSTANT numeric := LN(SQRT(7.0));                         -- = ln(√7)
    desired_edge numeric;
    r_est        integer;
BEGIN
    IF z < 0 THEN
        RAISE EXCEPTION 'Negative tile zoom levels are not supported';
    END IF;

    desired_edge := 40075016.6855785 / (tile_size * 2 ^ z) * hex_edge_pixels;

    r_est := ROUND( LN(e0 / desired_edge) / ln7 );

    RETURN GREATEST(min_h3_resolution,
           LEAST(r_est, max_h3_resolution));
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION
    h3_get_resolution_from_tile_zoom(integer, integer, integer, integer, integer)
IS 'Returns the optimal H3 resolution for a specified XYZ tile zoom level, based on hexagon size in pixels and resolution limits';
