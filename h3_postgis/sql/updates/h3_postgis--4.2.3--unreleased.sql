/*
 * Copyright 2025 Zacharias Knudsen
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
\echo Use "ALTER EXTENSION h3_postgis UPDATE TO 'unreleased'" to load this file. \quit

-- PostgreSQL 17+ uses a restricted search_path during maintenance operations.
-- Wrapper function definitions use extension-schema placeholders and are
-- resolved by CREATE EXTENSION / ALTER EXTENSION across supported versions.
-- Avoid function-level SET search_path here to preserve SQL-function inlining.

CREATE OR REPLACE FUNCTION h3_latlng_to_cell(@extschema:postgis@.geometry, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_latlng_to_cell($1::point, $2); $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_latlng_to_cell(@extschema:postgis@.geography, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_latlng_to_cell($1::@extschema:postgis@.geometry, $2); $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_cell_to_geometry(h3index) RETURNS @extschema:postgis@.geometry
  AS $$ SELECT @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_latlng($1)::@extschema:postgis@.geometry, 4326) $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_cell_to_geography(h3index) RETURNS @extschema:postgis@.geography
  AS $$ SELECT @extschema@.h3_cell_to_geometry($1)::@extschema:postgis@.geography $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geometry(h3index) RETURNS @extschema:postgis@.geometry
  AS $$ SELECT @extschema:h3@.h3_cell_to_boundary_wkb($1)::@extschema:postgis@.geometry $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geography(h3index) RETURNS @extschema:postgis@.geography
  AS $$ SELECT @extschema:h3@.h3_cell_to_boundary_wkb($1)::@extschema:postgis@.geography $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geometry(h3index, extend_antimeridian boolean) RETURNS @extschema:postgis@.geometry
  AS $$ SELECT @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_boundary($1, extend_antimeridian)::@extschema:postgis@.geometry, 4326) $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geography(h3index, extend_antimeridian boolean) RETURNS @extschema:postgis@.geography
  AS $$ SELECT @extschema:postgis@.ST_SetSRID(@extschema:h3@.h3_cell_to_boundary($1, extend_antimeridian)::@extschema:postgis@.geometry, 4326) $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_lat_lng_to_cell(@extschema:postgis@.geometry, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_lat_lng_to_cell($1::point, $2); $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

CREATE OR REPLACE FUNCTION h3_lat_lng_to_cell(@extschema:postgis@.geography, resolution integer) RETURNS h3index
    AS $$ SELECT @extschema:h3@.h3_lat_lng_to_cell($1::@extschema:postgis@.geometry, $2); $$ IMMUTABLE PARALLEL SAFE LANGUAGE SQL;

-- Keep installed-vs-upgraded function text identical for pg_validate_extupgrade.
CREATE OR REPLACE FUNCTION __h3_raster_class_summary_part(
    rast raster,
    nband integer,
    pixel_area double precision)
RETURNS SETOF h3_raster_class_summary_item
AS $$
    SELECT ROW(
        value::integer,
        count::double precision,
        count * pixel_area
    )::h3_raster_class_summary_item
    FROM ST_ValueCount(rast, nband, TRUE) t;
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;
