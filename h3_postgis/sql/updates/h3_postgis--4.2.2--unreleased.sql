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
