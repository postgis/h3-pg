/*
 * Copyright 2023-2025 Zacharias Knudsen
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

--| # PostGIS Grid Traversal Functions

--@ availability: 4.1.0
--@ refid: h3_grid_path_cells_recursive
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
