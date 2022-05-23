/*
 * Copyright 2019-2022 Bytes & Brains
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

--| # PostGIS Region Functions

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_polygon_to_cells(multi geometry, resolution integer) RETURNS SETOF h3index
    AS $$ SELECT h3_polygon_to_cells(exterior, holes, resolution) FROM (
        SELECT 
            -- extract exterior ring of each polygon
            ST_MakePolygon(ST_ExteriorRing(poly))::polygon exterior,
            -- extract holes of each polygon
            (SELECT array_agg(hole)
                FROM (
                    SELECT ST_MakePolygon(ST_InteriorRingN(
                        poly,
                        generate_series(1, ST_NumInteriorRings(poly))
                    ))::polygon AS hole
                ) q_hole
            ) holes
        -- extract single polygons from multipolygon
        FROM (
            select (st_dump(multi)).geom as poly
        ) q_poly GROUP BY poly
    ) h3_polygon_to_cells; $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT; -- NOT STRICT

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_polygon_to_cells(multi geography, resolution integer) RETURNS SETOF h3index
AS $$ SELECT h3_polygon_to_cells($1::geometry, $2) $$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT; -- NOT STRICT
