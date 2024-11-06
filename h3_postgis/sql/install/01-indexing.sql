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

--| The `GEOMETRY` data passed to `h3-pg` PostGIS functions should
--| be in SRID 4326. This is an expectation of the core H3 library.
--| Using other SRIDs, such as 3857, can result in either errors or
--| invalid data depending on the function.
--| For example, the `h3_polygon_to_cells()` function will fail with
--| an error in this scenario while the `h3_lat_lng_to_cell()` function
--| will return an invalid geometry.

--| # PostGIS Indexing Functions

--@ availability: 4.0.0
--@ refid: h3_lat_lng_to_cell_geometry
CREATE OR REPLACE FUNCTION h3_lat_lng_to_cell(geometry, resolution integer) RETURNS h3index
    AS $$ SELECT h3_lat_lng_to_cell($1::point, $2); $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_lat_lng_to_cell(geometry, resolution integer)
IS 'Indexes the location at the specified resolution.';

--@ availability: 4.0.0
--@ refid: h3_lat_lng_to_cell_geography
CREATE OR REPLACE FUNCTION h3_lat_lng_to_cell(geography, resolution integer) RETURNS h3index
    AS $$ SELECT h3_lat_lng_to_cell($1::geometry, $2); $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_lat_lng_to_cell(geometry, resolution integer)
IS 'Indexes the location at the specified resolution.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_geometry
CREATE OR REPLACE FUNCTION h3_cell_to_geometry(h3index) RETURNS geometry
  AS $$ SELECT ST_SetSRID(h3_cell_to_lat_lng($1)::geometry, 4326) $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_geometry(h3index)
IS 'Finds the centroid of the index.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_geography
CREATE OR REPLACE FUNCTION h3_cell_to_geography(h3index) RETURNS geography
  AS $$ SELECT h3_cell_to_geometry($1)::geography $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_geography(h3index)
IS 'Finds the centroid of the index.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_boundary_geometry
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geometry(h3index) RETURNS geometry
  AS $$ SELECT h3_cell_to_boundary_wkb($1)::geometry $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_boundary_geometry(h3index)
IS 'Finds the boundary of the index.

Splits polygons when crossing 180th meridian.';

--@ availability: 4.0.0
--@ refid: h3_cell_to_boundary_geography
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geography(h3index) RETURNS geography
  AS $$ SELECT h3_cell_to_boundary_wkb($1)::geography $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
COMMENT ON FUNCTION
    h3_cell_to_boundary_geography(h3index)
IS 'Finds the boundary of the index.

Splits polygons when crossing 180th meridian.';
