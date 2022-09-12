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

--| # PostGIS Indexing Functions

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_lat_lng_to_cell(geometry, resolution integer) RETURNS h3index
    AS $$ SELECT h3_lat_lng_to_cell($1::point, $2); $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_lat_lng_to_cell(geography, resolution integer) RETURNS h3index
    AS $$ SELECT h3_lat_lng_to_cell($1::geometry, $2); $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_cell_to_geometry(h3index) RETURNS geometry
  AS $$ SELECT ST_SetSRID(h3_cell_to_lat_lng($1)::geometry, 4326) $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_cell_to_geography(h3index) RETURNS geography
  AS $$ SELECT h3_cell_to_geometry($1)::geography $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geometry(h3index) RETURNS geometry
  AS $$ SELECT h3_cell_to_boundary_wkb($1)::geometry $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geography(h3index) RETURNS geography
  AS $$ SELECT h3_cell_to_boundary_wkb($1)::geography $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
