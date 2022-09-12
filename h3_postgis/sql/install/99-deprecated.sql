/*
 * Copyright 2022 Bytes & Brains
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

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geometry(h3index, extend_antimeridian boolean) RETURNS geometry
  AS $$ SELECT ST_SetSRID(h3_cell_to_boundary($1, extend_antimeridian)::geometry, 4326) $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION h3_cell_to_boundary_geography(h3index, extend_antimeridian boolean) RETURNS geography
  AS $$ SELECT ST_SetSRID(h3_cell_to_boundary($1, extend_antimeridian)::geometry, 4326) $$ IMMUTABLE STRICT PARALLEL SAFE LANGUAGE SQL;
