/*
 * Copyright 2018-2022 Bytes & Brains
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

--| # Indexing functions
--|
--| These function are used for finding the H3 index containing coordinates,
--| and for finding the center and boundary of H3 indexes.

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION
    h3_lat_lng_to_cell(latlng point, resolution integer) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_lat_lng_to_cell(point, integer)
IS 'Indexes the location at the specified resolution';

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION
    h3_cell_to_lat_lng(cell h3index) RETURNS point
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cell_to_lat_lng(h3index)
IS 'Finds the centroid of the index';

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION
    h3_cell_to_boundary(cell h3index, extend_at_meridian boolean DEFAULT FALSE) RETURNS polygon
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cell_to_boundary(h3index, boolean)
IS 'Finds the boundary of the index, second argument extends coordinates when crossing 180th meridian to help visualization';