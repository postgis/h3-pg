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

--| # WKB indexing functions

--@ availability: unreleased
CREATE OR REPLACE FUNCTION
    h3_cell_to_boundary_wkb(cell h3index) RETURNS bytea
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_cell_to_boundary_wkb(h3index)
IS 'Finds the boundary of the index, converts to EWKB.

Use `SET h3.split_antimeridian TO true` to split when crossing 180th meridian;

This function has to return WKB since Postgres does not provide multipolygon type.';
