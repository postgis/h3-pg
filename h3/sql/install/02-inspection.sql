/*
 * Copyright 2020-2024 Zacharias Knudsen
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

--| # Index inspection functions
--|
--| These functions provide metadata about an H3 index, such as its resolution
--| or base cell, and provide utilities for converting into and out of the
--| 64-bit representation of an H3 index.

--@ availability: 1.0.0
CREATE OR REPLACE FUNCTION
    h3_get_resolution(h3index) RETURNS integer
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_get_resolution(h3index)
IS 'Returns the H3 resolution encoded in the index (0 through 15).';

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION
    h3_get_base_cell_number(h3index) RETURNS integer
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_get_base_cell_number(h3index)
IS 'Returns the base cell number (0 through 121) associated with the index.';

--@ availability: unreleased
CREATE OR REPLACE FUNCTION
    h3_get_index_digit(h3index, resolution integer) RETURNS integer
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_get_index_digit(h3index, integer)
IS 'Returns the index digit at a specific resolution step. Resolution numbering is 1-based: pass 1 for the first digit below the base cell, 2 for the next, and so on.';

--@ availability: unreleased
CREATE OR REPLACE FUNCTION
    h3_construct_cell(resolution integer, base_cell_number integer, digits integer[]) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_construct_cell(integer, integer, integer[])
IS 'Builds a valid H3 cell from explicit components: the target resolution, the base cell number, and a digits array ordered from resolution 1 up to the target resolution. The digits array must contain exactly one non-NULL entry per resolution step.';

--@ availability: 1.0.0
CREATE OR REPLACE FUNCTION
    h3_is_valid_cell(h3index) RETURNS boolean
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_is_valid_cell(h3index)
IS 'Returns true only for valid H3 cell indexes (hexagons or pentagons). Directed edges, vertices, and malformed values return false.';

--@ availability: unreleased
CREATE OR REPLACE FUNCTION
    h3_is_valid_index(h3index) RETURNS boolean
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_is_valid_index(h3index)
IS 'Returns true for any valid H3 index mode: cell, directed edge, or vertex.';

--@ availability: 1.0.0
CREATE OR REPLACE FUNCTION
    h3_is_res_class_iii(h3index) RETURNS boolean
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_is_res_class_iii(h3index)
IS 'Returns true when the index is at a Class III resolution.';
  
--@ availability: 1.0.0
CREATE OR REPLACE FUNCTION
    h3_is_pentagon(h3index) RETURNS boolean
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_is_pentagon(h3index)
IS 'Returns true if this index represents a pentagonal cell.';

--@ availability: 4.0.0
CREATE OR REPLACE FUNCTION
    h3_get_icosahedron_faces(h3index) RETURNS integer[]
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; COMMENT ON FUNCTION
    h3_get_icosahedron_faces(h3index)
IS 'Returns the icosahedron face numbers intersected by the index. Some cells span more than one face.';
