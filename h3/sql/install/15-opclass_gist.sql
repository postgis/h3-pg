/*
 * Copyright 2019-2025 Bytes & Brains
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

--| ## GiST operator class (experimental)
--|
--| *This is still an experimental feature and may change in future versions.*
--| Add a GiST index using the `h3index_gist_ops_experimental` operator class:
--|
--| ```sql
--| -- CREATE INDEX [indexname] ON [tablename] USING gist([column] h3index_gist_ops_experimental);
--| CREATE INDEX gist_idx ON h3_data USING gist(hex h3index_gist_ops_experimental);
--| ```

--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_consistent(internal, h3index, smallint, oid, internal) RETURNS boolean
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_union(internal, internal) RETURNS h3index
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_penalty(internal, internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_picksplit(internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_same(h3index, h3index, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_distance(internal, h3index, smallint, oid, internal) RETURNS float8
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- intentionally *not* marked as DEFAULT,
-- until we are satisfied with the implementation
CREATE OPERATOR CLASS h3index_gist_ops_experimental
FOR TYPE h3index USING gist
AS
    OPERATOR  3   &&  ,  -- RTOverlapStrategyNumber
    OPERATOR  6   =   ,  -- RTSameStrategyNumber
    OPERATOR  7   @>  ,  -- RTContainsStrategyNumber
    OPERATOR  8   <@  ,  -- RTContainedByStrategyNumber
    OPERATOR  15  <-> (h3index, h3index) FOR ORDER BY float_ops,
    FUNCTION  1  h3index_gist_consistent(internal, h3index, smallint, oid, internal),
    FUNCTION  2  h3index_gist_union(internal, internal),
    FUNCTION  5  h3index_gist_penalty(internal, internal, internal),
    FUNCTION  6  h3index_gist_picksplit(internal, internal),
    FUNCTION  7  h3index_gist_same(h3index, h3index, internal),
    FUNCTION  8  (h3index, h3index) h3index_gist_distance(internal, h3index, smallint, oid, internal);
