/*
 * Copyright 2024-2025 Zacharias Knudsen
 * Copyright 2026 Eric Schoffstall
 * Copyright 2026 Darafei Praliaskouski
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
--| Supports containment queries (`@>`, `<@`), overlap (`&&`), equality (`=`),
--| and KNN distance ordering (`<->`) on `h3index` columns.
--| Add a GiST index using the `h3index_gist_ops_experimental` operator class:
--|
--| ```sql
--| -- CREATE INDEX [indexname] ON [tablename] USING gist([column] h3index_gist_ops_experimental);
--| CREATE INDEX gist_idx ON h3_data USING gist(hex h3index_gist_ops_experimental);
--|
--| -- containment query
--| SELECT * FROM h3_data WHERE hex <@ '831c02fffffffff'::h3index;
--|
--| -- KNN nearest-neighbor ordering
--| SELECT hex FROM h3_data ORDER BY hex <-> '831c02fffffffff'::h3index LIMIT 10;
--| ```

--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_consistent(entry internal, query h3index, strategy smallint, subtype oid, recheck internal) RETURNS boolean
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
CREATE OR REPLACE FUNCTION h3index_gist_distance(entry internal, query h3index, strategy smallint, subtype oid, recheck internal) RETURNS float8
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
--@ internal
CREATE OR REPLACE FUNCTION h3index_gist_sortsupport(internal) RETURNS void
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- Intentionally not marked as DEFAULT while this opclass remains experimental.
CREATE OPERATOR CLASS h3index_gist_ops_experimental
FOR TYPE h3index USING gist
AS
    OPERATOR  3   &&  ,  -- RTOverlapStrategyNumber
    OPERATOR  6   =   ,  -- RTSameStrategyNumber
    OPERATOR  7   @>  ,  -- RTContainsStrategyNumber
    OPERATOR  8   <@  ,  -- RTContainedByStrategyNumber
    OPERATOR  15  <-> (h3index, h3index) FOR ORDER BY integer_ops,
    FUNCTION  1  h3index_gist_consistent(internal, h3index, smallint, oid, internal),
    FUNCTION  2  h3index_gist_union(internal, internal),
    FUNCTION  5  h3index_gist_penalty(internal, internal, internal),
    FUNCTION  6  h3index_gist_picksplit(internal, internal),
    FUNCTION  7  h3index_gist_same(h3index, h3index, internal),
    FUNCTION  8  (h3index, h3index) h3index_gist_distance(internal, h3index, smallint, oid, internal);

DO $$
BEGIN
    IF current_setting('server_version_num')::int >= 140000 THEN
        EXECUTE $sql$
            ALTER OPERATOR FAMILY h3index_gist_ops_experimental USING gist ADD
                FUNCTION 11 (h3index, h3index) h3index_gist_sortsupport(internal)
        $sql$;
    END IF;
END
$$;
