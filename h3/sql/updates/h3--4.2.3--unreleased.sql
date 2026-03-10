/*
 * Copyright 2025 Zacharias Knudsen
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

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION h3 UPDATE TO 'unreleased'" to load this file. \quit

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- Fix <-> operator: return float8 (was bigint) so GiST KNN works with
-- FOR ORDER BY float_ops, and return INFINITY instead of -1 on error.
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

DROP OPERATOR IF EXISTS <-> (h3index, h3index);
DROP FUNCTION IF EXISTS h3index_distance(h3index, h3index);

CREATE OR REPLACE FUNCTION h3index_distance(h3index, h3index) RETURNS float8
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OPERATOR <-> (
  LEFTARG = h3index,
  RIGHTARG = h3index,
  PROCEDURE = h3index_distance,
  COMMUTATOR = <->
);
COMMENT ON OPERATOR <-> (h3index, h3index) IS
  'Returns the distance in grid cells between the two indices (at the lowest resolution of the two). Returns Infinity when gridDistance fails (e.g. near pentagons).';

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- GiST Operator Class (opclass_gist.c)
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

CREATE OR REPLACE FUNCTION h3index_gist_consistent(internal, h3index, smallint, oid, internal) RETURNS boolean
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_union(internal, internal) RETURNS h3index
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_penalty(internal, internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_picksplit(internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_same(h3index, h3index, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_distance(internal, h3index, smallint, oid, internal) RETURNS float8
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS h3index_gist_ops_experimental
FOR TYPE h3index USING gist
AS
    OPERATOR  3   &&  ,
    OPERATOR  6   =   ,
    OPERATOR  7   @>  ,
    OPERATOR  8   <@  ,
    OPERATOR  15  <-> (h3index, h3index) FOR ORDER BY float_ops,
    FUNCTION  1  h3index_gist_consistent(internal, h3index, smallint, oid, internal),
    FUNCTION  2  h3index_gist_union(internal, internal),
    FUNCTION  5  h3index_gist_penalty(internal, internal, internal),
    FUNCTION  6  h3index_gist_picksplit(internal, internal),
    FUNCTION  7  h3index_gist_same(h3index, h3index, internal),
    FUNCTION  8  (h3index, h3index) h3index_gist_distance(internal, h3index, smallint, oid, internal);
