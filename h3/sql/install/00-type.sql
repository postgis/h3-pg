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

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION h3" to load this file. \quit

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
--| # Base type
--|
--| An unsigned 64-bit integer representing any H3 object (hexagon, pentagon, directed edge ...)
--| represented as a (or 16-character) hexadecimal string, like '8928308280fffff'.
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

-- declare shell type, allowing us to reference while defining functions
-- before finally providing the full definition of the data type
CREATE TYPE h3index;

--@ internal
CREATE OR REPLACE FUNCTION
    h3index_in(cstring) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--@ internal
CREATE OR REPLACE FUNCTION
    h3index_out(h3index) RETURNS cstring
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE h3index (
  INPUT          = h3index_in,
  OUTPUT         = h3index_out,
  LIKE           = int8
);

--@ internal
CREATE OR REPLACE FUNCTION
    h3index_to_bigint(h3index) RETURNS bigint
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE CAST (h3index AS bigint) WITH FUNCTION h3index_to_bigint(h3index);
COMMENT ON CAST (h3index AS bigint) IS
    'Convert H3 index to bigint, which is useful when you need a decimal representation';

--@ internal
CREATE OR REPLACE FUNCTION
    bigint_to_h3index(bigint) RETURNS h3index
AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE CAST (bigint AS h3index) WITH FUNCTION bigint_to_h3index(bigint);
COMMENT ON CAST (h3index AS bigint) IS
    'Convert bigint to H3 index';

