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

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION h3 UPDATE TO 'unreleased'" to load this file. \quit

DROP FUNCTION IF EXISTS h3_cell_to_boundary_wkb(h3index);
DROP FUNCTION IF EXISTS h3_cells_to_multi_polygon_wkb(h3index[]);

ALTER OPERATOR FAMILY btree_h3index_ops USING btree RENAME TO h3index_ops;
ALTER OPERATOR CLASS  btree_h3index_ops USING btree RENAME TO h3index_ops;

ALTER OPERATOR FAMILY hash_h3index_ops USING hash RENAME TO h3index_ops;
ALTER OPERATOR CLASS  hash_h3index_ops USING hash RENAME TO h3index_ops;

CREATE OR REPLACE FUNCTION h3_pg_migrate_pass_by_reference(h3index) RETURNS h3index
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
    COMMENT ON FUNCTION h3_pg_migrate_pass_by_reference(h3index) IS
'Migrate h3index from pass-by-reference to pass-by-value.';
