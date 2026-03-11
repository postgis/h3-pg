/*
 * Copyright 2025 Zacharias Knudsen
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

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION h3 UPDATE TO 'unreleased'" to load this file. \quit

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- Keep the public bigint <-> signature upgrade-safe, refresh stored/indexed
-- distance results that may still contain the old sentinel, and wire in the
-- GiST KNN operator class support functions used below.
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

CREATE OR REPLACE FUNCTION h3index_distance(h3index, h3index) RETURNS bigint
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON OPERATOR <-> (h3index, h3index) IS
  'Returns the distance in grid cells between the two indices after refining the coarser input to its center child at the finer resolution. Returns the maximum bigint value when gridDistance fails (e.g. near pentagons).';

DO $$
DECLARE
    dep_index RECORD;
    dep_table RECORD;
    dep_matview RECORD;
    distance_op oid := '<->(h3index,h3index)'::regoperator;
    distance_fn oid := 'h3index_distance(h3index,h3index)'::regprocedure;
    original_replication_role text := current_setting('session_replication_role');
BEGIN
    /*
     * Stored generated columns need their rows recomputed, but the extension
     * upgrade must not fire arbitrary user UPDATE triggers while doing so.
     */
    PERFORM set_config('session_replication_role', 'replica', true);
    BEGIN
        FOR dep_table IN
            SELECT DISTINCT
                c.oid::regclass AS relid,
                base.attname AS base_attname
            FROM pg_depend d
            JOIN pg_attrdef ad
                ON ad.oid = d.objid
            JOIN pg_attribute a
                ON a.attrelid = ad.adrelid
               AND a.attnum = ad.adnum
               AND a.attgenerated = 's'
            JOIN pg_class c
                ON c.oid = ad.adrelid
            JOIN LATERAL (
                SELECT a2.attname
                FROM pg_attribute a2
                WHERE a2.attrelid = c.oid
                  AND a2.attnum > 0
                  AND NOT a2.attisdropped
                  AND a2.attgenerated = ''
                ORDER BY a2.attnum
                LIMIT 1
            ) base ON TRUE
            WHERE d.classid = 'pg_attrdef'::regclass
              AND (
                    (d.refclassid = 'pg_operator'::regclass AND d.refobjid = distance_op)
                 OR (d.refclassid = 'pg_proc'::regclass AND d.refobjid = distance_fn)
              )
        LOOP
            EXECUTE format(
                'UPDATE %s SET %I = %I',
                dep_table.relid,
                dep_table.base_attname,
                dep_table.base_attname
            );
        END LOOP;
    EXCEPTION
        WHEN OTHERS THEN
            PERFORM set_config(
                'session_replication_role',
                original_replication_role,
                true
            );
            RAISE;
    END;
    PERFORM set_config(
        'session_replication_role',
        original_replication_role,
        true
    );

    FOR dep_index IN
        SELECT c.oid::regclass AS relid
        FROM pg_depend d
        JOIN pg_class c
            ON c.oid = d.objid
        WHERE d.classid = 'pg_class'::regclass
          AND (
                (d.refclassid = 'pg_operator'::regclass AND d.refobjid = distance_op)
             OR (d.refclassid = 'pg_proc'::regclass AND d.refobjid = distance_fn)
          )
          AND c.relkind = 'i'
    LOOP
        EXECUTE format('REINDEX INDEX %s', dep_index.relid);
    END LOOP;

    FOR dep_matview IN
        SELECT c.oid::regclass AS relid
        FROM pg_depend d
        JOIN pg_rewrite r
            ON r.oid = d.objid
        JOIN pg_class c
            ON c.oid = r.ev_class
        WHERE d.classid = 'pg_rewrite'::regclass
          AND (
                (d.refclassid = 'pg_operator'::regclass AND d.refobjid = distance_op)
             OR (d.refclassid = 'pg_proc'::regclass AND d.refobjid = distance_fn)
          )
          AND c.relkind = 'm'
    LOOP
        EXECUTE format('REFRESH MATERIALIZED VIEW %s', dep_matview.relid);
    END LOOP;
END
$$;

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- Btree comparator sign fix: existing btree indexes on h3index columns are
-- physically sorted in the wrong order.  REINDEX every btree index that uses
-- the h3index_ops operator class (in any column position) so the on-disk sort
-- matches the corrected comparator.
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT DISTINCT ci.oid::regclass AS idx
        FROM pg_index i
        JOIN pg_class ci ON ci.oid = i.indexrelid
        JOIN pg_am am ON am.oid = ci.relam
        JOIN pg_opclass opc ON opc.oid = ANY(i.indclass)
        WHERE am.amname = 'btree'
          AND opc.opcname = 'h3index_ops'
    LOOP
        EXECUTE format('REINDEX INDEX %s', r.idx);
    END LOOP;
END
$$;

-- ---------- ---------- ---------- ---------- ---------- ---------- ----------
-- GiST Operator Class (opclass_gist.c)
-- ---------- ---------- ---------- ---------- ---------- ---------- ----------

CREATE OR REPLACE FUNCTION h3index_gist_consistent(entry internal, query h3index, strategy smallint, subtype oid, recheck internal) RETURNS boolean
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_union(internal, internal) RETURNS h3index
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_penalty(internal, internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_picksplit(internal, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_same(h3index, h3index, internal) RETURNS internal
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_distance(entry internal, query h3index, strategy smallint, subtype oid, recheck internal) RETURNS float8
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION h3index_gist_sortsupport(internal) RETURNS void
    AS 'h3' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OPERATOR CLASS h3index_gist_ops_experimental
FOR TYPE h3index USING gist
AS
    OPERATOR  3   &&  ,
    OPERATOR  6   =   ,
    OPERATOR  7   @>  ,
    OPERATOR  8   <@  ,
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
