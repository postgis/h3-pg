\pset tuples_only on

--
-- TEST h3_get_extension_version
--

SELECT h3_get_extension_version() ~ '^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$'
    OR h3_get_extension_version() = 'unreleased';

--
-- TEST upgrading from 4.2.3 with dependent distance objects
--

SET client_min_messages = warning;
DROP EXTENSION h3 CASCADE;
RESET client_min_messages;
CREATE EXTENSION h3 VERSION '4.2.3';

CREATE VIEW h3_distance_view AS
SELECT
    '831c02fffffffff'::h3index <-> '831c03fffffffff'::h3index AS op_dist,
    h3index_distance('831c02fffffffff'::h3index, '831c03fffffffff'::h3index) AS fn_dist;

CREATE TABLE h3_distance_expr (hex h3index);
INSERT INTO h3_distance_expr VALUES
    ('831c02fffffffff'::h3index),
    ('831c03fffffffff'::h3index);
CREATE INDEX h3_distance_expr_idx
    ON h3_distance_expr ((hex <-> '831c03fffffffff'::h3index));

CREATE TABLE h3_distance_generated (
    hex h3index,
    dist bigint GENERATED ALWAYS AS (hex <-> '831c03fffffffff'::h3index) STORED
);
INSERT INTO h3_distance_generated (hex) VALUES
    ('831c02fffffffff'::h3index),
    ('831c03fffffffff'::h3index);

CREATE TABLE h3_distance_expr_fail (hex h3index);
INSERT INTO h3_distance_expr_fail
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);
CREATE INDEX h3_distance_expr_fail_idx
    ON h3_distance_expr_fail ((hex <-> '831c02fffffffff'::h3index));

CREATE TABLE h3_distance_generated_fail (
    hex h3index,
    dist bigint GENERATED ALWAYS AS (hex <-> '831c02fffffffff'::h3index) STORED
);
INSERT INTO h3_distance_generated_fail
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);

CREATE OR REPLACE FUNCTION h3_distance_generated_fail_block_update() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'distance generated-row refresh should not fire user triggers';
END
$$;
CREATE TRIGGER h3_distance_generated_fail_block_update
BEFORE UPDATE ON h3_distance_generated_fail
FOR EACH ROW
EXECUTE FUNCTION h3_distance_generated_fail_block_update();

CREATE TABLE h3_distance_expr_fail_fn (hex h3index);
INSERT INTO h3_distance_expr_fail_fn
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);
CREATE INDEX h3_distance_expr_fail_fn_idx
    ON h3_distance_expr_fail_fn ((h3index_distance(hex, '831c02fffffffff'::h3index)));

CREATE TABLE h3_distance_generated_fail_fn (
    hex h3index,
    dist bigint GENERATED ALWAYS AS (h3index_distance(hex, '831c02fffffffff'::h3index)) STORED
);
INSERT INTO h3_distance_generated_fail_fn
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);

CREATE MATERIALIZED VIEW h3_distance_matview_fail_fn AS
SELECT
    hex,
    h3index_distance(hex, '831c02fffffffff'::h3index) AS dist
FROM h3_distance_expr_fail_fn;

CREATE OR REPLACE FUNCTION h3_distance_user_wrapper(hex h3index)
RETURNS bigint
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT public.h3index_distance(hex, '831c02fffffffff'::public.h3index);
$$;

CREATE TABLE h3_distance_expr_fail_userfn (hex h3index);
INSERT INTO h3_distance_expr_fail_userfn
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);
CREATE INDEX h3_distance_expr_fail_userfn_idx
    ON h3_distance_expr_fail_userfn ((h3_distance_user_wrapper(hex)));

CREATE TABLE h3_distance_generated_fail_userfn (
    hex h3index,
    dist bigint GENERATED ALWAYS AS (h3_distance_user_wrapper(hex)) STORED
);
INSERT INTO h3_distance_generated_fail_userfn
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);

CREATE TABLE h3_distance_generated_fail_userfn_identity (
    id bigint GENERATED ALWAYS AS IDENTITY,
    hex h3index,
    dist bigint GENERATED ALWAYS AS (h3_distance_user_wrapper(hex)) STORED
);
INSERT INTO h3_distance_generated_fail_userfn_identity (hex)
SELECT h3_grid_disk('831c02fffffffff'::h3index, 3);

CREATE MATERIALIZED VIEW h3_distance_matview_fail_userfn AS
SELECT
    hex,
    h3_distance_user_wrapper(hex) AS dist
FROM h3_distance_expr_fail_userfn;

ALTER EXTENSION h3 UPDATE TO 'unreleased';

SELECT (current_setting('server_version_num')::int >= 140000) = EXISTS (
    SELECT 1
    FROM pg_amproc ap
    JOIN pg_opfamily f ON f.oid = ap.amprocfamily
    JOIN pg_am am ON am.oid = f.opfmethod
    JOIN pg_proc p ON p.oid = ap.amproc
    WHERE am.amname = 'gist'
      AND f.opfname = 'h3index_gist_ops_experimental'
      AND ap.amprocnum = 11
      AND p.proname = 'h3index_gist_sortsupport'
      AND oidvectortypes(p.proargtypes) = 'internal'
);

SELECT '831c02fffffffff'::h3index = h3_construct_cell(
    h3_get_resolution('831c02fffffffff'::h3index),
    h3_get_base_cell_number('831c02fffffffff'::h3index),
    ARRAY(
        SELECT h3_get_index_digit('831c02fffffffff'::h3index, r)
        FROM generate_series(1, h3_get_resolution('831c02fffffffff'::h3index)) AS r
        ORDER BY r
    )
);

SELECT bool_and(h3_is_valid_index(r)) AND COUNT(*) = 6
FROM h3_grid_ring('831c02fffffffff'::h3index, 1) AS r;

SELECT op_dist = 1 AND fn_dist = 1 FROM h3_distance_view;

DROP VIEW h3_distance_view;

SET enable_seqscan = off;
SELECT COUNT(*) = 1
FROM h3_distance_expr
WHERE hex <-> '831c03fffffffff'::h3index = 1;
RESET enable_seqscan;

DROP TABLE h3_distance_expr;

SELECT COUNT(*) = 2
FROM h3_distance_generated
WHERE dist = hex <-> '831c03fffffffff'::h3index;

INSERT INTO h3_distance_generated (hex) VALUES
    ('831c06fffffffff'::h3index);
SELECT COUNT(*) = 3
FROM h3_distance_generated
WHERE dist = hex <-> '831c03fffffffff'::h3index;

DROP TABLE h3_distance_generated;

SELECT COUNT(*) > 0
FROM h3_distance_expr_fail
WHERE hex <-> '831c02fffffffff'::h3index = 9223372036854775807;

SET enable_seqscan = off;
SELECT COUNT(*) > 0
FROM h3_distance_expr_fail
WHERE hex <-> '831c02fffffffff'::h3index = 9223372036854775807;
RESET enable_seqscan;

DROP TABLE h3_distance_expr_fail;

SELECT COUNT(*) > 0
FROM h3_distance_generated_fail
WHERE dist = 9223372036854775807;

SELECT COUNT(*) = (
    SELECT COUNT(*) FROM h3_distance_generated_fail
)
FROM h3_distance_generated_fail
WHERE dist = hex <-> '831c02fffffffff'::h3index;

DROP TABLE h3_distance_generated_fail;
DROP FUNCTION h3_distance_generated_fail_block_update();

SELECT COUNT(*) > 0
FROM h3_distance_expr_fail_fn
WHERE h3index_distance(hex, '831c02fffffffff'::h3index) = 9223372036854775807;

SET enable_seqscan = off;
SELECT COUNT(*) > 0
FROM h3_distance_expr_fail_fn
WHERE h3index_distance(hex, '831c02fffffffff'::h3index) = 9223372036854775807;
RESET enable_seqscan;

SELECT COUNT(*) > 0
FROM h3_distance_generated_fail_fn
WHERE dist = 9223372036854775807;

SELECT COUNT(*) = (
    SELECT COUNT(*) FROM h3_distance_generated_fail_fn
)
FROM h3_distance_generated_fail_fn
WHERE dist = h3index_distance(hex, '831c02fffffffff'::h3index);

DROP TABLE h3_distance_generated_fail_fn;

SELECT COUNT(*) > 0
FROM h3_distance_expr_fail_userfn
WHERE h3_distance_user_wrapper(hex) = 9223372036854775807;

SET enable_seqscan = off;
SELECT COUNT(*) > 0
FROM h3_distance_expr_fail_userfn
WHERE h3_distance_user_wrapper(hex) = 9223372036854775807;
RESET enable_seqscan;

SELECT COUNT(*) > 0
FROM h3_distance_generated_fail_userfn
WHERE dist = 9223372036854775807;

SELECT COUNT(*) = (
    SELECT COUNT(*) FROM h3_distance_generated_fail_userfn
)
FROM h3_distance_generated_fail_userfn
WHERE dist = h3_distance_user_wrapper(hex);

DROP TABLE h3_distance_generated_fail_userfn;

SELECT COUNT(*) = (
    SELECT COUNT(*) FROM h3_distance_generated_fail_userfn_identity
)
FROM h3_distance_generated_fail_userfn_identity
WHERE dist = h3_distance_user_wrapper(hex);

DROP TABLE h3_distance_generated_fail_userfn_identity;

SELECT COUNT(*) > 0
FROM h3_distance_matview_fail_fn
WHERE dist = 9223372036854775807;

DROP MATERIALIZED VIEW h3_distance_matview_fail_fn;
SELECT COUNT(*) > 0
FROM h3_distance_matview_fail_userfn
WHERE dist = 9223372036854775807;

DROP MATERIALIZED VIEW h3_distance_matview_fail_userfn;
DROP TABLE h3_distance_expr_fail_fn;
DROP TABLE h3_distance_expr_fail_userfn;
DROP FUNCTION h3_distance_user_wrapper(h3index);
