\pset tuples_only on

-- neighbouring indexes (one hexagon, one pentagon) at resolution 3
\set invalid '\'0\''
\set hexagon '\'831c02fffffffff\'::h3index'
\set pentagon '\'831c00fffffffff\'::h3index'
\set resolution 3

--
-- TEST h3_get_resolution
--

SELECT h3_get_resolution(:hexagon) = :resolution AND h3_get_resolution(:pentagon) = :resolution;

--
-- TEST h3_get_base_cell_number
--

-- base cell is same for parents
SELECT h3_get_base_cell_number(:hexagon) = h3_get_base_cell_number(h3_cell_to_parent(:hexagon));
SELECT h3_get_base_cell_number(:pentagon) = h3_get_base_cell_number(h3_cell_to_parent(:pentagon));

--
-- TEST h3_get_index_digit and h3_construct_cell
--

SELECT :hexagon = h3_construct_cell(
    h3_get_resolution(:hexagon),
    h3_get_base_cell_number(:hexagon),
    ARRAY(
        SELECT h3_get_index_digit(:hexagon, r)
        FROM generate_series(1, h3_get_resolution(:hexagon)) AS r
        ORDER BY r
    )
);

SELECT h3_cell_to_parent(:hexagon, 0) = h3_construct_cell(
    0,
    h3_get_base_cell_number(h3_cell_to_parent(:hexagon, 0)),
    ARRAY[]::integer[]
);

CREATE FUNCTION h3_test_get_index_digit_invalid_resolution() RETURNS boolean LANGUAGE plpgsql
    AS $$
        BEGIN
            PERFORM h3_get_index_digit('831c02fffffffff'::h3index, 0);
            RETURN false;
        EXCEPTION WHEN OTHERS THEN
            RETURN true;
        END;
    $$;
SELECT h3_test_get_index_digit_invalid_resolution();
DROP FUNCTION h3_test_get_index_digit_invalid_resolution();

CREATE FUNCTION h3_test_construct_cell_invalid_base_cell() RETURNS boolean LANGUAGE plpgsql
    AS $$
        BEGIN
            PERFORM h3_construct_cell(1, 122, ARRAY[0]);
            RETURN false;
        EXCEPTION WHEN OTHERS THEN
            RETURN true;
        END;
    $$;
SELECT h3_test_construct_cell_invalid_base_cell();
DROP FUNCTION h3_test_construct_cell_invalid_base_cell();

CREATE FUNCTION h3_test_construct_cell_deleted_pentagon_digit() RETURNS boolean LANGUAGE plpgsql
    AS $$
        BEGIN
            PERFORM h3_construct_cell(1, 4, ARRAY[1]);
            RETURN false;
        EXCEPTION WHEN OTHERS THEN
            RETURN true;
        END;
    $$;
SELECT h3_test_construct_cell_deleted_pentagon_digit();
DROP FUNCTION h3_test_construct_cell_deleted_pentagon_digit();

--
-- TEST h3_is_valid_cell
--

SELECT h3_is_valid_cell(:hexagon) AND h3_is_valid_cell(:pentagon) AND NOT h3_is_valid_cell(:invalid);

--
-- TEST h3_is_valid_index
--

SELECT h3_is_valid_index(:hexagon)
    AND h3_is_valid_index((SELECT h3_origin_to_directed_edges(:hexagon) LIMIT 1))
    AND h3_is_valid_index(h3_cell_to_vertex(:hexagon, 0))
    AND NOT h3_is_valid_index(:invalid);

--
-- TEST h3_is_res_class_iii
--

-- if index is Class III then parent is not
SELECT h3_is_res_class_iii(:hexagon) AND NOT h3_is_res_class_iii(h3_cell_to_parent(:hexagon));
SELECT h3_is_res_class_iii(:pentagon) AND NOT h3_is_res_class_iii(h3_cell_to_parent(:pentagon));

--
-- TEST h3_is_pentagon
--

SELECT h3_is_pentagon(:pentagon) AND NOT h3_is_pentagon(:hexagon);

--
-- TEST h3_get_icosahedron_faces
--
SELECT h3_get_icosahedron_faces('851c0047fffffff') = ARRAY[11,6];
SELECT h3_get_icosahedron_faces('851c004bfffffff') = ARRAY[6];
