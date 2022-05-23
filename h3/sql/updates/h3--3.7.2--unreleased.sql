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

DROP FUNCTION IF EXISTS h3_lat_lng_to_cell(geometry, resolution integer);

DROP FUNCTION IF EXISTS h3_lat_lng_to_cell(geography, resolution integer);

DROP FUNCTION IF EXISTS h3_cell_to_geometry(h3index);

DROP FUNCTION IF EXISTS h3_cell_to_geography(h3index);

DROP FUNCTION IF EXISTS h3_cell_to_geo_boundary_geometry(h3index, extend boolean DEFAULT FALSE);

DROP FUNCTION IF EXISTS h3_cell_to_geo_boundary_geography(h3index, extend boolean DEFAULT FALSE);

DROP FUNCTION IF EXISTS h3_polygon_to_cells(multi geometry, resolution integer);

DROP FUNCTION IF EXISTS h3_polygon_to_cells(multi geography, resolution integer);

DROP CAST (h3index AS point);

DROP CAST (h3index AS geometry);

DROP CAST (h3index AS geography);
