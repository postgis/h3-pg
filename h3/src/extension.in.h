/*
 * Copyright 2018-2022 Bytes & Brains
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef PGH3_H
#define PGH3_H

#include <h3api.h> // Main H3 include

void		_PG_init(void);
extern bool h3_guc_strict;
extern bool h3_guc_extend_antimeridian;

typedef struct
{
	H3Index    *indices;
	int		   *distances;
}	hexDistanceTuple;

#define MAX_H3_RES 15

/*	configure extension version in makefile */
#define EXTVERSION "@EXTVERSION@"

/*
 * DatumGetH3Index
 *		Returns H3 index value of a datum.
 *
 * Note: this macro hides whether h3 index is pass by value or by reference.
 */

#ifdef USE_FLOAT8_BYVAL
#define DatumGetH3Index(X) ((H3Index) (X))
#else
#define DatumGetH3Index(X) (* ((H3Index *) DatumGetPointer(X)))
#endif

/*
 * H3IndexGetDatum
 *		Returns datum representation for an H3 index.
 *
 * Note: if H3 index is pass by reference, this function returns a reference
 * to palloc'd space.
 */

#ifdef USE_FLOAT8_BYVAL
#define H3IndexGetDatum(X) ((Datum) (X))
#else
#define H3IndexGetDatum(X) Int64GetDatum((int64) (X))
#endif

/* Macros for fetching arguments and returning results of h3 index type */

#define PG_GETARG_H3INDEX(n) DatumGetH3Index(PG_GETARG_DATUM(n))
#define PG_RETURN_H3INDEX(x) return H3IndexGetDatum(x)

/*	helper functions to return sets from user fctx */
Datum		srf_return_h3_indexes_from_user_fctx(PG_FUNCTION_ARGS);
Datum		srf_return_h3_index_distances_from_user_fctx(PG_FUNCTION_ARGS);

/*	macros to pass on fcinfo to above helpers */
#define SRF_RETURN_H3_INDEXES_FROM_USER_FCTX() \
	return srf_return_h3_indexes_from_user_fctx(fcinfo)
#define SRF_RETURN_H3_INDEX_DISTANCES_FROM_USER_FCTX() \
	return srf_return_h3_index_distances_from_user_fctx(fcinfo)

/* use origin resolution minus one when no resolution is given */
#define PG_GETARG_OPTIONAL_RES(n, cell, offset) \
	PG_NARGS() == (n + 1) ? PG_GETARG_INT32(1) : getResolution(cell) + offset

/*error reporting*/

#define H3_ERROR(error, func)						 \
	if (error) ereport(ERROR, (						 \
		errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION), \
		errmsg("%s error code: %i", func, error),		 \
		errhint("https://h3geo.org/docs/next/library/errors/#table-of-error-codes") \
	))

#define ASSERT(condition, code, msg, ...)  \
	if (0 == (condition)) ereport(ERROR, ( \
		errcode(code),					   \
		errmsg(msg, ##__VA_ARGS__)		   \
	))

#define ENSURE_TYPEFUNC_COMPOSITE(x)				   \
	ASSERT(											   \
		x == TYPEFUNC_COMPOSITE,					   \
		ERRCODE_INVALID_PARAMETER_VALUE,			   \
		"Function returning record called in context " \
		"that cannot accept type record"			   \
	)

#define H3_DEPRECATION(msg)           \
	ereport(WARNING, (			      \
		errmsg("Deprecated: %s", msg) \
	))

#define DEBUG(msg, ...)			   \
	ereport(NOTICE, (			   \
		errmsg(msg, ##__VA_ARGS__) \
	))

#define DEBUG_H3INDEX(h3index) DEBUG("index: %lx", h3index)

#endif
