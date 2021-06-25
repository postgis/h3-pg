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

#include <postgres.h>		 // Datum, etc.
#include <fmgr.h>			 // PG_FUNCTION_ARGS, etc.
#include <access/hash.h>	 // hash_any

#include <h3api.h> // Main H3 include
#include "extension.h"

PG_FUNCTION_INFO_V1(h3index_hash);
PG_FUNCTION_INFO_V1(h3index_hash_extended);

Datum
h3index_hash(PG_FUNCTION_ARGS)
{
	H3Index		index = PG_GETARG_H3INDEX(0);
	Datum		hash = hash_any((unsigned char *) &index, sizeof(index));

	PG_RETURN_DATUM(hash);
}

Datum
h3index_hash_extended(PG_FUNCTION_ARGS)
{
	H3Index		index = PG_GETARG_H3INDEX(0);
	uint64		seed = PG_GETARG_INT64(1);
	Datum		hash = hash_any_extended((unsigned char *) &index, sizeof(index), seed);

	PG_RETURN_DATUM(hash);
}
