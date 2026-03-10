/*
 * Copyright 2024-2025 Zacharias Knudsen, Eric Schoffstall
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

#include <math.h>			 // INFINITY

#include <postgres.h>		 // Datum, etc.
#include <fmgr.h>			 // PG_FUNCTION_ARGS, etc.
#include <access/stratnum.h> // RTOverlapStrategyNumber, etc.
#include <access/gist.h>	 // GiST

#include <h3api.h> // Main H3 include
#include "algos.h"
#include "error.h"
#include "operators.h"
#include "type.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_consistent);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_union);
/* compress/decompress are no-ops for fixed-size h3index and not registered */
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_penalty);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_picksplit);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_same);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_distance);

/* Penalty for entries in different base cells (MAX_H3_RES + 1) */
#define GIST_CROSS_BASE_PENALTY 16.0f

/* Entry for sorting in picksplit */
typedef struct
{
	OffsetNumber offset;
	H3Index		key;
} SortEntry;

static int
sort_entry_cmp(const void *a, const void *b)
{
	H3Index		ka = ((const SortEntry *) a)->key;
	H3Index		kb = ((const SortEntry *) b)->key;

	if (ka < kb)
		return -1;
	if (ka > kb)
		return 1;
	return 0;
}

/**
 * The GiST Consistent method for H3 indexes.
 * Should return false if for all data items x below entry,
 * the predicate x op query == false, where op is the operation
 * corresponding to strategy in the pg_amop table.
 */
Datum
h3index_gist_consistent(PG_FUNCTION_ARGS)
{
	GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	H3Index		query = PG_GETARG_H3INDEX(1);
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);

	/* Oid subtype = PG_GETARG_OID(3); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(4);
	H3Index		key = DatumGetH3Index(entry->key);
	int			cmp;

	/* H3_NULL key means union of entries from different base cells */
	if (key == H3_NULL)
	{
		*recheck = true;
		PG_RETURN_BOOL(true);
	}

	/*
	 * For equality, we only need key == query. Skip the more expensive
	 * containment() call since it is not needed for this strategy.
	 */
	if (strategy == RTSameStrategyNumber && GIST_LEAF(entry))
	{
		*recheck = false;
		PG_RETURN_BOOL(key == query);
	}

	/*
	 * containment() returns +1 when a contains b (including a == b),
	 * -1 when b contains a, and 0 when neither contains the other.
	 * Note: containment(x, x) returns +1, not 0, because x == xParent.
	 */
	cmp = containment(key, query);

	if (GIST_LEAF(entry))
	{
		/* leaf checks are exact */
		*recheck = false;

		switch (strategy)
		{
			case RTOverlapStrategyNumber:
				PG_RETURN_BOOL(cmp != 0);
			case RTContainsStrategyNumber:
				PG_RETURN_BOOL(cmp > 0);
			case RTContainedByStrategyNumber:
				PG_RETURN_BOOL(cmp < 0 || key == query);
			default:
				ereport(ERROR, (
								errcode(ERRCODE_INTERNAL_ERROR),
						   errmsg("unrecognized StrategyNumber: %d", strategy)));
		}
	}
	else
	{
		/* internal node checks need recheck */
		*recheck = true;

		switch (strategy)
		{
			case RTOverlapStrategyNumber:
			case RTContainedByStrategyNumber:
				/* key must overlap query for children to be contained */
				PG_RETURN_BOOL(cmp != 0);
			case RTSameStrategyNumber:
			case RTContainsStrategyNumber:
				/* key must contain query for children to possibly match */
				PG_RETURN_BOOL(cmp > 0);
			default:
				ereport(ERROR, (
								errcode(ERRCODE_INTERNAL_ERROR),
						   errmsg("unrecognized StrategyNumber: %d", strategy)));
		}
	}

	/* unreachable, but keep compiler happy */
	PG_RETURN_BOOL(false);
}

/**
 * The GiST Union method for H3 indexes.
 * Returns the minimal H3 index that encloses all the entries in entryvec.
 */
Datum
h3index_gist_union(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GISTENTRY  *entries = entryvec->vector;
	int			n = entryvec->n;
	H3Index		out = DatumGetH3Index(entries[0].key);
	H3Index		tmp;

	for (int i = 1; i < n; i++)
	{
		tmp = DatumGetH3Index(entries[i].key);
		out = finest_common_ancestor(out, tmp);
	}

	PG_RETURN_H3INDEX(out);
}

/**
 * The GiST Penalty method for H3 indexes.
 * Uses the resolution change required to accommodate the new entry as penalty.
 */
Datum
h3index_gist_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY  *origentry = (GISTENTRY *) PG_GETARG_POINTER(0);
	GISTENTRY  *newentry = (GISTENTRY *) PG_GETARG_POINTER(1);
	float	   *penalty = (float *) PG_GETARG_POINTER(2);

	H3Index		orig = DatumGetH3Index(origentry->key);
	H3Index		new = DatumGetH3Index(newentry->key);
	H3Index		ancestor;

	if (orig == H3_NULL || new == H3_NULL)
	{
		/* H3_NULL key — fixed maximum penalty */
		*penalty = GIST_CROSS_BASE_PENALTY;
		PG_RETURN_POINTER(penalty);
	}

	ancestor = finest_common_ancestor(orig, new);

	if (ancestor == H3_NULL)
	{
		/* different base cells — fixed maximum penalty */
		*penalty = GIST_CROSS_BASE_PENALTY;
	}
	else
	{
		*penalty = (float) (getResolution(orig) - getResolution(ancestor));
	}

	PG_RETURN_POINTER(penalty);
}

/**
 * The GiST PickSplit method for H3 indexes.
 *
 * Sorts entries by H3 index value and splits at the median. H3's natural
 * ordering groups by base cell then hierarchical digits, providing good
 * spatial locality. This replaces the previous O(n^2) seed-selection
 * approach with a simple O(n log n) sort.
 */
Datum
h3index_gist_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	OffsetNumber maxoff = entryvec->n - 1;
	GISTENTRY  *ent = entryvec->vector;
	int			nbytes;
	int			nentries = maxoff;
	int			split;
	OffsetNumber *left,
			   *right;
	H3Index		unionL,
				unionR;
	SortEntry  *sorted;

	nbytes = (maxoff + 1) * sizeof(OffsetNumber);

	v->spl_left = (OffsetNumber *) palloc(nbytes);
	left = v->spl_left;
	v->spl_nleft = 0;

	v->spl_right = (OffsetNumber *) palloc(nbytes);
	right = v->spl_right;
	v->spl_nright = 0;

	/* Sort entries by H3 index value for spatial locality */
	sorted = (SortEntry *) palloc(nentries * sizeof(SortEntry));
	for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		sorted[i - FirstOffsetNumber].offset = i;
		sorted[i - FirstOffsetNumber].key = DatumGetH3Index(ent[i].key);
	}
	qsort(sorted, nentries, sizeof(SortEntry), sort_entry_cmp);

	/* Split at median */
	split = nentries / 2;

	/* Left side: first half of sorted entries */
	unionL = sorted[0].key;
	for (int i = 0; i < split; i++)
	{
		if (i > 0)
			unionL = finest_common_ancestor(unionL, sorted[i].key);
		*left = sorted[i].offset;
		++left;
		++(v->spl_nleft);
	}

	/* Right side: second half of sorted entries */
	unionR = sorted[split].key;
	for (int i = split; i < nentries; i++)
	{
		if (i > split)
			unionR = finest_common_ancestor(unionR, sorted[i].key);
		*right = sorted[i].offset;
		++right;
		++(v->spl_nright);
	}

	pfree(sorted);

	v->spl_ldatum = H3IndexGetDatum(unionL);
	v->spl_rdatum = H3IndexGetDatum(unionR);

	PG_RETURN_POINTER(v);
}

/**
 * Returns true if two index entries are identical.
 */
Datum
h3index_gist_same(PG_FUNCTION_ARGS)
{
	H3Index		a = PG_GETARG_H3INDEX(0);
	H3Index		b = PG_GETARG_H3INDEX(1);
	bool	   *result = (bool *) PG_GETARG_POINTER(2);

	*result = (a == b);
	PG_RETURN_POINTER(result);
}

/**
 * The GiST Distance method for H3 indexes.
 * Returns the grid distance from the entry to the query.
 * For internal nodes, returns a lower-bound distance.
 */
Datum
h3index_gist_distance(PG_FUNCTION_ARGS)
{
	GISTENTRY  *entry = (GISTENTRY *) PG_GETARG_POINTER(0);
	H3Index		query = PG_GETARG_H3INDEX(1);
	StrategyNumber strategy = (StrategyNumber) PG_GETARG_UINT16(2);

	/* Oid		subtype = PG_GETARG_OID(3); */
	bool	   *recheck = (bool *) PG_GETARG_POINTER(4);
	H3Index		key = DatumGetH3Index(entry->key);
	double		retval = INFINITY;

	/* internal node distances are lower bounds; leaf distances are exact */
	*recheck = !GIST_LEAF(entry);

	switch (strategy)
	{
		case RTKNNSearchStrategyNumber:
		{
			int64_t		distance;
			H3Error		error;

			/*
			 * Internal nodes: return 0 as a conservative lower bound.
			 * Using center-child distance is NOT a valid lower bound
			 * because edge descendants can be closer than the center
			 * child. GiST KNN requires lower bounds for correctness;
			 * recheck (set above) ensures final ordering is exact.
			 */
			if (!GIST_LEAF(entry))
			{
				retval = 0.0;
				break;
			}

			/* Leaf node: compute exact distance */
			if (key == H3_NULL)
			{
				retval = INFINITY;
				break;
			}

			{
				int			keyRes = getResolution(key);
				int			queryRes = getResolution(query);

				if (keyRes <= queryRes)
				{
					/* key is coarser — get center child at query resolution */
					H3Index		child;
					error = cellToCenterChild(key, queryRes, &child);
					if (error)
					{
						retval = INFINITY;
						break;
					}
					error = gridDistance(query, child, &distance);
				}
				else
				{
					/* key is finer — refine query to key resolution */
					H3Index		queryChild;
					error = cellToCenterChild(query, keyRes, &queryChild);
					if (error)
					{
						retval = INFINITY;
						break;
					}
					error = gridDistance(queryChild, key, &distance);
				}
			}

			if (error)
			{
				retval = INFINITY;
				break;
			}
			retval = (double) distance;
			break;
		}
		default:
			ereport(ERROR, (
							errcode(ERRCODE_INTERNAL_ERROR),
					   errmsg("unrecognized StrategyNumber: %d", strategy)));
	}

	PG_RETURN_FLOAT8(retval);
}
