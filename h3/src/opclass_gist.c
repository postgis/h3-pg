/*
 * Copyright 2019-2025 Bytes & Brains
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

/* Number of random entries to sample when finding picksplit seeds */
#define GIST_SAMPLE_SIZE 20

/* Penalty for entries in different base cells (MAX_H3_RES + 1) */
#define GIST_CROSS_BASE_PENALTY 16.0f

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
 * Uses a sampled seed-selection approach to avoid O(n^2) on large pages.
 */
Datum
h3index_gist_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	OffsetNumber maxoff = entryvec->n - 1;
	GISTENTRY  *ent = entryvec->vector;
	int			nbytes;
	OffsetNumber *left,
			   *right;

	H3Index		unionL,
				unionR,
				seed_left,
				seed_right;

	OffsetNumber seed_left_idx = FirstOffsetNumber,
				seed_right_idx = FirstOffsetNumber + 1;

	int			sample_size;
	OffsetNumber *sample;

	nbytes = (maxoff + 1) * sizeof(OffsetNumber);

	v->spl_left = (OffsetNumber *) palloc(nbytes);
	left = v->spl_left;
	v->spl_nleft = 0;

	v->spl_right = (OffsetNumber *) palloc(nbytes);
	right = v->spl_right;
	v->spl_nright = 0;

	/* Build a sample of entries for seed selection */
	sample_size = Min(maxoff, GIST_SAMPLE_SIZE);
	sample = (OffsetNumber *) palloc(sample_size * sizeof(OffsetNumber));

	if (maxoff <= GIST_SAMPLE_SIZE)
	{
		/* Small enough to check all entries */
		for (int i = 0; i < sample_size; i++)
			sample[i] = FirstOffsetNumber + i;
	}
	else
	{
		/* Pick evenly spaced entries as sample */
		for (int i = 0; i < sample_size; i++)
			sample[i] = FirstOffsetNumber + (i * maxoff) / sample_size;
	}

	/* Find the seed pair with maximum waste among the sample */
	{
		int64_t		max_waste = -1;
		H3Index		a,
					b,
					seed_union;
		int			res_a,
					res_b,
					res_finest;
		int64_t		waste,
					nchildren;

		seed_left = DatumGetH3Index(ent[sample[0]].key);
		seed_right = DatumGetH3Index(ent[sample[sample_size > 1 ? 1 : 0]].key);

		for (int i = 0; i < sample_size; i++)
		{
			a = DatumGetH3Index(ent[sample[i]].key);
			for (int j = i + 1; j < sample_size; j++)
			{
				b = DatumGetH3Index(ent[sample[j]].key);

				/* if one contains the other, no waste */
				if (containment(a, b) != 0)
				{
					waste = 0;
				}
				else
				{
					seed_union = finest_common_ancestor(a, b);
					if (seed_union == H3_NULL)
					{
						/* different base cells — very high waste */
						waste = INT64_MAX;
					}
					else
					{
						res_a = getResolution(a);
						res_b = getResolution(b);
						res_finest = (res_a > res_b) ? res_a : res_b;

						h3_assert(cellToChildrenSize(seed_union, res_finest, &waste));
						h3_assert(cellToChildrenSize(a, res_finest, &nchildren));
						waste -= nchildren;
						h3_assert(cellToChildrenSize(b, res_finest, &nchildren));
						waste -= nchildren;
					}
				}

				if (waste > max_waste)
				{
					max_waste = waste;
					seed_left = a;
					seed_right = b;
					seed_left_idx = sample[i];
					seed_right_idx = sample[j];
				}
			}
		}
	}

	pfree(sample);

	/* Pre-assign seed entries to guarantee both sides are non-empty */
	unionL = seed_left;
	unionR = seed_right;

	*left = seed_left_idx;
	++left;
	v->spl_nleft = 1;

	*right = seed_right_idx;
	++right;
	v->spl_nright = 1;

	/* Assign remaining entries to the closest seed */
	for (OffsetNumber i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		H3Index		a;
		H3Index		check_left,
					check_right;
		int			size_change_l,
					size_change_r;

		/* Skip seed entries — already assigned */
		if (i == seed_left_idx || i == seed_right_idx)
			continue;

		a = DatumGetH3Index(ent[i].key);
		check_left = finest_common_ancestor(unionL, a);
		check_right = finest_common_ancestor(unionR, a);

		/* Compute resolution change for each side */
		if (check_left == H3_NULL)
			size_change_l = (int) GIST_CROSS_BASE_PENALTY;
		else
			size_change_l = getResolution(unionL) - getResolution(check_left);

		if (check_right == H3_NULL)
			size_change_r = (int) GIST_CROSS_BASE_PENALTY;
		else
			size_change_r = getResolution(unionR) - getResolution(check_right);

		if (size_change_l < size_change_r ||
			(size_change_l == size_change_r &&
			 v->spl_nleft <= v->spl_nright))
		{
			unionL = check_left;
			*left = i;
			++left;
			++(v->spl_nleft);
		}
		else
		{
			unionR = check_right;
			*right = i;
			++right;
			++(v->spl_nright);
		}
	}

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
