/*
 * Copyright 2024-2025 Zacharias Knudsen
 * Copyright 2026 Eric Schoffstall
 * Copyright 2026 Darafei Praliaskouski
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

#include <limits.h>
#include <math.h>

#include <postgres.h>
#include <fmgr.h>
#include <access/gist.h>
#include <access/stratnum.h>
#include <utils/sortsupport.h>

#include <h3api.h>
#include "algos.h"
#include "operators.h"
#include "type.h"
#include "upstream_macros.h"

PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_consistent);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_union);
/* compress/decompress are no-ops for fixed-size h3index and not registered */
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_penalty);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_picksplit);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_same);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_distance);
PGDLLEXPORT PG_FUNCTION_INFO_V1(h3index_gist_sortsupport);

/* Fixed penalty for unions that cross base-cell boundaries. */
#define GIST_CROSS_BASE_PENALTY 16.0f
#define GIST_LIMIT_RATIO 0.3333333333333333
/*
 * Leaf-page fanout observed on PostgreSQL 18 by bulk-building large
 * h3index_gist_ops_experimental indexes and inspecting them with
 * pageinspect.
 */
#define GIST_INDEX_TUPLES_PER_PAGE 407

/*
 * Maximum grid distance from a cell's center child to any descendant at the
 * given additional depth. This was verified empirically for both hexagons and
 * pentagons and follows the exact recurrence r(d) = 7 * r(d - 2) + 4.
 */
static const int64_t gist_descendant_radius[MAX_H3_RES + 1] = {
	0, 1, 4, 11, 32, 81, 228, 571,
	1600, 4001, 11204, 28011, 78432, 196081, 549028, 1372571
};

/* Entry for sorting in picksplit */
typedef struct
{
	OffsetNumber offset;
	H3Index		key;
} SortEntry;

/* Candidate next assignment while the greedy split grows inward. */
typedef struct
{
	bool		to_left;
	H3Index		unionL;
	H3Index		unionR;
	float		widen;
	int			nleft;
	int			nright;
} PickSplitMove;

/* Return index itself or its center child at the requested resolution. */
static inline bool
h3index_center_child_at(H3Index index, int resolution, H3Index *out)
{
	int			index_res = getResolution(index);

	if (index_res == resolution)
	{
		*out = index;
		return true;
	}

	if (index_res > resolution)
		return false;

	return cellToCenterChild(index, resolution, out) == E_SUCCESS;
}

/*
 * Bound the nearest possible descendant distance by comparing center-child
 * representatives across candidate resolutions and subtracting the maximum
 * descendant radius for the indexed subtree at each resolution.
 */
static double
h3index_gist_distance_lower_bound(H3Index key, H3Index query)
{
	int			key_res = getResolution(key);
	int			min_res = Max(key_res, getResolution(query));
	int64_t		best = INT64_MAX;

	for (int resolution = min_res; resolution <= MAX_H3_RES; resolution++)
	{
		H3Index		key_at_resolution;
		H3Index		query_at_resolution;
		int64_t		distance;

		if (!h3index_center_child_at(key, resolution, &key_at_resolution) ||
			!h3index_center_child_at(query, resolution, &query_at_resolution))
			continue;

		if (gridDistance(key_at_resolution, query_at_resolution, &distance))
			continue;

		distance = Max(distance - gist_descendant_radius[resolution - key_res], 0);
		best = Min(best, distance);

		if (best == 0)
			break;
	}

	if (best == INT64_MAX)
		return 0.0;

	return (double) best;
}

/* qsort comparator for the picksplit input array. */
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

/* Compare abbreviated H3 datums during sortsupport-driven sorts. */
static int
h3index_gist_cmp_abbrev(Datum x, Datum y, SortSupport ssup)
{
	if (x == y)
		return 0;
	else if (x < y)
		return -1;
	return 1;
}

/* Compare full H3 datums when abbreviation is unavailable or disabled. */
static int
h3index_gist_cmp_full(Datum x, Datum y, SortSupport ssup)
{
	H3Index		a = DatumGetH3Index(x);
	H3Index		b = DatumGetH3Index(y);

	if (a == b)
		return 0;
	else if (a < b)
		return -1;
	return 1;
}

/* Keep abbreviation enabled; H3 keys already fit cleanly in a Datum. */
static bool
h3index_gist_abbrev_abort(int memtupcount, SortSupport ssup)
{
	return false;
}

/* Reuse the pass-by-value h3index datum itself as the abbreviated key. */
static Datum
h3index_gist_abbrev_convert(Datum original, SortSupport ssup)
{
	return DatumGetH3Index(original);
}

/* Measure how much coarser a union becomes after adding a key. */
static inline float
h3index_union_widening(H3Index current, H3Index next_union)
{
	if (current == H3_NULL || current == next_union)
		return 0.0f;
	if (next_union == H3_NULL)
		return GIST_CROSS_BASE_PENALTY;
	return (float) (getResolution(current) - getResolution(next_union));
}

/* Score how well two candidate unions stay separated from one another. */
static int
h3index_union_separation_score(H3Index unionL, H3Index unionR)
{
	if (unionL == H3_NULL || unionR == H3_NULL)
		return INT_MAX;
	if (getBaseCellNumber(unionL) != getBaseCellNumber(unionR))
		return -1;

	H3Index ancestor = finest_common_ancestor(unionL, unionR);

	if (ancestor == H3_NULL)
		return -1;
	return getResolution(ancestor);
}

/* Match the natural H3 key order used by picksplit and buffered GiST build. */
Datum
h3index_gist_sortsupport(PG_FUNCTION_ARGS)
{
	SortSupport ssup = (SortSupport) PG_GETARG_POINTER(0);

	ssup->comparator = h3index_gist_cmp_full;
	ssup->ssup_extra = NULL;

	if (ssup->abbreviate && sizeof(Datum) == 8)
	{
		ssup->comparator = h3index_gist_cmp_abbrev;
		ssup->abbrev_converter = h3index_gist_abbrev_convert;
		ssup->abbrev_abort = h3index_gist_abbrev_abort;
		ssup->abbrev_full_comparator = h3index_gist_cmp_full;
	}

	PG_RETURN_VOID();
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

	/* containment() returns +1 for contains/equality, -1 for contained-by. */
	int cmp = containment(key, query);

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
	H3Index		out = DatumGetH3Index(entries[FirstOffsetNumber].key);

	for (OffsetNumber i = OffsetNumberNext(FirstOffsetNumber);
		 i < entryvec->n;
		 i = OffsetNumberNext(i))
		out = finest_common_ancestor(out, DatumGetH3Index(entries[i].key));

	PG_RETURN_H3INDEX(out);
}

/**
 * The GiST Penalty method for H3 indexes.
 * Uses the widening required to accommodate the new entry as penalty.
 */
Datum
h3index_gist_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY  *origentry = (GISTENTRY *) PG_GETARG_POINTER(0);
	GISTENTRY  *newentry = (GISTENTRY *) PG_GETARG_POINTER(1);
	float	   *penalty = (float *) PG_GETARG_POINTER(2);

	H3Index		orig = DatumGetH3Index(origentry->key);
	H3Index		new = DatumGetH3Index(newentry->key);

	if (orig == H3_NULL)
	{
		/*
		 * H3_NULL already represents a mixed-base union, so adding another
		 * entry cannot widen it any further.
		 */
		*penalty = 0.0f;
		PG_RETURN_POINTER(penalty);
	}

	if (new == H3_NULL)
	{
		/* Mixed-base subtrees immediately widen any concrete union fully. */
		*penalty = GIST_CROSS_BASE_PENALTY;
		PG_RETURN_POINTER(penalty);
	}

	H3Index ancestor = finest_common_ancestor(orig, new);

	if (ancestor == orig)
	{
		/* Existing unions should accept descendants at zero insertion cost. */
		*penalty = 0.0f;
		PG_RETURN_POINTER(penalty);
	}

	if (ancestor == H3_NULL)
	{
		/* Different base cells always pay the fixed cross-base penalty. */
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
 * Sorts entries by H3 index value, seeds both sides from the extremes, and
 * greedily grows inward. H3's natural ordering groups by base cell then
 * hierarchical digits, providing good spatial locality.
 */
Datum
h3index_gist_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GIST_SPLITVEC *v = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	OffsetNumber maxoff = entryvec->n - 1;
	GISTENTRY  *ent = entryvec->vector;
	int			nentries = maxoff;
	OffsetNumber *left,
			   *right;
	SortEntry  *sorted;

	v->spl_left = (OffsetNumber *) palloc((maxoff + 1) * sizeof(OffsetNumber));
	left = v->spl_left;
	v->spl_nleft = 0;

	v->spl_right = (OffsetNumber *) palloc((maxoff + 1) * sizeof(OffsetNumber));
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

	/* Seed both sides from the extremes, then greedily grow inward. */
	H3Index unionL = sorted[0].key;
	*left++ = sorted[0].offset;
	++(v->spl_nleft);

	H3Index unionR = sorted[nentries - 1].key;
	*right++ = sorted[nentries - 1].offset;
	++(v->spl_nright);

	int minfill = (int) ceil(GIST_LIMIT_RATIO * (double) nentries);
	if (nentries > GIST_INDEX_TUPLES_PER_PAGE &&
		nentries <= 2 * GIST_INDEX_TUPLES_PER_PAGE)
		minfill = Max(minfill, nentries - GIST_INDEX_TUPLES_PER_PAGE);
	minfill = Max(minfill, 1);

	int lo = 1;
	int hi = nentries - 2;

	while (lo <= hi)
	{
		int remaining = hi - lo + 1;

		if (v->spl_nleft + remaining <= minfill)
		{
			unionL = finest_common_ancestor(unionL, sorted[lo].key);
			*left++ = sorted[lo++].offset;
			++(v->spl_nleft);
			continue;
		}

		if (v->spl_nright + remaining <= minfill)
		{
			unionR = finest_common_ancestor(unionR, sorted[hi].key);
			*right++ = sorted[hi--].offset;
			++(v->spl_nright);
			continue;
		}

		{
			PickSplitMove leftMove = {
				.to_left = true,
				.unionL = finest_common_ancestor(unionL, sorted[lo].key),
				.unionR = unionR,
				.nleft = v->spl_nleft + 1,
				.nright = v->spl_nright
			};
			PickSplitMove rightMove = {
				.to_left = false,
				.unionL = unionL,
				.unionR = finest_common_ancestor(unionR, sorted[hi].key),
				.nleft = v->spl_nleft,
				.nright = v->spl_nright + 1
			};
			int			leftConcrete;
			int			rightConcrete;
			int			leftSeparation;
			int			rightSeparation;
			int			leftBalance;
			int			rightBalance;

			leftMove.widen = h3index_union_widening(unionL, leftMove.unionL);
			rightMove.widen = h3index_union_widening(unionR, rightMove.unionR);

			leftConcrete = (leftMove.unionL != H3_NULL) +
				(leftMove.unionR != H3_NULL);
			rightConcrete = (rightMove.unionL != H3_NULL) +
				(rightMove.unionR != H3_NULL);
			leftSeparation = h3index_union_separation_score(
				leftMove.unionL, leftMove.unionR);
			rightSeparation = h3index_union_separation_score(
				rightMove.unionL, rightMove.unionR);
			leftBalance = abs(leftMove.nleft - leftMove.nright);
			rightBalance = abs(rightMove.nleft - rightMove.nright);

			if (leftConcrete > rightConcrete ||
				(leftConcrete == rightConcrete &&
				 (leftSeparation < rightSeparation ||
				  (leftSeparation == rightSeparation &&
				   (leftMove.widen < rightMove.widen ||
					(leftMove.widen == rightMove.widen &&
					 (leftBalance < rightBalance ||
					  (leftBalance == rightBalance &&
					   leftMove.to_left && !rightMove.to_left))))))))
			{
				unionL = leftMove.unionL;
				*left++ = sorted[lo++].offset;
				++(v->spl_nleft);
			}
			else
			{
				unionR = rightMove.unionR;
				*right++ = sorted[hi--].offset;
				++(v->spl_nright);
			}
		}
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

	/* internal node distances are lower bounds; leaf distances are exact */
	*recheck = !GIST_LEAF(entry);

	switch (strategy)
	{
		case RTKNNSearchStrategyNumber:
		{
			int64_t		distance;

			if (key == H3_NULL)
				PG_RETURN_FLOAT8(GIST_LEAF(entry) ? INFINITY : 0.0);

			if (!GIST_LEAF(entry))
				PG_RETURN_FLOAT8(h3index_gist_distance_lower_bound(key, query));

			if (h3index_grid_distance(key, query, &distance))
				PG_RETURN_FLOAT8(INFINITY);

			PG_RETURN_FLOAT8((double) distance);
		}
		default:
			ereport(ERROR, (
							errcode(ERRCODE_INTERNAL_ERROR),
					   errmsg("unrecognized StrategyNumber: %d", strategy)));
	}
}
