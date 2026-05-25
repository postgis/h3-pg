#ifndef PGH3_POLYGON_H
#define PGH3_POLYGON_H

#include <postgres.h>
#include <utils/geo_decls.h>

static inline void
h3_polygon_init_boundbox(POLYGON *polygon)
{
	double		xmin,
				xmax,
				ymin,
				ymax;

	if (polygon->npts <= 0)
	{
		polygon->boundbox.high.x = 0;
		polygon->boundbox.high.y = 0;
		polygon->boundbox.low.x = 0;
		polygon->boundbox.low.y = 0;
		return;
	}

	xmin = xmax = polygon->p[0].x;
	ymin = ymax = polygon->p[0].y;

	for (int i = 1; i < polygon->npts; i++)
	{
		if (polygon->p[i].x < xmin)
			xmin = polygon->p[i].x;
		if (polygon->p[i].x > xmax)
			xmax = polygon->p[i].x;
		if (polygon->p[i].y < ymin)
			ymin = polygon->p[i].y;
		if (polygon->p[i].y > ymax)
			ymax = polygon->p[i].y;
	}

	polygon->boundbox.high.x = xmax;
	polygon->boundbox.high.y = ymax;
	polygon->boundbox.low.x = xmin;
	polygon->boundbox.low.y = ymin;
}

#endif
