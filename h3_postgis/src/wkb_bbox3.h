#ifndef PGH3_WKB_BBOX3_H
#define PGH3_WKB_BBOX3_H

#include <h3api.h>
#include "wkb_vect3.h"

typedef struct
{
	double		xmin;
	double		xmax;
	double		ymin;
	double		ymax;
	double		zmin;
	double		zmax;
}	Bbox3;

void
			bbox3_from_vect3(const Vect3 * vect, Bbox3 * bbox);

void
			bbox3_merge(const Bbox3 * other, Bbox3 * bbox);

void
			bbox3_from_linked_loop(const LinkedGeoLoop * loop, Bbox3 * bbox);

int
			bbox3_contains_vect3(const Bbox3 * bbox, const Vect3 * vect);

int
			bbox3_contains_lat_lng(const Bbox3 * bbox, const LatLng * coord);

void
			bbox3_from_segment_vect3(const Vect3 * vect1, const Vect3 * vect2, Bbox3 * bbox);

void
			bbox3_from_segment_lat_lng(const LatLng * coord1, const LatLng * coord2, Bbox3 * bbox);

#endif
