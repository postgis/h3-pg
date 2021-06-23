#!/bin/sh -l
#set -e

DISTRIBUTION=$1

service postgresql start
chmod -R a+w h3/test

if [ -z "$DISTRIBUTION" ]; then
    make install
    su postgres -p -c "make ci"
    su postgres -p -c "make installcheck"
else
    pgxn install $DISTRIBUTION
    su postgres -p -c "pgxn load $DISTRIBUTION"
    su postgres -p -c "pgxn check $DISTRIBUTION"
fi
