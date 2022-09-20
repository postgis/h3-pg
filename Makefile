# Copyright 2018-2022 Bytes & Brains
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

EXTENSION = h3 h3_postgis

# extract extension version from .control file
EXTVERSION = $(shell grep default_version h3.control | \
	sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

# h3 core library version to clone and statically link
LIBH3_VERSION = v4.0.1
# directory that h3 core repository is cloned into
LIBH3_SOURCE = libh3-$(LIBH3_VERSION)
# h3 static library location
LIBH3_BUILD = $(LIBH3_SOURCE)/build
# sql files used for installation, update and testing
SQL_INSTALLS = $(wildcard h3/sql/install/*.sql)
SQL_UPDATES = $(wildcard h3/sql/updates/*.sql)
SQL_TESTS = $(wildcard h3/test/sql/*.sql)
SQL_FULLINSTALL = h3--$(EXTVERSION).sql

# postgis extension
SQL_INSTALLS_H3_POSTGIS = $(wildcard h3_postgis/sql/install/*.sql)
SQL_UPDATES_H3_POSTGIS = $(wildcard h3_postgis/sql/updates/*.sql)
SQL_FULLINSTALL_H3_POSTGIS = h3_postgis--$(EXTVERSION).sql

# a shared library to build from multiple source files
MODULE_big = h3
# object files to be linked together
OBJS = $(patsubst %.c,%.o,$(wildcard h3/src/*.c))
# random files to install into $PREFIX/share/$MODULEDIR
DATA = $(SQL_UPDATES) $(SQL_UPDATES_H3_POSTGIS)
DATA_built = $(SQL_FULLINSTALL) $(SQL_FULLINSTALL_H3_POSTGIS)
# will be added to MODULE_big link line
SHLIB_LINK += -lh3 -L$(LIBH3_BUILD)/lib
# will be added to CPPFLAGS
PG_CPPFLAGS += -I$(LIBH3_BUILD)/src/h3lib/include -Ih3/src
# list of regression test cases (without suffix)
REGRESS = $(basename $(notdir $(SQL_TESTS)))
# additional switches to pass to pg_regress
REGRESS_OPTS = \
	--inputdir=h3/test \
	--outputdir=h3/test \
	--load-extension=h3
# extra files to remove in make clean
EXTRA_CLEAN += \
	$(LIBH3_SOURCE) \
	$(DATA_built) \
	h3/src/extension.h \
	$(wildcard h3/test/sql/ci-*.sql) \
	$(wildcard h3/test/expected/ci-*.out) \
	$(wildcard *.BAK) \
	/tmp/extra-functions \
	/tmp/excluded-functions \
	h3/test/regression.diffs h3/test/regression.out h3/test/results \
	h3-*.zip

PG_CONFIG = pg_config

# additional flags
CFLAGS_ADD :=
PG_CC := $(shell $(PG_CONFIG) --cc)
CHECK_FLAG_CMD := echo 'int main() { return 0; }' | $(PG_CC) -x c - -o /dev/null {flag} 2>&1 1>/dev/null; echo $$?

# add -mpc64 flag is supported
CHECK_FLAG_MPC64 := $(shell $(subst {flag},-mpc64,$(CHECK_FLAG_CMD)))
ifeq ($(CHECK_FLAG_MPC64),0)
	CFLAGS_ADD += -mpc64
endif

# PGXS boilerplate
CUSTOM_COPT=-flto $(CFLAGS_ADD)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

###########################################################################
# Non-standard PGXS stuff below
###########################################################################

$(OBJS): $(LIBH3_BUILD) h3/src/extension.h

# targets for building H3 library internals
$(LIBH3_SOURCE):
	git clone --branch $(LIBH3_VERSION) --depth 1 \
	 	https://github.com/uber/h3 $(LIBH3_SOURCE)
$(LIBH3_BUILD): $(LIBH3_SOURCE)
	mkdir -p $(LIBH3_BUILD)
	cd $(LIBH3_BUILD) && cmake \
		-DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_C_FLAGS="-fPIC -fvisibility=hidden -flto -fwrapv $(CFLAGS_ADD)" \
		-DBUILD_TESTING=OFF \
		-DENABLE_COVERAGE=OFF \
		-DENABLE_DOCS=OFF \
		-DENABLE_FORMAT=OFF \
		-DENABLE_LINTING=OFF \
		..
	cmake --build $(LIBH3_BUILD) --target h3
	cmake --build $(LIBH3_BUILD) --target binding-functions

# generate header file with extension version baked in
h3/src/extension.h: h3/src/extension.in.h
	sed -e 's/@EXTVERSION@/$(EXTVERSION)/g' \
		$< > $@

# generate full installation sql (from uninstalled to latest)
$(SQL_FULLINSTALL): $(sort $(SQL_INSTALLS))
	cat $^ > $@
$(SQL_FULLINSTALL_H3_POSTGIS): $(sort $(SQL_INSTALLS_H3_POSTGIS))
	cat $^ > $@

# package for distribution
dist: $(SQL_FULLINSTALL) $(SQL_FULLINSTALL_H3_POSTGIS)
	git archive --prefix=h3-$(EXTVERSION)/ \
				--output h3-${EXTVERSION}.zip \
				--add-file=$(SQL_FULLINSTALL) \
				--add-file=$(SQL_FULLINSTALL_H3_POSTGIS) \
				HEAD

###########################################################################
# Extra CI testing targets
###########################################################################

format: clean
	pgindent

# Run on dev using:
# PIPENV_PIPFILE=.github/documentation/Pipfile pipenv run make docs/api.md
docs/api.md: $(SQL_INSTALLS)
	python .github/documentation/generate.py "h3/sql/install/*" > $@
	npx doctoc $@

# functions which we have decided not to provide bindings for
EXCLUDED_BINDING_FUNCTIONS = \
	degs_to_rads \
	rads_to_degs \
	to_string \
	string_to_h3

# functions provided that are not part of expected binding functions
EXTRA_BINDING_FUNCTIONS = \
	get_extension_version \
	cell_to_children_slow \
	cell_to_geo_boundary_geography \
	cell_to_geo_boundary_geometry \
	cell_to_geography \
	cell_to_geometry \
	cell_to_boundary_wkb

/tmp/excluded-functions:
	echo "$(EXCLUDED_BINDING_FUNCTIONS)" | tr " " "\n" > $@

/tmp/extra-functions:
	echo "$(EXTRA_BINDING_FUNCTIONS)" | tr " " "\n" > $@

ARCH_SQL = "SELECT typbyval FROM pg_type WHERE typname = 'h3index';"

ifeq ($(ARCH),amd64)
    ARCH_BOOL:=t
endif
ifndef ARCH_BOOL
	ARCH_BOOL:=f
endif

# rules for testing if arch determines pass by value/reference
h3/test/sql/ci-arch-$(ARCH).sql: $(SQL_FULLINSTALL)
	echo $(ARCH_SQL) > $@
h3/test/expected/ci-arch-$(ARCH).out: $(SQL_UPDATES)
	psql -c "DROP DATABASE IF EXISTS pg_regress;"
	psql -c "CREATE DATABASE pg_regress;"
	psql -d pg_regress -c "CREATE EXTENSION postgis;"
	psql -d pg_regress -c "CREATE EXTENSION h3;"
	echo $(ARCH_SQL) > $@
	psql -d pg_regress -c $(ARCH_SQL) | sed '3 s/.*/ ${ARCH_BOOL}/' -   >> $@
	psql -c "DROP DATABASE pg_regress;"


PRINT_FUNCTIONS_SQL = "SELECT proname, proisstrict, provolatile, proparallel, prosrc FROM pg_proc WHERE proname LIKE '%h3%' ORDER BY proname, prosrc;"

# rules for testing the update path against full install
h3/test/expected/ci-install.out: $(SQL_UPDATES)
	psql -c "DROP DATABASE IF EXISTS pg_regress;"
	psql -c "CREATE DATABASE pg_regress;"
	psql -d pg_regress -c "CREATE EXTENSION h3;"
	echo $(PRINT_FUNCTIONS_SQL) >> $@
	psql -d pg_regress -c $(PRINT_FUNCTIONS_SQL) >> $@
	psql -c "DROP DATABASE pg_regress;"

# generate expected bindings from h3 generated binding function list
h3/test/expected/ci-bindings.out: $(LIBH3_BUILD)/binding-functions /tmp/excluded-functions
	psql -c "DROP DATABASE IF EXISTS pg_regress;"
	psql -c "CREATE DATABASE pg_regress;"
	echo "\\\echo '$(shell \
	    cat ${LIBH3_BUILD}/binding-functions \
        | sed -r 's/\bh3//g' \
        | sed -r 's/([A-Z]|[0-9]+)/_\L\1/g' | sed 's/^_//' \
        | sed -r 's/h_3/h3/g' \
        | sed -r 's/i_i_i/iii/g' \
        | sed -e 's/\(.*\)/\L\1/' \
        | grep -v -x -F -f /tmp/excluded-functions \
        | sort | uniq \
	)'" > /tmp/ci-bindings.out
	cat /tmp/ci-bindings.out > $@
	psql -d pg_regress -f /tmp/ci-bindings.out >> $@
	psql -c "DROP DATABASE pg_regress;"

# generate actual bindings from installed extension
h3/test/sql/ci-bindings.sql: h3/test/expected/ci-install.out /tmp/extra-functions
	echo "\\\echo '$(shell \
		cat h3/test/expected/ci-install.out \
		| grep -o '^ h3_\w*' \
		| sed -r 's/ \bh3_//g' \
		| grep -v -x -F -f /tmp/extra-functions \
		| sort | uniq \
	)'" > $@

ci: h3/test/sql/ci-arch-$(ARCH).sql h3/test/expected/ci-arch-$(ARCH).out h3/test/sql/ci-bindings.sql h3/test/expected/ci-bindings.out
.PHONY: ci format