#!/usr/bin/env bash
set -e

chmod -R a+w build

systemctl start postgresql

#export PGPORT=49152
#pushd $(pg_config --bindir)
##su postgres -p -c "./initdb -D /tmp/data --no-clean --no-sync"
#pg_createcluster
#echo -e "\n# Configuration added by pg_regress\n\n" >> /tmp/data/postgresql.conf
#echo -e "log_autovacuum_min_duration = 0\n" >> /tmp/data/postgresql.conf
#echo -e "log_checkpoints = on\n" >> /tmp/data/postgresql.conf
#echo -e "log_line_prefix = '%m %b[%p] %q%a '\n" >> /tmp/data/postgresql.conf
#echo -e "log_lock_waits = on\n" >> /tmp/data/postgresql.conf
#echo -e "log_temp_files = 128kB\n" >> /tmp/data/postgresql.conf
#echo -e "max_prepared_transactions = 2\n" >> /tmp/data/postgresql.conf
#cat /tmp/data/postgresql.conf
#su postgres -p -c "./postgres -D /tmp/data" &
#popd

# first regular cmake
su postgres -p -c "cmake -B build ."

cd build
su postgres -p -c "cmake --build .  --config Release"
cmake --install . --component extension --config Release # sudo

su postgres -p -c "ctest --output-on-failure --build-config Release"

# also try makefile pgxn wrapper
#sudo -u postgres make all
#make install # sudo
#sudo -u postgres make test