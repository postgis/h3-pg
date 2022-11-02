ARG UBUNTU=focal
FROM ubuntu:${UBUNTU}
ARG UBUNTU

# set timezone
RUN ln --symbolic --no-dereference --force \
        /usr/share/zoneinfo/Europe/Copenhagen /etc/localtime 

RUN apt-get update && apt-get install -y \
        build-essential \
        #cmake \
        git \
        pgxnclient

# H3 requires CMake 3.20, which has not landed in ubuntu
# as of writing. So we setup cmake apt repository
RUN if [ "$UBUNTU" != "impish" ] ; then apt-key adv --fetch-keys https://apt.kitware.com/keys/kitware-archive-latest.asc ; fi
RUN if [ "$UBUNTU" != "impish" ] ; then echo "deb https://apt.kitware.com/ubuntu/ ${UBUNTU} main" >> /etc/apt/sources.list.d/pgdg.list ; fi
RUN apt-get update && apt-get install -y cmake
# we can remove the block above when 3.20 lands

# setup postgresql apt repository
RUN apt-key adv --fetch-keys https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ ${UBUNTU}-pgdg main" >> /etc/apt/sources.list.d/pgdg.list

ARG POSTGRESQL=14
ARG POSTGIS=3

# install postgresql and postgis
RUN apt-get update && apt-get install -y \
        postgresql-${POSTGRESQL}-postgis-${POSTGIS}-scripts \
        postgresql-${POSTGRESQL}-postgis-${POSTGIS} \
        postgresql-server-dev-${POSTGRESQL} \
        postgresql-${POSTGRESQL}

# install pg_validate_extupgrade
RUN apt-get update && apt install -y cargo
RUN cargo install \
        --root /usr \
        --locked \
        --git https://github.com/rjuju/pg_validate_extupgrade.git \
        --rev ad5edf11e5dbdb25a8029ba0bc24347876837d81
ENV PGHOST /var/run/postgresql

# install sudo
RUN apt-get update && apt-get install -y \
        sudo

# run it!
WORKDIR /github/workspace
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
