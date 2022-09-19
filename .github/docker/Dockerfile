ARG ARCH=amd64
ARG UBUNTU=focal
FROM ubuntu:$UBUNTU

ARG ARCH
ARG UBUNTU
ENV DEBIAN_FRONTEND=noninteractive
ENV ARCH=$ARCH

RUN apt-get update && apt-get install -y \
    build-essential \
    # cmake \ # see following comments
    git \
    pgxnclient

# H3 requires CMake 3.20, which has not landed in ubuntu
# as of writing. So we setup cmake apt repository
RUN if [ "$UBUNTU" != "impish" ] ; then apt-key adv --fetch-keys https://apt.kitware.com/keys/kitware-archive-latest.asc ; fi
RUN if [ "$UBUNTU" != "impish" ] ; then echo "deb https://apt.kitware.com/ubuntu/ ${UBUNTU} main" >> /etc/apt/sources.list.d/pgdg.list ; fi
RUN apt-get update && apt-get install -y cmake
# we can remove the block above when 3.20 lands

# Setup PostgreSQL apt repository
RUN apt-key adv --fetch-keys https://www.postgresql.org/media/keys/ACCC4CF8.asc
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ ${UBUNTU}-pgdg main" >> /etc/apt/sources.list.d/pgdg.list

ARG POSTGRESQL=14
ARG POSTGIS=3

RUN apt-get update && apt-get install -y \
    postgresql-${POSTGRESQL}-postgis-${POSTGIS}-scripts \
    postgresql-${POSTGRESQL}-postgis-${POSTGIS} \
    postgresql-server-dev-${POSTGRESQL} \
    postgresql-${POSTGRESQL}

# Setup pg_validate_extupgrade
RUN apt-get update && apt install -y cargo
RUN git clone https://github.com/rjuju/pg_validate_extupgrade.git
WORKDIR pg_validate_extupgrade
RUN cargo fetch --locked
RUN RUSTUP_TOOLCHAIN=stable CARGO_TARGET_DIR=target \
        cargo build --frozen --release --all-features
# RUN RUSTUP_TOOLCHAIN=stable cargo test --frozen --all-features
RUN ./target/release/pg_validate_extupgrade --version
RUN cp ./target/release/pg_validate_extupgrade /usr/bin/
# For some reason i need this to run pg_validate_extupgrade
RUN echo "local all postgres peer" > /etc/postgresql/${POSTGRESQL}/main/pg_hba.conf
RUN echo "host all all 0.0.0.0/0 trust" >> /etc/postgresql/${POSTGRESQL}/main/pg_hba.conf

# Set workdir
WORKDIR /github/workspace

COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
