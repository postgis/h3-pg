#!/bin/bash
set -e

BASEDIR=$(dirname $(realpath "$0"))
REPOSITORY="docker.pkg.github.com/bytesandbrains/h3-pg"

# i386 being phased out from postgres apt :-(
#ARCHS=(amd64 i386)
ARCHS=(amd64)
UBUNTUS=(impish focal) # latest and LTS
POSTGRESQLS=(14 13) # two latest

cd $BASEDIR

printhelp () { echo \
"Usage: $0 [-a i386] [-u bionic] [-g 12] -b|-p|-t

Options:
  -a   Architecture (amd64 or i386)
  -u   Ubuntu release (focal, bionic, etc.)
  -g   PostgreSQL version (15, 14, etc.)

Commands
  -b   Build images
  -p   Push images
  -t   Run tests"

  exit 0;
}

while getopts ':hbpta::u::g::' o; do
case "${o}" in
  a)  # set arch
      ARCHS=($OPTARG)
      ;;
  u)  # set release name
      UBUNTUS=($OPTARG)
      ;;
  g)  # set postgresql version
      POSTGRESQLS=($OPTARG)
      ;;

  b)  # build images
      work=build
      for postgresql in "${POSTGRESQLS[@]}"; do
        for ubuntu in "${UBUNTUS[@]}"; do
          for arch in "${ARCHS[@]}"; do
            echo "=============================="
            echo "$postgresql-$ubuntu-$arch"
            docker build \
              --tag $REPOSITORY/test:$postgresql-$ubuntu-$arch \
              --build-arg POSTGRESQL=$postgresql \
              --build-arg UBUNTU=$ubuntu \
              --build-arg ARCH=$arch \
              .
          done
        done
      done
      ;;

  p)  # push images
      work=push
      for postgresql in "${POSTGRESQLS[@]}"; do
        for ubuntu in "${UBUNTUS[@]}"; do
          for arch in "${ARCHS[@]}"; do
            echo "=============================="
            echo "$postgresql-$ubuntu-$arch"
            docker push \
              $REPOSITORY/test:$postgresql-$ubuntu-$arch
          done
        done
      done
      ;;

  t)  # run tests
      work=test
      for postgresql in "${POSTGRESQLS[@]}"; do
        for ubuntu in "${UBUNTUS[@]}"; do
          for arch in "${ARCHS[@]}"; do
            echo "=============================="
            echo "$postgresql-$ubuntu-$arch"
            docker run \
              --rm \
              -v "$PWD"/../..:/github/workspace \
              $REPOSITORY/test:$postgresql-$ubuntu-$arch
          done
        done
      done
      ;;

  *) # print help
      printhelp
      exit 1;;
  esac
done

shift $((OPTIND-1))

# print help if no actual work was done
if [ -z "${work}" ]; then
  printhelp
fi