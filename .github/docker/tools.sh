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

function help {
  echo -e "Usage: $0"\\n
  echo -e "-c   --Clean tags"
  echo -e "-b   --Build images"
  echo -e "-p   --Push images"
  echo -e "-t   --Run tests"

  echo -e "-a   --Arch: amd64 or i386"
  echo -e "-n   --Name: focal, bionic, etc"
  echo -e "-g   --Postgres version"
}

while getopts ':chbpta::n::g::' o; do
case "$o" in
  a)  # set arch
      ARCHS=($OPTARG)
      ;;
  n)  # set release name
      UBUNTUS=($OPTARG)
      ;;
  g)  # set postgresql version
      POSTGRESQLS=($OPTARG)
      ;;

  b)  # build images
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

  *)  # print help
      help;;
  esac
done

shift $((OPTIND-1))

if [ -z "${s}" ] || [ -z "${p}" ]; then
  help
fi