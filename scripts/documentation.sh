#!/usr/bin/env bash

# ensure we are in script dir
cd "$(dirname "$0")"

# go to python project dir
cd documentation

# ensure dependencies are present
poetry install

# generate markdown from sql files
poetry run -q -- python generate.py "../../h3/sql/install/*.sql" > "../../docs/api.md"

# include table of contents
npx doctoc "../../docs/api.md"
