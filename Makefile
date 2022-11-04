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

# this file only exists to support pgxnclient

.PHONY: all generate build install installcheck format

all: build

generate:
	cmake -B build -DCMAKE_BUILD_TYPE=Release

build: generate
	cmake --build build

install: build
	cmake --install build --component h3-pg

installcheck: install
	ctest --output-on-failure --build-config Release

###########################################################################
# Extra CI testing targets
###########################################################################

format:
	pgindent
