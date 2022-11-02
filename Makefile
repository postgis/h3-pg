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

all: generate
	cmake --build build

generate:
	cmake -B build -DCMAKE_BUILD_TYPE=Release

install:
	cmake --install build --component h3-pg

installcheck:
	ctest --output-on-failure --build-config Release

.PHONY: cmake format

###########################################################################
# Extra CI testing targets
###########################################################################

format:
	pgindent
