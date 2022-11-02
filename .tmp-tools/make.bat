cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
cmake --install build --config Release
ctest --test-dir build --output-on-failure --build-config Release
