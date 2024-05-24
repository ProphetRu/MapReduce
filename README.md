# Map  Reduce
Educational project with [googletest](https://github.com/google/googletest) and [doxygen](https://github.com/doxygen/doxygen)

## Build local Linux
```shell
sudo apt-get update && sudo apt-get install cmake libgtest-dev -y

cd MapReduce
mkdir build && cd build

cmake ..

# build release
cmake --build . --config Release

# build deb-package
cmake --build . --target package
```

## Build local Windows
```shell
vcpkg install gtest
vcpkg integrate install

cd MapReduce
mkdir build && cd build

cmake .. -DCMAKE_TOOLCHAIN_FILE="path/to/vcpkg/scripts/buildsystems/vcpkg.cmake"

# build release
cmake --build . --config Release
```
