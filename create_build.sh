mkdir -p build
cd build
rm -rf ./*
cmake ..
make -j 4
