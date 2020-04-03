Library to offload to data to burst buffers
-------------------------------------------

    Using _LD_PRELOAD_ to automatically stage in and out workflow data
    to burst buffers asynchronously on Cray DataWarp architectures.

On macOS: DYLD_INSERT_LIBRARIES=libstagebb.dylib DYLD_FORCE_FLAT_NAMESPACE=1 ./main
On Linux: LD_PRELOAD=libstagebb.so ./main

