# Build
```
mkdir build
cmake -S . -B build/
cd build && make -j && cd ..
```
# Setup
You must have a file called `fs.dbc` in the directory of the executable

It will attempt to open a socket and bind it to `vcan0`, so use ip tools to create that interface:
```
sudo ip link add name vcan0 type vcan
sudo ip link set dev vcan0 up
```

You will also need can-utils and a candump capture (or other way to put messages onto vcan0)
```
canplayer -i -v vcan0=can0 -I candump.log
```


# Usage
```
./decoder file.dbc file.dbc [of output.parquet] [if vcan0] [socket|file]
```
You must always provide a dbc file and it must be the first argument. The order of the others might matter, idk.

If you wish to sample from a live can interface or a replaying vcan interface as shown above, you must pass `socket` as an argument, and provide an `if` (can interface name like "can0" or "vcan2") and `of` (desired file name of parquet output)

For Example:
```
./decoder fs.dbc if can0 of sep13data_run3.parquet socket
```

If you wish to convert a candump file directly, use something similar to this example:
```
./decoder fs.dbc if candump-sep13data_run3.log of sep13data_run3.parquet file
```
