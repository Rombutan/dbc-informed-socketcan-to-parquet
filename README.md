# Build
I of all people do not know what system libraries this uses. Just read the cmake errors until it works :/

```
mkdir build
cmake -S . -B build/
cd build && make -j && cd ..
```
# Setup
You must have a dbc file in the directory of the executable

It will attempt to open a socket and bind it to `vcan0` (soon to be removed defualt), so use ip tools to create that interface:
```
sudo ip link add name vcan0 type vcan
sudo ip link set dev vcan0 up
```

You will also need a can-utils candump capture or live can bus and socketcan interface to it

If you wish to test socket mode using a candump capture you can invoke canplay on vcan0:
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
