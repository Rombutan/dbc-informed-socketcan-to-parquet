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
./decoder file.dbc [-of output.parquet] [-if vcan0] [-socket|-file|-stdin] [-cache 10] [-forward-fill]
```
You must always provide a dbc file and it must be the first argument. The order of the others might matter, idk.

There are three input options:
- `-file` will read from the file specified by the `-if <>` argument. This file must be created in the candump format where each line is: `(<epoch in seconds>) <interface> <ID>#<Content>`. This is the default output of `candump -l`
- `-socket` will read from the socketcan socket specified by the `-if <>` argument.
- `-stdin` will read from stdin, expecting each line to be in the `candump -l` format. In this mode, `-if` is ignored and optional.

If you want to cache some period of messages into a single database row, use the `-cache` argument followed by a float or int with unit ms. The timestamp of each output row will be that of the first message which is cached into that row.

The `-forward-fill` option will not clear the cache after writting a row, so once a value is set by the recieve of a message with it's corresponding signal, it will not be set back to null, and will only reset if another message is recieved with that signal.

For Example:
```
./decoder fs.dbc -if can0 -of sep13data_run3.parquet -socket
```

If you wish to convert a candump file directly, use something similar to this example:
```
./decoder fs.dbc -if candump-sep13data_run3.log -of sep13data_run3.parquet -file
```

You can also record on another device using netcat, or anything else of your chosing (including candump in stdout mode for bettter(?) timestamping) by using the `-stdin` argument
```
nc -l 9000 | ./decodere fs.dbc -of test.parquet -cache 50 -stdin
```
