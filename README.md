To build:
```
mkdir build
cmake -S . -B build/
cd build && make -j && cd ..
```

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
