#ifndef SOCKET_INPUT_H
#define SOCKET_INPUT_H

#include <string>
#include <linux/can.h>
#include "genericInput.h"
#include <chrono>

class SocketInput : public GenericInput{
public:
    // Sets socket name, create and attach socket
    // @param socketName name of input can device, like `can0`
    SocketInput(const std::string socketName);

    ~SocketInput();

    // Read first packet to set `time_start_offset_ms`
    // @param name interface/file/whatever name
    void initialize() override;

    // reads a single can frame, sets *frame to it's contents, and returns the time since instantiation in ms
    // @param frame output location for can bits
    // @param EOI unused
    double getPacket(can_frame * const frame, std::atomic<bool>& EOI) override;

private:
    // Stores the time of the first available packet, which should be thrown out. set by `initialize()`
    std::chrono::_V2::system_clock::time_point time_start_offset_chrono;

    // Stores name of input socket, like `can0`
    std::string socketName;

    // Allocated socket
    int soc;
};

#endif