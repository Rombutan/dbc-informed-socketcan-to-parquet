#ifndef STDIN_INPUT_H
#define STDIN_INPUT_H

#include <string>
#include "genericInput.h"
#include <fstream>
#include <linux/can.h>

class StdinInput : public GenericInput{
public:
    // @param fileName is thrown out
    StdinInput(const std::string fileName);

    ~StdinInput();

    // Read first packet to set `time_start_offset_ms`
    // @param name interface/file/whatever name
    void initialize() override;

    // reads a single can frame, sets *frame to it's contents, and returns the difference between it's and the first timestamp in ms
    // @param frame output location for can bits
    // @param EOI unused
    double getPacket(can_frame * const frame, std::atomic<bool>& EOI) override;

private:
    // Stores the time of the first available packet, which should be thrown out. set by `initialize()`
    double start_time_ms;
};

#endif