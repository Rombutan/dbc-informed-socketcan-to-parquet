#ifndef FILE_INPUT_H
#define FILE_INPUT_H

#include <string>
#include "genericInput.h"
#include <fstream>
#include <linux/can.h>

class FileInput : public GenericInput{
public:
    // Sets socket name, create and attach socket
    // @param fileName name of input can device, like `test.log`
    FileInput(const std::string fileName);

    ~FileInput();

    // Read first packet to set `time_start_offset_ms`
    // @param name interface/file/whatever name
    void initialize() override;

    // reads a single can frame, sets *frame to it's contents, and returns the difference between it's and the first timestamp in ms
    // @param frame output location for can bits
    double getPacket(can_frame * const frame, std::atomic<bool>& EOI) override;

private:
    // Stores the time of the first available packet, which should be thrown out. set by `initialize()`
    double start_time_ms;

    // Stores name of input can device, like `test.log`
    std::string fileName;

    // File Object
    std::ifstream infile;
};

#endif