#ifndef GEN_INPUT_H
#define GEN_INPUT_H

#include <string>
#include <linux/can.h>
#include <atomic>
 

class GenericInput{
public:
    virtual ~GenericInput();

    // Reads first packet to set time, if necessary. Or anything else like that.
    // @param name interface/file/whatever name
    virtual void initialize(bool adjust_timestamp);

    // reads a single can frame, sets *frame to it's contents, and returns the time since instantiation in ms
    // @param frame output location for can bits
    // @param EOI set true if input has run out
    virtual double getPacket(can_frame * const frame, std::atomic<bool>& EOI);
};

#endif