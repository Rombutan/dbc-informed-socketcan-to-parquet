#ifndef TCPSTREAM_H
#define TCPSTREAM_H

#include <linux/can.h>
#include <linux/can/raw.h>

struct CanTcpPacket {
    can_frame frame;
    double epoch;
    long int packetID;
};

#endif