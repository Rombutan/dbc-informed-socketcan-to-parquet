#ifndef TCPSTREAM_H
#define TCPSTREAM_H

#include <linux/can.h>
#include <linux/can/raw.h>

#pragma pack(push, 1)

struct CanTcpPacket {
    can_frame frame;
    double epoch;
    long int packetID;
};

#pragma pack(pop)

#endif