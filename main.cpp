
#include <fstream>
#include <unordered_map>

#include "dbcppp/CApi.h"
#include "dbcppp/Network.h"

#include <iostream>
#include <cstring>
#include <cerrno>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/can.h>
#include <linux/can/raw.h>

// from uapi/linux/can.h
using canid_t = uint32_t;
//struct can_frame
//{
//	canid_t    can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
//	uint8_t    can_dlc; /* frame payload length in byte (0 .. CAN_MAX_DLEN) */
//	uint8_t    __pad;   /* padding */
//	uint8_t    __res0;  /* reserved / padding */
//	uint8_t    __res1;  /* reserved / padding */
//	uint8_t    data[8];
//};

int main()
{
    std::unique_ptr<dbcppp::INetwork> net;
    {
        std::ifstream idbc("fs.dbc");
        net = dbcppp::INetwork::LoadDBCFromIs(idbc);
    }

    if (net.get() == nullptr) {
        std::cerr << "failed to parse dbc!\n";
        return -1;
    }

    std::unordered_map<uint64_t, const dbcppp::IMessage *> messages;
    for (const dbcppp::IMessage& msg : net->Messages())
    {
        messages.insert(std::make_pair(msg.Id(), &msg));
    }
    can_frame frame;

    // 1. Create socket
    int s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if (s < 0) {
        std::cerr << "Error while opening socket: " << strerror(errno) << std::endl;
        return 1;
    }

    // 2. Locate the interface (e.g., can0)
    struct ifreq ifr {};
    std::strncpy(ifr.ifr_name, "vcan0", IFNAMSIZ - 1);
    if (ioctl(s, SIOCGIFINDEX, &ifr) < 0) {
        std::cerr << "Error getting interface index: " << strerror(errno) << std::endl;
        return 1;
    }

    // 3. Bind the socket to the CAN interface
    struct sockaddr_can addr {};
    addr.can_family = AF_CAN;
    addr.can_ifindex = ifr.ifr_ifindex;
    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        std::cerr << "Error in socket bind: " << strerror(errno) << std::endl;
        return 1;
    }

    //struct can_frame frame {};

    while (1)
    {
        // receive meaningful data
        int nbytes = read(s, &frame, sizeof(frame));
        if (nbytes < 0) {
	    std::cerr << "Read error: " << strerror(errno) << std::endl;
            break;
        }

        if (nbytes < sizeof(struct can_frame)) {
            std::cerr << "Incomplete CAN frame" << std::endl;
            continue;
        }

        auto iter = messages.find(frame.can_id);
        if (iter != messages.end())
        {
            const dbcppp::IMessage* msg = iter->second;
            std::cout << "Received Message: " << msg->Name() << "\n";
            for (const dbcppp::ISignal& sig : msg->Signals())
            {
                const dbcppp::ISignal* mux_sig = msg->MuxSignal();
                if (sig.MultiplexerIndicator() != dbcppp::ISignal::EMultiplexer::MuxValue ||
                    (mux_sig && mux_sig->Decode(frame.data) == sig.MultiplexerSwitchValue()))
                {
                    std::cout << "\t" << sig.Name() << "=" << sig.RawToPhys(sig.Decode(frame.data)) << sig.Unit() << "\n";
                }
            }
        }
    }
    close(s);
    std::cout << std::flush;
}
