#include "socketInput.h"
#include <cerrno>
#include <iostream>
#include <string.h>
#include <cstring>
#include <net/if.h>
#include <chrono>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include <iomanip>
#include <unistd.h>


SocketInput::SocketInput(const std::string socketName): socketName(socketName){
    // 1. Create socket
    soc = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if (soc < 0) {
        std::cerr << "Error while opening socket: " << strerror(errno) << std::endl;
    }

    // 2. Locate the interface (e.g., can0)
    struct ifreq ifr {};
    std::strncpy(ifr.ifr_name, socketName.c_str(), IFNAMSIZ - 1);
    if (ioctl(soc, SIOCGIFINDEX, &ifr) < 0) {
        std::cerr << "Error getting interface index: " << strerror(errno) << std::endl;
    }

    // 3. Bind the socket to the CAN interface
    struct sockaddr_can addr {};
    addr.can_family = AF_CAN;
    addr.can_ifindex = ifr.ifr_ifindex;
    if (bind(soc, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        std::cerr << "Error in socket bind: " << strerror(errno) << std::endl;
    }
}

void SocketInput::initialize(){
    time_start_offset_chrono = std::chrono::high_resolution_clock::now();
}

double SocketInput::getPacket(can_frame * const frame, std::atomic<bool>& EOI){
    int nbytes = read(soc, frame, sizeof(*frame));

    // Get shitty system timestamp
    auto now_time_point = std::chrono::high_resolution_clock::now();
    auto difference_duration = now_time_point - time_start_offset_chrono;
    double rcv_time_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(difference_duration).count();

    if (nbytes < 0) {
        std::cerr << "Read error: " << strerror(errno) << std::endl;
    }

    if (nbytes < sizeof(struct can_frame)) {
        std::cerr << "Incomplete CAN frame" << std::endl;
    }

    return rcv_time_ms;
}

SocketInput::~SocketInput() {}
