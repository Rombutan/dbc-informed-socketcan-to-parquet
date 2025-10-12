#ifndef CANDUMP_PARSE_H
#define CANDUMP_PARSE_H

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
#include <fstream>
#include <sstream>
#include <unistd.h>

#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/can.h>
#include <linux/can/raw.h>

static inline void trim(std::string &s) {
    while (!s.empty() && (s.back() == '\r' || s.back() == '\n' || std::isspace((unsigned char)s.back())))
        s.pop_back();
    size_t i = 0;
    while (i < s.size() && std::isspace((unsigned char)s[i]))
        ++i;
    s.erase(0, i);
}

void parse_can_line(const std::string line, double& timestamp, can_frame& frame, bool& good) { 
    std::string clean = line;
    // trim trailing CR/LF
    while (!clean.empty() && (clean.back() == '\r' || clean.back() == '\n'))
        clean.pop_back();

    if (clean.empty()){
        good=false;
        std::cout << "emptey line in candump... trying next\n";
        return;
    }

    std::istringstream iss(clean);
    std::string ts_block, channel, iddata;

    if (!(iss >> ts_block >> channel >> iddata)) {
        std::cerr << "DEBUG: input=[" << clean << "]\n";
        std::cout <<("parse_can_line: unable to split line into 3 fields");
        good=false;
        return;
    }

    // now extract timestamp
    if (ts_block.size() < 2 || ts_block.front() != '(' || ts_block.back() != ')') {
        throw std::runtime_error("parse_can_line: bad timestamp format: " + ts_block);
    }
    std::string ts_str = ts_block.substr(1, ts_block.size() - 2);
    timestamp = std::stod(ts_str);
    //std::cout << "Timestamp String: " << ts_str << "\n";
    //std::cout << "Timestamp Parsed: " <<  std::stod(ts_str) << "\n";
    
    std::string id_data_payload = iddata;
    
    // Find the '#' separator
    size_t hash_pos = id_data_payload.find('#');
    if (hash_pos == std::string::npos) {
        std::cerr << "Error: Could not find '#' separator." << std::endl;
        return;
    }

    std::string id_str = id_data_payload.substr(0, hash_pos);
    std::string data_str = id_data_payload.substr(hash_pos + 1);

    // 3. Convert CAN ID (Hex String to Integer)
    // std::stoul converts string to unsigned long, using base 16 (hex)
    frame.can_id = std::stoul(id_str, nullptr, 16);

    // 4. Convert Data Payload (Hex String to Byte Array)
    size_t data_len = data_str.length() / 2; // Two hex characters per byte
    if (data_len > CAN_MAX_DLEN) {
        data_len = CAN_MAX_DLEN;
    }
    frame.len = (unsigned char)data_len;

    // Iterate through the data string, two characters at a time
    for (size_t i = 0; i < data_len; ++i) {
        std::string byte_str = data_str.substr(i * 2, 2);
        // std::stoul converts the two hex chars to a byte value
        frame.data[i] = static_cast<__u8>(std::stoul(byte_str, nullptr, 16));
    }

    //std::cout << "successfully parsed line with id: " << frame.can_id << "\n";
    good=true;
}

// needed to get timestamp of beginning of file
void peek_line(std::istream& is, std::string& line) {
    // 1. Get the current position of the input stream pointer
    // This marks the beginning of the line you're about to read.
    std::streampos current_pos = is.tellg();

    // 2. Read the line using the standard method (this advances the pointer)
    if (std::getline(is, line)) {
        // 3. Rewind the input stream pointer back to the saved position
        is.seekg(current_pos);
    } else {
        // Clear the line if reading failed (e.g., end of file)
        line.clear();
    }
}

#endif