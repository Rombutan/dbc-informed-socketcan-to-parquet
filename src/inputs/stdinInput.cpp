#include <string>
#include "stdinInput.h"
#include <fstream>
#include <linux/can.h>
#include <stdio.h>
#include <cerrno>
#include <iostream>
#include <cstring>
#include <sstream>
#include "candump_parse.h"

StdinInput::StdinInput(const std::string fileName){}

void StdinInput::initialize(){
    can_frame fframe= {};
    double timestamp;
    bool good = false;
    while (!good){
        std::string line;
        std::getline(std::cin, line);
        parse_can_line(line, timestamp, fframe, good);
    }
    start_time_ms = timestamp*1000;
}

double StdinInput::getPacket(can_frame * const frame, std::atomic<bool>& EOI){
    double timestamp;
    bool good = false;
    while (!good){
        std::string line;
        std::getline(std::cin, line);
        parse_can_line(line, timestamp, *frame, good);
    }
    
    return ((timestamp*1000) - start_time_ms);
}

StdinInput::~StdinInput() {}
