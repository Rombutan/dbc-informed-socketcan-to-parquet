#include <string>
#include "fileInput.h"
#include <fstream>
#include <linux/can.h>
#include <stdio.h>
#include <cerrno>
#include <iostream>
#include <cstring>
#include <sstream>
#include "candump_parse.h"

FileInput::FileInput(const std::string fileName): fileName(fileName){
    infile = std::ifstream(fileName.c_str());
}

void FileInput::initialize(){
    can_frame fframe= {};
    double timestamp;
    bool good = false;
    while (!good){
        std::string line;
        std::getline(infile, line);
        parse_can_line(line, timestamp, fframe, good);
    }
    start_time_ms = timestamp*1000;
}

double FileInput::getPacket(can_frame * const frame, std::atomic<bool>& EOI){
    double timestamp;
    bool good = false;
    int tries = 0;
    while (!good && tries < 5){
        std::string line;
        std::getline(infile, line);
        parse_can_line(line, timestamp, *frame, good);
        tries++;
    }
    EOI.store(!good); // EIO is actually `shouldExit`, which is inverted... this is a bit stupid
    return ((timestamp*1000) - start_time_ms);
}

FileInput::~FileInput() {}
