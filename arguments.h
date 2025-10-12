#ifndef ARGUMENTS_H
#define ARGUMENTS_H

#include <iostream>
#include <cstring>
#include <cerrno>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>




struct CommandLineArugments {
    bool use_socketcan = true; // if false, read from candump file
    int num_packets_to_read = 1000;
    std::string dbc_filename = "fs.dbc";
    std::string parquet_filename = "test.parquet";
    std::string can_interface = "vcan0";
    double cache_ms = 0;
};

CommandLineArugments parse_cli_arguments(int argc, char* argv[]){
    CommandLineArugments args_out;
    if(argc < 1){
        std::cout << "you must provide at least a dbc file name... \n ./decoder file.dbc [-of output.parquet] [-if vcan0] [-socket|-file] [-cache 10] \n \"if\" is used for the interface name in socket mode and the file name in file mode \n";
    }

    int arg = 2;
    while (arg < argc){
        if(std::strcmp(argv[arg], "-of") == 0){
            if (arg + 1 >= argc) {
                std::cerr << "Error: Missing filename for 'of' option; test.parquet will be used\n";
            }
            std::cout << "Got output file=" << argv[arg+1] << "\n";
            args_out.parquet_filename=argv[arg+1];
            arg++;

        } else if(std::strcmp(argv[arg], "-if") == 0){
            if (arg + 1 >= argc) {
                std::cerr << "Error: Missing filename for 'if' option; File name will be vcan0\n";
            }
            std::cout << "Got input file / can interface=" << argv[arg+1] << "\n";
            args_out.can_interface=argv[arg+1];
            arg++;
        } else if(std::strcmp(argv[arg], "-socket") == 0){
            std::cout << "Using SocketCan for input\n";
            args_out.use_socketcan = true;
        } else if(std::strcmp(argv[arg], "-file") == 0){
            std::cout << "Using file for input\n";
            args_out.use_socketcan = false;
        } else if(std::strcmp(argv[arg], "-cache") == 0)  {
            if (arg + 1 >= argc) {
                std::cerr << "Error: Missing caching period in ms; No caching active.\n";
            }
            std::cout << "Got caching period=" << argv[arg+1] << "ms\n";
            args_out.cache_ms = std::stod(argv[arg+1]);
            arg++;
        } else {
            std::cout << "Incorrect argument " << argv[argc] << ". Example: \n ./decoder file.dbc [-of output.parquet] [-if vcan0] [-socket|-file] [-cache 10] \n \"if\" is used for the interface name in socket mode and the file name in file mode \n";
        }

        arg++;
    }

    return args_out;
}

#endif