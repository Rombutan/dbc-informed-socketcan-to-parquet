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

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "parquet/arrow/writer.h"
#include "parquet/api/writer.h"
#include "parquet/stream_writer.h"
#include "arrow/io/file.h"

#include <parquet/arrow/reader.h>
#include <memory>

#include <chrono>
#include <atomic>

#include <algorithm>

// In this project
#include "custom_types.h"
#include "arguments.h"
#include "influxupload.h"
#include "decoder.h"

#include "inputs/genericInput.h"
#include "inputs/fileInput.h"
#include "inputs/parquetInput.h"
#include "inputs/socketInput.h"
#include "inputs/stdinInput.h"

#include "writeparquet.h"

// Should the program exit?
std::atomic<bool> shouldExit;

int main(int argc, char* argv[])
{
    // CLI arguments. All behavioral logic should come from this struct
    CommandLineArugments args = parse_cli_arguments(argc, argv);

    // Parse and generate 
    Decoder decoder(args.dbc_filename);

    // ----------------- Setup Input --------------------
    std::unique_ptr<GenericInput> input;

    if(args.input == SOCKETCAN){
        input = std::make_unique<SocketInput>(args.can_interface);
        signal(SIGINT, [](int){shouldExit.store(true);});
    } else if (args.input == CANDUMP) {
        input = std::make_unique<FileInput>(args.can_interface);
    } else if (args.input == STDIN) {
        input = std::make_unique<StdinInput>(args.can_interface);
        signal(SIGINT, [](int){shouldExit.store(true);});
    }

    // ----------------- Setup Database (If en) --------------------


    
    // In-process table, stores all rows exactly as they will be written out
    std::vector<std::vector<DataTypeOrVoid>> rowsIP;

    // Current row, only "complete" when `rowRecentMs - rowStartMs > args.cache_ms`
    std::vector<DataTypeOrVoid> curRow(decoder.schema_fields.size(), std::monostate{});

    // Timestamp of most recent row, relative to start of log/program
    double rowRecentMs = 0;

    // Beginning of recording period for curRow, will always be <= the time of the first message which fills that row, relative to start of log/program
    double rowStartMs = 0;

    // Number of messages processed
    int messages = 0;

    shouldExit.store(false);
    while(!shouldExit.load()){
        can_frame frame;
        rowRecentMs = input->getPacket(&frame, shouldExit);
        decoder.decode(frame, &curRow);

        if(rowRecentMs - rowStartMs > args.cache_ms){ // Finish Single Row
            curRow[find_index_by_name(decoder.schema_fields, "Time_ms")] = rowStartMs;
            rowStartMs = rowRecentMs;
            rowsIP.push_back(curRow); // Add current row to in-process table
            
            if(!args.forward_fill){ // If forward fill is disabled, reset curRow to monostates/nulls
                int d = 0;
                while (d < curRow.size()){
                    curRow[d] = std::monostate{};
                    d++;
                }
            }
        }
        messages++;
    }

    // ----------------- Build Schema --------------------

    std::vector<std::shared_ptr<arrow::Field>> fields;

    std::vector<arrow::Array> arrays;
    for (const auto& sig_ptr : decoder.schema_fields)
    {
        fields.push_back(arrow::field(sig_ptr.signal_name, sig_ptr.arrow_datatype));
    }

    // Arrow schema for export
    auto schema = arrow::schema(fields);

    auto schema_names = schema->field_names(); 
    auto table = BuildArrowTable(rowsIP, schema);
    WriteParquet(table, "output.parquet");

    std::cout << "Wrote output.parquet\n";
    return 0;
}