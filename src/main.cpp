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

#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <stdexcept>

#include <httplib.h>

#include <parquet/arrow/reader.h>
#include <memory>

#include <chrono>
#include <atomic>

#include <algorithm>

// In this project
#include "custom_types.h"
#include "arguments.h"
#include "decoder.h"

#include "inputs/genericInput.h"
#include "inputs/fileInput.h"
// #include "inputs/parquetInput.h"
#include "inputs/socketInput.h"
#include "inputs/stdinInput.h"

#include "writeparquet.h"

// Should the program exit?
std::atomic<bool> shouldExit;

std::unique_ptr<parquet::arrow::FileWriter> writer;
std::shared_ptr<arrow::io::FileOutputStream> outfile;


int main(int argc, char* argv[])
{
    // CLI arguments. All behavioral logic should come from this struct
    CommandLineArugments args = parse_cli_arguments(argc, argv);

    // Parse and generate 
    Decoder decoder(args.dbc_filename);

    // ----------------- Setup Input --------------------
    std::unique_ptr<GenericInput> input;
//    ParquetInput pqInput;

    if(args.input == SOCKETCAN){
        input = std::make_unique<SocketInput>(args.can_interface);
        signal(SIGINT, [](int){shouldExit.store(true);});
    } else if (args.input == CANDUMP) {
        input = std::make_unique<FileInput>(args.can_interface);
        signal(SIGINT, [](int){shouldExit.store(true);});
    } else if (args.input == STDIN) {
        input = std::make_unique<StdinInput>(args.can_interface);
        signal(SIGINT, [](int){shouldExit.store(true);});
    }

    // ----------------- Setup Database (If en) --------------------


    // ----------------- Build Schema --------------------

    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (const auto& sig_ptr : decoder.schema_fields)
    {
        fields.push_back(arrow::field(sig_ptr.signal_name, sig_ptr.arrow_datatype, true));
    }

    // Arrow schema for export
    auto schema = arrow::schema(fields);
    auto builders = CreateBuildersFromSchema(schema);

    httplib::Client client("http://localhost:8123");

    // Database initial setup stuff
    if (args.host.length() > 2){
        client = httplib::Client(args.host);

        client.set_keep_alive(true);

        // Standard ClickHouse authentication
        client.set_default_headers({
            {"Connection", "keep-alive"},
            {"X-ClickHouse-User",     args.clickhouse_user},
            {"X-ClickHouse-Key",      args.clickhouse_password},
            {"Content-Type",          "application/octet-stream"},
            {"Expect", ""}
        });

        // This tries to disable system TCP caching...
        client.set_tcp_nodelay(true);
    }

    // Init input source
    input->initialize(args.adjust_timestamp);

    // Most recent values (for live decode only)
    std::vector<ValueVariant> lastValues(decoder.schema_fields.size(), std::monostate{});

    // Timestamp of most recent row, relative to start of log/program
    double rowRecentMs = 0;

    // Beginning of recording period for curRow, will always be <= the time of the first message which fills that row, relative to start of log/program
    double rowStartMs = 0;

    // Number of messages processed
    int messages = 0;

    // Number of rows outputted (resets after each db write / output package)
    int rows = 0;

    // Flag to add columns to the database (if enabled) only on the first write
    bool need_to_add_columns = true;

    shouldExit.store(false);
    while(!shouldExit.load()){
        can_frame frame;
        rowRecentMs = input->getPacket(&frame, shouldExit);
        decoder.decode(frame, builders, rows, lastValues);

        if(rowRecentMs - rowStartMs > args.cache_ms){ // Finish Single Row
            SetValueAt(builders, find_index_by_name(decoder.schema_fields, "Time_ms"), rowStartMs, rows, lastValues);
            rowStartMs = rowRecentMs;
            rows++; // Add current row to in-process table

            // Live deocode
            int ldi = 0;
            while(ldi < args.live_decode_signals.size()){
                int signal_index = find_index_by_name(decoder.schema_fields, args.live_decode_signals[ldi]);
                if(signal_index > -1){
                    std::cout << decoder.schema_fields[signal_index].signal_name << ", ";
                    std::cout << variant_to_string(lastValues[signal_index]) << ", ";
                }
                ldi++;
            }
            if(ldi > 0){
                std::cout << decoder.msg_count << "\n";
            }
            
            if(args.forward_fill){ // If forward fill is disabled, reset curRow to monostates/nulls
                std::cout << "FORWARD FILL NOT WRITTEN\n";
            }

            if((rows % args.cache_rows) == 0 && rows >= args.cache_rows){            
                auto table_res = FinishTable(schema, builders);
                auto table = table_res.ValueOrDie();
                
                for (auto& builder : builders) {
                    builder->Reset();
                }
                rows = 0;

                if(args.parquet_filename.length() > 2){
                    auto st = AppendTableToParquet(table, args.parquet_filename, writer, outfile);
                }

                // Clickhouse Write
                if(args.host.size() > 2){
                    auto output = arrow::io::BufferOutputStream::Create().ValueOrDie();

                    auto writer =
                        arrow::ipc::MakeStreamWriter(output, schema).ValueOrDie();

                    auto status = writer->WriteTable(*table);
                    if (!status.ok()) {
                        throw std::runtime_error(status.ToString());
                    }

                    status = writer->Close();
                    if (!status.ok()) {
                        throw std::runtime_error(status.ToString());
                    }

                    auto buffer = output->Finish().ValueOrDie();

                    std::string arrow_payload(
                        reinterpret_cast<const char*>(buffer->data()),
                        buffer->size()
                    );

                    if(need_to_add_columns){
                        std::ostringstream query;

                        query << "ALTER TABLE default.test_arrow ";

                        bool first = true;

                        for (const auto& field : decoder.schema_fields) {
                            auto type_id = field.arrow_datatype->id();

                            auto it = arrow_to_ch_type.find(type_id);
                            if (it == arrow_to_ch_type.end()) {
                                throw std::runtime_error(
                                    "Unsupported Arrow type for column: " + field.signal_name
                                );
                            }

                            std::string ch_type = it->second;

                            if (!first) {
                                query << ", ";
                            }
                            first = false;

                            ch_type = "Nullable(" + ch_type + ")";

                            query << "ADD COLUMN IF NOT EXISTS "
                                << field.signal_name << " "
                                << ch_type;

                            need_to_add_columns = false;
                        }

                        std::cout << query.str() << "\n";

                        auto res = client.Post(
                            "/",
                            query.str(),
                            "application/octet-stream"
                        );
                        if (!res) {
                            throw std::runtime_error("HTTP request failed");
                        }

                        if (res->status != 200) {
                            std::cout << res->body << "\n";
                            throw std::runtime_error(
                                "ClickHouse error: HTTP " + std::to_string(res->status) +
                                "\n" + res->body
                            );
                        }
                    }
                    auto start = std::chrono::high_resolution_clock::now();
                    auto res = client.Post(
                        "/?query=INSERT INTO default.test_arrow FORMAT ArrowStream&async_insert=1&wait_for_async_insert=0&insert_null_as_default=0&input_format_arrow_import_nested=1",
                        arrow_payload,
                        "application/octet-stream"
                    );
                    auto end = std::chrono::high_resolution_clock::now();
                    std::cout << "Transmission time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms\n";

                    if (!res) {
                        throw std::runtime_error("HTTP request failed");
                    }

                    if (res->status != 200) {
                        std::cout << res->body << "\n";
                        throw std::runtime_error(
                            "ClickHouse error: HTTP " + std::to_string(res->status) +
                            "\n" + res->body
                        );
                    }
                }
                std::cout << "Processed: " << messages << " messages.\n";
            }
        }
        messages++;
    }

    auto table_res = FinishTable(schema, builders);
    auto table = table_res.ValueOrDie();
    auto st = AppendTableToParquet(table, args.parquet_filename, writer, outfile);
    std::cerr << st.ToString() << std::endl;
    writer->Close();
    outfile->Close();

    std::cout << "Wrote " << args.parquet_filename << std::endl;
    return 0;
}

