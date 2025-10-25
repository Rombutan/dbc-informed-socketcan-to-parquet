
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

#include <algorithm>

// In this project
#include "custom_types.h"
#include "arguments.h"
#include "candump_parse.h"
#include "influxupload.h"
#include "decoder.h"


// from uapi/linux/can.h
//struct can_frame
//{
//	canid_t    can_id;  /* 32 bit CAN_ID + EFF/RTR/ERR flags */
//	uint8_t    can_dlc; /* frame payload length in byte (0 .. CAN_MAX_DLEN) */
//	uint8_t    __pad;   /* padding */
//	uint8_t    __res0;  /* reserved / padding */
//	uint8_t    __res1;  /* reserved / padding */
//	uint8_t    data[8];
//};

static std::shared_ptr<arrow::io::FileOutputStream> outfile;

std::shared_ptr<arrow::Table> table; // For parquet input


int main(int argc, char* argv[])
{
    
    CommandLineArugments args = parse_cli_arguments(argc, argv);

    Decoder decoder(args.dbc_filename);

    
    int s; //need to instantiate here for scope reasons
    std::ifstream infile;

    if(args.input == SOCKETCAN){
        // 1. Create socket
        s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
        if (s < 0) {
            std::cerr << "Error while opening socket: " << strerror(errno) << std::endl;
            return 1;
        }

        // 2. Locate the interface (e.g., can0)
        struct ifreq ifr {};
        std::strncpy(ifr.ifr_name, args.can_interface.c_str(), IFNAMSIZ - 1);
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
    } else if (args.input == CANDUMP) {
        infile = std::ifstream(args.can_interface.c_str());
    } else if (args.input == PARQUET) {
        std::shared_ptr<arrow::io::ReadableFile> infile;
        PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(args.can_interface));

        // Create Parquet file reader
        std::unique_ptr<parquet::arrow::FileReader> parquet_reader;
        PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &parquet_reader)
        );

        // Read entire file into an Arrow Table
        PARQUET_THROW_NOT_OK(parquet_reader->ReadTable(&table));

        std::cout << "Read table with " << table->num_rows()
                << " rows and " << table->num_columns() << " columns.\n";

    }

    // Setup influx upload struct thingimajiger
    influxWriter.schema_fields = decoder.schema_fields;

    if(args.input == source::CANDUMP){
        influxWriter.table = "dacar";
    } else {
        influxWriter.table = "live";
    }
    
    influxWriter.tags.push_back("srcfile="+args.can_interface); // give tag of filename
    influxWriter.host = args.host;
    influxWriter.token = args.token;

    initInflux();

    // Build schema from signal list
    parquet::schema::NodeVector fields;

    for (const auto& sig_ptr : decoder.schema_fields)
    {
        fields.push_back(parquet::schema::PrimitiveNode::Make(
            sig_ptr.signal_name, parquet::Repetition::OPTIONAL, sig_ptr.arrow_type, parquet::ConvertedType::NONE));
    }

    auto node = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));


    // Make stream writter thingie
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

   PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(args.parquet_filename));

    parquet::WriterProperties::Builder builder;
    parquet::StreamWriter os{
        parquet::ParquetFileWriter::Open(outfile, node, builder.build())
    };

    const auto start_time_point = std::chrono::high_resolution_clock::now();
    double start_time_s;

    can_frame fframe= {};

    // get time of first can packet
    if(args.input == CANDUMP){
        double timestamp;
        std::string line;
        std::getline(infile, line);
        bool good = false;
        while (!good){
            parse_can_line(line, timestamp, fframe, good);
        }
        
        start_time_s = timestamp;
        //std::cout << "Start Time of Can Log (epoch): " << timestamp << "\n"; 
    } else if (args.input == STDIN){ // This does not currently work with nc
        double timestamp;
        std::string line;
        do {
            std::getline(std::cin, line);
            if (!std::cin) { // Handle failure/EOF
                break;
            }
        } while (line.empty());
        bool good = false;
        while (!good){
            parse_can_line(line, timestamp, fframe, good);
        }
    }

    can_frame frame= {};
    std::vector<DataTypeOrVoid> cache_object(decoder.schema_fields.size(), std::monostate{});
    double cache_start_ms = 0;

    while (1)
    {
        double rcv_time_ms;
        // receive meaningful data
        if(args.input == SOCKETCAN){
            int nbytes = read(s, &frame, sizeof(frame));

            // Get shitty system timestamp that will be off and is not race-condition safe
            auto now_time_point = std::chrono::high_resolution_clock::now();
            auto difference_duration = now_time_point - start_time_point;
            rcv_time_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(difference_duration).count();


            if (nbytes < 0) {
            std::cerr << "Read error: " << strerror(errno) << std::endl;
                break;
            }

            if (nbytes < sizeof(struct can_frame)) {
                std::cerr << "Incomplete CAN frame" << std::endl;
                continue;
            }
        } else if (args.input == CANDUMP){
            double timestamp;
            std::string line;
            std::getline(infile, line);
            bool good = false;

            int empteylinecounter = 0;
            while (!good && empteylinecounter < 11){
                parse_can_line(line, timestamp, frame, good);
                empteylinecounter++;
            }

            if(empteylinecounter > 10){
                std::cout << "Breaking for EOF\n";
                break;
            }

            rcv_time_ms = (timestamp-start_time_s)*1000;
            //std::cout << std::setprecision(20) << "Message Delta Timestamp: " << rcv_time_ms << "   Message abs Timestamp" << timestamp << "\n";

        } else if (args.input == STDIN){
            double timestamp;
            std::string line;
            do {
                std::getline(std::cin, line);
                if (!std::cin) { // Handle failure/EOF
                    break;
                }
            } while (line.empty());
            bool good = false;

            int empteylinecounter = 0;
            while (!good && empteylinecounter < 11){
                parse_can_line(line, timestamp, frame, good);
                empteylinecounter++;
            }

            if(empteylinecounter > 10){
                postRows();
                std::cout << "Breaking for EOF\n";
                break;
            }

            rcv_time_ms = (timestamp-start_time_s)*1000;
            //std::cout << std::setprecision(20) << "Message Delta Timestamp: " << rcv_time_ms << "   Message abs Timestamp" << timestamp << "\n";

        } else if (args.input == PARQUET){
            if(decoder.msg_count > table->num_rows()-1){
                break;
                // EOF of parquet
            }

            bool foundTime = false;
            for (int i = 0; i < table->num_columns(); ++i) {
                auto col = table->column(i);
                auto field = table->schema()->field(i);
                std::string name = field->name();

                // Get chunked array for this column
                auto chunked_array = col->chunk(0);
                if (decoder.msg_count < chunked_array->length()) {
                    auto scalar_result = chunked_array->GetScalar(decoder.msg_count);
                    if (scalar_result.ok()) {
                        auto scalar = scalar_result.ValueOrDie();

                        // Skip nulls — don't store in cache_object
                        if (!scalar->is_valid) {
                            continue;
                            std::cout << "_";
                        }

                        std::shared_ptr<arrow::DataType> arrow_type;

                        std::string name = field->name();

                        int v = find_index_by_name(decoder.schema_fields, name);

                        if (v >= 0) {
                            auto cast_result = scalar->CastTo(map_parquet_to_arrow(decoder.schema_fields[v].arrow_type));
                            if (cast_result.ok()) {
                                if(name == "Time" or name == "timestamp"){ // Support legacy format conversion of time
                                    v = find_index_by_name(decoder.schema_fields, "Time_ms");
                                    cache_object[v] = std::get<double>(scalar_to_variant(cast_result.ValueOrDie())) * 1000;
                                    rcv_time_ms = std::get<double>(scalar_to_variant(cast_result.ValueOrDie())) * 1000;
                                    foundTime = true;
                                    continue;
                                } else if (name == "Time_ms"){
                                    foundTime = true;
                                    rcv_time_ms = std::get<double>(scalar_to_variant(cast_result.ValueOrDie()));
                                }

                                cache_object[v] = scalar_to_variant(cast_result.ValueOrDie());

                                // EI SWEAR TO GOD IT DOES NOT WORK WITHOUT THIS. DON'T FUCKING TOUCH IT'S NOT THAT SERIOUS
                                bool bruh = *std::get_if<bool>(&cache_object[v]);
                                double bruhh = *std::get_if<double>(&cache_object[v]);
                                int bruhhh = *std::get_if<int32_t>(&cache_object[v]);
                                int bruhhhh = *std::get_if<int64_t>(&cache_object[v]);
                                float bruhhhhhh = *std::get_if<float>(&cache_object[v]);

                            } else {
                                std::cerr << "Failed to cast field " << name
                                        << " to " << map_parquet_to_arrow(decoder.schema_fields[v].arrow_type)->ToString()
                                        << ": " << cast_result.status().ToString() << "\n";
                            }
                            
                        } else {
                            std::cerr << "Warning: no matching field for name " << name << "\n";
                        }
                    }
                    
                }
            }
            if(!foundTime){
                cache_object[find_index_by_name(decoder.schema_fields, "Time_ms")] = static_cast<double>(decoder.msg_count * 11.91);
                rcv_time_ms = static_cast<double>(decoder.msg_count * 11.91);
            }
        }


        // ----------------------------- DECODE HERE ----------------------
        bool validrow = false;
        if(args.input == source::PARQUET){
            validrow = true;
            decoder.msg_count++;
        } else if(decoder.decode(frame, &cache_object)){
            validrow = true;
        }

        if(validrow){ // Do decode, and if the message matches something from dbc, do allat

            if (rcv_time_ms > cache_start_ms+args.cache_ms){
                cache_start_ms = rcv_time_ms;

                cache_object[0] = cache_start_ms;
                int v=0;
                while(v < cache_object.size()){
                    const auto& value = cache_object[v];
                    //std::cout << "Value type: " << schema_fields[v].arrow_type << " Belonging to signal: " << schema_fields[v].signal_name << "\n";
                    if (std::holds_alternative<std::monostate>(value)) {
                        os.SkipColumns(1);
                    } else if (decoder.schema_fields[v].arrow_type == parquet::Type::type::DOUBLE) {
                        os << std::get<double>(value);
                    } else if (decoder.schema_fields[v].arrow_type == parquet::Type::type::FLOAT) {
                        os << std::get<float>(value);
                    } else if (decoder.schema_fields[v].arrow_type == parquet::Type::type::INT32) {
                        os << std::get<int32_t>(value);
                    } else if (decoder.schema_fields[v].arrow_type == parquet::Type::type::INT64) {
                        os << std::get<int64_t>(value);
                    } else if (decoder.schema_fields[v].arrow_type == parquet::Type::type::INT96) {
                        std::cerr << "WARNING.. BIG INTS CURRENTLY UNHANDLED\n";
                        os << std::get<int64_t>(value);
                    } else if (decoder.schema_fields[v].arrow_type == parquet::Type::type::BOOLEAN) {
                        os << std::get<bool>(value);
                    } else {
                        std::cerr << "smth kerfuckedered\n";
                    }


                    v++;
                }
                os << parquet::EndRow;

                // influx upload
                if(args.host.size() > 2){
                    writeRow(cache_object);
                }
                


                // Live deocode
                int ldi = 0;
                while(ldi < args.live_decode_signals.size()){
                    int signal_index = find_index_by_name(decoder.schema_fields, args.live_decode_signals[ldi]);
                    if(signal_index > -1){
                        std::cout << decoder.schema_fields[signal_index].signal_name << ", ";
                        std::cout << variant_to_string(cache_object[signal_index]) << ", ";
                    }
                    ldi++;
                }
                if(ldi > 0){
                    std::cout << decoder.msg_count << "\n";
                }

                if(!args.forward_fill){
                    int d = 0;
                    while (d < cache_object.size()){
                        cache_object[d] = std::monostate{};
                        d++;
                    }
                }
            }

            if (decoder.msg_count % args.num_packets_to_read == 0){
                if(args.live_decode_signals.size() < 1){
                    std::cout << "Received " << decoder.msg_count << " packets\r" << std::flush << "\n";
                }
                os << parquet::EndRowGroup;

                if(args.host.size() > 2){
                    postRows();
                }
                

                if(args.input == SOCKETCAN){
                    break;
                }
            }
        }
    }
    //close(s);
    //std::cout << std::flush;
}
