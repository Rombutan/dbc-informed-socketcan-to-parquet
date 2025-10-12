
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

#include <chrono>

// In this project
#include "custom_types.h"
#include "arguments.h"
#include "candump_parse.h"


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

using ArrowSchemaList = std::vector<SignalTypeOrderTracker>;
static std::shared_ptr<arrow::io::FileOutputStream> outfile;

int main(int argc, char* argv[])
{
    
    CommandLineArugments args = parse_cli_arguments(argc, argv);

    std::unique_ptr<dbcppp::INetwork> net;
    {
        std::ifstream idbc(args.dbc_filename.c_str());
        net = dbcppp::INetwork::LoadDBCFromIs(idbc);
    }

    if (net.get() == nullptr) {
        std::cerr << "failed to parse dbc\n";
        return -1;
    }

    std::unordered_map<uint64_t, const dbcppp::IMessage *> messages;
    for (const dbcppp::IMessage& msg : net->Messages())
    {
        messages.insert(std::make_pair(msg.Id(), &msg));
    }
    

    
    int s; //need to instantiate here for scope reasons
    std::ifstream infile;

    if(args.use_socketcan){
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
    } else {
        infile = std::ifstream(args.can_interface.c_str());
    }

    // Build list of signals used to match up by index, and also to construct the schema
    ArrowSchemaList schema_fields;

    schema_fields.push_back(SignalTypeOrderTracker{"Time_ms", parquet::Type::type::DOUBLE}); // First column is always timestamp

    int i = 0;
    while (i < net->Messages_Size()){
        const dbcppp::IMessage& msg_ref = net->Messages_Get(i);
        //std::cout << "Message " << i << ": " << msg_ref.Name() << "\n";
        int n = 0;
        while(n<msg_ref.Signals_Size()){
            const dbcppp::ISignal& sig_ref = msg_ref.Signals_Get(n);
            dbcppp::ISignal::EExtendedValueType ev_type = sig_ref.ExtendedValueType();
            char type_name[6] = "-----";
            if(ev_type == dbcppp::ISignal::EExtendedValueType::Float){
                std::cerr << "Unhandled Extended Value Type " << strerror(errno) << std::endl;
            } else if (ev_type == dbcppp::ISignal::EExtendedValueType::Double){
                std::cerr << "Unhandled Extended Value Type " << strerror(errno) << std::endl;

            // The only useful case: where the set type in DBC is Integer (honestly not sure where this comes from since it's not in the DBC spec..)
            } else if (ev_type == dbcppp::ISignal::EExtendedValueType::Integer){
                SignalTypeOrderTracker signal;
                signal.signal_name = sig_ref.Name();
                if (sig_ref.BitSize() == 1){
                    std::strncpy(type_name, "bool ", 5);
                    signal.arrow_type = parquet::Type::type::BOOLEAN;
                } else if (sig_ref.Factor() < 1.0001 && sig_ref.Factor() > 9.9999){
                    std::strncpy(type_name, "int  ", 5);
                    signal.arrow_type = parquet::Type::type::INT32;
                    if (sig_ref.BitSize() > 30){
                        signal.arrow_type = parquet::Type::type::INT64;
                    } else if (sig_ref.BitSize() > 62){
                        signal.arrow_type = parquet::Type::type::INT96;
                    }
                } else {
                    std::strncpy(type_name, "float", 5);
                    signal.arrow_type = parquet::Type::type::DOUBLE;
                    if (sig_ref.BitSize() < 32){
                        signal.arrow_type = parquet::Type::type::FLOAT;
                    }
                }
                schema_fields.push_back(std::move(signal));


            } else {
                std::cerr << "Unhandled Extended Value Type " << strerror(errno) << std::endl;
            }
            //std::cout << "\tSignal " << n << ": " << sig_ref.Name() << " type: " << type_name << "\n";
            n++;
        }

        i++;
    }

    // Build schema from signal list
    parquet::schema::NodeVector fields;

    for (const auto& sig_ptr : schema_fields)
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
    if(!args.use_socketcan){
        double timestamp;
        std::string line;
        std::getline(infile, line);
        bool good = false;
        while (!good){
            parse_can_line(line, timestamp, fframe, good);
        }
        
        start_time_s = timestamp;
        //std::cout << "Start Time of Can Log (epoch): " << timestamp << "\n"; 
    }

    int num_packets_rx = 0;

    can_frame frame= {};

    std::vector<DataTypeOrVoid> cache_object;
    cache_object.reserve(schema_fields.size());
    double cache_start_ms = 0;

    while (1)
    {
        double rcv_time_ms;
        // receive meaningful data
        if(args.use_socketcan){
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
        } else {
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

        }

        
        auto iter = messages.find(frame.can_id);

        if (iter != messages.end())
        {
            // Create a row of empty values to be filled in.. no idea what monostate is but chatgpt said to use it
            std::vector<DataTypeOrVoid> row_values(schema_fields.size(), std::monostate{});

            const dbcppp::IMessage* msg = iter->second;
            //std::cout << "Received Message: " << msg->Name() << "\n";
            for (const dbcppp::ISignal& sig : msg->Signals())
            {
                const dbcppp::ISignal* mux_sig = msg->MuxSignal();
                if (sig.MultiplexerIndicator() != dbcppp::ISignal::EMultiplexer::MuxValue ||
                    (mux_sig && mux_sig->Decode(frame.data) == sig.MultiplexerSwitchValue()))
                {
                    //std::cout << "\t" << sig.Name() << "=" << sig.RawToPhys(sig.Decode(frame.data)) << sig.Unit() << "\n";
                    // Find the index of this signal in the schema list
                    auto it = std::find_if(schema_fields.begin(), schema_fields.end(),
                        [&sig](const SignalTypeOrderTracker& tracker) { return tracker.signal_name == sig.Name(); });
                    if (it != schema_fields.end()){
                        //std::cout << "Found signal " << sig.Name() << " in schema at index " << std::distance(schema_fields.begin(), it) << " With type: " << it->arrow_type << "\n";
                        int index = std::distance(schema_fields.begin(), it);
                        // Set the value in the row_values based on the type
                        if (it->arrow_type == parquet::Type::type::DOUBLE){
                            row_values[index] = static_cast<double>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->arrow_type == parquet::Type::type::FLOAT){
                            row_values[index] = static_cast<float>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->arrow_type == parquet::Type::type::INT32){
                            row_values[index] = static_cast<int32_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->arrow_type == parquet::Type::type::INT64){
                            row_values[index] = static_cast<int64_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->arrow_type == parquet::Type::type::INT96){
                            row_values[index] = static_cast<__int128_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->arrow_type == parquet::Type::type::BOOLEAN){
                            row_values[index] = static_cast<bool>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else {
                            std::cerr << "Unhandled arrow type for signal " << sig.Name() << "\n";
                        }
                    }

                }

            }
            // Output row_values to parquet stream writer
            int v = 1;
            if(args.cache_ms < 0.1){
                os << rcv_time_ms;
                while(v < row_values.size()){
                    const auto& value = row_values[v];
                    //std::cout << "Value type: " << schema_fields[v].arrow_type << " Belonging to signal: " << schema_fields[v].signal_name << "\n";
                    if (std::holds_alternative<std::monostate>(value)) {
                        os.SkipColumns(1);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::DOUBLE) {
                        os << std::get<double>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::FLOAT) {
                        os << std::get<float>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::INT32) {
                        os << std::get<int32_t>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::INT64) {
                        os << std::get<int64_t>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::INT96) {
                        std::cerr << "WARNING.. BIG INTS CURRENTLY UNHANDLED\n";
                        os << std::get<int64_t>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::BOOLEAN) {
                        os << std::get<bool>(value);
                    } else {
                        std::cerr << "smth kerfuckedered\n";
                    }
                    v++;
                }
                os << parquet::EndRow;
            } else {
                v = 1;
                while(v < row_values.size()){
                    const auto& value = row_values[v];
                    if (std::holds_alternative<std::monostate>(value)) {
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::DOUBLE) {
                        cache_object[v] = std::get<double>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::FLOAT) {
                        cache_object[v] = std::get<float>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::INT32) {
                        cache_object[v] = std::get<int32_t>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::INT64) {
                        cache_object[v] = std::get<int32_t>(value);
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::INT96) {
                        cache_object[v] = std::get<int64_t>(value);
                        std::cerr << "WARNING.. BIG INTS CURRENTLY UNHANDLED\n";
                    } else if (schema_fields[v].arrow_type == parquet::Type::type::BOOLEAN) {
                        cache_object[v] = std::get<bool>(value);
                    } else {
                        std::cerr << "smth kerfuckedered mk2\n";
                    }
                    v++;
                }

                // Output cached row
                if (rcv_time_ms > cache_start_ms+args.cache_ms){
                
                    
                    cache_start_ms = rcv_time_ms;
                    v=1;
                    os << cache_start_ms;
                    while(v < row_values.size()){
                        const auto& value = cache_object[v];
                        //std::cout << "Value type: " << schema_fields[v].arrow_type << " Belonging to signal: " << schema_fields[v].signal_name << "\n";
                        if (std::holds_alternative<std::monostate>(value)) {
                        os.SkipColumns(1);
                        } else if (schema_fields[v].arrow_type == parquet::Type::type::DOUBLE) {
                            os << std::get<double>(value);
                        } else if (schema_fields[v].arrow_type == parquet::Type::type::FLOAT) {
                            os << std::get<float>(value);
                        } else if (schema_fields[v].arrow_type == parquet::Type::type::INT32) {
                            os << std::get<int32_t>(value);
                        } else if (schema_fields[v].arrow_type == parquet::Type::type::INT64) {
                            os << std::get<int64_t>(value);
                        } else if (schema_fields[v].arrow_type == parquet::Type::type::INT96) {
                            std::cerr << "WARNING.. BIG INTS CURRENTLY UNHANDLED\n";
                            os << std::get<int64_t>(value);
                        } else if (schema_fields[v].arrow_type == parquet::Type::type::BOOLEAN) {
                            os << std::get<bool>(value);
                        } else {
                            std::cerr << "smth kerfuckedered\n";
                        }
                        v++;
                    }
                    os << parquet::EndRow;
                }
            }

            //std::cout << "---- ROW END ----\n";
            //os << parquet::EndRowGroup;
            //std::cout << "--- WRITE END ---\n";
            num_packets_rx++;

            if (num_packets_rx % args.num_packets_to_read == 0){
                std::cout << "Received " << num_packets_rx << " packets\r" << std::flush << "\n";
                os << parquet::EndRowGroup;
                //std::cout << outfile->Flush() << "\n";
                //std::cout << std::flush;
                if(args.use_socketcan){
                    break;
                }
            }
        }
    }
    //close(s);
    //std::cout << std::flush;
}
