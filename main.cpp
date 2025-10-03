
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

struct SignalTypeOrderTracker
{
    // The name of the CAN signal (e.g., "Engine_Speed")
    std::string signal_name; 
    
    // Arrow style type of the signal (will either be float, or int... but maybe we'll extend to treat bool as bool and not int in the future)
    parquet::Type::type arrow_type; 
};

// for using candump only
static inline void trim(std::string &s) {
    while (!s.empty() && (s.back() == '\r' || s.back() == '\n' || std::isspace((unsigned char)s.back())))
        s.pop_back();
    size_t i = 0;
    while (i < s.size() && std::isspace((unsigned char)s[i]))
        ++i;
    s.erase(0, i);
}

void parse_can_line(const std::string line, float& timestamp, can_frame& frame, bool& good) { 
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
        throw std::runtime_error("parse_can_line: unable to split line into 3 fields");
    }

    // now extract timestamp
    if (ts_block.size() < 2 || ts_block.front() != '(' || ts_block.back() != ')') {
        throw std::runtime_error("parse_can_line: bad timestamp format: " + ts_block);
    }
    std::string ts_str = ts_block.substr(1, ts_block.size() - 2);
    timestamp = std::stof(ts_str);
    
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

using ArrowSchemaList = std::vector<SignalTypeOrderTracker>;
static std::shared_ptr<arrow::io::FileOutputStream> outfile;

bool use_socketcan = true; // if false, read from candump file
int num_packets_to_read = 1000;
std::string dbc_filename = "fs.dbc";
std::string parquet_filename = "test.parquet";
std::string can_interface = "vcan0";

int main(int argc, char* argv[])
{
    // process arguments
    if(argc < 1){
        std::cout << "you must provide at least a dbc file name... \n dbcparquetdecoder file.dbc [of output.parquet] [if vcan0] [socket|file] \n \"if\" is used for the interface name in socket mode and the file name in file mode \n";
    }

    int arg = 2;
    while (arg < argc){
        if(std::strcmp(argv[arg], "of") == 0){
            if (arg + 1 >= argc) {
                std::cerr << "Error: Missing filename for 'of' option.\n";
                return 1; // Or handle error appropriately
            }
            std::cout << "Got output file=" << argv[arg+1] << "\n";
            parquet_filename=argv[arg+1];
            arg++;

        } else if(std::strcmp(argv[arg], "if") == 0){
            if (arg + 1 >= argc) {
                std::cerr << "Error: Missing filename for 'of' option.\n";
                return 1; // Or handle error appropriately
            }
            std::cout << "Got input file / can interface=" << argv[arg+1] << "\n";
            can_interface=argv[arg+1];
            arg++;
        } else if(std::strcmp(argv[arg], "socket") == 0){
            std::cout << "Using SocketCan for input\n";
            use_socketcan = true;
        } else if(std::strcmp(argv[arg], "file") == 0){
            std::cout << "Using file for input\n";
            use_socketcan = false;
        } else {
            std::cout << "Incorrect argument " << argv[argc] << ". Example: \n dbcparquetdecoder file.dbc [of output.parquet] [if vcan0] [socket|file] \n \"if\" is used for the interface name in socket mode and the file name in file mode \n";
        }

        arg++;
    }

    // done processing arguments


    std::unique_ptr<dbcppp::INetwork> net;
    {
        std::ifstream idbc(dbc_filename.c_str());
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

    if(use_socketcan){
        // 1. Create socket
        s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
        if (s < 0) {
            std::cerr << "Error while opening socket: " << strerror(errno) << std::endl;
            return 1;
        }

        // 2. Locate the interface (e.g., can0)
        struct ifreq ifr {};
        std::strncpy(ifr.ifr_name, can_interface.c_str(), IFNAMSIZ - 1);
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
        infile = std::ifstream(can_interface.c_str());
    }

    // Build list of signals used to match up by index, and also to construct the schema
    ArrowSchemaList schema_fields;

    schema_fields.push_back(SignalTypeOrderTracker{"Time_ms", parquet::Type::type::FLOAT}); // First column is always timestamp

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
                if (sig_ref.Factor() == 1.0){
                    std::strncpy(type_name, "int  ", 5);
                    signal.arrow_type = parquet::Type::type::INT32;
                } else {
                    std::strncpy(type_name, "float", 5);
                    signal.arrow_type = parquet::Type::type::FLOAT;
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
        if(sig_ptr.arrow_type == parquet::Type::type::FLOAT){
            //std::cout << "Signal: " << sig_ptr.signal_name << " type: " << static_cast<int>(sig_ptr.arrow_type) << "\n";
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                sig_ptr.signal_name, parquet::Repetition::OPTIONAL, sig_ptr.arrow_type, parquet::ConvertedType::NONE));
        } else if(sig_ptr.arrow_type == parquet::Type::type::INT32){
            //std::cout << "Signal: " << sig_ptr.signal_name << " type: " << static_cast<int>(sig_ptr.arrow_type) << "\n";
            fields.push_back(parquet::schema::PrimitiveNode::Make(
                sig_ptr.signal_name, parquet::Repetition::OPTIONAL, sig_ptr.arrow_type, parquet::ConvertedType::INT_32));
        }

    }

    auto node = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));


    // Make stream writter thingie
    std::shared_ptr<arrow::io::FileOutputStream> outfile;

   PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open(parquet_filename));

    parquet::WriterProperties::Builder builder;
    parquet::StreamWriter os{
    parquet::ParquetFileWriter::Open(outfile, node, builder.build())};

    const auto start_time_point = std::chrono::high_resolution_clock::now();
    float start_time_s;

    can_frame fframe= {};

    // get time of first can packet
    if(!use_socketcan){
        float timestamp = 0.0f;
        std::string line;
        std::getline(infile, line);
        bool good = false;
        while (!good){
            parse_can_line(line, timestamp, fframe, good);
        }
        
        start_time_s = timestamp;
    }

    int num_packets_rx = 0;

    can_frame frame= {};

    while (1)
    {
        float rcv_time_ms;
        // receive meaningful data
        if(use_socketcan){
            int nbytes = read(s, &frame, sizeof(frame));

            // Get shitty system timestamp that will be off and is not race-condition safe
            auto now_time_point = std::chrono::high_resolution_clock::now();
            auto difference_duration = now_time_point - start_time_point;
            rcv_time_ms = std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(difference_duration).count();


            if (nbytes < 0) {
            std::cerr << "Read error: " << strerror(errno) << std::endl;
                break;
            }

            if (nbytes < sizeof(struct can_frame)) {
                std::cerr << "Incomplete CAN frame" << std::endl;
                continue;
            }
        } else {
            float timestamp = 0.0f;
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
        }

        
        auto iter = messages.find(frame.can_id);

        if (iter != messages.end())
        {
            // Create a row of empty values to be filled in.. no idea what monostate is but chatgpt said to use it
            std::vector<std::variant<std::monostate, float, int32_t>> row_values(schema_fields.size(), std::monostate{});

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
                        if (it->arrow_type == parquet::Type::type::FLOAT){
                            row_values[index] = static_cast<float>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->arrow_type == parquet::Type::type::INT32){
                            row_values[index] = static_cast<int32_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else {
                            std::cerr << "Unhandled arrow type for signal " << sig.Name() << "\n";
                        }
                    }

                }

            }
            // Output row_values to parquet stream writer
            int v = 1;
            os << rcv_time_ms;
            while(v < row_values.size()){
                const auto& value = row_values[v];
                //std::cout << "Value type: " << schema_fields[v].arrow_type << " Belonging to signal: " << schema_fields[v].signal_name << "\n";
                if (std::holds_alternative<std::monostate>(value)) {
                    os.SkipColumns(1);
                } else if (schema_fields[v].arrow_type == parquet::Type::type::FLOAT) {
                    os << std::get<float>(value);
                } else if (schema_fields[v].arrow_type == parquet::Type::type::INT32) {
                    os << std::get<int32_t>(value);
                }
                v++;
            }

            //std::cout << "---- ROW END ----\n";
            os << parquet::EndRow;
            //os << parquet::EndRowGroup;
            //std::cout << "--- WRITE END ---\n";
            num_packets_rx++;

            if (num_packets_rx % num_packets_to_read == 0){
                std::cout << "Received " << num_packets_rx << " packets\r" << std::flush << "\n";
                os << parquet::EndRowGroup;
                //std::cout << outfile->Flush() << "\n";
                //std::cout << std::flush;
                if(use_socketcan){
                    break;
                }
            }
        }
    }
    //close(s);
    //std::cout << std::flush;
}
