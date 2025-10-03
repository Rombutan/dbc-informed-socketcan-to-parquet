
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
#include <unistd.h>

#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <linux/can.h>
#include <linux/can/raw.h>

#ifndef SIOCGSTAMP
#define SIOCGSTAMP 0x8906
#endif

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

using ArrowSchemaList = std::vector<SignalTypeOrderTracker>;
static std::shared_ptr<arrow::io::FileOutputStream> outfile;


int main()
{
    std::unique_ptr<dbcppp::INetwork> net;
    {
        std::ifstream idbc("fs.dbc");
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
    can_frame frame;

    // 1. Create socket
    int s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if (s < 0) {
        std::cerr << "Error while opening socket: " << strerror(errno) << std::endl;
        return 1;
    }

    // 2. Locate the interface (e.g., can0)
    struct ifreq ifr {};
    std::strncpy(ifr.ifr_name, "vcan0", IFNAMSIZ - 1);
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

    // Build list of signals used to match up by index, and also to construct the schema
    ArrowSchemaList schema_fields;

    schema_fields.push_back(SignalTypeOrderTracker{"Time_ms", parquet::Type::type::FLOAT}); // First column is always timestamp

    int i = 0;
    while (i < net->Messages_Size()){
        const dbcppp::IMessage& msg_ref = net->Messages_Get(i);
        std::cout << "Message " << i << ": " << msg_ref.Name() << "\n";
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
            std::cout << "\tSignal " << n << ": " << sig_ref.Name() << " type: " << type_name << "\n";
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
      arrow::io::FileOutputStream::Open("test.parquet"));

    parquet::WriterProperties::Builder builder;
    parquet::StreamWriter os{
      parquet::ParquetFileWriter::Open(outfile, node, builder.build())};

    const auto start_time_point = std::chrono::high_resolution_clock::now();

    int num_packets_rx = 0;

    while (1)
    {
        // receive meaningful data
        int nbytes = read(s, &frame, sizeof(frame));

        // Get shitty system timestamp that will be off and is not race-condition safe
        auto now_time_point = std::chrono::high_resolution_clock::now();
        auto difference_duration = now_time_point - start_time_point;
        float rcv_time_ms = std::chrono::duration_cast<std::chrono::duration<float, std::milli>>(difference_duration).count();


        if (nbytes < 0) {
	    std::cerr << "Read error: " << strerror(errno) << std::endl;
            break;
        }

        if (nbytes < sizeof(struct can_frame)) {
            std::cerr << "Incomplete CAN frame" << std::endl;
            continue;
        }

        // Put the timestamp in only if the message passes the error and incomplete filter
        // os << rcv_time_ms;

        
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

            if (num_packets_rx % 1000 == 0){
                std::cout << "Received " << num_packets_rx << " packets\r" << std::flush << "\n";
                os << parquet::EndRowGroup;
                //std::cout << outfile->Flush() << "\n";
                //std::cout << std::flush;
                break;
            }
        }
    }
    //close(s);
    //std::cout << std::flush;
}
