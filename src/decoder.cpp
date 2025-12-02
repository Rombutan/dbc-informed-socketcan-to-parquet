#include "decoder.h"

Decoder::Decoder(std::string dbc_filename){
    std::ifstream idbc(dbc_filename.c_str());
    net = dbcppp::INetwork::LoadDBCFromIs(idbc);

    if (net.get() == nullptr) {
        std::cerr << "failed to parse dbc\n";
    }

    for (const dbcppp::IMessage& msg : net->Messages())
    {
        messages.insert(std::make_pair(msg.Id(), &msg));
    }
    schema_fields.push_back(SignalTypeOrderTracker{"Time_ms", parquet::Type::type::DOUBLE, arrow::float64()}); // First column is always timestamp

    int i = 0;
    while (i < net->Messages_Size()){
        const dbcppp::IMessage& msg_ref = net->Messages_Get(i);
        //std::cout << "Message " << i << ": " << msg_ref.Name() << "\n";
        int n = 0;
        while(n<msg_ref.Signals_Size()){
            const dbcppp::ISignal& sig_ref = msg_ref.Signals_Get(n);
            dbcppp::ISignal::EExtendedValueType ev_type = sig_ref.ExtendedValueType();
            char type_name[6] = "-----";
            SignalTypeOrderTracker signal;
            
            if (sig_ref.Name().substr(0, 6) == "flt32_"){
                signal.signal_name = sig_ref.Name().substr(6);

                std::cout << signal.signal_name << " Is an IEEE float encoded \n";
                
                std::strncpy(type_name, "float", 5);

                signal.parquet_type = parquet::Type::type::FLOAT;
                signal.arrow_datatype = arrow::float32();
            }

            else { // All integer encoded signals
                signal.signal_name = sig_ref.Name();
                if (sig_ref.BitSize() == 1){
                    std::strncpy(type_name, "bool ", 5);
                    signal.parquet_type = parquet::Type::type::BOOLEAN;
                    signal.arrow_datatype = arrow::boolean();
                } else if (sig_ref.Factor() < 1.0001 && sig_ref.Factor() > 9.9999){
                    std::strncpy(type_name, "int  ", 5);
                    signal.parquet_type = parquet::Type::type::INT32;
                    signal.arrow_datatype = arrow::int32();
                    if (sig_ref.BitSize() > 32){
                        signal.parquet_type = parquet::Type::type::INT64;
                        signal.arrow_datatype = arrow::int64();
                    } else if (sig_ref.BitSize() > 64){
                        signal.parquet_type = parquet::Type::type::INT96;
                        signal.arrow_datatype = arrow::int64();
                    }
                } else {
                    std::strncpy(type_name, "float", 5);
                    signal.parquet_type = parquet::Type::type::DOUBLE;
                    signal.arrow_datatype = arrow::float64();
                    if (sig_ref.BitSize() < 32){
                        signal.parquet_type = parquet::Type::type::FLOAT;
                        signal.arrow_datatype = arrow::float32();
                    }
                }
            }
            schema_fields.push_back(std::move(signal));

            //std::cout << "\tSignal " << n << ": " << sig_ref.Name() << " type: " << type_name << "\n";
            n++;
        }

        i++;
    }
}

bool Decoder::decode(can_frame frame, std::vector<DataTypeOrVoid>* row_values){
    auto iter = messages.find(frame.can_id);

        if (iter != messages.end())
        {
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

                    std::string CleanName = sig.Name();


                    auto it = std::find_if(schema_fields.begin(), schema_fields.end(),
                        [CleanName](const SignalTypeOrderTracker& tracker) { return tracker.signal_name == CleanName; });
                    
                    if (it != schema_fields.end()){
                        //std::cout << "Found signal " << sig.Name() << " in schema at index " << std::distance(schema_fields.begin(), it) << " With type: " << it->parquet_type << "\n";
                        int index = std::distance(schema_fields.begin(), it);
                        // Set the value in the row_values based on the type
                        
                        if(sig.Name().substr(0, 6) == "flt32_"){
                            std::array<unsigned char, 4> bytes = extract_32_bits(frame.data, sig.StartBit());
                            uint32_t bits;
                            
                            std::memcpy(&bits, bytes.data(), sizeof(bits));
                            row_values->at(index) = static_cast<float>(le_uint32_to_float(bits));
                        }
                        
                        else if (it->parquet_type == parquet::Type::type::DOUBLE){
                            row_values->at(index) = static_cast<double>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->parquet_type == parquet::Type::type::FLOAT){
                            row_values->at(index) = static_cast<float>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->parquet_type == parquet::Type::type::INT32){
                            row_values->at(index) = static_cast<int32_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->parquet_type == parquet::Type::type::INT64){
                            row_values->at(index) = static_cast<int64_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->parquet_type == parquet::Type::type::INT96){
                            row_values->at(index) = static_cast<__int128_t>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else if (it->parquet_type == parquet::Type::type::BOOLEAN){
                            row_values->at(index) = static_cast<bool>(sig.RawToPhys(sig.Decode(frame.data)));
                        } else {
                            std::cerr << "Unhandled parquet type for signal " << sig.Name() << "\n";
                        }
                    } else {
                        std::cerr << "signal not found in schema_fields: " << CleanName << "\n";
                    }

                }

            }            

            msg_count++;
            return true;
        }
    return false;
}