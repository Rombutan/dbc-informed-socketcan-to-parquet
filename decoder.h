#ifndef DECODER_H
#define DECODER_H

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

#include <linux/can.h>
#include <linux/can/raw.h>

#include "custom_types.h"

class Decoder{
public:
    Decoder(std::string dbc_filename);

    bool decode(can_frame frame, std::vector<DataTypeOrVoid>* row_values);

    uint32_t msg_count = 0;

    ArrowSchemaList schema_fields;

private:
    std::unique_ptr<dbcppp::INetwork> net;
    std::unordered_map<uint64_t, const dbcppp::IMessage *> messages;
};

#endif