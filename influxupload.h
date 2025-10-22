#ifndef INFLUXUPLOAD_H
#define INFLUXUPLOAD_H

#define CPPHTTPLIB_OPENSSL_SUPPORT

#include <iostream>
#include <future>
#include <chrono>
#include <thread>

#include "httplib.h"
#include "custom_types.h"

struct InfluxWriterState{
    int retryingWrites = 0;
    std::string endpoint = "/api/v3/write_lp?db=test&no_sync=true";
    std::string host = "localhost:8181";
    std::string table;
    std::string token;
    std::vector<SignalTypeOrderTracker> schema_fields;
    std::vector<std::string> tags;
} influxWriter;



std::unique_ptr<httplib::Client> cli;

void initInflux(){
    cli = std::make_unique<httplib::Client>(influxWriter.host.c_str());
    cli->set_keep_alive(true);
}

std::string lpstring;

void postRows(){
    auto res = cli->Post(influxWriter.endpoint.c_str(),{{"Authorization", "Bearer " + influxWriter.token}}, lpstring, "text/plain; charset=utf-8");
    httplib::Error err = res.error();
    std::cout << res->body << "        " << res->status << "\n";
    //std::cout << lpstring;
    lpstring.clear();
}

void writeRow(std::vector<DataTypeOrVoid> vec){
    int i = 0;
    lpstring.append(influxWriter.table);

    while(i<influxWriter.tags.size()){
        lpstring.append(",");
        lpstring.append(influxWriter.tags[i]);
        i++;
    }
    lpstring.append(" ");

    i=0;
    while (i < influxWriter.schema_fields.size()){
                
        if (std::holds_alternative<std::monostate>(vec[i])) {
            i++;
            continue;
        }

        if(i > 0){
            lpstring.append(",");
        }
        
        lpstring.append(influxWriter.schema_fields[i].signal_name);
        lpstring.append("=");
        lpstring.append(variant_to_string(vec[i]));
        i++;
    }
    lpstring.append(" ");
    lpstring.append(std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count()));

    lpstring.append("\n");
}

#endif
