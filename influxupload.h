#ifndef INFLUXUPLOAD_H
#define INFLUXUPLOAD_H

//#define CPPHTTPLIB_OPENSSL_SUPPORT

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

httplib::Client cli(influxWriter.host);
//httplib::SSLClient cli(influxWriter.host);

void writeRow(std::vector<DataTypeOrVoid> vec){
    std::string lpstring = influxWriter.table;

    int i = 0;
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

    lpstring.append("\n");
    httplib::Result res = cli.Post(influxWriter.endpoint,{{"Authorization", "Bearer "+ influxWriter.token}}, lpstring, "/text");
    httplib::Error err = res.error();
    
    std::cout << httplib::to_string(err) << res.ssl_error() << "\n";
}


#endif
