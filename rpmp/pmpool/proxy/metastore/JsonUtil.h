#ifndef RPMP_PROXY_METASTORE_JSONUTIL_H
#define RPMP_PROXY_METASTORE_JSONUTIL_H

#include "json/json.h"

using namespace std;

static inline string rootToString(Json::Value root){
  Json::StreamWriterBuilder builder;
  std::string json_str = Json::writeString(builder, root);
  json_str.erase(remove(json_str.begin(), json_str.end(), ' '), json_str.end());
  return json_str;
}

#endif