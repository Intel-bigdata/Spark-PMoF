#include "json/json.h"
#include <iostream>

using namespace std;

string rootToString(Json::Value root){
  Json::StreamWriterBuilder builder;
  std::string json_str = Json::writeString(builder, root);
  json_str.erase(remove(json_str.begin(), json_str.end(), ' '), json_str.end());
  return json_str;
}
