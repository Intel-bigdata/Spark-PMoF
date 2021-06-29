add_library(pmpool_client_jni SHARED Event.cc ProxyEvent.cc client/PmPoolClient.cc client/NetworkClient.cc client/native/com_intel_rpmp_PmPoolClient.cc client/ProxyClient.cc)
target_link_libraries(pmpool_client_jni LINK_PUBLIC ${Boost_LIBRARIES} hpnl)
set_target_properties(pmpool_client_jni PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

add_library(pmpool SHARED DataServer.cc Protocol.cc Event.cc ProxyEvent.cc NetworkServer.cc proxy/metastore/MetastoreFacade.cc proxy/metastore/ConnectionFacade.cc proxy/metastore/redis/Redis.cc proxy/metastore/rocksdb/Rocks.cc proxy/metastore/rocksdb/RocksConnection.cc HeartbeatClient.cc HeartbeatEvent.cc hash/xxhash.cc client/PmPoolClient.cc client/NetworkClient.cc client/ProxyClient.cc proxy/clientService/ClientService.cc Proxy.cc proxy/replicaService/ReplicaService.cc DataService/DataServerService.cc proxy/NodeManager.cc  proxy/tracker/Tracker.cc)

target_link_libraries(pmpool LINK_PUBLIC ${Boost_LIBRARIES} hpnl pmemobj)
set_target_properties(pmpool PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

if(UNIX AND NOT APPLE)
  set(LINUX TRUE)
endif()

if(APPLE)
  set(JNI_INCLUDE "$ENV{JAVA_HOME}/include" "$ENV{JAVA_HOME}/include/darwin")
endif()
if(LINUX)
  set(JNI_INCLUDE "$ENV{JAVA_HOME}/include" "$ENV{JAVA_HOME}/include/linux")
endif()
include_directories(${JNI_INCLUDE})

set(CMAKE_INSTALL_PREFIX "/usr/local")
install(TARGETS pmpool LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
