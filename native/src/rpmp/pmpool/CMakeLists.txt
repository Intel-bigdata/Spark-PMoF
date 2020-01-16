add_library(pmpool SHARED DataServer.cc Digest.cc Protocol.cc Request.cc NetworkServer.cc hash/xxhash.cc client/PmPoolClient.cc client/NetworkClient.cc)
target_link_libraries(pmpool LINK_PUBLIC ${Boost_LIBRARIES} hpnl pmemobj)
set_target_properties(pmpool PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

set(CMAKE_INSTALL_PREFIX "/usr/local")
install(TARGETS pmpool LIBRARY DESTINATION lib ARCHIVE DESTINATION lib)
