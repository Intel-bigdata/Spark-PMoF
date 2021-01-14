#add_executable(local_allocate local_allocate.cc)
#target_link_libraries(local_allocate pmpool)
#
#add_executable(remote_allocate remote_allocate.cc)
#target_link_libraries(remote_allocate pmpool_client_jni)
#
#add_executable(remote_write remote_write.cc)
#target_link_libraries(remote_write pmpool_client_jni)
#
#add_executable(remote_allocate_write remote_allocate_write.cc)
#target_link_libraries(remote_allocate_write pmpool_client_jni)
#
#add_executable(circularbuffer circularbuffer.cc)
#target_link_libraries(circularbuffer pmpool_client_jni)
#
#add_executable(remote_read remote_read.cc)
#target_link_libraries(remote_read pmpool_client_jni)
#
#add_executable(remote_put remote_put.cc)
#target_link_libraries(remote_put pmpool_client_jni)

add_executable(put_and_read put_and_read.cc)
target_link_libraries(put_and_read pmpool_client_jni)

add_executable(put_and_get put_and_get.cc)
target_link_libraries(put_and_get pmpool_client_jni)

add_executable(put put.cc)
target_link_libraries(put pmpool_client_jni)

add_executable(get get.cc)
target_link_libraries(get pmpool_client_jni)