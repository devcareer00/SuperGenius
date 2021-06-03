set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)


adadsasdas

add_executable(crtd_globaldb_app globaldb_app.cpp)
target_link_libraries(crtd_globaldb_app 
    crdt_globaldb
#    graphsync_dagsyncer
#    ipfs-pubsub
#    ipfs-lite-cpp::ipfs_datastore_rocksdb
#    p2p::p2p_peer_address
#    Boost::filesystem
#    Boost::program_options
#    p2p::p2p_protocol_echo
#    ${WIN_CRYPT_LIBRARY}
    )
