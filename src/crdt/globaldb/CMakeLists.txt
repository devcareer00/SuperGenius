set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)

file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

add_library(crdt_globaldb 
    globaldb.cpp
    pubsub_broadcaster.cpp
    pubsub_broadcaster_ext.cpp
    )

target_include_directories(crdt_globaldb PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate(
    LANGUAGE cpp
    TARGET crdt_globaldb
    PROTOS ${CMAKE_CURRENT_SOURCE_DIR}/proto/broadcast.proto
    IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/proto"
    PROTOC_OUT_DIR ${PROTO_SRC_DIR}
    )

target_link_libraries(crdt_globaldb
    crdt_datastore
    crdt_graphsync_dagsyncer
    ipfs-pubsub
    ipfs-lite-cpp::ipfs_datastore_rocksdb
    p2p::p2p_peer_address
    Boost::filesystem
    p2p::p2p_protocol_echo
    ${WIN_CRYPT_LIBRARY}
    )
