add_library(crdt_globaldb 
    globaldb.cpp
    pubsub_broadcaster.cpp
    )

target_link_libraries(crdt_globaldb
    crdt_datastore
    graphsync_dagsyncer
    ipfs-pubsub
    ipfs-lite-cpp::ipfs_datastore_rocksdb
    p2p::p2p_peer_address
    Boost::filesystem
    p2p::p2p_protocol_echo
    ${WIN_CRYPT_LIBRARY}
    )
