add_executable(ipfs_client
    ipfs.cpp
    )


# Copy the required headers to install directory
include_directories(
    ${PROJECT_SOURCE_DIR}/src
    )

target_include_directories(ipfs_client PRIVATE ${GSL_INCLUDE_DIR})


target_link_libraries(ipfs_client
#    ipfs-lite-cpp::ipfs_datastore_rocksdb
#    ipfs-lite-cpp::buffer
#    ipfs-lite-cpp::ipld_node
#    ipfs-lite-cpp::ipfs_merkledag_service
#    ipfs-lite-cpp::graphsync
    Boost::Boost.DI
    crdt_globaldb
    Boost::program_options    
    )
