
addtest(graphsync_app
    graphsync_app.cpp
    graphsync_acceptance_common.cpp
    )
target_link_libraries(graphsync_app
    ipfs-lite-cpp::graphsync
    ipfs-lite-cpp::ipfs_merkledag_service
    p2p::asio_scheduler
    Boost::date_time
    Boost::regex
    ${WIN_CRYPT_LIBRARY}
    )
set_target_properties(graphsync_app PROPERTIES LINK_FLAGS "/FORCE:MULTIPLE")
