add_library(api_processing_server
    processing_server.cpp
    )

#target_include_directories(api_processing_server PUBLIC ${PROTO_SRC_DIR} ${PROTO_SRC_DIR}/proto-include)

target_link_libraries(api_processing_server
    protobuf::libprotobuf
    gRPCForSuperGenius
    )
