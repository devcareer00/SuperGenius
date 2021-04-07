
set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

set(protobuf_generate_PROTOC_OUT_DIR ${PROTO_SRC_DIR})

add_library(api_processing_server
    processing_server.cpp
    )

#protobuf_generate(
#    LANGUAGE cpp
#    TARGET api_processing_server
#    PROTOS ${PROTO_SRC_DIR}/SGProcessing_OpenAPI.proto
#    )

#supergenius_install(api_processing_server)

target_include_directories(api_processing_server PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(api_processing_server
    protobuf::libprotobuf
    )
