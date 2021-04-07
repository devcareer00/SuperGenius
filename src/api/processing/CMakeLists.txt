
set(PROTO_SRC_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto)
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

add_library(api_processing_server
    processing_server.cpp
    )

# gnostic-grpc throws an error when an absolute file path is used for input or output (at least on Windows)
# As a workaround the input .yaml file is copied into CMAKE_CURRENT_BINARY_DIR
# and relative paths are used for input and output files in the gnostic call
file(COPY "${CMAKE_SOURCE_DIR}/gRPCForSuperGenius/openapi_yaml/SGProcessing-OpenAPI.yaml"
    DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/openapi_yaml"
    )

# PATH environment variable has to be set to allow gnostic to find gnostic-grpc module
set(ENV{PATH} "${_GOPATH}/bin" )
set(GNOSTIC_APP "gnostic")
set(GNOSTIC_APP_ARGS
    "--grpc-out=proto"
    "openapi_yaml/SGProcessing-OpenAPI.yaml"
    )

message(${GNOSTIC_APP}, ${GNOSTIC_APP_ARGS})

execute_process(COMMAND  ${GNOSTIC_APP} ${GNOSTIC_APP_ARGS}
    WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    RESULT_VARIABLE GNOSTIC_RESULT)
    
message("GNOSTIC_RESULT: " ${GNOSTIC_RESULT})

protobuf_generate(
    LANGUAGE cpp
    TARGET api_processing_server
    PROTOS "${PROTO_SRC_DIR}/SGProcessing_OpenAPI.proto"
    IMPORT_DIRS "${CMAKE_SOURCE_DIR}/gRPCForSuperGenius/proto-include" "${PROTOBUF_INCLUDE_DIR}" "${PROTO_SRC_DIR}"
    PROTOC_OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}"
    OUT_VAR _PROTOC_RESULT
    )

supergenius_install(api_processing_server)

target_include_directories(api_processing_server PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(api_processing_server
    protobuf::libprotobuf
    )
