add_executable(processing_app
    processing_app.cpp
    )


# Copy the required headers to install directory
include_directories(
    ${PROJECT_SOURCE_DIR}/src
    )

target_include_directories(processing_app PRIVATE ${GSL_INCLUDE_DIR})


target_link_libraries(processing_dapp
    processing_service
    crdt_globaldb
    Boost::program_options    
    )
