add_executable(processing_dapp
    processing_dapp.cpp
    processing_task_queue_impl.hpp
    )


target_link_libraries(processing_dapp
    processing_service
    crdt_globaldb
    logger
    Boost::program_options    
    )

add_executable(processing_dapp_processor
    processing_dapp_processor.cpp
    processing_subtask_result_storage.cpp
    processing_subtask_result_storage_impl.hpp
    processing_task_queue_impl.hpp
    )


target_link_libraries(processing_dapp_processor
    processing_service
    crdt_globaldb
    logger
    Boost::program_options    
    )

