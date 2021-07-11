/**
* Header file for the distrubuted processing Room
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP

#include "SGProcessing.pb.h"

class ProcessingTaskQueue
{
public:
    virtual ~ProcessingTaskQueue() = default;

    virtual bool GrabTask(SGProcessing::Task& task) = 0;
    virtual bool CompleteTask(SGProcessing::TaskResult& task) = 0;
};

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_TASK_QUEUE_HPP