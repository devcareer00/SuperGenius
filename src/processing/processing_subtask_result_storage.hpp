/**
* Header file for processing data storage interface
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_SUTASK_RESULT_STORAGE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_SUTASK_RESULT_STORAGE_HPP

#include <processing/proto/SGProcessing.pb.h>
#include <chrono>

namespace sgns::processing
{
/** Handles processing data storage
*/
class SubTaskResultStorage
{
public:

    virtual ~SubTaskResultStorage() = default;

    virtual void AddSubTaskResult(const SGProcessing::SubTaskResult& subTaskResult) = 0;
    virtual void RemoveSubTaskResult(const std::string& subTaskId) = 0;
    virtual void GetSubTaskResults(
        const std::vector<std::string>& subTaskIds, 
        std::vector<SGProcessing::SubTaskResult>& results) = 0;

};
}

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_SUTASK_RESULT_STORAGE_HPP
