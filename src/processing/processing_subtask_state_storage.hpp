/**
* Header file for processing data storage interface
* @author creativeid00
*/

#ifndef GRPC_FOR_SUPERGENIUS_PROCESSING_SUBTASK_STATE_STORAGE_HPP
#define GRPC_FOR_SUPERGENIUS_PROCESSING_SUBTASK_STATE_STORAGE_HPP

#include <processing/proto/SGProcessing.pb.h>
#include <optional>

namespace sgns::processing
{
/** Handles processing data storage
*/
class SubTaskStateStorage
{
public:
    virtual ~SubTaskStateStorage() = default;

    virtual void ChangeSubTaskState(const std::string& subTaskId, SGProcessing::SubTaskState::Type state) = 0;
    virtual std::optional<SGProcessing::SubTaskState> GetSubTaskState(const std::string& subTaskId) = 0;
};
}

#endif // GRPC_FOR_SUPERGENIUS_PROCESSING_SUBTASK_STORAGE_HPP
