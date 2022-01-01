#ifndef SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_CHANNEL_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_CHANNEL_HPP

#include <processing/proto/SGProcessing.pb.h>

#include <string>
#include <memory>

namespace sgns::processing
{
class ProcessingSubTaskQueueChannel
{
public:
    virtual ~ProcessingSubTaskQueueChannel() = default;

    virtual void RequestQueueOwnership(const std::string & nodeId) = 0;
    virtual void PublishQueue(std::shared_ptr<SGProcessing::SubTaskQueue> queue) = 0;
};
}
#endif // SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_CHANNEL_HPP