/**
* Header file for a view of distributed queue data
* @author creativeid00
*/

#ifndef SUPERGENIUS_PROCESSING_SHARED_QUEUE_DATA_VIEW_HPP
#define SUPERGENIUS_PROCESSING_SHARED_QUEUE_DATA_VIEW_HPP

#include <string>
#include <vector>
#include <chrono>

namespace sgns::processing
{
class SharedQueueDataView
{
public:
    virtual ~SharedQueueDataView() = default;

    virtual const std::string& GetQueueOwnerNodeId() const = 0;
    virtual void SetQueueOwnerNodeId(const std::string& nodeId) const = 0;

    virtual std::chrono::system_clock::time_point GetQueueLastUpdateTimestamp() const = 0;
    virtual void SetQueueLastUpdateTimestamp(std::chrono::system_clock::time_point ts) const = 0;

    virtual const std::string& GetItemLockNodeId(size_t itemIdx) const = 0;
    virtual void SetItemLockNodeId(size_t itemIdx, const std::string& nodeId) const = 0;

    virtual std::chrono::system_clock::time_point GetItemLockTimestamp(size_t itemIdx) const = 0;
    virtual void SetItemLockTimestamp(size_t itemIdx, std::chrono::system_clock::time_point ts) const = 0;

    virtual bool IsExcluded(size_t itemIdx) const = 0;

    virtual size_t Size() const = 0;
};
}

#endif // SUPERGENIUS_PROCESSING_SHARED_QUEUE_DATA_VIEW_HPP