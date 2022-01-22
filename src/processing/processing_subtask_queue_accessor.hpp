/**
* Header file for subtask queue accessor interface
* @author creativeid00
*/

#ifndef SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_HPP

#include <processing/proto/SGProcessing.pb.h>
#include <boost/optional.hpp>

namespace sgns::processing
{
/** Subtask queue accessor interface
*/
class SubTaskQueueAccessor
{
public:
    typedef std::function<void(boost::optional<const SGProcessing::SubTask&>)> SubTaskGrabbedCallback;
    virtual ~SubTaskQueueAccessor() = default;

    /** Asynchronous getting of a subtask from the queue
    * @param onSubTaskGrabbedCallback a callback that is called when a grapped iosubtask is locked by the local node
    */
    virtual void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback) = 0;
    virtual void CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult) = 0;

    // @todo Add SetErrorsHandler method
};
}

#endif // SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_HPP
