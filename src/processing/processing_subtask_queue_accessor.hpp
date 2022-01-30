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

    /** Creates a queue based on subtask list
    * @param subTasks - a list of enqueued subtasks
    */
    virtual void Create(std::list<SGProcessing::SubTask>& subTasks) = 0;

    /** Asynchronous getting of a subtask from the queue
    * @param onSubTaskGrabbedCallback a callback that is called when a subtask is grabbed by the local node
    */
    virtual void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback) = 0;

    /** Finalizes subtask execution
    * @param subTaskId - id of processed subtask
    * @param subTaskResult - result of subtask processing
    */
    virtual void CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult) = 0;

    // @todo Add SetErrorsHandler method
};
}

#endif // SUPERGENIUS_PROCESSING_SUBTASK_QUEUE_ACCESSOR_HPP
