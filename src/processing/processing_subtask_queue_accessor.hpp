/**
* Header file for subtask storage interface
* @author creativeid00
*/

#ifndef SUPERGENIUS_PROCESSING_SUBTASK_STORAGE_HPP
#define SUPERGENIUS_PROCESSING_SUBTASK_STORAGE_HPP

#include <processing/proto/SGProcessing.pb.h>
#include <boost/optional.hpp>

namespace sgns::processing
{
/** Subtask storage interface
*/
class SubTaskStorage
{
public:
    typedef std::function<void(boost::optional<const SGProcessing::SubTask&>)> SubTaskGrabbedCallback;
    virtual ~SubTaskStorage() = default;

    /** Asynchronous getting of a subtask from the queue
    * @param onSubTaskGrabbedCallback a callback that is called when a grapped iosubtask is locked by the local node
    */
    virtual void GrabSubTask(SubTaskGrabbedCallback onSubTaskGrabbedCallback) = 0;
    virtual void CompleteSubTask(const std::string& subTaskId, const SGProcessing::SubTaskResult& subTaskResult) = 0;

    // @todo Add SetErrorsHandler method
};
}

#endif // SUPERGENIUS_PROCESSING_SUBTASK_ACCESSOR_HPP
