#ifndef HERDSMAN_I_WORKER_SERVICE_HPP
#define HERDSMAN_I_WORKER_SERVICE_HPP

#include "herd/common/uuid.hpp"
#include <memory>


class IWorkerService
{
public:
	enum class WorkerStatus
	{
		STARTING,
		READY,
		BUSY,
		FAILED
	};

	class IWorkersGroup
	{
	public:
		virtual ~IWorkersGroup() = default;

		virtual WorkerStatus status() = 0;
	};

	virtual ~IWorkerService() = default;

	virtual std::unique_ptr<IWorkersGroup> setup_workers(const herd::common::UUID& session) = 0;
};

#endif //HERDSMAN_I_WORKER_SERVICE_HPP
