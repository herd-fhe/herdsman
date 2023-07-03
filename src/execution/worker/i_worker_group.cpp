#include "execution/worker/i_worker_group.hpp"
#include "spdlog/spdlog.h"

void IWorkerGroup::TaskHandle::mark_completed()
{
	std::unique_lock lock(callback_mutex_);
	completed_ = true;

	if (callback_)
	{
		callback_(status());
	}
}

void IWorkerGroup::TaskHandle::set_completion_callback(std::function<void(Status)> callback) noexcept
{
	std::unique_lock lock(callback_mutex_);

	callback_ = std::move(callback);
}

bool IWorkerGroup::TaskHandle::completed() const noexcept
{
	return completed_;
}
