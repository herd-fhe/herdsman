#ifndef HERDSMAN_EXECUTION_CONTROLLER_HPP
#define HERDSMAN_EXECUTION_CONTROLLER_HPP

#include <execution.grpc.pb.h>

#include "service/execution_service.hpp"
#include "service/session_service.hpp"


class ExecutionController: public herd::proto::Execution::Service
{
public:
	explicit ExecutionController(
			ExecutionService& execution_service,
			SessionService& session_service
	) noexcept;

	grpc::Status describe_job(
			grpc::ServerContext* context,
			const herd::proto::DescribeJobRequest* request,
			herd::proto::JobDescription* response
	) override;

	grpc::Status get_job_state(
			grpc::ServerContext* context,
			const herd::proto::GetJobStateRequest* request,
			herd::proto::JobState* response
	) override;

	grpc::Status list_jobs(
			grpc::ServerContext* context,
			const herd::proto::ListJobsRequest* request,
			herd::proto::JobStateList* response
	) override;

	grpc::Status schedule_job(
			grpc::ServerContext* context,
			const herd::proto::ScheduleJobRequest* request,
			herd::proto::JobDescription* response
	) override;

private:
	ExecutionService& execution_service_;
	SessionService& session_service_;
};

#endif //HERDSMAN_EXECUTION_CONTROLLER_HPP
