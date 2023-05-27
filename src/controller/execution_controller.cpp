#include "controller/execution_controller.hpp"

#include <spdlog/spdlog.h>

#include "mapper/model_proto_mapper.hpp"
#include "utils/controller_utils.hpp"


ExecutionController::ExecutionController(
		ExecutionService& execution_service,
		SessionService& session_service
) noexcept
	: execution_service_(execution_service), session_service_(session_service)
{
}

grpc::Status ExecutionController::describe_job(::grpc::ServerContext* context, const ::herd::proto::DescribeJobRequest* request, ::herd::proto::JobDescription* response)
{
	return Service::describe_job(context, request, response);
}

grpc::Status ExecutionController::get_job_state(::grpc::ServerContext* context, const ::herd::proto::GetJobStateRequest* request, ::herd::proto::JobState* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const auto session_uuid = herd::common::UUID(request->session_uuid());
		const auto job_uuid = herd::common::UUID(request->uuid());

		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", session_uuid.as_string(), user_id);
			return {StatusCode::FAILED_PRECONDITION, "Session not found"};
		}

		const auto state = execution_service_.get_job_state(session_uuid, job_uuid);

		response->set_uuid(state.uuid.as_string());
		response->set_status(mapper::to_proto(state.status));
		if(state.current_stage.has_value())
		{
			response->set_current_stage(state.current_stage.value());
		}
		if(state.message.has_value())
		{
			response->set_message(state.message.value());
		}
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}

grpc::Status ExecutionController::list_jobs(::grpc::ServerContext* context, const ::herd::proto::ListJobsRequest* request, ::herd::proto::JobStateList* response)
{
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const auto session_uuid = herd::common::UUID(request->session_uuid());

		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", session_uuid.as_string(), user_id);
			return {StatusCode::FAILED_PRECONDITION, "Session not found"};
		}

		const auto states = execution_service_.get_job_states_for_session(session_uuid);
		const auto states_proto = response->mutable_states();
		states_proto->Reserve(static_cast<int>(states.size())); //todo: fix casting

		for(const auto& state: states)
		{
			const auto state_proto = states_proto->Add();
			state_proto->set_uuid(state.uuid.as_string());
			state_proto->set_status(mapper::to_proto(state.status));
			if(state.current_stage.has_value())
			{
				state_proto->set_current_stage(state.current_stage.value());
			}

			if(state.message.has_value())
			{
				state_proto->set_message(state.message.value());
			}
		}
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}

grpc::Status ExecutionController::schedule_job(grpc::ServerContext* context, const herd::proto::ScheduleJobRequest* request, herd::proto::JobDescription* response)
{
	static_cast<void>(response);
	using namespace grpc;

	const auto user_id = extract_user_id_from_context(context);

	try
	{
		const auto session_uuid = herd::common::UUID(request->session_uuid());

		if(!session_service_.session_exists_by_uuid(user_id, session_uuid))
		{
			spdlog::info("There is no session with uuid: {} associated to user: {}", session_uuid.as_string(), user_id);
			return {StatusCode::FAILED_PRECONDITION, "Session not found"};
		}

		const auto execution_plan = mapper::to_model(request->plan());

		const auto description = execution_service_.schedule_job(session_uuid, execution_plan);

		response->set_uuid(description.uuid.as_string());
		response->set_estimated_complexity(description.estimated_complexity);
		response->mutable_plan()->CopyFrom(mapper::to_proto(description.plan));

		return Status::OK;
	}
	catch(const mapper::MappingError&)
	{
		spdlog::info("Failed to schedule job for session {} associated to user {}. Invalid Execution Plan", request->session_uuid(), user_id);
		return {StatusCode::INVALID_ARGUMENT, "Invalid Execution Plan"};
	}
	catch(const std::runtime_error& error)
	{
		spdlog::error(error.what());
		return {StatusCode::INTERNAL, INTERNAL_ERROR_MSG};
	}

	return Status::OK;
}
