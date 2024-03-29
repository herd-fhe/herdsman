#ifndef HERDSMAN_STORAGE_CONTROLLER_HPP
#define HERDSMAN_STORAGE_CONTROLLER_HPP

#include <storage.grpc.pb.h>

#include "service/storage_service.hpp"
#include "service/session_service.hpp"
#include "service/key_service.hpp"


class StorageController: public herd::proto::Storage::Service
{
public:
	StorageController(
			StorageService& storage_service,
			SessionService& session_service,
			KeyService& key_service
	) noexcept;

	grpc::Status add_data_frame(
			grpc::ServerContext* context,
			grpc::ServerReaderWriter<::herd::proto::DataFrameAddResponse,herd::proto::DataFrameAddRequest>* stream
	) override;

	grpc::Status remove_data_frame(
			grpc::ServerContext* context,
			const herd::proto::DataFrameRemoveRequest* request,
			herd::proto::Empty* response
	) override;

	grpc::Status list_data_frames(
			grpc::ServerContext* context,
			const herd::proto::DataFrameListRequest* request,
			herd::proto::DataFrameMetadataList* response
	) override;

	grpc::Status download_data_frame(
			grpc::ServerContext* context,
			const herd::proto::DataFrameDownloadDataRequest* request,
			herd::proto::DataFrameDownloadDataResponse* response
	) override;

private:
	StorageService& storage_service_;
	SessionService& session_service_;
	KeyService& key_service_;
};

#endif //HERDSMAN_STORAGE_CONTROLLER_HPP
