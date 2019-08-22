import logging

from ingest.exporter.metadata import MetadataResource
from ingest.exporter.exceptions import FileDuplication

from ingest.api.ingestapi import IngestApi

logger = logging.getLogger(__name__)


class StagingInfo:

    def __init__(self, staging_area_uuid, file_name, metadata_uuid='', cloud_url=''):
        self.staging_area_uuid = staging_area_uuid
        self.file_name = file_name
        self.metadata_uuid = metadata_uuid
        self.cloud_url = cloud_url


class StagingInfoRepository:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def delete_staging_locks(self, staging_area_uuid: str):
        self.ingest_client.delete_staging_jobs(staging_area_uuid)

    def save(self, staging_info: StagingInfo):
        pass


class StagingService:

    def __init__(self, staging_client, staging_info_repository: StagingInfoRepository):
        self.staging_client = staging_client
        self.staging_info_repository = staging_info_repository

    def stage_metadata(self, staging_area_uuid, metadata_resource: MetadataResource) -> StagingInfo:
        try:
            staging_info = StagingInfo(staging_area_uuid, metadata_resource.get_staging_file_name())
            self.staging_info_repository.save(staging_info)
            formatted_type = f'metadata/{metadata_resource.metadata_type}'
            file_description = self.staging_client.stageFile(staging_area_uuid,
                                                             metadata_resource.get_staging_file_name(),
                                                             metadata_resource.to_bundle_metadata(),
                                                             formatted_type)

            staging_info.metadata_uuid = metadata_resource.uuid
            staging_info.cloud_url = file_description.url
            return staging_info
        except FileDuplication as file_duplication:
            logger.warning(file_duplication)

    def cleanup_staging_locks(self, staging_area_uuid):
        self.staging_info_repository.delete_staging_locks(staging_area_uuid)
