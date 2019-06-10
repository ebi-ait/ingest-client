from copy import deepcopy
from typing import Iterable

from ingest.api.dssapi import DssApi


class MetadataResource:

    def __init__(self, metadata_type=None, metadata_json=None, uuid=None, dcp_version=None):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = dcp_version
        self.metadata_type = metadata_type
        if not metadata_type:
            self._determine_metadata_type()

    def _determine_metadata_type(self):
        metadata_type = None
        if self.metadata_json:
            described_by = self.metadata_json.get('describedBy')
            metadata_type = described_by.split('/')[-1] if described_by else None
        self.metadata_type = metadata_type

    @staticmethod
    def from_dict(data: dict):
        uuid_object = data.get('uuid')
        uuid = uuid_object.get('uuid') if uuid_object else None
        content = data.get('content')
        metadata_resource = MetadataResource(uuid=uuid, metadata_json=content,
                                             dcp_version=data.get('dcpVersion'))
        return metadata_resource

    def get_staging_file_name(self):
        return f'{self.uuid}.{self.dcp_version}.json'


class Bundle:

    def __init__(self, source={}):
        self._source = deepcopy(source)
        self._bundle = self._source.get('bundle')  # because bundle is nested in the root ¯\_(ツ)_/¯
        self._prepare_file_map()
        self.uuid = self._bundle.get('uuid')

    def _prepare_file_map(self):
        bundle_files = self._bundle.get('files') if self._bundle else None
        if not bundle_files:
            bundle_files = []
        self._file_map = {file.get('uuid'): file for file in bundle_files}

    def get_version(self):
        return self._bundle.get('version')

    def get_file(self, uuid):
        return self._file_map.get(uuid)

    def get_files(self):
        return list(self._file_map.values())

    def count_files(self):
        return len(self._file_map)

    def update_version(self, version):
        self._bundle['version'] = version

    def update_file(self, metadata_resource: MetadataResource):
        target_file = self.get_file(metadata_resource.uuid)
        target_file['version'] = metadata_resource.dcp_version
        target_file['content-type'] = f'metadata/{metadata_resource.metadata_type}'


class MetadataService:

    def __init__(self, ingest_client):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)


class StagingInfo:

    def __init__(self, metadata_uuid='', file_name='', cloud_url=''):
        self.metadata_uuid = metadata_uuid
        self.file_name = file_name
        self.cloud_url = cloud_url


class StagingService:

    def __init__(self, staging_client):
        self.staging_client = staging_client

    def stage_update(self, staging_area_uuid,
                     metadata_resource: MetadataResource) -> StagingInfo:
        file_description = self.staging_client.stageFile(staging_area_uuid,
                                                         metadata_resource.get_staging_file_name(),
                                                         metadata_resource.metadata_json,
                                                         metadata_resource.metadata_type)
        return StagingInfo(metadata_uuid=metadata_resource.uuid,
                           file_name=file_description.name, cloud_url=file_description.url)


class BundleService:

    def __init__(self, dss_client: DssApi):
        self.dss_client = dss_client

    def fetch(self, uuid: str) -> Bundle:
        bundle_source = self.dss_client.get_bundle(uuid)
        return Bundle(source=bundle_source)

    def update(self, bundle: Bundle, staging_details: list):
        cloud_url_map = {info.metadata_uuid: info.cloud_url for info in staging_details}
        bundle_files = bundle.get_files()
        for file in bundle_files:
            uuid = file.get('uuid')
            cloud_url = cloud_url_map.get(uuid)
            self.dss_client.put_file(None, {'url': cloud_url, 'dss_uuid': uuid,
                                            'update_date': file.get('version')})
        self.dss_client.put_bundle(bundle.uuid, bundle.get_version(), bundle_files)


class Exporter:

    def __init__(self, metadata_service: MetadataService, staging_service: StagingService,
                 bundle_service: BundleService):
        self.metadata_service = metadata_service
        self.staging_service = staging_service
        self.bundle_service = bundle_service

    def export_update(self, update_submission: dict, bundle_uuid: str, metadata_urls: list,
                      update_version: str):
        bundle = self.bundle_service.fetch(bundle_uuid)
        staging_details = []
        for url in metadata_urls:
            metadata_resource = self.metadata_service.fetch_resource(url)
            staging_info = self.staging_service.stage_update(metadata_resource)
            staging_details.append(staging_info)
            bundle.update_file(metadata_resource)
        self.bundle_service.update(bundle, staging_details)
