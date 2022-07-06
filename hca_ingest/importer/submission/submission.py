import logging
from typing import List

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.importer.submission.entity import Entity
from hca_ingest.importer.submission.entity_map import EntityMap

format = '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=format)


class Submission(object):
    def __init__(self, ingest_api: IngestApi, submission_url: str):
        self.ingest_api = ingest_api
        self.submission_url = submission_url
        self.metadata_dict = {}
        self.manifest = None
        self.logger = logging.getLogger(__name__)

    def is_update(self):
        submission = self.ingest_api.get_submission(self.submission_url)
        return submission.get('isUpdate', False)

    def get_submission_url(self):
        return self.submission_url

    def add_entity(self, entity: Entity):
        self.metadata_dict[entity.type + '.' + entity.id] = entity
        return entity

    def get_entity(self, entity_type: str, id: str):
        key = entity_type + '.' + id
        return self.metadata_dict[key]

    def define_manifest(self, entity_map: EntityMap):
        # TODO provide a better way to serialize
        manifest_json = {
            'totalCount': entity_map.count_total(),
            'expectedBiomaterials': entity_map.count_entities_of_type('biomaterial'),
            'expectedProcesses': entity_map.count_entities_of_type('process'),
            'expectedFiles': entity_map.count_entities_of_type('file'),
            'expectedProtocols': entity_map.count_entities_of_type('protocol'),
            'expectedProjects': entity_map.count_entities_of_type('project'),
            'expectedLinks': entity_map.count_links(),
            'actualLinks': 0
        }

        self.manifest = self.ingest_api.create_submission_manifest(self.submission_url, manifest_json)
        return self.manifest

    def get_entities(self) -> List[Entity]:
        return self.metadata_dict.values()
