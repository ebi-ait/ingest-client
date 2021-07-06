import logging
from typing import List

from ingest.api.ingestapi import IngestApi
from ingest.importer.submission.entity import Entity
from ingest.importer.submission.entity_map import EntityMap

format = '[%(filename)s:%(lineno)s - %(funcName)20s() ] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=format)

ENTITY_LINK = {
    'biomaterial': 'biomaterials',
    'process': 'processes',
    'file': 'files',
    'protocol': 'protocols',
    'project': 'projects',
    'submission_envelope': 'submissionEnvelopes'
}


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
        link_name = ENTITY_LINK[entity.type]

        # TODO: how to get filename?!!!
        if entity.type == 'file':
            file_name = entity.content['file_core']['file_name']
            response = self.ingest_api.create_file(self.submission_url,
                                                   file_name,
                                                   entity.content)
        elif entity.type == 'project':
            response = self.ingest_api.create_project(self.submission_url,
                                                      entity.content)
        else:
            response = self.ingest_api.create_entity(self.submission_url,
                                                     {"content": entity.content},
                                                     link_name)
        entity.ingest_json = response
        self.metadata_dict[entity.type + '.' + entity.id] = entity

        return entity

    def get_entity(self, entity_type: str, id: str):
        key = entity_type + '.' + id
        return self.metadata_dict[key]

    def link_entity(self, from_entity: Entity, to_entity: Entity, relationship: str, is_collection=True):
        if from_entity.is_linking_reference and not from_entity.ingest_json:
            from_entity.ingest_json = self.ingest_api.get_entity_by_uuid(ENTITY_LINK[from_entity.type],
                                                                         from_entity.id)

        if to_entity.is_linking_reference and not to_entity.ingest_json:
            to_entity.ingest_json = self.ingest_api.get_entity_by_uuid(ENTITY_LINK[to_entity.type], to_entity.id)

        from_entity_ingest = from_entity.ingest_json
        to_entity_ingest = to_entity.ingest_json
        self.ingest_api.link_entity(from_entity_ingest, to_entity_ingest, relationship, is_collection)

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
