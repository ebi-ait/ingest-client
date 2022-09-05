import copy
import json
import logging

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.importer.submission.entity import Entity
from hca_ingest.importer.submission.entity_map import EntityMap
from hca_ingest.importer.submission.submission import Submission

ENTITY_LINK = {
    'biomaterial': 'biomaterials',
    'process': 'processes',
    'file': 'files',
    'protocol': 'protocols',
    'project': 'projects',
    'submission_envelope': 'submissionEnvelopes'
}


def json_equals(json1: dict, json2: dict):
    return json.dumps(json1, sort_keys=True) == json.dumps(json2, sort_keys=True)


class IngestSubmitter(object):
    def __init__(self, ingest_api: IngestApi):
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)
        self.PROGRESS_CTR = 50
        self.logger = logging.getLogger(__name__)

    def add_entity(self, entity: Entity, submission_url: str):
        link_name = ENTITY_LINK[entity.type]

        if entity.type == 'file':
            file_name = entity.content['file_core']['file_name']
            response = self.ingest_api.create_file(submission_url,
                                                   file_name,
                                                   entity.content)
        elif entity.type == 'project':
            response = self.ingest_api.create_project(submission_url,
                                                      entity.content)
        else:
            response = self.ingest_api.create_entity(submission_url,
                                                     {"content": entity.content},
                                                     link_name)
        entity.ingest_json = response

        return entity

    def add_entities(self, entity_map: EntityMap, submission_url: str) -> Submission:
        submission = Submission(self.ingest_api, submission_url)
        submission.define_manifest(entity_map)
        for e in entity_map.get_new_entities():
            self.add_entity(e, submission_url)
            submission.add_entity(e)

        return submission

    def update_entities(self, entity_map: EntityMap):
        updated_entities = [self.update_entity(e) for e in entity_map.get_entities() if e.is_reference]
        return updated_entities

    def update_entity(self, entity: Entity):
        if not entity.ingest_json:
            entity.ingest_json = self.ingest_api.get_entity_by_uuid(ENTITY_LINK[entity.type], entity.id)

        patch = copy.deepcopy(entity.ingest_json.get('content'))
        patch.update(entity.content)
        if not json_equals(entity.ingest_json.get('content'), patch):
            self.ingest_api.patch(entity.url, json={'content': patch})

        return entity

    def link_submission_to_project(self, project_uuid: str, submission_url: str):
        project_entity = self.ingest_api.get_entity_by_uuid('projects', project_uuid)
        submission_envelope = self.ingest_api.get_submission(submission_url)
        self.ingest_api.link_entity(project_entity, submission_envelope, 'submissionEnvelopes')

    def link_entity(self, from_entity: Entity, to_entity: Entity, relationship: str, is_collection=True):
        if from_entity.is_linking_reference and not from_entity.ingest_json:
            from_entity.ingest_json = self.ingest_api.get_entity_by_uuid(ENTITY_LINK[from_entity.type],
                                                                         from_entity.id)

        if to_entity.is_linking_reference and not to_entity.ingest_json:
            to_entity.ingest_json = self.ingest_api.get_entity_by_uuid(ENTITY_LINK[to_entity.type], to_entity.id)

        from_entity_ingest = from_entity.ingest_json
        to_entity_ingest = to_entity.ingest_json

        self.ingest_api.link_entity(from_entity_ingest, to_entity_ingest, relationship, is_collection)

    def link_entities(self, entity_map: EntityMap, submission: Submission):
        progress = 0
        for entity in entity_map.get_entities():
            for link in entity.direct_links:
                to_entity = entity_map.get_entity(link['entity'], link['id'])
                try:
                    self.link_entity(entity, to_entity, relationship=link['relationship'],
                                     is_collection=link.get('is_collection', True))
                    progress = progress + 1
                    expected_links = int(submission.manifest.get('expectedLinks', 0))
                    if progress % self.PROGRESS_CTR == 0 or (progress == expected_links):
                        manifest_url = self.ingest_api.get_link_from_resource(submission.manifest, 'self')
                        self.ingest_api.patch(manifest_url, json={'actualLinks': progress})
                        self.logger.info(f"links progress: {progress}/ {submission.manifest.get('expectedLinks')}")

                except Exception as link_error:
                    error_message = f'''The {entity.type} with id {entity.id} could not be linked to {to_entity.type} \
                    with id {to_entity.id}.'''
                    self.logger.error(error_message)
                    self.logger.error(f'{str(link_error)}')
                    raise
