import copy
import json
import logging
from typing import List

from ingest.api.ingestapi import IngestApi
from ingest.importer.submission.entity import Entity
from ingest.importer.submission.entity_map import EntityMap
from ingest.importer.submission.submission import Submission, ENTITY_LINK


def json_equals(json1:dict, json2: dict):
    return json.dumps(json1, sort_keys=True) == json.dumps(json2, sort_keys=True)


class IngestSubmitter(object):
    def __init__(self, ingest_api: IngestApi):
        # TODO the IngestSubmitter should probably build its own instance of IngestApi
        self.ingest_api = ingest_api
        self.logger = logging.getLogger(__name__)
        self.PROGRESS_CTR = 50
        self.logger = logging.getLogger(__name__)

    def add_entities(self, entity_map: EntityMap, submission_url: str) -> Submission:
        submission = Submission(self.ingest_api, submission_url)
        submission.define_manifest(entity_map)
        entities = entity_map.get_new_entities()
        for e in entities:
            submission.add_entity(e)
        self.link_submission_to_project(entity_map, submission, submission_url)
        self.link_entities(entities, entity_map, submission)

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
            self.ingest_api.patch(entity.url, {'content': patch})

        return entity

    def link_submission_to_project(self, entity_map: EntityMap, submission: Submission, submission_url: str):
        project = entity_map.get_project()
        submission_envelope = self.ingest_api.get_submission(submission_url)
        submission_entity = Entity('submission_envelope',
                                   submission_envelope['uuid']['uuid'],
                                   None,
                                   is_linking_reference=True,
                                   ingest_json=submission_envelope
                                   )
        submission.link_entity(project, submission_entity, 'submissionEnvelopes')

    def link_entities(self, entities: List[Entity], entity_map: EntityMap, submission: Submission):
        progress = 0
        for entity in entities:
            for link in entity.direct_links:
                to_entity = entity_map.get_entity(link['entity'], link['id'])
                try:
                    submission.link_entity(entity, to_entity, relationship=link['relationship'],
                                           is_collection=link.get('is_collection', True))
                    progress = progress + 1
                    expected_links = int(submission.manifest.get('expectedLinks', 0))
                    if progress % self.PROGRESS_CTR == 0 or (progress == expected_links):
                        manifest_url = self.ingest_api.get_link_from_resource(submission.manifest, 'self')
                        self.ingest_api.patch(manifest_url, {'actualLinks': progress})
                        self.logger.info(f"links progress: {progress}/ {submission.manifest.get('expectedLinks')}")

                except Exception as link_error:
                    error_message = f'''The {entity.type} with id {entity.id} could not be linked to {to_entity.type} \
                    with id {to_entity.id}.'''
                    self.logger.error(error_message)
                    self.logger.error(f'{str(link_error)}')
                    raise

