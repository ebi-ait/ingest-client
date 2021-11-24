from typing import Dict

from ingest.api.ingestapi import IngestApi
from ingest.downloader.entity import Entity
from ingest.downloader.entity_map import EntityMap


class DataCollector:

    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid) -> Dict[str, Entity]:
        submission = self.api.get_submission_by_uuid(submission_uuid)
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        project_json = self.api.get_related_project(submission_id)

        if project_json:
            data_by_submission = [
                project_json
            ]
        else:
            raise Exception('There should be a project')

        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'biomaterials')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'processes')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'protocols')
        self.__get_entities_by_submission_and_type(data_by_submission, submission, 'files')

        linking_map_url = submission['_links']['linkingMap']['href']
        linking_map = self.api.get(linking_map_url)

        entity_dict = {}
        for entity_json in data_by_submission:
            entity = Entity.from_json(entity_json)
            entity_dict[entity.id] = entity

        entities_with_inputs = list(linking_map['biomaterials'].keys()) + list(linking_map['files'].keys())
        for entity_id in entities_with_inputs:
            entity = entity_dict[entity_id]
            entity_link = linking_map[entity.domain_type + 's'][entity.id]
            derived_by_processes = entity_link.get('derivedByProcesses')

            if derived_by_processes and len(derived_by_processes) > 0:
                # TODO check if derivedByProcesses returns more than 1
                # It shouldn't happen because it's not possible to do it via spreadsheet
                if len(derived_by_processes) > 1:
                    raise Exception(f'Found more than one derived by process for biomaterial {entity.id} {entity.uuid}')

                process_id = entity_link['derivedByProcesses'][0]
                protocol_ids = linking_map['processes'][process_id]['protocols']
                input_biomaterial_ids = linking_map['processes'][process_id]['inputBiomaterials']
                input_files_ids = linking_map['processes'][process_id]['inputFiles']

                process = entity_dict[process_id]
                protocols = [entity_dict[protocol_id] for protocol_id in protocol_ids]
                inputs = [entity_dict[id] for id in input_biomaterial_ids + input_files_ids]
                entity.set_input(inputs, process, protocols)

        return entity_dict

    def __get_entities_by_submission_and_type(self, data_by_submission, submission, entity_type):
        entity_json = \
            self.api.get_related_entities(entity_type, submission, entity_type)
        if entity_json:
            data_by_submission.extend(list(entity_json))
