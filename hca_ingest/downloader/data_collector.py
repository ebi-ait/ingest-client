import logging
from itertools import chain
from operator import itemgetter
from typing import Dict

from hca_ingest.api.ingestapi import IngestApi
from .entity import Entity


class DataCollector:
    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid) -> Dict[str, Entity]:
        submission = self.api.get_submission_by_uuid(submission_uuid)
        entity_dict = self.__build_entity_dict(submission)
        return entity_dict

    def __build_entity_dict(self, submission):
        data_by_submission = self.__get_submission_data(submission)
        entity_dict = {}
        linking_map = {
            "processes": {},
            "protocols": {},
            "biomaterials": {},
            "files": {},
        }
        for entity_number, entity_json in enumerate(data_by_submission):
            if entity_number % 1000 == 0:
                logging.info(f'read {entity_number} entities')
            entity = Entity(entity_json)
            entity_dict[entity.id] = entity
            self.add_to_linking_map(entity, entity_json, linking_map)
        self.__set_inputs(entity_dict, linking_map)
        return entity_dict

    @staticmethod
    def add_to_linking_map(entity, entity_json, linking_map):
        if entity.schema.domain_type in ('project', 'protocol'):
            return

        if entity.schema.domain_type in ['file', 'biomaterial']:
            entity_links = linking_map[entity.schema.domain_type + 's'].setdefault(entity.id, {})
            entity_links.setdefault('derivedByProcesses', [])\
                .extend(map(itemgetter('id'), entity_json['derivedByProcesses']))
            for process_id in map(itemgetter('id'), entity_json['inputToProcesses']):
                entity_links.setdefault('inputToProcesses', []).append(process_id)
                process_links = linking_map['processes'].setdefault(process_id, {})
                process_links.setdefault(f'input{entity.schema.domain_type.title()}s', []).append(entity.id)
        elif entity.schema.domain_type == 'process':
            entity_links = linking_map[entity.schema.domain_type + 'es'].setdefault(entity.id, {})
            entity_links.setdefault('protocols', []).extend(map(itemgetter('id'), entity_json['protocols']))

    def __get_submission_data(self, submission):
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        project_json = self.api.get_related_project(submission_id)
        if project_json:
            submission_data = [
                project_json
            ]
        else:
            raise Exception('There should be a project')

        biomaterials_iter = self.__get_entities_by_submission_and_type(submission, 'biomaterials', projection='withLinks')
        processes_iter = self.__get_entities_by_submission_and_type(submission, 'processes', projection='withLinks')
        protocols_iter = self.__get_entities_by_submission_and_type(submission, 'protocols')
        files_iter = self.__get_entities_by_submission_and_type(submission, 'files', projection='withLinks')
        return chain(submission_data, biomaterials_iter, processes_iter, protocols_iter, files_iter)

    def __get_linking_map(self, submission):
        linking_map_url = submission['_links']['linkingMap']['href']
        headers = {'Content-type': 'application/json', 'Accept': 'application/hal+json'}
        r = self.api.get(linking_map_url, headers=headers)
        r.raise_for_status()
        linking_map = r.json()
        return linking_map

    @staticmethod
    def __set_inputs(entity_dict, linking_map):
        entities_with_inputs = chain(linking_map['biomaterials'].keys(), linking_map['files'].keys())

        for entity_id in entities_with_inputs:
            entity = entity_dict[entity_id]
            entity_link = linking_map[entity.schema.domain_type + 's'][entity.id]
            derived_by_processes = entity_link.get('derivedByProcesses')

            if derived_by_processes and len(derived_by_processes) > 0:
                # Check if derivedByProcesses returns more than 1
                # It shouldn't happen because it's not possible to do it via spreadsheet
                if len(derived_by_processes) > 1:
                    raise ValueError(f'The {entity.schema.concrete_type} with {entity.uuid} '
                                     f'has more than one processes which derived it')

                process_id = entity_link['derivedByProcesses'][0]
                process_links = linking_map['processes'][process_id]
                protocol_ids = process_links.get('protocols',[])
                input_biomaterial_ids = process_links.get('inputBiomaterials', [])
                input_files_ids = process_links.get('inputFiles', [])

                process = entity_dict[process_id]
                protocols = [entity_dict[protocol_id] for protocol_id in protocol_ids]
                input_biomaterials = [entity_dict[id] for id in input_biomaterial_ids]
                input_files = [entity_dict[id] for id in input_files_ids]

                entity.set_input(input_biomaterials, input_files, process, protocols)

    def __get_entities_by_submission_and_type(self, submission, entity_type, projection=None):
        yield from self.api.get_related_entities(relation=entity_type,
                                                 entity=submission,
                                                 entity_type=entity_type,
                                                 projection=projection)
