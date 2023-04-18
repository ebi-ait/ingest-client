import os
from typing import Dict

from hca_ingest.api.ingestapi import IngestApi
from .entity import Entity


class DataCollector:
    def __init__(self, ingest_api: IngestApi):
        self.api = ingest_api

    def collect_data_by_submission_uuid(self, submission_uuid) -> Dict[str, Entity]:
        submission = self.api.get_submission_by_uuid(submission_uuid)
        projects_url = self.api.get_link_from_resource(submission, 'projects')
        project = next(self.api.get_all(projects_url, entity_type='projects'))
        submissions_url = self.api.get_link_from_resource(project, link_name='submissionEnvelopes')
        project_submissions = self.api.get_all(submissions_url, entity_type='submissionEnvelopes')

        entity_dict = {}
        project_linking_map = {}
        for sub in project_submissions:
            entity_dict = self.__build_entity_dict(sub, entity_dict)
            # TODO Need to merge all linking maps or have a linking map per project not by submission
            linking_map = self.__get_linking_map(sub)
            project_linking_map = self.__merge_linking_map(linking_map, project_linking_map)

        try:
            self.__set_inputs_using_linking_map(entity_dict, project_linking_map)
        except RuntimeError as e:
            raise RuntimeError(
                f"problem setting inputs of entities for submission {submission['uuid']['uuid']}: {str(e)}") from e

        return entity_dict

    def __build_entity_dict(self, submission, entity_dict=None) -> dict:
        data_by_submission = self.__get_submission_data(submission)
        entity_dict = entity_dict if entity_dict else {}
        try:
            for entity_json in data_by_submission:
                entity = Entity(entity_json)
                entity_dict[entity.id] = entity
        except RuntimeError as e:
            raise RuntimeError(
                f"problem building entity dictionary for submission {submission['uuid']['uuid']}: {str(e)}") from e
        return entity_dict

    def __get_submission_data(self, submission):
        submission_id = submission['_links']['self']['href'].split('/')[-1]
        project_json = self.api.get_related_project(submission_id)
        if project_json:
            submission_data = [
                project_json
            ]
        else:
            raise Exception('There should be a project')

        yield from submission_data
        yield from self.__get_entities_by_submission_and_type(submission, 'biomaterials')
        yield from self.__get_entities_by_submission_and_type(submission, 'processes')
        yield from self.__get_entities_by_submission_and_type(submission, 'protocols')
        yield from self.__get_entities_by_submission_and_type(submission, 'files')

    def __get_linking_map(self, submission):
        linking_map_url = submission['_links']['linkingMap']['href']
        headers = {'Content-type': 'application/json', 'Accept': 'application/hal+json'}
        r = self.api.get(linking_map_url, headers=headers)
        r.raise_for_status()
        linking_map = r.json()
        return linking_map

    @staticmethod
    def __set_inputs_using_linking_map(entity_dict, linking_map):
        entities_with_inputs = list(linking_map['biomaterials'].keys()) + list(
            linking_map['files'].keys())

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
                protocol_ids = linking_map['processes'][process_id]['protocols']
                input_biomaterial_ids = linking_map['processes'][process_id]['inputBiomaterials']
                input_files_ids = linking_map['processes'][process_id]['inputFiles']

                process = entity_dict[process_id]
                try:
                    protocols = [entity_dict[protocol_id] for protocol_id in protocol_ids]
                except Exception as e:
                    raise RuntimeError(
                        f'problem with process {process_id} and protocol: {str(e)}, for  entity {entity}') from e
                try:
                    input_biomaterials = [entity_dict[id] for id in input_biomaterial_ids]
                except Exception as e:
                    raise RuntimeError(
                        f'problem with process {process_id} and biomaterial: {str(e)}, for  entity {entity}') from e
                try:
                    input_files = [entity_dict[id] for id in input_files_ids]
                except Exception as e:
                    raise RuntimeError(
                        f'problem with process {process_id} and file: {str(e)}, for  entity {entity}') from e

                entity.set_input(input_biomaterials, input_files, process, protocols)

    def __set_inputs(self, entity_dict):
        for entity_id in entity_dict:
            entity = entity_dict[entity_id]
            if entity.schema.domain_type in ['biomaterial', 'file']:
                derived_by_processes = [Entity(entity) for entity in
                                        self.api.get_related_entities('derivedByProcesses', entity.entity_json,
                                                                      'processes')]

                if derived_by_processes and len(derived_by_processes) > 0:
                    # Check if derivedByProcesses returns more than 1
                    # It shouldn't happen because it's not possible to do it via spreadsheet
                    if len(derived_by_processes) > 1:
                        raise ValueError(f'The {entity.schema.concrete_type} with {entity.uuid} '
                                         f'has more than one processes which derived it')
                    process = derived_by_processes[0]
                    protocols = [Entity(protocol) for protocol in
                                 list(self.api.get_related_entities('protocols', process.entity_json, 'protocols'))]
                    input_biomaterials = [Entity(biomaterial) for biomaterial in list(
                        self.api.get_related_entities('inputBiomaterials', process.entity_json, 'biomaterials'))]
                    input_files = [Entity(file) for file in
                                   list(self.api.get_related_entities('inputFiles', process.entity_json, 'files'))]
                    entity.set_input(input_biomaterials, input_files, process, protocols)

    def __get_entities_by_submission_and_type(self, submission, entity_type):
        self.api.page_size = 10
        yield from self.api.get_related_entities(entity_type, submission, entity_type)

    def __merge_linking_map(self, src_linking_map, target_linking_map):
        link_fields_by_entity_type = {
            'biomaterials': ['derivedByProcesses', 'inputToProcesses'],
            'files': ['derivedByProcesses', 'inputToProcesses'],
            'processes': ['inputBiomaterials', 'inputFiles', 'protocols'],
            'protocols': []
        }
        target_linking_map = target_linking_map if target_linking_map else {}
        for entity_type in list(link_fields_by_entity_type.keys()):
            src_entity_type_map = src_linking_map.get(entity_type, {})
            if not target_linking_map.get(entity_type):
                target_linking_map[entity_type] = src_entity_type_map

            for entity_id in list(src_entity_type_map.keys()):
                src_entity_id_map = src_entity_type_map.get(entity_id, {})
                if not target_linking_map[entity_type].get(entity_id):
                    target_linking_map[entity_type][entity_id] = src_entity_id_map

                for field in list(src_entity_id_map.keys()):
                    src_field_list = src_entity_id_map.get(field, [])

                    if not target_linking_map[entity_type][entity_id].get(field):
                        target_linking_map[entity_type][entity_id][field] = src_field_list

                    src_set = set(src_field_list)
                    target_set = set(target_linking_map[entity_type][entity_id][field])
                    target_set.update(src_set)
                    target_linking_map[entity_type][entity_id][field] = list(target_set)

        return target_linking_map
