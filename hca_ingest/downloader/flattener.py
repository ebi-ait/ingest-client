import copy
from itertools import groupby
from typing import Iterable

from hca_ingest.downloader.entity import Entity
from hca_ingest.importer.spreadsheet.ingest_workbook import SCHEMAS_WORKSHEET

MODULE_WORKSHEET_NAME_CONNECTOR = ' - '
SCALAR_LIST_DELIMITER = '||'

ONTOLOGY_PROPS = ['ontology', 'ontology_label', 'text']
EXCLUDE_KEYS = ['describedBy', 'schema_type']


class Flattener:
    def __init__(self):
        self.workbook = {}
        self.schemas = {}

    def flatten(self, entity_list: Iterable[Entity]):
        self.workbook = {}
        self.schemas = {}
        for entity in entity_list:
            if entity.concrete_type != 'process':
                self.__flatten_entity(entity)
            self.__extract_schema_url(entity.content, entity.concrete_type)

        self.workbook[SCHEMAS_WORKSHEET] = list(self.schemas.values())
        flattened_json = copy.deepcopy(self.workbook)
        return flattened_json

    def __flatten_entity(self, entity: Entity):
        worksheet_name = entity.concrete_type
        row = {f'{worksheet_name}.uuid': entity.uuid}
        if not worksheet_name:
            raise ValueError('There should be a worksheet name')

        self.__flatten_any(entity.content, row, key=worksheet_name)

        if entity.input_biomaterials or entity.input_files:
            embedded_content = self.__embed_link_columns(entity)
            self.__flatten_any(embedded_content, row)

        self.__add_row_to_worksheet(row, worksheet_name)

    def __flatten_module_list(self, module_list: list, object_key: str):
        for module in module_list:
            self.__flatten_module(module, object_key)

    def __flatten_module(self, module: dict, object_key: str):
        worksheet_name = object_key
        module_row = {}

        if not worksheet_name:
            raise ValueError('There should be a worksheet name')

        self.__flatten_any(module, module_row, key=worksheet_name)
        self.__add_row_to_worksheet(module_row, worksheet_name)

    def __add_row_to_worksheet(self, row: dict, worksheet_name: str):
        user_friendly_worksheet_name = self.__format_worksheet_name(worksheet_name)
        worksheet = self.workbook.get(user_friendly_worksheet_name, {'headers': [], 'values': []})
        rows = worksheet.get('values')
        rows.append(row)
        headers = self.__update_headers(row, worksheet)
        self.workbook[user_friendly_worksheet_name] = {
            'headers': headers,
            'values': rows
        }

    def __extract_schema_url(self, content: dict, concrete_entity: str):
        schema_url = content.get('describedBy')
        existing_schema_url = self.schemas.get(concrete_entity)
        self.__validate_no_schema_version_conflicts(existing_schema_url, schema_url)

        if not existing_schema_url:
            self.schemas[concrete_entity] = schema_url

    def __flatten_any(self, content: any, flattened_object: dict, key: str = ''):
        if isinstance(content, dict):
            self.__flatten_dict(content, flattened_object, key)
        elif isinstance(content, list):
            self.__flatten_list(content, flattened_object, key)
        else:
            flattened_object[key] = str(content)

    def __flatten_dict(self, content: dict, flattened_object: dict, parent_key: str):
        for child_key, value in content.items():
            if child_key in EXCLUDE_KEYS:
                continue
            full_key = f'{parent_key}.{child_key}' if parent_key else child_key
            self.__flatten_any(value, flattened_object, key=full_key)

    def __flatten_list(self, content: list, flattened_object: dict, key: str):
        if self.__is_list_of_objects(content):
            self.__flatten_object_list(content, flattened_object, key)
        else:
            self.__flatten_scalar_list(content, flattened_object, key)

    def __flatten_object_list(self, content: list, flattened_object: dict, key: str):
        if self.__is_list_of_ontology_objects(content):
            self.__flatten_object_list_to_main_worksheet(content, flattened_object, key)
        elif self.__is_project(key):
            self.__flatten_module_list(content, key)
        else:
            self.__flatten_object_list_to_main_worksheet(content, flattened_object, key)

    def __flatten_object_list_to_main_worksheet(self, content: list, flattened_object: dict, parent_key: str):
        keys = self.__get_keys_of_a_list_of_object(content)
        for child_key in keys:
            values = [elem.get(child_key) for elem in content if elem.get(child_key)]
            full_key = f'{parent_key}.{child_key}' if parent_key else child_key
            self.__flatten_list(values, flattened_object, full_key)

    @staticmethod
    def __embed_link_columns(entity: Entity):
        embedded_content = {}
        Flattener.__embed_process(entity, embedded_content)
        Flattener.__embed_protocol_ids(entity, embedded_content)
        Flattener.__embed_input_ids(entity, embedded_content)
        return embedded_content

    @staticmethod
    def __embed_process(entity: Entity, embedded_content: dict):
        embed_process = {
            'process': {
                'uuid': entity.process.uuid
            }
        }
        embed_process['process'].update(entity.process.content)
        embedded_content.update(embed_process)

    @staticmethod
    def __embed_protocol_ids(entity: Entity, embedded_content: dict):
        protocols_by_type = {}
        for p in entity.protocols:
            p: Entity
            protocols = protocols_by_type.get(p.concrete_type, [])
            protocols.append(p)
            protocols_by_type[p.concrete_type] = protocols

        for concrete_type, protocols in protocols_by_type.items():
            protocol_ids = [p.content['protocol_core']['protocol_id'] for p in protocols]
            protocol_uuids = [p.uuid for p in protocols]
            embedded_protocol_ids = {
                concrete_type: {
                    'protocol_core': {
                        'protocol_id': protocol_ids
                    },
                    'uuid': protocol_uuids
                }
            }
            embedded_content.update(embedded_protocol_ids)

    @staticmethod
    def __embed_input_ids(entity: Entity, embedded_content: dict):
        Flattener.__embed_input_biomaterial_ids(embedded_content, entity)
        Flattener.__embed_input_file_ids(embedded_content, entity)

    @staticmethod
    def __embed_input_biomaterial_ids(embedded_content: dict, entity: Entity):
        for concrete_type, inputs_iter in groupby(entity.input_biomaterials, lambda e: e.concrete_type):
            inputs = list(inputs_iter)
            input_ids_ids = [i.content['biomaterial_core']['biomaterial_id'] for i in inputs]
            input_ids_uuids = [i.uuid for i in inputs]
            embedded_input_ids = {
                concrete_type: {
                    'biomaterial_core': {
                        'biomaterial_id': input_ids_ids
                    },
                    'uuid': input_ids_uuids
                }
            }
            embedded_content.update(embedded_input_ids)

    @staticmethod
    def __embed_input_file_ids(embedded_content: dict, entity: Entity):
        for concrete_type, inputs_iter in groupby(entity.input_files, lambda e: e.concrete_type):
            inputs = list(inputs_iter)
            input_ids_ids = [i.content['file_core']['file_name'] for i in inputs]
            input_ids_uuids = [i.uuid for i in inputs]
            embedded_input_ids = {
                concrete_type: {
                    'file_core': {
                        'file_name': input_ids_ids
                    },
                    'uuid': input_ids_uuids
                }
            }
            embedded_content.update(embedded_input_ids)

    @staticmethod
    def __validate_no_schema_version_conflicts(existing_schema_url: str, schema_url: str):
        if existing_schema_url and existing_schema_url != schema_url:
            raise ValueError(f'The concrete entity schema version should be consistent across entities.\
                    Multiple versions of same concrete entity schema is found:\
                     {schema_url} and {existing_schema_url}')

    @staticmethod
    def __update_headers(row: dict, worksheet: dict):
        headers = worksheet.get('headers')
        for key in row.keys():
            if key not in headers:
                headers.append(key)
        return headers

    @staticmethod
    def __flatten_scalar_list(scalar_list: list, flattened_object: dict, key: str):
        stringified = [str(scalar_item) for scalar_item in scalar_list]
        flattened_object[key] = SCALAR_LIST_DELIMITER.join(stringified)

    @staticmethod
    def __format_worksheet_name(worksheet_name: str):
        names = worksheet_name.split('.')
        names = [n.replace('_', ' ') for n in names]
        new_worksheet_name = MODULE_WORKSHEET_NAME_CONNECTOR.join([n.capitalize() for n in names])
        return new_worksheet_name

    @staticmethod
    def __is_list_of_objects(content: list):
        return content and isinstance(content[0], dict)

    @staticmethod
    def __is_list_of_ontology_objects(content: list):
        first_elem = content[0] if content else {}
        result = [prop in first_elem for prop in ONTOLOGY_PROPS]
        # TODO better check the schema if field is ontology
        return any(result)

    @staticmethod
    def __get_keys_of_a_list_of_object(objects: list) -> Iterable[str]:
        keys_obj = {}
        for obj in objects:
            if obj:
                keys_obj.update(obj)
        return list(keys_obj.keys())

    @staticmethod
    def __is_project(key: str):
        entity_type = key.split('.')[0]
        return entity_type == 'project'
