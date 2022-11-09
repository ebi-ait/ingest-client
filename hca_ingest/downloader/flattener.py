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

        row.update(self.__flatten_any(entity.content, key=worksheet_name))

        if entity.input_biomaterials or entity.input_files:
            link_columns = self.__get_link_columns(entity)
            row.update(self.__flatten_any(link_columns))

        self.__add_row_to_worksheet(row, worksheet_name)

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

    def __flatten_any(self, content: any, key: str = '') -> dict:
        if isinstance(content, dict):
            return self.__flatten_dict(content, key)
        if isinstance(content, list):
            return self.__flatten_list(content, key)
        return {key: str(content)}

    def __flatten_dict(self, content: dict, parent_key: str) -> dict:
        flattened_object = {}
        for child_key, value in content.items():
            if child_key in EXCLUDE_KEYS:
                continue
            full_key = f'{parent_key}.{child_key}' if parent_key else child_key
            flattened_object.update(self.__flatten_any(value, key=full_key))
        return flattened_object

    def __flatten_list(self, content: list, key: str) -> dict:
        if self.__is_list_of_objects(content):
            return self.__flatten_object_list(content, key)
        return {key: self.__flatten_scalar_list(content)}

    def __flatten_object_list(self, content: list, key: str) -> dict:
        if self.__is_list_of_ontology_objects(content):
            return self.__flatten_object_list_to_main_worksheet(content, key)
        if self.__is_project(key):
            self.__flatten_module_list(content, key)
            return {}
        return self.__flatten_object_list_to_main_worksheet(content, key)

    def __flatten_object_list_to_main_worksheet(self, content: list, parent_key: str) -> dict:
        keys = self.__get_keys_of_a_list_of_object(content)
        flattened_object = {}
        for child_key in keys:
            values = [elem.get(child_key) for elem in content if elem.get(child_key)]
            full_key = f'{parent_key}.{child_key}' if parent_key else child_key
            flattened_object.update(self.__flatten_list(values, full_key))
        return flattened_object

    def __flatten_module_list(self, module_list: list, object_key: str):
        for module in module_list:
            self.__flatten_module(module, object_key)

    def __flatten_module(self, module: dict, object_key: str):
        worksheet_name = object_key
        if not worksheet_name:
            raise ValueError('There should be a worksheet name')

        flat_module = self.__flatten_any(module, key=worksheet_name)
        self.__add_row_to_worksheet(flat_module, worksheet_name)

    @staticmethod
    def __get_link_columns(entity: Entity) -> dict:
        link_columns = {}
        link_columns.update(Flattener.__get_concrete_process(entity))
        link_columns.update(Flattener.__get_concrete_ids(entity.protocols, 'protocol_core', 'protocol_id'))
        link_columns.update(Flattener.__get_concrete_ids(entity.input_biomaterials, 'biomaterial_core', 'biomaterial_id'))
        link_columns.update(Flattener.__get_concrete_ids(entity.input_files, 'file_core', 'file_name'))
        return link_columns

    @staticmethod
    def __get_concrete_process(entity: Entity) -> dict:
        process = {
            'process': {
                'uuid': entity.process.uuid
            }
        }
        process['process'].update(entity.process.content)
        return process

    @staticmethod
    def __get_concrete_ids(input_entities: Iterable[Entity], core_name: str, id_name: str):
        concrete_ids = {}
        for concrete_type, inputs_iter in groupby(input_entities, lambda e: e.concrete_type):
            inputs = list(inputs_iter)
            input_ids = [i.content[core_name][id_name] for i in inputs]
            input_uuids = [i.uuid for i in inputs]
            concrete_ids.update({
                concrete_type: {
                    core_name: {
                        id_name: input_ids
                    },
                    'uuid': input_uuids
                }
            })
        return concrete_ids

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
    def __flatten_scalar_list(scalar_list: list) -> str:
        stringified = [str(scalar_item) for scalar_item in scalar_list]
        return SCALAR_LIST_DELIMITER.join(stringified)

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
