from itertools import groupby
from typing import List

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

    def flatten(self, entity_list: List[Entity]):
        self.schemas = {}
        for entity in entity_list:
            if entity.concrete_type != 'process':
                self._flatten_entity(entity)
            self._extract_schema_url(entity.content, entity.concrete_type)

        self.workbook[SCHEMAS_WORKSHEET] = list(self.schemas.values())
        return self.workbook

    def _flatten_entity(self, entity: Entity):
        worksheet_name = entity.concrete_type
        row = {f'{worksheet_name}.uuid': entity.uuid}

        if not worksheet_name:
            raise ValueError('There should be a worksheet name')

        self._flatten_object(entity.content, row, parent_key=worksheet_name)

        if entity.input_biomaterials or entity.input_files:
            embedded_content = self.embed_link_columns(entity)
            self._flatten_object(embedded_content, row)

        self._add_row_to_worksheet(row, worksheet_name)

    def _flatten_module_list(self, module_list: dict, object_key: str):
        for module in module_list:
            self._flatten_module(module, object_key)

    def _flatten_module(self, obj: dict, object_key: str):
        worksheet_name = object_key
        module_row = {}

        if not worksheet_name:
            raise ValueError('There should be a worksheet name')

        self._flatten_object(obj, module_row, parent_key=worksheet_name)

        self._add_row_to_worksheet(module_row, worksheet_name)

    def _add_row_to_worksheet(self, row, worksheet_name):
        user_friendly_worksheet_name = self._format_worksheet_name(worksheet_name)
        worksheet = self.workbook.get(user_friendly_worksheet_name, {'headers': [], 'values': []})
        rows = worksheet.get('values')
        rows.append(row)
        headers = self._update_headers(row, worksheet)
        self.workbook[user_friendly_worksheet_name] = {
            'headers': headers,
            'values': rows
        }

    def embed_link_columns(self, entity: Entity):
        embedded_content = {}
        self._embed_process(entity, embedded_content)
        self._embed_protocol_ids(entity, embedded_content)
        self._embed_input_ids(entity, embedded_content)
        return embedded_content

    def _embed_process(self, entity: Entity, embedded_content):
        embed_process = {
            'process': {
                'uuid': entity.process.uuid
            }
        }
        embed_process['process'].update(entity.process.content)
        embedded_content.update(embed_process)

    def _embed_protocol_ids(self, entity: Entity, embedded_content):
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

    def _embed_input_ids(self, entity: Entity, embedded_content):
        self._embed_input_biomaterial_ids(embedded_content, entity)
        self._embed_input_file_ids(embedded_content, entity)

    def _embed_input_biomaterial_ids(self, embedded_content, entity):
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

    def _embed_input_file_ids(self, embedded_content, entity):
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

    def _extract_schema_url(self, content: dict, concrete_entity: str):
        schema_url = content.get('describedBy')
        existing_schema_url = self.schemas.get(concrete_entity)
        self._validate_no_schema_version_conflicts(existing_schema_url, schema_url)

        if not existing_schema_url:
            self.schemas[concrete_entity] = schema_url

    def _validate_no_schema_version_conflicts(self, existing_schema_url, schema_url):
        if existing_schema_url and existing_schema_url != schema_url:
            raise ValueError(f'The concrete entity schema version should be consistent across entities.\
                    Multiple versions of same concrete entity schema is found:\
                     {schema_url} and {existing_schema_url}')

    def _update_headers(self, row, worksheet):
        headers = worksheet.get('headers')
        for key in row.keys():
            if key not in headers:
                headers.append(key)
        return headers

    def _flatten_object(self, object: dict, flattened_object: dict, parent_key: str = ''):
        if isinstance(object, dict):
            for key in object:
                if key in EXCLUDE_KEYS:
                    continue

                value = object[key]
                full_key = f'{parent_key}.{key}' if parent_key else key
                if isinstance(value, dict) or isinstance(value, list):
                    self._flatten_object(value, flattened_object, parent_key=full_key)
                else:
                    flattened_object[full_key] = str(value)
        elif isinstance(object, list):
            self._flatten_list(flattened_object, object, parent_key)

    def _flatten_list(self, flattened_object, object, parent_key):
        if self._is_list_of_objects(object):
            self._flatten_object_list(flattened_object, object, parent_key)
        else:
            self._flatten_scalar_list(flattened_object, object, parent_key)

    def _flatten_scalar_list(self, flattened_object, object, parent_key):
        stringified = [str(e) for e in object]
        flattened_object[parent_key] = SCALAR_LIST_DELIMITER.join(stringified)

    def _flatten_object_list(self, flattened_object: dict, object: dict, parent_key: str):
        if self._is_list_of_ontology_objects(object):
            self._flatten_to_include_object_list_to_main_entity_worksheet(object, flattened_object, parent_key)
        elif self._is_project(parent_key):
            self._flatten_module_list(object, parent_key)
        else:
            self._flatten_to_include_object_list_to_main_entity_worksheet(object, flattened_object, parent_key)

    def _flatten_to_include_object_list_to_main_entity_worksheet(self, object: dict, flattened_object: dict,
                                                                 parent_key: str):
        keys = self._get_keys_of_a_list_of_object(object)

        for key in keys:
            flattened_object[f'{parent_key}.{key}'] = SCALAR_LIST_DELIMITER.join(
                [elem.get(key) for elem in object
                 if elem.get(key) is not None and elem.get(key) != ''])

    def _format_worksheet_name(self, worksheet_name):
        names = worksheet_name.split('.')
        names = [n.replace('_', ' ') for n in names]
        new_worksheet_name = MODULE_WORKSHEET_NAME_CONNECTOR.join([n.capitalize() for n in names])
        return new_worksheet_name

    def _is_list_of_objects(self, content):
        return content and isinstance(content[0], dict)

    def _is_list_of_ontology_objects(self, object: dict):
        first_elem = object[0] if object else {}
        result = [prop in first_elem for prop in ONTOLOGY_PROPS]
        # TODO better check the schema if field is ontology
        return any(result)

    def _get_keys_of_a_list_of_object(self, object: dict):
        first_elem = object[0] if object else {}
        return list(first_elem.keys())

    def _is_project(self, parent_key: str):
        entity_type = parent_key.split('.')[0]
        return entity_type == 'project'
