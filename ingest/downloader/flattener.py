from typing import List

MODULE_WORKSHEET_NAME_CONNECTOR = ' - '
SCALAR_LIST_DELIMETER = '||'

ONTOLOGY_PROPS = ['ontology', 'ontology_label', 'text']
EXCLUDE_KEYS = ['describedBy', 'schema_type']


class Flattener:
    def __init__(self):
        self.workbook = {}

    def flatten(self, entity_list: List[dict], object_key: str = ''):
        for entity in entity_list:
            self._flatten_entity(entity, object_key)
        return self.workbook

    def _flatten_entity(self, entity, object_key):
        worksheet_name = object_key
        row = {}
        content = entity

        if not object_key:
            content = entity['content']
            worksheet_name = self._get_concrete_entity(content)
            row = {f'{worksheet_name}.uuid': entity['uuid']['uuid']}

        if not worksheet_name:
            raise Exception('There should be a worksheet name')

        self._flatten_object(content, row, parent_key=worksheet_name)

        user_friendly_worksheet_name = self._format_worksheet_name(worksheet_name)
        worksheet = self.workbook.get(user_friendly_worksheet_name, {'headers': [], 'values': []})

        rows = self._append_row_to_worksheet(row, worksheet)
        headers = self._update_headers(row, worksheet)

        self.workbook[user_friendly_worksheet_name] = {
            'headers': headers,
            'values': rows
        }

    def _append_row_to_worksheet(self, row, worksheet):
        rows = worksheet.get('values')
        rows.append(row)
        return rows

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
        flattened_object[parent_key] = SCALAR_LIST_DELIMETER.join(stringified)

    def _flatten_object_list(self, flattened_object: dict, object: dict, parent_key: str):
        if self._is_list_of_ontology_objects(object):
            self._flatten_to_include_object_list_to_main_entity_worksheet(object, flattened_object, parent_key)
        elif self._is_project(parent_key):
            self.flatten(object, parent_key)
        else:
            self._flatten_to_include_object_list_to_main_entity_worksheet(object, flattened_object, parent_key)

    def _flatten_to_include_object_list_to_main_entity_worksheet(self, object: dict, flattened_object: dict,
                                                                 parent_key: str):
        keys = self._get_keys_of_a_list_of_object(object)

        for key in keys:
            flattened_object[f'{parent_key}.{key}'] = SCALAR_LIST_DELIMETER.join(
                [elem.get(key, '') for elem in object if elem.get(key) is not None])

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

    def _get_concrete_entity(self, content: dict):
        return content.get('describedBy').rsplit('/', 1)[-1]

    def _is_project(self, parent_key: str):
        entity_type = parent_key.split('.')[0]
        return entity_type == 'project'
