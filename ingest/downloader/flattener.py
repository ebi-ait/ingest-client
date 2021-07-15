from typing import List

ONTOLOGY_REQUIRED_PROPS = ['ontology', 'ontology_label']
EXCLUDE_KEYS = ['describedBy', 'schema_type']


class Flattener:
    def __init__(self):
        self.workbook = {}

    def flatten(self, entity_list: List[dict], object_key: str = ''):
        for entity in entity_list:
            self.flatten_entity(entity, object_key)
        return self.workbook

    def flatten_entity(self, entity, object_key):
        worksheet_name = object_key
        row = {}
        content = entity
        if not object_key:
            content = entity['content']
            worksheet_name = self.get_concrete_entity(content)
            row = {f'{worksheet_name}.uuid': entity['uuid']['uuid']}
        if not worksheet_name:
            raise Exception('There should be a worksheet name')
        user_friendly_worksheet_name = self._format_worksheet_name(worksheet_name)
        rows = self.workbook.get(user_friendly_worksheet_name, [])
        self.workbook[user_friendly_worksheet_name] = rows
        self.flatten_object(content, row, parent_key=worksheet_name)
        rows.append(row)

    def flatten_object(self, object: dict, flattened_object: dict, parent_key: str = ''):
        if isinstance(object, dict):
            for key in object:
                if key in EXCLUDE_KEYS:
                    continue

                value = object[key]
                full_key = f'{parent_key}.{key}' if parent_key else key
                if isinstance(value, dict) or isinstance(value, list):
                    self.flatten_object(value, flattened_object, parent_key=full_key)
                else:
                    flattened_object[full_key] = str(value)
        elif isinstance(object, list):
            self.flatten_list(flattened_object, object, parent_key)

    def flatten_list(self, flattened_object, object, parent_key):
        if self._is_list_of_objects(object):
            self.flatten_object_list(flattened_object, object, parent_key)
        else:
            self.flatten_scalar_list(flattened_object, object, parent_key)

    def flatten_scalar_list(self, flattened_object, object, parent_key):
        stringified = [str(e) for e in object]
        flattened_object[parent_key] = '||'.join(stringified)

    def flatten_object_list(self, flattened_object: dict, object: dict, parent_key: str):
        if self._is_list_of_ontology_objects(object):
            self.flatten_ontology_list(object, flattened_object, parent_key)
        else:
            self.flatten(object, parent_key)

    def flatten_ontology_list(self, object: dict, flattened_object:dict, parent_key: str):
        keys = self._get_keys_of_a_list_of_object(object)
        for key in keys:
            flattened_object[f'{parent_key}.{key}'] = '||'.join([elem[key] for elem in object])

    def _format_worksheet_name(self, worksheet_name):
        names = worksheet_name.split('.')
        names = [n.replace('_', ' ') for n in names]
        new_worksheet_name = ' - '.join([n.capitalize() for n in names])
        return new_worksheet_name

    def _is_list_of_objects(self, content):
        return content and isinstance(content[0], dict)

    def _is_list_of_ontology_objects(self, object: dict):
        first_elem = object[0] if object else {}
        result = [prop in first_elem for prop in ONTOLOGY_REQUIRED_PROPS]
        return all(result)

    def _get_keys_of_a_list_of_object(self, object: dict):
        first_elem = object[0] if object else {}
        return list(first_elem.keys())

    @staticmethod
    def get_concrete_entity(content):
        return content.get('describedBy').rsplit('/', 1)[-1]