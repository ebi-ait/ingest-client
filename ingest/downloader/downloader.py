from typing import List

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

ONTOLOGY_REQUIRED_PROPS = ['ontology', 'ontology_label']
EXCLUDE_KEYS = ['describedBy', 'schema_type']
FIRST_DATA_ROW_NO = 4


class XlsDownloader:
    def __init__(self):
        self.workbook = {}
        self.row = FIRST_DATA_ROW_NO

    def convert_json(self, entity_list: List[dict]):
        self._flatten(entity_list)
        return self.workbook

    def _flatten(self, object_list: List[dict], object_key: str = ''):
        for entity in object_list:
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
            self._flatten_object(content, row, parent_key=worksheet_name)
            rows.append(row)

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
            if self.is_list_of_objects(object):
                self._flatten_object_list(flattened_object, object, parent_key)
            else:
                self._flatten_scalar_list(flattened_object, object, parent_key)

    def _flatten_scalar_list(self, flattened_object, object, parent_key):
        stringified = [str(e) for e in object]
        flattened_object[parent_key] = '||'.join(stringified)

    def _flatten_object_list(self, flattened_object, object, parent_key):
        if self.is_list_of_ontology_objects(object):
            self._flatten_ontology_list(object, flattened_object, parent_key)
        else:
            self._flatten(object, parent_key)

    def _flatten_ontology_list(self, object, flattened_object, parent_key):
        keys = self.get_keys_of_a_list_of_object(object)
        for key in keys:
            flattened_object[f'{parent_key}.{key}'] = '||'.join([elem[key] for elem in object])

    def _format_worksheet_name(self, worksheet_name):
        names = worksheet_name.split('.')
        names = [n.replace('_', ' ') for n in names]
        new_worksheet_name = ' - '.join([n.capitalize() for n in names])
        return new_worksheet_name

    def is_list_of_objects(self, content):
        return content and isinstance(content[0], dict)

    @staticmethod
    def get_concrete_entity(content):
        return content.get('describedBy').rsplit('/', 1)[-1]

    def create_workbook(self, input_json: dict) -> Workbook:
        workbook = Workbook()
        workbook.remove(workbook.active)

        for ws_title, ws_elements in input_json.items():
            worksheet: Worksheet = workbook.create_sheet(title=ws_title)
            self.add_worksheet_content(worksheet, ws_elements)

        return workbook

    def add_worksheet_content(self, worksheet, ws_elements: dict):
        is_header = True
        if isinstance(ws_elements, list):
            for content in ws_elements:
                self.add_row_content(worksheet, content, is_header)
                is_header = False
                self.row += 1
        else:
            self.add_row_content(worksheet, ws_elements)

    def add_row_content(self, worksheet, content, is_header=True):
        col = 1
        for header, cell_value in content.items():
            if is_header:
                self.row = FIRST_DATA_ROW_NO
                worksheet.cell(row=self.row, column=col, value=header)
                self.row += 1
            worksheet.cell(row=self.row, column=col, value=cell_value)
            col += 1

    def is_list_of_ontology_objects(self, object: dict):
        first_elem = object[0] if object else {}
        result = [prop in first_elem for prop in ONTOLOGY_REQUIRED_PROPS]
        return all(result)

    def get_keys_of_a_list_of_object(self, object: dict):
        first_elem = object[0] if object else {}
        return list(first_elem.keys())
