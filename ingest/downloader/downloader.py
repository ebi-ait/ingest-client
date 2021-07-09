from typing import List

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

EXCLUDE_KEYS = ['describedBy', 'schema_type']


class XlsDownloader:
    def __init__(self):
        self.workbook = {}

    def convert_json(self, entity_list: List[dict]):
        self._flatten_object_list(entity_list)
        return self.workbook

    def _flatten_object_list(self, object_list: List[dict], object_key: str = ''):
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

            rows = self.workbook.get(worksheet_name, [])
            self.workbook[worksheet_name] = rows
            self._flatten_object(content, row, parent_key=worksheet_name)
            rows.append(row)

    def _flatten_object(self, object: dict, flattened_object: dict, parent_key: str = ''):
        if isinstance(object, dict):
            for key in object:
                full_key = f'{parent_key}.{key}' if parent_key else key
                if key in EXCLUDE_KEYS:
                    continue
                value = object[key]
                if isinstance(value, dict) or isinstance(value, list):
                    self._flatten_object(value, flattened_object, parent_key=full_key)
                else:
                    flattened_object[full_key] = str(value)
        elif isinstance(object, list):
            if self.is_object_list(object):
                self._flatten_object_list(object, parent_key)
            else:
                stringified = [str(e) for e in object]
                flattened_object[parent_key] = '||'.join(stringified)

    def is_object_list(self, content):
        return content and isinstance(content[0], dict)

    @staticmethod
    def get_concrete_entity(content):
        return content.get('describedBy').rsplit('/', 1)[-1]

    @staticmethod
    def create_workbook(input_json: dict) -> Workbook:
        workbook = Workbook()
        workbook.remove(workbook.active)

        for ws_title, ws_elements in input_json.items():
            worksheet: Worksheet = workbook.create_sheet(title=ws_title)
            XlsDownloader.add_worksheet_content(worksheet, ws_elements)

        return workbook

    @staticmethod
    def add_worksheet_content(worksheet, ws_elements: dict):
        row = col = 1
        for header, cell_value in ws_elements.items():
            worksheet.cell(row=row, column=col, value=header)
            worksheet.cell(row=row + 1, column=col, value=cell_value)
            col += 1
