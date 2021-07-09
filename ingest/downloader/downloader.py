from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

EXCLUDE_KEYS = ['describedBy', 'schema_type']


class XlsDownloader:
    def __init__(self):
        self.row = 1

    def convert_json(self, entity_list):
        workbook = {}

        for entity in entity_list:
            content = entity['content']
            concrete_type = self.get_concrete_entity(content)
            worksheet = workbook.get(concrete_type, [])
            workbook[concrete_type] = worksheet
            row = {f'{concrete_type}.uuid': entity['uuid']['uuid']}
            self.flatten(content, row, parent_key='project')
            worksheet.append(row)
        return workbook

    def flatten(self, content, output, parent_key=''):
        if isinstance(content, dict):
            for key in content:
                full_key = f'{parent_key}.{key}' if parent_key else key
                if key in EXCLUDE_KEYS:
                    continue
                value = content[key]

                if isinstance(value, dict) or isinstance(value, list):
                    self.flatten(value, output, parent_key=full_key)
                else:
                    output[full_key] = value
        elif isinstance(content, list):
            for elem in content:
                if isinstance(elem, dict):
                    self.flatten(elem, output, parent_key=parent_key)
                else:
                    stringified = [str(e) for e in content]
                    output[parent_key] = '||'.join(stringified)

    def get_concrete_entity(self, content):
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
                self.row = 1
                worksheet.cell(row=self.row, column=col, value=header)
                self.row += 1
            worksheet.cell(row=self.row, column=col, value=cell_value)
            col += 1
