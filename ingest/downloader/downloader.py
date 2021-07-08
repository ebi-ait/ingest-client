from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

EXCLUDE_KEYS = ['describedBy', 'schema_type']


class XlsDownloader:
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
