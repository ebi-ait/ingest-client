from typing import List

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from ingest.downloader.flattener import Flattener

FIRST_DATA_ROW_NO = 4


class XlsDownloader:
    def __init__(self):
        self.row = FIRST_DATA_ROW_NO
        self.flattener = Flattener()

    def convert_json(self, metadata_list: List[dict]):
        return self.flattener.flatten(metadata_list)

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
