from typing import List

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from ingest.downloader.flattener import Flattener
from ingest.importer.spreadsheet.ingest_worksheet import START_DATA_ROW

HEADER_ROW_NO = 4


class XlsDownloader:
    def __init__(self):
        self.row = HEADER_ROW_NO
        self.flattener = Flattener()

    def convert_json(self, metadata_list: List[dict]):
        return self.flattener.flatten(metadata_list)

    def create_workbook(self, input_json: dict) -> Workbook:
        workbook = Workbook()
        workbook.remove(workbook.active)

        for ws_title, ws_elements in input_json.items():
            if ws_title == 'Project':
                worksheet: Worksheet = workbook.create_sheet(title=ws_title, index=0)
            else:
                worksheet: Worksheet = workbook.create_sheet(title=ws_title)

            self.add_worksheet_content(worksheet, ws_elements)

        return workbook

    def add_worksheet_content(self, worksheet, ws_elements: dict):
        headers = ws_elements.get('headers')
        self.__add_header_row(worksheet, headers)
        all_values = ws_elements.get('values')

        self.row = START_DATA_ROW - 1
        for row_values in all_values:
            self.row += 1
            self.__add_row_content(worksheet, headers, row_values)

    def __add_header_row(self, worksheet, headers: list):
        self.row = HEADER_ROW_NO
        col = 1
        for header in headers:
            worksheet.cell(row=self.row, column=col, value=header)
            col += 1

    def __add_row_content(self, worksheet, headers: list, values: dict):
        for header, value in values.items():
            index = headers.index(header)
            worksheet.cell(row=self.row, column=index+1, value=value)
