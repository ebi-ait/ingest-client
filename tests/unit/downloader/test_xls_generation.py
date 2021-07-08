import unittest

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from ingest.downloader.downloader import XlsDownloader


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.downloader = XlsDownloader()
        self.workbook = Workbook()
        self.workbook.create_sheet(title='Project')

    def test_given_input_json_successfully_creates_a_workbook(self):
        #given
        project_sheet_title = 'Project'
        input_json = {
            project_sheet_title: {
                'project.uuid': '3e329187-a9c4-48ec-90e3-cc45f7c2311c',
                'project.project_core.project_short_name': 'kriegsteinBrainOrganoids',
                'project.project_core.project_title': 'Establishing Cerebral Organoids as Models'
            }
        }

        workbook: Workbook = self.downloader.create_workbook(input_json)

        #expect
        self.assertEqual(len(workbook.worksheets), 1)
        project_sheet: Worksheet = workbook[project_sheet_title]
        self.assertEqual(project_sheet.title, project_sheet_title)

        rows = list(project_sheet.rows)
        header_row = rows.pop(0)
        for cell in header_row:
            self.assertTrue(cell.value in input_json[project_sheet_title].keys())

        for row in rows:
            for cell in row:
                self.assertTrue(cell.value in input_json[project_sheet_title].values())


if __name__ == '__main__':
    unittest.main()
