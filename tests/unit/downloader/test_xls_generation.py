import unittest

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from ingest.downloader.downloader import XlsDownloader


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.downloader = XlsDownloader()
        self.workbook = Workbook()
        self.workbook.create_sheet(title='Project')

    def test_given_input_has_only_1_set_of_data_successfully_creates_a_workbook(self):
        #given
        project_sheet_title = 'Project'
        input_json = {
            project_sheet_title: {
                'project.uuid': '3e329187-a9c4-48ec-90e3-cc45f7c2311c',
                'project.project_core.project_short_name': 'kriegsteinBrainOrganoids',
                'project.project_core.project_title': 'Establishing Cerebral Organoids as Models'
            }
        }

        # when
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

    def test_given_input_has_many_rows_of_data_successfully_creates_a_workbook(self):
        #given
        contributors_sheet_title = 'Project - Contributors'

        input_json = {
            contributors_sheet_title: [
                {
                    'project.contributors.name': 'Karel Gott',
                    'project.contributors.email': 'karel@gott.com',
                    'project.contributors.institution': 'University of Prague',
                    'project.contributors.laboratory': 'Department of Neurology',
                    'project.contributors.address': '123 Test Street',
                    'project.contributors.country': 'UK',
                    'project.contributors.corresponding_contributor': 'yes',
                    'project.contributors.project_role.text': 'experimental scientist',
                    'project.contributors.project_role.ontology': 'EFO:0009741',
                    'project.contributors.project_role.ontology_label': 'experimental scientist',
                    'project.contributors.orcid_id': 'https://orcid.org/1234-5678-9012-3456'
                },
                {
                    'project.contributors.name': 'Lady Carneval',
                    'project.contributors.email': 'ladyc@gott.com',
                    'project.contributors.institution': 'University of Prague',
                    'project.contributors.laboratory': 'Department of Neurology',
                    'project.contributors.address': '123 Test Street',
                    'project.contributors.country': 'UK',
                    'project.contributors.corresponding_contributor': 'yes',
                    'project.contributors.project_role.text': 'experimental scientist',
                    'project.contributors.project_role.ontology': 'EFO:0009741',
                    'project.contributors.project_role.ontology_label': 'experimental scientist',
                    'project.contributors.orcid_id': 'https://orcid.org/9999-9999-9999-9999'
                },
                {
                    'project.contributors.name': 'Baby Shark',
                    'project.contributors.email': 'sharkb@gott.com',
                    'project.contributors.institution': 'University of Cambridge',
                    'project.contributors.laboratory': 'Department of Neurology',
                    'project.contributors.address': '123 Ocean Drive',
                    'project.contributors.country': 'USA',
                    'project.contributors.corresponding_contributor': 'no',
                    'project.contributors.project_role.text': 'data wrangler',
                    'project.contributors.project_role.ontology': 'EFO:0009737',
                    'project.contributors.project_role.ontology_label': 'data curator',
                    'project.contributors.orcid_id': 'https://orcid.org/0000-0000-0000-0001'
                },
            ]
        }

        # when
        workbook: Workbook = self.downloader.create_workbook(input_json)

        #expect
        self.assertEqual(len(workbook.worksheets), 1)
        contributors_sheet: Worksheet = workbook[contributors_sheet_title]
        self.assertEqual(contributors_sheet.title, contributors_sheet_title)

        rows = list(contributors_sheet.rows)
        header_row = rows.pop(0)
        for cell in header_row:
            self.assertTrue(cell.value in input_json[contributors_sheet_title][0].keys())

        i = 0
        for row in rows:
            for cell in row:
                self.assertTrue(cell.value in input_json[contributors_sheet_title][i].values())
            i += 1


if __name__ == '__main__':
    unittest.main()
