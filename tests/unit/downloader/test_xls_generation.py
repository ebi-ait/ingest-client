import json
import unittest

from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from ingest.downloader.downloader import XlsDownloader


class XLSGenerationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.downloader = XlsDownloader()
        self.workbook = Workbook()
        self.workbook.create_sheet(title='Project')

    def test_given_input_has_only_1_set_of_data_successfully_creates_a_workbook(self):
        # given
        project_sheet_title = 'Project'
        input_json = self.__input_json1(project_sheet_title)

        # when
        workbook: Workbook = self.downloader.create_workbook(input_json)

        # expect
        self.assertEqual(len(workbook.worksheets), 2)

        self.__assert_sheet(workbook, project_sheet_title, input_json)

    def test_given_input_has_many_rows_of_data_successfully_creates_a_workbook(self):
        # given
        contributors_sheet_title = 'Project - Contributors'

        input_json = self.__input_json2(contributors_sheet_title)

        # when
        workbook: Workbook = self.downloader.create_workbook(input_json)

        # expect
        self.assertEqual(len(workbook.worksheets), 2)

        self.__assert_sheet(workbook, contributors_sheet_title, input_json)

    def test_given_input_has_many_rows_and_many_sheets_of_data_successfully_creates_a_workbook(self):
        # given
        sheet_titles = ['Project', 'Project - Contributors', 'Project - Publications', 'Project - Funders']

        # when
        with open('project-list-flattened.json') as file:
            input_json = json.load(file)

        # when
        workbook: Workbook = self.downloader.create_workbook(input_json)

        # expect
        self.assertEqual(len(workbook.worksheets), 5)

        map(lambda title: self.__assert_sheet(workbook, title, input_json), sheet_titles)

    def test_given_input_create_workbook_with_schemas_worksheet(self):
        # given
        with open('project-list-flattened.json') as file:
            input_json = json.load(file)

        # when
        workbook: Workbook = self.downloader.create_workbook(input_json)

        # expect
        self.assertTrue('Schemas' in workbook.sheetnames)
        sheet: Worksheet = workbook['Schemas']
        rows_iter = sheet.iter_rows(min_col=1, min_row=1, max_col=1, max_row=sheet.max_row)
        schemas = [[cell.value for cell in row] for row in rows_iter]
        self.assertTrue(schema in input_json.get('Schemas') for schema in schemas)




    @staticmethod
    def __input_json1(project_sheet_title):
        return {
            project_sheet_title: {
                "headers": [
                    "project.uuid",
                    "project.project_core.project_short_name",
                    "project.project_core.project_title",
                    "project.project_core.project_description",
                    "project.insdc_project_accessions",
                    "project.geo_series_accessions",
                    "project.insdc_study_accessions"
                ],
                "values": [
                    {
                        "project.uuid": "3e329187-a9c4-48ec-90e3-cc45f7c2311c",
                        "project.project_core.project_short_name": "kriegsteinBrainOrganoids",
                        "project.project_core.project_title": "Establishing Cerebral Organoids as Models of Human-Specific Brain Evolution",
                        "project.project_core.project_description": "Direct comparisons of human and non-human primate brain tissue have the potential to reveal molecular pathways underlying remarkable specializations of the human brain. However, chimpanzee tissue is largely inaccessible during neocortical neurogenesis when differences in brain size first appear. To identify human-specific features of cortical development, we leveraged recent innovations that permit generating pluripotent stem cell-derived cerebral organoids from chimpanzee. First, we systematically evaluated the fidelity of organoid models to primary human and macaque cortex, finding organoid models preserve gene regulatory networks related to cell types and developmental processes but exhibit increased metabolic stress. Second, we identified 261 genes differentially expressed in human compared to chimpanzee organoids and macaque cortex. Many of these genes overlap with human-specific segmental duplications and a subset suggest increased PI3K/AKT/mTOR activation in human outer radial glia. Together, our findings establish a platform for systematic analysis of molecular changes contributing to human brain development and evolution. Overall design: Single cell mRNA sequencing of iPS-derived neural and glial progenitor cells using the Fluidigm C1 system This series includes re-analysis of publicly available data in accessions: phs000989.v3, GSE99951, GSE86207, GSE75140. Sample metadata and accession IDs for the re-analyzed samples are included in the file \"GSE124299_metadata_on_processed_samples.xlsx\" available on the foot of this record. The following samples have no raw data due to data loss: GSM3569728, GSM3569738, GSM3571601, GSM3571606, GSM3571615, GSM3571621, GSM3571625, and GSM3571631",
                        "project.insdc_project_accessions": "SRP180337",
                        "project.geo_series_accessions": "GSE124299",
                        "project.insdc_study_accessions": "PRJNA515930"
                    }
                ]
            },
            "Schemas": ["dummy-schema-url"]
        }

    @staticmethod
    def __input_json2(contributors_sheet_title):
        return {
            contributors_sheet_title: {
                'headers': [
                    "project.contributors.name",
                    "project.contributors.email",
                    "project.contributors.institution",
                    "project.contributors.laboratory",
                    "project.contributors.address",
                    "project.contributors.country",
                    "project.contributors.corresponding_contributor",
                    "project.contributors.project_role.text",
                    "project.contributors.project_role.ontology",
                    "project.contributors.project_role.ontology_label",
                    "project.contributors.orcid_id"
                ],
                'values': [
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
                    }
                ]
            },
            "Schemas": ["dummy-schema-url"]
        }

    def __assert_sheet(self, workbook, sheet_title, input_json):
        sheet: Worksheet = workbook[sheet_title]
        self.assertEqual(sheet.title, sheet_title)

        rows = list(sheet.rows)
        rows = rows[3:]
        header_row = rows.pop(0)
        for cell in header_row:
            self.assertTrue(cell.value in input_json[sheet_title]['headers'])

        rows.pop(0)
        input_values = input_json[sheet_title]['values']
        for i, row in enumerate(rows):
            for cell in row:
                self.assertTrue(cell.value in input_values[i].values())


if __name__ == '__main__':
    unittest.main()
