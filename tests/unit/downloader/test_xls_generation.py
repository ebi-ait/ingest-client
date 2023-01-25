import pytest

from assertpy import assert_that
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet

from hca_ingest.downloader.downloader import XlsDownloader
from hca_ingest.importer.spreadsheet.ingest_workbook import SCHEMAS_WORKSHEET

from .conftest import get_json_file


@pytest.fixture
def downloader():
    return XlsDownloader()


@pytest.fixture
def project_sheet_title():
    return 'Project'


@pytest.fixture
def project_json(project_sheet_title):
    return {
        project_sheet_title : {
            "headers": {
                "project.uuid": {},
                "project.project_core.project_short_name": {},
                "project.project_core.project_title": {},
                "project.project_core.project_description": {},
                "project.insdc_project_accessions": {},
                "project.geo_series_accessions": {},
                "project.insdc_study_accessions": {}
            },
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


def test_given_input_has_only_1_set_of_data_successfully_creates_a_workbook(project_json, downloader, project_sheet_title):
    # when
    workbook: Workbook = downloader.create_workbook(project_json)
    # expect
    assert_that(workbook.worksheets).is_length(2)
    assert_sheet(workbook, project_sheet_title, project_json)


@pytest.fixture
def contributors_sheet_title():
    return 'Project - Contributors'

@pytest.fixture
def contributors_json(contributors_sheet_title):
    return {
        contributors_sheet_title: {
            'headers': {
                "project.contributors.name": {},
                "project.contributors.email": {},
                "project.contributors.institution": {},
                "project.contributors.laboratory": {},
                "project.contributors.address": {},
                "project.contributors.country": {},
                "project.contributors.corresponding_contributor": {},
                "project.contributors.project_role.text": {},
                "project.contributors.project_role.ontology": {},
                "project.contributors.project_role.ontology_label": {},
                "project.contributors.orcid_id": {}
            },
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


def test_given_input_has_many_rows_of_data_successfully_creates_a_workbook(downloader, contributors_json, contributors_sheet_title):
    # when
    workbook: Workbook = downloader.create_workbook(contributors_json)
    # expect
    assert_that(workbook.worksheets).is_length(2)
    assert_sheet(workbook, contributors_sheet_title, contributors_json)


@pytest.fixture
def project_list_flattened(script_dir):
    return get_json_file(script_dir+ '/project-list-flattened.json')

def test_given_input_has_many_rows_and_many_sheets_of_data_successfully_creates_a_workbook(downloader, project_list_flattened):
    # given
    sheet_titles = ['Project', 'Project - Contributors', 'Project - Publications', 'Project - Funders']
    # when
    workbook: Workbook = downloader.create_workbook(project_list_flattened)
    # expect
    assert_that(workbook.worksheets).is_length(5)
    for sheet in sheet_titles:
        assert_sheet(workbook, sheet, project_list_flattened)


def test_given_input_create_workbook_with_schemas_worksheet(downloader, project_list_flattened):
    # when
    workbook: Workbook = downloader.create_workbook(project_list_flattened)
    # expect
    assert_schema(project_list_flattened, workbook)


def test_given_input_raises_error_when_no_schemas_worksheet(downloader):
    with pytest.raises(ValueError) as value_error:
        downloader.create_workbook({})
        assert_that(str(value_error.value)).is_equal_to("The schema urls are missing")


def assert_sheet(workbook, sheet_title, input_json):
    sheet: Worksheet = workbook[sheet_title]
    assert_that(sheet.title).is_equal_to(sheet_title)

    rows = list(sheet.rows)
    rows = rows[3:]
    header_row = rows.pop(0)
    for cell in header_row:
        assert_that(input_json[sheet_title]['headers']).contains_key(cell.value)

    rows.pop(0)
    input_values = input_json[sheet_title]['values']
    for i, row in enumerate(rows):
        for cell in row:
            assert_that(input_values[i]).contains_value(cell.value)


def assert_schema(project_list_flattened, workbook):
    assert_that(workbook.sheetnames).contains(SCHEMAS_WORKSHEET)
    expected_schemas = project_list_flattened.get(SCHEMAS_WORKSHEET)
    sheet: Worksheet = workbook[SCHEMAS_WORKSHEET]
    rows_iter = sheet.iter_rows(min_col=1, min_row=2, max_col=1, max_row=sheet.max_row)
    schemas = []
    for row in rows_iter:
        for cell in row:
            schemas.append(cell.value)
    assert_that(schemas).is_equal_to(expected_schemas)
