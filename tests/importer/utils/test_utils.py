from openpyxl import Workbook
from mock import Mock, MagicMock
from ingest.importer.importer import WorkbookImporter
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook


def create_test_workbook(*worksheet_titles, include_default_sheet=False):
    workbook = Workbook()
    for title in worksheet_titles:
        workbook.create_sheet(title)

    if not include_default_sheet:
        default_sheet = workbook['Sheet']
        workbook.remove(default_sheet)

    return workbook


def create_workbook_importer(person):
    template_manager = MagicMock()
    template_manager.get_concrete_type = Mock(return_value=person)

    return WorkbookImporter(template_manager)


def create_ingest_workbook(person, column_names):
    header_row_idx = 4

    workbook = Workbook()
    worksheet = workbook.create_sheet(person)
    worksheet[f'A{header_row_idx}'] = f'{person}.{column_names[0]}'
    worksheet[f'B{header_row_idx}'] = f'{person}.{column_names[1]}'

    return IngestWorkbook(workbook)
