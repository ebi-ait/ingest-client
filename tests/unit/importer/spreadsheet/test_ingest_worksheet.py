from unittest import TestCase
from unittest.mock import Mock

import hca_ingest.utils.spreadsheet as spreadsheet_utils
from hca_ingest.importer.spreadsheet.ingest_worksheet import IngestWorksheet
from tests.utils import create_test_workbook


class IngestWorksheetTest(TestCase):

    def test_get_title(self):
        # given:
        workbook = create_test_workbook('User', 'User - SN Profiles')
        user_sheet = workbook['User']
        sn_profiles_sheet = workbook['User - SN Profiles']

        # and:
        user = IngestWorksheet(user_sheet)
        sn_profiles = IngestWorksheet(sn_profiles_sheet)

        # expect:
        self.assertEqual('User', user.title)
        self.assertEqual('User - SN Profiles', sn_profiles.title)

    def test_get_column_headers(self):
        # given:
        header_row = 4
        rows = [['name', 'address', 'mobile', 'email']]

        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 4)
        self.assertEqual(column_headers, ['name', 'address', 'mobile', 'email'])

    def test_get_column_headers_trim_whitespace(self):
        # given:
        header_row = 4
        rows = [[' name ', 'address        ', ' ', ' email']]

        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 4)
        self.assertEqual(column_headers, ['name', 'address', '', 'email'])

    def test_get_column_headers_includes_blank_cells(self):
        # given:
        header_row = 4
        rows = [['name', 'address', '', 'email']]
        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 4)
        self.assertEqual(column_headers, ['name', 'address', '', 'email'])

    def test_get_column_headers_skip_none_cells(self):
        # given:
        header_row = 4
        rows = [['name', 'address', None, 'email']]

        worksheet = spreadsheet_utils.create_worksheet('person', rows, start_row=header_row)

        # when:
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row)
        column_headers = ingest_worksheet.get_column_headers()

        # then:
        self.assertEqual(len(column_headers), 3)
        self.assertEqual(column_headers, ['name', 'address', 'email'])

    def test_get_data_rows(self):
        # given:
        start_row_idx = 6
        header_row_idx = 4

        header_row = ['name', 'address', 'mobile', 'email']
        expected_data_row = ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']

        rows = [[], [], [], [], [], []]  # initialise 6 rows
        rows[header_row_idx - 1] = header_row
        rows[start_row_idx - 1] = expected_data_row

        worksheet = spreadsheet_utils.create_worksheet('person', rows)

        # when
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row_idx)
        data_rows = ingest_worksheet.get_data_rows(start_row=start_row_idx)

        data_row_values = []
        for row in data_rows:
            cell_values = [cell.value for cell in row.values]
            data_row_values.append(cell_values)

        # then:
        self.assertEqual(len(data_row_values), 1)
        self.assertEqual(data_row_values, [expected_data_row])

    def test_get_data_rows_skip_blank_rows(self):
        # given:
        start_row_idx = 6
        header_row_idx = 4

        header_row = ['name', 'address', 'mobile', 'email']
        expected_data_row = ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']
        blank_row = [None, None, None, None]

        rows = [[], [], [], [], [], [], []]  # initialise 7 rows
        rows[header_row_idx - 1] = header_row
        rows[start_row_idx - 1] = expected_data_row
        rows[start_row_idx] = blank_row

        worksheet = spreadsheet_utils.create_worksheet('person', rows)

        # when
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=header_row_idx)
        data_rows = ingest_worksheet.get_data_rows(start_row=start_row_idx)

        data_row_values = []
        for row in data_rows:
            cell_values = [cell.value for cell in row.values]
            data_row_values.append(cell_values)

        # then:
        self.assertEqual(len(data_row_values), 1)
        self.assertEqual(data_row_values, [expected_data_row])

    def test_is_module_tab(self):
        # given:
        workbook = create_test_workbook('Product', 'Product - History')
        product_sheet = workbook['Product']
        history_sheet = workbook['Product - History']

        # and:
        product = IngestWorksheet(product_sheet)
        history = IngestWorksheet(history_sheet)

        # expect:
        self.assertFalse(product.is_module_tab())
        self.assertTrue(history.is_module_tab())

    def test_get_module_field_name(self):
        # given:
        workbook = create_test_workbook('Product - Reviews', 'User - SN Profiles',
                                        'Log - file-names', 'Account')

        # and: simple
        reviews_sheet = workbook['Product - Reviews']
        reviews = IngestWorksheet(reviews_sheet)

        # and: with space in between
        sn_profiles_sheet = workbook['User - SN Profiles']
        sn_profiles = IngestWorksheet(sn_profiles_sheet)

        # and: with hyphen
        file_names_sheet = workbook['Log - file-names']
        file_names = IngestWorksheet(file_names_sheet)

        # and: not module worksheet
        account_sheet = workbook['Account']
        account = IngestWorksheet(account_sheet)

        # expect:
        self.assertEqual('reviews', reviews.get_module_field_name())
        self.assertEqual('sn_profiles', sn_profiles.get_module_field_name())
        self.assertEqual('file_names', file_names.get_module_field_name())
        self.assertIsNone(account.get_module_field_name())

    def test__has_column__returns_true__when_column_exists(self):
        # given:
        rows = [
            ['name', 'address', 'mobile', 'email'],
            ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']
        ]

        worksheet = spreadsheet_utils.create_worksheet('person', rows)
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=1)

        # when:
        result = ingest_worksheet.has_column('name')

        # then:
        self.assertTrue(result)

    def test__has_column__returns_false__when_column_is_missing(self):
        # given:
        rows = [
            ['name', 'address', 'mobile', 'email'],
            ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']
        ]

        worksheet = spreadsheet_utils.create_worksheet('person', rows)
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=1)

        # when:
        result = ingest_worksheet.has_column('unknown')

        # then:
        self.assertFalse(result)

    def test__has_column__returns_false__when_column_is_none(self):
        # given:
        rows = [
            ['name', 'address', 'mobile', 'email'],
            ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']
        ]

        worksheet = spreadsheet_utils.create_worksheet('person', rows)
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=1)

        # when:
        result = ingest_worksheet.has_column(None)

        # then:
        self.assertFalse(result)

    def test__has_column__returns_false__when_column_is_empty(self):
        # given:
        rows = [
            ['name', 'address', 'mobile', 'email'],
            ['Jane Doe', 'Cambridge', '12-345-67', 'jane.doe@domain.com']
        ]

        worksheet = spreadsheet_utils.create_worksheet('person', rows)
        ingest_worksheet = IngestWorksheet(worksheet, header_row_idx=1)

        # when:
        result = ingest_worksheet.has_column('')

        # then:
        self.assertFalse(result)

    def test_is_project__returns_true(self):
        # given
        worksheet = spreadsheet_utils.create_worksheet('Project', [])
        ingest_worksheet = IngestWorksheet(worksheet)

        # when and then
        self.assertTrue(ingest_worksheet.is_project())

    def test_is_project__returns_true__when_case_is_lower(self):
        # given
        worksheet = spreadsheet_utils.create_worksheet('project', [])
        ingest_worksheet = IngestWorksheet(worksheet)

        # when and then
        self.assertTrue(ingest_worksheet.is_project())

    def test_is_project__returns_false(self):
        # given
        worksheet = spreadsheet_utils.create_worksheet('not a Project', [])
        ingest_worksheet = IngestWorksheet(worksheet)

        # when and then
        self.assertFalse(ingest_worksheet.is_project())

    def test_is_project_module__returns_true(self):
        # given
        worksheet = spreadsheet_utils.create_worksheet('Project - Contributors', [])
        ingest_worksheet = IngestWorksheet(worksheet)

        # when and then
        self.assertTrue(ingest_worksheet.is_project_module())

    def test_is_project_module__returns_false(self):
        # given
        worksheet = spreadsheet_utils.create_worksheet('Not A Project module', [])
        ingest_worksheet = IngestWorksheet(worksheet)

        # when and then
        self.assertFalse(ingest_worksheet.is_project_module())
