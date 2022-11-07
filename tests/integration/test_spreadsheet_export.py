from unittest import TestCase

from downloader.workbook import WorkbookDownloader
from hca_ingest.api.ingestapi import IngestApi


class TestSpreadsheetExport(TestCase):
    def setUp(self) -> None:
        self.url = 'https://api.ingest.dev.archive.data.humancellatlas.org'
        self.api = IngestApi(self.url)

    def test_one_spreadsheet_export(self):
        case = self.get_test_cases(1)[0]

        # When
        downloader = WorkbookDownloader(self.api)
        workbook = downloader.get_workbook_from_submission(case['submission_uuid'])

        # Then
        self.workbook_only_contains_one_project(workbook, case['project_uuid'])

    def test_multiple_spreadsheet_exports(self):
        cases = self.get_test_cases(5)
        # When
        downloader = WorkbookDownloader(self.api)
        for case in cases:
            workbook = downloader.get_workbook_from_submission(case['submission_uuid'])
            # Then
            self.workbook_only_contains_one_project(workbook, case['project_uuid'])

    def workbook_only_contains_one_project(self, workbook, project_uuid):
        # Then
        workbook_project_uuids = []
        for row in workbook['Project'].iter_rows(min_row=6, min_col=1, max_col=1):
            workbook_project_uuids.append(row[0].value)
        self.assertListEqual([project_uuid], workbook_project_uuids)

    def get_test_cases(self, number=1):
        entity_type = 'projects'
        url = f'{self.url}/{entity_type}/search/catalogue'
        last_url = self.api.get_link_from_resource_url(url, 'last', self.api.page_size)

        params = {'size': self.api.page_size}
        result = self.api.get(last_url, params=params).json()

        all_test_cases = []
        projects = result["_embedded"][entity_type] if '_embedded' in result else []
        test_cases = self.get_uuids_from_projects(projects)
        all_test_cases.extend(test_cases)

        while "previous" in result["_links"] and len(all_test_cases) < number:
            next_url = result["_links"]["previous"]["href"]
            result = self.api.get(next_url).json()
            projects = result["_embedded"][entity_type]
            test_cases = self.get_uuids_from_projects(projects)
            all_test_cases.extend(test_cases)
        return all_test_cases

    def get_uuids_from_projects(self, projects):
        test_cases = []
        entity_type = 'submissionEnvelopes'
        for project in projects:
            project_uuid = project.get('uuid', {}).get('uuid', '')
            submissions_url = self.api.get_link_from_resource(project, entity_type)
            submissions = self.api.get_all(submissions_url, entity_type)
            for submission in submissions:
                submission_uuid = submission.get('uuid', {}).get('uuid', '')
                if submission_uuid:
                    test_case = {
                        'project_uuid': project_uuid,
                        'submission_uuid': submission_uuid
                    }
                    test_cases.append(test_case)
        return test_cases
