from unittest import TestCase
from unittest.mock import MagicMock, patch

from ingest.importer.conversion.metadata_entity import MetadataEntity
from ingest.importer.importer import WorksheetImporter, WorkbookImporter, UnexpectedEntityUUIDFound, \
    MissingEntityUUIDFound, MultipleProjectsFound, NoProjectFound
from ingest.importer.spreadsheet.ingest_workbook import IngestWorkbook

from tests.utils import create_test_workbook, create_ingest_workbook


class WorkbookImporterTest(TestCase):
    mock_json_schemas = [
        {'name': 'project', 'properties': ['contributors']},
        {'name': 'users', 'properties': ['sn_profiles']}
    ]

    @patch('ingest.importer.importer.WorksheetImporter')
    def setUp(self, worksheet_importer_constructor) -> None:
        self.template_mgr = MagicMock(name='template_manager')
        self.template_mgr.template.json_schemas = self.mock_json_schemas
        self.concrete_type_map = {'Project': 'project', 'Users': 'users'}
        self.template_mgr.get_concrete_type = lambda key: self.concrete_type_map.get(key)
        self.worksheet_importer = WorksheetImporter(self.template_mgr)
        worksheet_importer_constructor.return_value = self.worksheet_importer
        self.workbook_importer = WorkbookImporter(self.template_mgr)
        self.workbook = create_test_workbook('Project', 'Users')
        self.ingest_workbook = IngestWorkbook(self.workbook)

    def test_do_import(self, ):
        # given:
        project = MetadataEntity(concrete_type='project', domain_type='project')
        jdelacruz = MetadataEntity(concrete_type='user', domain_type='user', object_id=1,
                                   content={'user_name': 'jdelacruz'})
        setsuna_f_seiei = MetadataEntity(concrete_type='user', domain_type='user', object_id=96,
                                         content={'user_name': 'sayyeah'})
        no_errors = []
        self.worksheet_importer.do_import = MagicMock(
            side_effect=[([project], no_errors), ([jdelacruz, setsuna_f_seiei], no_errors)])

        # when:
        workbook_json, errors = self.workbook_importer.do_import(self.ingest_workbook, is_update=False)

        # then:
        self.assertIsNotNone(workbook_json)
        self.assertEqual(errors, [])

        # and:
        user_map = workbook_json.get('user')
        self.assertIsNotNone(user_map)
        self.assertEqual(2, len(user_map))
        self.assertEqual([jdelacruz.object_id, setsuna_f_seiei.object_id], list(user_map.keys()))

        # and:
        self.assertEqual({'user_name': 'jdelacruz'}, user_map.get(1)['content'])
        self.assertEqual({'user_name': 'sayyeah'}, user_map.get(96)['content'])

    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import_with_module_tab(self, worksheet_importer_constructor):
        # given:
        concrete_type_map = {'Project': 'project', 'User': 'users', 'User - SN Profiles': 'users'}
        self.template_mgr.get_concrete_type = lambda key: concrete_type_map.get(key)

        project = MetadataEntity(concrete_type='Project', domain_type='Project')
        user = MetadataEntity(concrete_type='user', domain_type='user', object_id=773,
                              content={'user_name': 'janedoe'})
        fb_profile = MetadataEntity(concrete_type='sn_profile', domain_type='user', object_id=773,
                                    content={'sn_profiles': {'name': 'facebook', 'id': '392'},
                                             'description': 'extra fb field'})
        ig_profile = MetadataEntity(concrete_type='sn_profile', domain_type='user', object_id=773,
                                    content={'sn_profiles': {'name': 'instagram', 'id': 'a92'},
                                             'description': 'extra ig field'})

        no_errors = []
        self.worksheet_importer.do_import = MagicMock(side_effect=[
            ([project], no_errors),
            ([user], no_errors),
            ([fb_profile, ig_profile], no_errors)])

        self.workbook = create_test_workbook('Project', 'User', 'User - SN Profiles')
        self.ingest_workbook = IngestWorkbook(self.workbook)

        # when:
        spreadsheet_json, errors = self.workbook_importer.do_import(self.ingest_workbook, is_update=False)

        # then:
        expected_errors = [
            {'key': 'description', 'value': 'extra fb field'},
            {'key': 'description', 'value': 'extra ig field'}
        ]

        self.assertIsNotNone(spreadsheet_json)
        self.assertEqual(errors, self.workbook_importer.list_data_removal_errors('User - SN Profiles', expected_errors))
        self.assertEqual(2, len(spreadsheet_json))

        # and:
        user_map = spreadsheet_json.get('user')
        self.assertIsNotNone(user_map)

        # and:
        janedoe = user_map.get(773)
        self.assertIsNotNone(janedoe)
        content = janedoe.get('content')
        self.assertEqual('janedoe', content.get('user_name'))
        self.assertEqual(['user_name', 'sn_profiles'], list(content.keys()))

        # and:
        sn_profiles = content.get('sn_profiles')
        self.assertIsNotNone(sn_profiles)
        self.assertEqual(2, len(sn_profiles))

        # and:
        self.assertEqual({'name': 'facebook', 'id': '392'}, sn_profiles[0])
        self.assertEqual({'name': 'instagram', 'id': 'a92'}, sn_profiles[1])

    # NOTE: this is added because the team chose not to define an identity label for Project schema
    # This breaks the schema agnosticism of the importer framework.
    # The assumption is that there's only one Project per submission
    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import_project_worksheet(self, worksheet_importer_constructor):
        # given:
        template_mgr = MagicMock(name='template_manager')
        template_mgr.template.json_schemas = self.mock_json_schemas
        template_mgr.get_concrete_type = MagicMock(return_value='project')

        worksheet_importer = WorkbookImporter(template_mgr)
        worksheet_importer_constructor.return_value = worksheet_importer
        no_errors = []

        # and:
        project = MetadataEntity(domain_type='project', concrete_type='project',
                                 content={'description': 'test project'})
        jsmith = MetadataEntity(domain_type='project', concrete_type='contact',
                                content={'contributors': {'name': 'John',
                                                          'email': 'jsmith@email.com'}})
        ppan = MetadataEntity(domain_type='project', concrete_type='contact',
                              content={'contributors': {'name': 'Peter',
                                                        'email': 'peterpan@email.com'}})
        worksheet_importer.do_import = MagicMock(side_effect=[([project], no_errors), ([jsmith, ppan], no_errors)])

        # and:
        workbook = create_test_workbook('Project', 'Project - Contributors')
        ingest_workbook = IngestWorkbook(workbook)
        workbook_importer = WorkbookImporter(template_mgr)

        # when:
        is_update = False
        spreadsheet_json, errors = workbook_importer.do_import(ingest_workbook, is_update)

        # then:
        project_map = spreadsheet_json.get('project')
        self.assertEqual(1, len(project_map))
        project_content = list(project_map.values())[0].get('content')
        self.assertEqual('test project', project_content.get('description'))
        self.assertEqual(errors, [])

        # and:
        contributors = project_content.get('contributors')
        self.assertEqual(2, len(contributors))
        self.assertIn({'name': 'John', 'email': 'jsmith@email.com'}, contributors)
        self.assertIn({'name': 'Peter', 'email': 'peterpan@email.com'}, contributors)

    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import_multiple_projects(self, worksheet_importer_constructor):
        # given:
        template_mgr = MagicMock(name='template_manager')
        template_mgr.template.json_schemas = self.mock_json_schemas
        template_mgr.get_concrete_type = MagicMock(return_value='project')

        worksheet_importer = WorksheetImporter(template_mgr)
        worksheet_importer_constructor.return_value = worksheet_importer
        no_errors = []
        expected_error = {
            'location': 'sheet=Project',
            'type': 'MultipleProjectsFound',
            'detail': 'The spreadsheet should only be associated to a single project.'
        }

        # and:
        project_1 = MetadataEntity(concrete_type='project', domain_type='project', object_id=1)
        project_2 = MetadataEntity(concrete_type='project', domain_type='project', object_id=2)
        worksheet_importer.do_import = MagicMock(side_effect=[([project_1, project_2], no_errors)])

        # and:
        workbook = create_test_workbook('Project')
        workbook_importer = WorkbookImporter(template_mgr)

        # when:
        is_update = False
        spreadsheet_json, errors = workbook_importer.do_import(IngestWorkbook(workbook), is_update)

        # then:
        self.assertIn(expected_error, errors, f'Errors expected to contain {MultipleProjectsFound.__name__}.')

    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import_no_projects(self, worksheet_importer_constructor):
        # given:
        template_mgr = MagicMock(name='template_manager')
        worksheet_importer = WorksheetImporter(template_mgr)
        worksheet_importer_constructor.return_value = worksheet_importer
        no_errors = []
        expected_error = {
            'location': 'File',
            'type': 'NoProjectFound',
            'detail': 'The spreadsheet should be associated to a project.'
        }

        # and:
        item = MetadataEntity(concrete_type='product', domain_type='product', object_id=910)
        worksheet_importer.do_import = MagicMock(side_effect=[([item], no_errors)])

        # and:
        workbook = create_test_workbook('Item')
        workbook_importer = WorkbookImporter(template_mgr)

        # when:
        is_update = False
        spreadsheet_json, errors = workbook_importer.do_import(IngestWorkbook(workbook), is_update)

        # then:
        self.assertIn(expected_error, errors, f'Errors expected to contain {NoProjectFound.__name__}.')

    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import_with_create_spreadsheet(self, worksheet_importer_constructor):
        # given:
        template_mgr = MagicMock(name='template_manager')
        template_mgr.template.json_schemas = self.mock_json_schemas
        concrete_type_map = {'project': 'project', 'users': 'users'}
        template_mgr.get_concrete_type = lambda key: concrete_type_map.get(key)
        sheet_names = ['project', 'users']

        workbook = create_ingest_workbook(sheet_names, ['name', 'address'])
        workbook_importer = WorkbookImporter(template_mgr)

        # and
        worksheet_importer = WorksheetImporter(template_mgr)
        worksheet_importer_constructor.return_value = worksheet_importer
        no_errors = []
        expected_errors = []
        for sheet_name in sheet_names:
            expected_errors.append({
                'location': f'sheet={sheet_name}',
                'type': 'MissingEntityUUIDFound',
                'detail': f'The {sheet_name} entities in the spreadsheet should have UUIDs.'
            })

        # and:
        item = MetadataEntity(concrete_type='product', domain_type='product', object_id=910)
        worksheet_importer.do_import = MagicMock(side_effect=[([item], no_errors)])

        # when:
        is_update = True
        spreadsheet_json, errors = workbook_importer.do_import(workbook, is_update)

        # then:
        self.assertTrue(
            all(elem in errors for elem in expected_errors),
            f'Errors expected to contain {MissingEntityUUIDFound.__name__}.')

    @patch('ingest.importer.importer.WorksheetImporter')
    def test_do_import_with_update_spreadsheet(self, worksheet_importer_constructor):
        # given:
        template_mgr = MagicMock(name='template_manager')
        template_mgr.template.json_schemas = self.mock_json_schemas
        concrete_type_map = {'project': 'project', 'users': 'users'}
        template_mgr.get_concrete_type = lambda key: concrete_type_map.get(key)
        sheet_names = ['project', 'users']

        workbook = create_ingest_workbook(sheet_names, ['uuid', 'description'])
        workbook_importer = WorkbookImporter(template_mgr)

        # and
        worksheet_importer = WorksheetImporter(template_mgr)
        worksheet_importer_constructor.return_value = worksheet_importer
        no_errors = []
        expected_errors = []
        for sheet_name in sheet_names:
            expected_errors.append({
                'location': f'sheet={sheet_name}',
                'type': 'UnexpectedEntityUUIDFound',
                'detail': f'The {sheet_name} entities in the spreadsheet shouldn’t have UUIDs.'
            })

        # and:
        project = MetadataEntity(domain_type='project', concrete_type='project', object_id=910)
        user1 = MetadataEntity(concrete_type='user', domain_type='user', object_id=1,
                               content={'user_name': 'jdelacruz'})

        worksheet_importer.do_import = \
            MagicMock(side_effect=[([project], no_errors), ([user1], no_errors)])

        # when:
        is_update = False
        spreadsheet_json, errors = workbook_importer.do_import(workbook, is_update)

        # then:
        self.assertTrue(
            all(elem in errors for elem in expected_errors),
            f'Errors expected to contain {UnexpectedEntityUUIDFound.__name__}.')
