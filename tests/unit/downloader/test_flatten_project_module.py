from ingest.downloader.entity import Entity
from ingest.downloader.flattener import Flattener
from tests.unit.downloader.test_flattener import FlattenerTest


class FlattenProjectModuleTest(FlattenerTest):
    def test_flatten__has_project_modules(self):
        # given
        self.content.update({
            "contributors": [{
                "name": "Alex A,,Pollen",
                "email": "alex.pollen@ucsf.edu",
                "institution": "University of California, San Francisco (UCSF)",
                "laboratory": "Department of Neurology",
                "country": "USA",
                "corresponding_contributor": True,
                "project_role": {
                    "text": "experimental scientist",
                    "ontology": "EFO:0009741",
                    "ontology_label": "experimental scientist"
                }
            }]
        })

        metadata_entity = {
            'content': self.content,
            'uuid': self.uuid
        }

        entity_list = [metadata_entity]

        # when
        flattener = Flattener()
        actual = flattener.flatten(Entity.from_json_list(entity_list))

        self.flattened_metadata_entity.update({
            'Project - Contributors': {
                'headers': [
                    'project.contributors.name',
                    'project.contributors.email',
                    'project.contributors.institution',
                    'project.contributors.laboratory',
                    'project.contributors.country',
                    'project.contributors.corresponding_contributor',
                    'project.contributors.project_role.text',
                    'project.contributors.project_role.ontology',
                    'project.contributors.project_role.ontology_label'
                ],
                'values': [{
                    'project.contributors.corresponding_contributor': 'True',
                    'project.contributors.country': 'USA',
                    'project.contributors.email': 'alex.pollen@ucsf.edu',
                    'project.contributors.institution': 'University of California, San Francisco (UCSF)',
                    'project.contributors.laboratory': 'Department of Neurology',
                    'project.contributors.name': 'Alex A,,Pollen',
                    'project.contributors.project_role.ontology': 'EFO:0009741',
                    'project.contributors.project_role.ontology_label': 'experimental scientist',
                    'project.contributors.project_role.text': 'experimental scientist'}
                ]}
        })

        # then
        self.assertEqual(actual, self.flattened_metadata_entity)
