import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity


@pytest.fixture
def project_content(content):
    content.update({
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
    return content


@pytest.fixture
def expected_project(expected):
    expected.update({
        'Project - Contributors': {
            'headers': {
                'project.contributors.name': {},
                'project.contributors.email': {},
                'project.contributors.institution': {},
                'project.contributors.laboratory': {},
                'project.contributors.country': {},
                'project.contributors.corresponding_contributor': {},
                'project.contributors.project_role.text': {},
                'project.contributors.project_role.ontology': {},
                'project.contributors.project_role.ontology_label': {}
            },
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
            ]
        }
    })
    return expected


@pytest.fixture
def project_metadata(project_content, metadata_uuid):
    return {
        'content': project_content,
        'uuid': {
            'uuid': metadata_uuid
        }
    }


@pytest.fixture
def project_entity(project_metadata):
    return Entity(project_metadata)


@pytest.fixture
def entity_list(project_entity):
    return [project_entity]


def test_flatten_project_modules(flattener, entity_list, expected_project):
    actual = flattener.flatten(entity_list)
    assert_that(actual).is_equal_to(expected_project)
