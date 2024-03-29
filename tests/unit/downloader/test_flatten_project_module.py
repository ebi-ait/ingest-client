import pytest
from assertpy import assert_that

from .conftest import get_entities_from_content_list


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
def expected_project(expected, blank_header):
    expected.update({
        'Project - Contributors': {
            'headers': {
                'project.contributors.name': blank_header,
                'project.contributors.email': blank_header,
                'project.contributors.institution': blank_header,
                'project.contributors.laboratory': blank_header,
                'project.contributors.country': blank_header,
                'project.contributors.corresponding_contributor': blank_header,
                'project.contributors.project_role.text': blank_header,
                'project.contributors.project_role.ontology': blank_header,
                'project.contributors.project_role.ontology_label': blank_header
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
def entity_list(project_content):
    return get_entities_from_content_list([project_content])


def test_flatten_project_modules(flattener, entity_list, expected_project):
    actual = flattener.flatten(entity_list)
    assert_that(actual).is_equal_to(expected_project)
