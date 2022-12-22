import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity


@pytest.fixture
def with_multiple_elements(content, expected):
    content.update({
        'organ_parts': [
            {
                'ontology': 'UBERON:0000376',
                'ontology_label': 'dummylabel1',
                'text': 'dummytext1'
            },
            {
                'ontology': 'UBERON:0002386',
                'ontology_label': 'dummylabel2',
                'text': 'dummytext2'
            }
        ]
    })
    expected['Project']['values'][0].update({
        'project.organ_parts.ontology': 'UBERON:0000376||UBERON:0002386',
        'project.organ_parts.ontology_label': 'dummylabel1||dummylabel2',
        'project.organ_parts.text': 'dummytext1||dummytext2',

    })
    expected['Project']['headers'].extend(
        [
            'project.organ_parts.ontology',
            'project.organ_parts.ontology_label',
            'project.organ_parts.text'
        ]
    )
    return content


@pytest.fixture
def with_multiple_elements_but_inconsistent_columns(content, expected):
    content.update({
        'diseases': [
            {
                'ontology': 'UBERON:0000376',
                'ontology_label': 'dummylabel1',
                'text': 'dummytext1'
            },
            {
                'text': 'dummytext2'
            }
        ]
    })
    expected['Project']['values'][0].update({
        'project.diseases.ontology': 'UBERON:0000376',
        'project.diseases.ontology_label': 'dummylabel1',
        'project.diseases.text': 'dummytext1||dummytext2',

    })
    expected['Project']['headers'].extend(
        [
            'project.diseases.ontology',
            'project.diseases.ontology_label',
            'project.diseases.text'
        ]
    )
    return content


@pytest.fixture
def with_multiple_elements_but_with_empty_ontology_values(content, expected):
    content.update({
        'diseases': [
            {
                'ontology': 'UBERON:0000376',
                'ontology_label': 'dummylabel1',
                'text': 'dummytext1'
            },
            {
                'ontology': '',
                'ontology_label': '',
                'text': 'dummytext2'
            }
        ]
    })
    expected['Project']['values'][0].update({
        'project.diseases.ontology': 'UBERON:0000376',
        'project.diseases.ontology_label': 'dummylabel1',
        'project.diseases.text': 'dummytext1||dummytext2',

    })
    expected['Project']['headers'].extend(
        [
            'project.diseases.ontology',
            'project.diseases.ontology_label',
            'project.diseases.text'
        ]
    )
    return content


@pytest.fixture
def with_single_element(content, expected):
    content.update({
        "organ_parts": [{
            "ontology": "UBERON:0000376",
            "ontology_label": "hindlimb stylopod",
            "text": "hindlimb stylopod"
        }]
    })
    expected['Project']['values'][0].update({
        'project.organ_parts.ontology': 'UBERON:0000376',
        'project.organ_parts.ontology_label': 'hindlimb stylopod',
        'project.organ_parts.text': 'hindlimb stylopod',

    })
    expected['Project']['headers'].extend([
        'project.organ_parts.ontology',
        'project.organ_parts.ontology_label',
        'project.organ_parts.text'
    ])
    return content


@pytest.fixture
def with_single_element_but_only_with_text_attr(content, expected):
    content.update({
        'diseases': [
            {
                'text': 'dummytext2'
            }
        ]
    })
    expected['Project']['values'][0].update({
        'project.diseases.text': 'dummytext2'
    })
    expected['Project']['headers'].extend(
        [
            'project.diseases.text'
        ]
    )
    return content


@pytest.fixture(params=[
    pytest.lazy_fixture('with_multiple_elements'),
    pytest.lazy_fixture('with_multiple_elements_but_inconsistent_columns'),
    pytest.lazy_fixture('with_multiple_elements_but_with_empty_ontology_values'),
    pytest.lazy_fixture('with_single_element'),
    pytest.lazy_fixture('with_single_element_but_only_with_text_attr')
])
def from_content(request, metadata_uuid):
    return [{
        'content': request.param,
        'uuid': {
            'uuid': metadata_uuid
        }
    }]


@pytest.fixture
def entity_list(from_content):
    return Entity.from_json_list(from_content)


def test_flatten_ontology_module(flattener, entity_list, expected):
    # when
    actual = flattener.flatten(entity_list)
    # then
    assert_that(actual).is_equal_to(expected)
