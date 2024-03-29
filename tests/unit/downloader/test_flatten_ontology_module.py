import pytest
from assertpy import assert_that

from .conftest import get_entities_from_content_list


@pytest.fixture
def with_multiple_elements(content, expected, blank_header):
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
    expected['Project']['headers'].update({
        'project.organ_parts.ontology': blank_header,
        'project.organ_parts.ontology_label': blank_header,
        'project.organ_parts.text': blank_header
    })
    return content


@pytest.fixture
def with_multiple_elements_but_inconsistent_columns(content, expected, blank_header):
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
    expected['Project']['headers'].update({
        'project.diseases.ontology': blank_header,
        'project.diseases.ontology_label': blank_header,
        'project.diseases.text': blank_header
    })
    return content


@pytest.fixture
def with_multiple_elements_but_with_empty_ontology_values(content, expected, blank_header):
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
    expected['Project']['headers'].update({
        'project.diseases.ontology': blank_header,
        'project.diseases.ontology_label': blank_header,
        'project.diseases.text': blank_header
    })
    return content


@pytest.fixture
def with_single_element(content, expected, blank_header):
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
    expected['Project']['headers'].update({
        'project.organ_parts.ontology': blank_header,
        'project.organ_parts.ontology_label': blank_header,
        'project.organ_parts.text': blank_header
    })
    return content


@pytest.fixture
def with_single_element_but_only_with_text_attr(content, expected, blank_header):
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
    expected['Project']['headers'].update({
        'project.diseases.text': blank_header
    })
    return content


@pytest.fixture(params=[
    pytest.lazy_fixture('with_multiple_elements'),
    pytest.lazy_fixture('with_multiple_elements_but_inconsistent_columns'),
    pytest.lazy_fixture('with_multiple_elements_but_with_empty_ontology_values'),
    pytest.lazy_fixture('with_single_element'),
    pytest.lazy_fixture('with_single_element_but_only_with_text_attr')
])
def entity_list(request):
    return get_entities_from_content_list([request.param])


def test_flatten_ontology_module(flattener, entity_list, expected):
    # when
    actual = flattener.flatten(entity_list)
    # then
    assert_that(actual).is_equal_to(expected)
