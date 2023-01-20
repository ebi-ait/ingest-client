import pytest
from assertpy import assert_that

from .conftest import get_json_file, get_entity_file, get_entities_file, get_entities_from_content_list


@pytest.fixture
def donor(script_dir):
    return get_entity_file(script_dir + '/entities-with-inputs-donor.json')


@pytest.fixture
def process(script_dir):
    return get_entity_file(script_dir + '/entities-with-inputs-process.json')


@pytest.fixture
def protocols(script_dir):
    return get_entities_file(script_dir + '/entities-with-inputs-protocols.json')


@pytest.fixture
def specimen_with_inputs(script_dir, donor, process, protocols):
    entity = get_entity_file(script_dir + '/entities-with-inputs-specimen.json')
    entity.set_input([donor], [], process, protocols)
    return [entity, process] + protocols


@pytest.fixture
def expected_with_inputs(script_dir, expected):
    return get_json_file(script_dir + '/entities-with-inputs-flattened.json')



@pytest.fixture
def with_no_modules(content, expected):
    return content


@pytest.fixture
def with_string_array(content, expected, blank_header):
    content.update({
        "insdc_project_accessions": [
            "SRP180337"
        ],
        "geo_series_accessions": [
            "GSE124298", "GSE124299"
        ]
    })
    expected['Project']['values'][0].update({
        'project.insdc_project_accessions': 'SRP180337',
        'project.geo_series_accessions': "GSE124298||GSE124299"
    })
    expected['Project']['headers'].update({
        'project.insdc_project_accessions': blank_header,
        'project.geo_series_accessions': blank_header
    })
    return content


@pytest.fixture
def with_list_property(expected, metadata_uuid, blank_header):
    expected.clear()
    expected.update({
        'Collection protocol': {
            'values': [{
                'collection_protocol.organ_parts.field_1': 'UBERON:0000376',
                'collection_protocol.organ_parts.field_2': 'hindlimb stylopod',
                'collection_protocol.organ_parts.field_3': 'hindlimb stylopod',
                'collection_protocol.uuid': metadata_uuid
            }],
            'headers': {
                'collection_protocol.uuid': blank_header,
                'collection_protocol.organ_parts.field_1': blank_header,
                'collection_protocol.organ_parts.field_2': blank_header,
                'collection_protocol.organ_parts.field_3': blank_header
            }
        },
        'Schemas': [
            'https://schema.humancellatlas.org/type/project/14.2.0/collection_protocol'
        ]
    })
    return {
        "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/collection_protocol",
        "schema_type": "protocol",
        "organ_parts": [
            {
                "field_1": "UBERON:0000376",
                "field_2": "hindlimb stylopod",
                "field_3": "hindlimb stylopod"
            }
        ]
    }


@pytest.fixture()
def with_boolean(content, expected, blank_header):
    content.update({
        'boolean_field': True
        }
    )
    expected['Project']['values'][0].update({
        'project.boolean_field': 'True'
    })
    expected['Project']['headers']['project.boolean_field'] = blank_header
    return content


@pytest.fixture
def with_integer(content, expected, blank_header):
    content.update({
        'int_field': 1
    })
    expected['Project']['values'][0].update({
        'project.int_field': '1'
    })
    expected['Project']['headers']['project.int_field'] = blank_header
    return content


@pytest.fixture
def with_different_columns(expected, metadata_uuid, blank_header):
    expected.clear()
    expected.update({
        'Project': {
            'headers': {
                'project.uuid': blank_header,
                'project.project_core.project_short_name': blank_header,
                'project.project_core.project_title': blank_header,
                'project.project_core.project_description': blank_header
            },
            'values': [
                {
                    'project.uuid': metadata_uuid,
                    'project.project_core.project_short_name': 'label1'
                },
                {
                    'project.uuid': 'uuid2',
                    'project.project_core.project_short_name': 'label2',
                    'project.project_core.project_title': 'title',
                    'project.project_core.project_description': 'desc'
                }
            ]
        },
        'Schemas': [
            'https://schema.humancellatlas.org/type/project/14.2.0/project'
        ]
    })
    return [{
        "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
        "schema_type": "project",
        "project_core": {
            "project_short_name": "label1",
        }
    },{
        "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/project",
        "schema_type": "project",
        "project_core": {
            "project_short_name": "label2",
            "project_title": "title",
            "project_description": "desc"
        }
    }]


@pytest.fixture(params=[
    pytest.lazy_fixture('with_no_modules'),
    pytest.lazy_fixture('with_string_array'),
    pytest.lazy_fixture('with_list_property'),
    pytest.lazy_fixture('with_boolean'),
    pytest.lazy_fixture('with_integer')
])
def from_content(request):
    return [request.param]


@pytest.fixture(params=[
    pytest.lazy_fixture('from_content'),
    pytest.lazy_fixture('with_different_columns')
])
def entities(request):
    return get_entities_from_content_list(request.param)



@pytest.fixture
def entities_with_inputs(specimen_with_inputs, expected, expected_with_inputs):
    expected.clear()
    expected.update(expected_with_inputs)
    return specimen_with_inputs


@pytest.fixture(params=[
    pytest.lazy_fixture('entities'),
    pytest.lazy_fixture('entities_with_inputs'),
])
def entity_list(request):
    return request.param


def test_flatten(flattener, entity_list, expected):
    # when
    actual = flattener.flatten(entity_list)
    # then
    assert_that(actual).is_equal_to(expected, ignore='Schemas')
    assert_that(set(actual.get('Schemas', []))).is_equal_to(set(expected.get('Schemas', [])))
