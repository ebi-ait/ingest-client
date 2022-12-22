import pytest
from assertpy import assert_that

from hca_ingest.downloader.entity import Entity

from .conftest import get_json_file, get_entity_file, get_entities_file


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
def with_string_array(content, expected):
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
    expected['Project']['headers'].extend(
        [
            'project.insdc_project_accessions',
            'project.geo_series_accessions'
        ]
    )
    return content


@pytest.fixture
def with_list_property(expected, metadata_uuid):
    expected.clear()
    expected.update({
        'Collection protocol': {
            'values': [{
                'collection_protocol.organ_parts.field_1': 'UBERON:0000376',
                'collection_protocol.organ_parts.field_2': 'hindlimb stylopod',
                'collection_protocol.organ_parts.field_3': 'hindlimb stylopod',
                'collection_protocol.uuid': metadata_uuid
            }],
            'headers': [
                'collection_protocol.uuid',
                'collection_protocol.organ_parts.field_1',
                'collection_protocol.organ_parts.field_2',
                'collection_protocol.organ_parts.field_3'
            ]
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
def with_boolean(content, expected):
    content.update({
        'boolean_field': True
        }
    )
    expected['Project']['values'][0].update({
        'project.boolean_field': 'True'
    })
    expected['Project']['headers'].append('project.boolean_field')
    return content


@pytest.fixture
def with_integer(content, expected):
    content.update({
        'int_field': 1
    })
    expected['Project']['values'][0].update({
        'project.int_field': '1'
    })
    expected['Project']['headers'].append('project.int_field')
    return content


@pytest.fixture
def with_different_columns(expected, metadata_uuid):
    expected.clear()
    expected.update({
        'Project': {
            'headers': [
                'project.uuid',
                'project.project_core.project_short_name',
                'project.project_core.project_title',
                'project.project_core.project_description'
            ],
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


def get_entities_from_content_list(content_list):
    entity_list = []
    for index, content in enumerate(content_list):
        entity_list.append({
            'content': content,
            'uuid': {
                'uuid': f'uuid{index+1}'
            }
        })
    return Entity.from_json_list(entity_list)


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
    assert_that(actual).is_equal_to(expected)


@pytest.fixture
def multiple_schema_versions_of_same_concrete_entity():
    return [
        {
            "describedBy": "https://schema.humancellatlas.org/type/project/14.2.0/donor_organism",
            "schema_type": "biomaterial",
            "field": "value"
        },{
            "describedBy": "https://schema.humancellatlas.org/type/project/14.3.0/donor_organism",
            "schema_type": "biomaterial",
            "field": "value"
        }
    ]


def test_flatten_raises_error(multiple_schema_versions_of_same_concrete_entity, flattener):
    entity_list = get_entities_from_content_list(multiple_schema_versions_of_same_concrete_entity)
    with pytest.raises(ValueError) as value_error:
        flattener.flatten(entity_list)
        assert_that(str(value_error.value)).is_equal_to("Multiple versions of same concrete entity schema")
