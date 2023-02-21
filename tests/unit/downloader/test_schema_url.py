import pytest
from assertpy import assert_that
from _pytest.fixtures import SubRequest


from hca_ingest.downloader.schema_url import SchemaUrl
from hca_ingest.downloader.schema_url import ACCEPTED_DOMAINS


@pytest.fixture
def accepted_domains():
    return ACCEPTED_DOMAINS


@pytest.fixture
def correct_url():
    return "https://raw.githubusercontent.com/ebi-ait/ag2pi-2-ingest/main/json_schema/type/biomaterial/1.0.0/faang_samples_single_cell_specimen"


@pytest.fixture
def incorrect_url():
    return "https://raw.githubusercontent.com/FAANG/dcc-metadata/hca/json_schema/type/samples/faang_samples_single_cell_specimen.metadata_rules.json"


@pytest.fixture(params=[
    pytest.lazy_fixture('correct_url'),
    pytest.lazy_fixture('incorrect_url')
])
def url(request):
    return request.param


@pytest.fixture()
def expected(request):
    fixture_names = request.fixturenames
    return True if "correct_url" in fixture_names else False


def test_correct_domain(url, accepted_domains, expected):
    # when
    schema_domain = SchemaUrl(url).domain_type
    # then
    assert_that(schema_domain in accepted_domains).is_equal_to(expected)

