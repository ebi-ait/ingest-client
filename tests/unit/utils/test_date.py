from datetime import datetime, timezone
from unittest import TestCase

from hca_ingest.utils.date import parse_date_string, date_to_json_string


class TestDateUtils(TestCase):
    def test_parse_date_string__returns_correct_date_obj__given_iso(self):
        # given:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)
        # and:
        iso_date = '2019-06-12T09:49:25.000Z'

        # when:
        actual_date = parse_date_string(iso_date)

        # expect:
        self.assertEqual(expected_date, actual_date)

    def test_parse_date_string__returns_correct_date_obj__given_iso_short(self):
        # given:
        iso_date_short = '2019-06-12T09:49:25Z'

        # when:
        actual_date = parse_date_string(iso_date_short)

        # expect:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)
        self.assertEqual(expected_date, actual_date)

    def test_parse_date_string__returns_correct_date_obj__given_unknown_format(self):
        # given:
        unknown = '2019:06:12Y09-49-25.000X'

        # expect:
        with self.assertRaises(ValueError):
            parse_date_string(unknown)

    def test_utc_date_to_json_string__returns_correct_string_with_z_suffix__given_utc_date_time(self):
        # given:
        utc_date_obj = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25, tzinfo=timezone.utc)

        # expect:
        expected_string = date_to_json_string(utc_date_obj)

        self.assertEqual('2019-06-12T09:49:25Z', expected_string)
