from datetime import datetime

INGEST_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
INGEST_DATE_FORMAT_SHORT = "%Y-%m-%dT%H:%M:%SZ"

_expected_formats = [INGEST_DATE_FORMAT, INGEST_DATE_FORMAT_SHORT]


def parse_date_string(date_str: str):
    for date_format in _expected_formats:
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    raise ValueError(f'unknown date format for [{date_str}]')


def date_to_json_string(date: datetime):
    return date.isoformat().replace("+00:00", "Z")

