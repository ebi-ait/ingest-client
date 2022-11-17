from datetime import datetime


def parse_date_string(date_str: str) -> datetime:
    edited_date = date_str.removesuffix('Z')
    return datetime.fromisoformat(edited_date)


def date_to_json_string(date: datetime) -> str:
    return date.isoformat().replace("+00:00", "Z")

