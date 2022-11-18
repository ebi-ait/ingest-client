from datetime import datetime


def parse_date_string(date_str: str) -> datetime:
    edited_date = date_str.removesuffix('Z')
    date_time, dot, milliseconds = edited_date.partition('.')
    if milliseconds:
        edited_date = date_time + dot + milliseconds.ljust(6, '0')
    return datetime.fromisoformat(edited_date)


def date_to_json_string(date: datetime) -> str:
    return date.isoformat().removesuffix("+00:00") + 'Z'

