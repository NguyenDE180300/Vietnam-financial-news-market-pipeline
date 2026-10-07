"""Text normalization shared by RSS and ticker extraction."""
import html
import re


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = re.sub(r"<[^>]+>", " ", value)
    value = re.sub(r"(?<!&)#(\d+);", r"&#\1;", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()
