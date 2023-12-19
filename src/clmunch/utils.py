import pathlib as pl
import re

_RX_MARKDOWN_HEADING_ID_LEGAL_CHARS = re.compile(r"[^0-9a-z_-]")

HTML_SYMBOL_SUCCESS = "&#9989;"  # check mark
HTML_SYMBOL_FAILURE = "&#10060;"  # cross mark


def bool_to_emoji(x: bool) -> str:
    """Return a checkmark if x is True, a crossmark if x is False."""
    return HTML_SYMBOL_SUCCESS if x else HTML_SYMBOL_FAILURE


def file_tail(file: pl.Path, n: int = 10) -> str:
    """Return the last n lines of a file."""
    with open(file, "r", encoding="UTF-8") as f:
        lines = f.readlines()
        return "".join(lines[-n:])


def _markdown_heading_to_id(heading: str) -> str:
    """Convert a markdown heading to a valid id to link to via (my link)[#id]."""
    return _RX_MARKDOWN_HEADING_ID_LEGAL_CHARS.sub("", heading.lower())


def markdown_heading_to_link(heading: str, title: str | None = None) -> str:
    """Convert a Markdown heading to a link."""
    title = heading if title is None else title
    return f"[{title}](#{_markdown_heading_to_id(heading)})"


def unique_substrings(strings: list[str]) -> list[str]:
    """
    Remove substrings that are contained in other strings in the list.
    So the returned list is still unique, but individual strings are shorter.
    """
    # Check that strings are unique
    assert len(set(strings)) == len(strings), "Strings must be unique"
    assert all(len(s) > 0 for s in strings), "Strings must be non-empty"

    max_len = max(len(x) for x in strings)

    for i in range(1, max_len):
        new_strings = [s[i:] for s in strings]
        if len(set(new_strings)) == len(new_strings):
            return new_strings
    return strings
