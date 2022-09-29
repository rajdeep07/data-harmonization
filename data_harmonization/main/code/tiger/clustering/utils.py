from typing import Any, Optional


def _flatten_list(l: list) -> list:
    """Convert nested list into one dimensional list

    :return: one dimensional list
    """
    return [
        item if isinstance(sublist, list) else sublist
        for sublist in l
        for item in sublist
    ]


def _isNotEmpty(input: Optional[Any] = None) -> bool:
    """Check if provided input is not empty

    :return: if not empty
    """
    if input is None:
        return False
    elif isinstance(input, str):
        return bool(input.strip())
