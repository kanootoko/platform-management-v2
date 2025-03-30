"""pickle command logic (deprecated) is defined here"""

import datetime
from functools import partial
from typing import Any


def print_upto_level(  # pylint: disable=too-many-arguments,too-many-locals
    value: Any,
    max_level: int,
    *,
    array_elements: int = 2,
    dict_elements: int = 5,
    indent: int = 4,
    last_line_break: bool = True,
    no_first_indent: bool = False,
    current_level: int = 0,
) -> None:
    """Pretty print given value with constraints."""
    internal = partial(
        print_upto_level, max_level=max_level, array_elements=array_elements, indent=indent, dict_elements=dict_elements
    )
    last_line_break_symbol = "\n" if last_line_break else ""
    indentation = " " * (indent * current_level)
    first_indentation = "" if no_first_indent else indentation

    if current_level >= max_level:
        if isinstance(value, (int, float, str, type(None), datetime.datetime)):
            val = value
        else:
            val = f"({type(value).__name__})"
        print(f"{first_indentation}{val}", end=last_line_break_symbol)
        return
    if isinstance(value, list):
        _print_list(
            value,
            internal=internal,
            array_elements=array_elements,
            indent=indent,
            indentation=indentation,
            first_indentation=first_indentation,
            last_line_break_symbol=last_line_break_symbol,
            current_level=current_level,
        )
    elif isinstance(value, dict):
        _print_dict(
            value,
            internal=internal,
            dict_elements=dict_elements,
            indent=indent,
            indentation=indentation,
            first_indentation=first_indentation,
            last_line_break_symbol=last_line_break_symbol,
            current_level=current_level,
        )
    else:
        print(f"{first_indentation}{value}", end=last_line_break_symbol)


def _print_list(  # pylint: disable=too-many-arguments
    value: Any,
    *,
    internal: callable,
    array_elements: int,
    indent: int,
    indentation: str,
    first_indentation: str,
    last_line_break_symbol: str,
    current_level: int,
):
    print(f"{first_indentation}{{{len(value)}}}[", end="")
    if len(value) == 0:
        print("]", end=last_line_break_symbol)
    else:
        print()
    for i, el in enumerate(value):
        if i >= array_elements:
            print(f"{' ' * (indent * (current_level + 1))}...")
            break
        internal(value=el, current_level=current_level + 1)
    if len(value) != 0:
        print(f"{indentation}]", end=last_line_break_symbol)


def _print_dict(  # pylint: disable=too-many-arguments
    value: Any,
    *,
    internal: callable,
    dict_elements: int,
    indent: int,
    indentation: str,
    first_indentation: str,
    last_line_break_symbol: str,
    current_level: int,
):
    print(f"{first_indentation}{{{len(value)}}}{{", end="")
    if len(value) == 0:
        print("}", end=last_line_break_symbol)
    else:
        print()
    for i, (key, val) in enumerate(value.items()):
        if i >= dict_elements:
            print(f"{' ' * (indent * (current_level + 1))}...")
            break
        internal(value=key, current_level=current_level + 1, last_line_break=False)
        print(": ", end="")
        internal(value=val, current_level=current_level + 1, no_first_indent=True)
    if len(value) != 0:
        print(f"{indentation}}}", end=last_line_break_symbol)
