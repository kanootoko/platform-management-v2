"""CLI mappers for uploader helpers are defined here.

Mappers are functions which return some value from given dictionary and give a callback function to remove this value
from the dictionary later. Function should be safe as other mapper could have already removed the value by the time of
actual callback call.

This mappers should be used like an example to create a custom properties mapper for a given input file structure.
"""

from typing import Any, Callable

_Data = dict[str, Any]
_Callback = Callable[[_Data], None]


def none_mapper(_: _Data) -> tuple[None, _Callback]:
    """Return None and empty callback."""
    return None, _empty_callback


def empty_dict_mapper(_: _Data) -> tuple[_Data, _Callback]:
    """Return empty dict and empty callback."""
    return {}, _empty_callback


def get_filter_dict_mapper(keys: list[str]) -> Callable[[_Data], _Data]:
    """Return function that will create a new dictionary containing given keys."""
    keys_set = set(keys)

    def filter_dict_mapper(data: _Data) -> _Data:
        return {key: value for key, value in data.items() if key in keys_set}

    return filter_dict_mapper


def get_first_occurance_filter_dict_mapper(keys_lists: list[list[str]]) -> Callable[[_Data], tuple[_Data, _Callback]]:
    """Return function that will create a new dictionary containing first of the given keys for each inner list."""

    def filter_dict_mapper(data: _Data) -> _Data:
        result = {}
        for keys in keys_lists:
            for key in keys:
                if key in data:
                    result[key] = data[key]
                    break

        return result, _remove_from_dict_multiple_callback(list(result.keys()))

    return filter_dict_mapper


def get_value_mapper(value: Any) -> tuple[Any, _Callback]:
    """Return given value and empty callback."""
    return lambda _: (value, _empty_callback)


def get_attribute_mapper(
    possible_names: list[str],
    default_value: Any = None,
) -> tuple[Callable[[_Data], Any], _Callback]:
    """Search for a possible names in data dictionary, return value and remove its key in callback if key is found.
    Otherwise return default and empty callback.
    """

    def attribute_mapper(data: _Data) -> Any:
        for possible_name in possible_names:
            value = data.get(possible_name)
            if value:
                return value, _remove_from_dict_callback(possible_name)
        return default_value, _empty_callback

    return attribute_mapper


def get_attribute_mapper_no_default(
    possible_names: list[str],
) -> Callable[[_Data], tuple[Any, bool, _Callback]]:
    """Search for a possible names in data dictionary, return value, True and removing callback if key is found.
    Otherwise return (None, False <empty callback>).
    """

    def attribute_mapper(data: _Data) -> Any:
        for possible_name in possible_names:
            value = data.get(possible_name)
            if value:
                return value, True, _remove_from_dict_callback(possible_name)
        return None, False, _empty_callback

    return attribute_mapper


def get_func_mapper(
    possible_names: list[str],
    func: Callable[[Any], tuple[Any, bool, _Callback]],
    default_value: Any = None,
) -> Callable[[_Data], tuple[Any, _Callback]]:
    """Search for a possible name in data dictionary, if found then apply given function, and if the second
    return value is true - return first value and callback.

    Return default value if no possible names were found or func returned False for each call,
    default value is returned.
    """

    def attribute_mapper(data: _Data) -> tuple[Any, _Callback]:
        for possible_name in possible_names:
            if possible_name in data:
                value, is_ok = func(data[possible_name])
                if is_ok:
                    return value, _remove_from_dict_callback(possible_name)
        return default_value, _empty_callback

    return attribute_mapper


def get_service_capacity_mapper(
    default_capacity: int | None,
) -> Callable[[_Data], tuple[int, _Callback]]:
    """Search for a possible names in data dictionary, return value if key is found - otherwise
    return default capacity and callback to set is_capacity_real=False if default_capacity is set to value.
    """

    def service_capacity_mapper(service_data: _Data) -> int | None:
        for possible_name in ("capacity", "мощность"):
            if possible_name in service_data:
                try:
                    capacity = int(service_data[possible_name])
                except ValueError:
                    continue
                return capacity, _remove_from_dict_callback(possible_name)

        if default_capacity is not None:
            return default_capacity, _set_value_callback("is_capacity_real", False)
        return None, _empty_callback

    return service_capacity_mapper


def full_dictionary_mapper(upload_data: _Data) -> tuple[_Data, _Callback]:
    """Return full data dictionary and empty callback."""
    return upload_data, _empty_callback


def get_string_checker_func(func: Callable[[str], str]) -> Callable[[Any], tuple[str | None, bool]]:
    """Return a function that return string, True and callback based on a given value if it is non-empty string itself.
    Otherwise function will return (None, False, <empty callback>).
    """

    def string_checker(possible_service_name: Any) -> tuple[str | None, bool]:
        if not isinstance(possible_service_name, str) or len(possible_service_name) == 0:
            return None, False
        return func(possible_service_name), True

    return string_checker


def _empty_callback(_: _Data):
    pass


def _remove_from_dict_callback(key: str) -> _Callback:
    def remove(data: _Data) -> None:
        if key in data:
            del data[key]

    return remove


def _remove_from_dict_multiple_callback(keys: list[str]) -> _Callback:
    def remove(data: _Data) -> None:
        for key in keys:
            if key in data:
                del data[key]

    return remove


def _set_value_callback(key: str, value: Any) -> _Callback:
    def set_value(data: _Data) -> None:
        data[key] = value

    return set_value
