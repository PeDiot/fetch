from typing import Dict

from vinted.models.filters import FiltersResponse
from .enums import VALID_FILTER_KEYS


def parse(response: FiltersResponse) -> Dict:
    filters = dict()

    for entry in response.filters:
        if entry.code in VALID_FILTER_KEYS:
            filter_option_ids = [option.id for option in entry.options]
            filters[entry.code] = filter_option_ids

    return filters
