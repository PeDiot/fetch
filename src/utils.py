from typing import List, Dict, Optional
from copy import deepcopy
import random, time
from .enums import N_ITEMS_MAX, VINTAGE_BRAND_ID


def random_sleep(min_sleep: int = 1, max_sleep: int = 10) -> None:
    sleep_time = random.randint(min_sleep, max_sleep)
    time.sleep(sleep_time)


def create_batches(input_list: List, batch_size: int) -> List[List]:
    batches = []

    for i in range(0, len(input_list), batch_size):
        batch = input_list[i : i + batch_size]
        batches.append(batch)

    return batches


def prepare_search_kwargs(
    catalog_id: int,
    filters: Dict,
    filter_key: Optional[str] = None,
    batch_size: int = 1,
    only_vintage: bool = False,
) -> List[Dict]:
    base_search_kwargs = {"catalog_ids": [catalog_id], "per_page": N_ITEMS_MAX}

    if only_vintage:
        filter_search_kwargs = deepcopy(base_search_kwargs)
        filter_search_kwargs["brand_ids"] = [VINTAGE_BRAND_ID]

        return [filter_search_kwargs]

    search_kwargs = [base_search_kwargs]
    filter_options = filters.get(filter_key, [])
    filter_options = create_batches(filter_options, batch_size)

    for batch_filter_options in filter_options:
        filter_search_kwargs = deepcopy(base_search_kwargs)
        filter_search_kwargs[f"{filter_key}_ids"] = batch_filter_options
        search_kwargs.append(filter_search_kwargs)

    return search_kwargs
