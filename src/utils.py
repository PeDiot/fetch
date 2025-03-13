from typing import List, Dict, Optional, Tuple

import random, time, json
from copy import deepcopy
from datetime import datetime
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
    max_filter_options: Optional[int] = 10,
    only_vintage: bool = False,
) -> List[Dict]:
    base_search_kwargs = {"catalog_ids": [catalog_id], "per_page": N_ITEMS_MAX}

    if only_vintage:
        filter_search_kwargs = deepcopy(base_search_kwargs)
        filter_search_kwargs["brand_ids"] = [VINTAGE_BRAND_ID]

        return [filter_search_kwargs]

    filter_options = filters.get(filter_key, {}).get("id", [])
    filter_options = _select_filter_options(filter_options, max_filter_options)

    if filter_options:
        search_kwargs = []
        random.shuffle(filter_options)
        filter_options = create_batches(filter_options, batch_size)

        for batch_filter_options in filter_options:
            filter_search_kwargs = deepcopy(base_search_kwargs)
            filter_search_kwargs[f"{filter_key}_ids"] = batch_filter_options
            search_kwargs.append(filter_search_kwargs)

        return search_kwargs

    return [base_search_kwargs]


def update_filter_entries(
    filter_data: Dict, catalog_id: int, entries: List[Dict], index: List[int]
) -> Tuple[List[Dict], List[int]]:
    current_timestamp = datetime.now().isoformat()
    iterator = zip(filter_data.get("id", []), filter_data.get("title", []))

    for id_, title_ in iterator:
        if id_ not in index:
            entries.append(
                {
                    "id": id_,
                    "catalog_id": catalog_id,
                    "title": title_,
                    "created_at": current_timestamp,
                }
            )

            index.append(id_)

    return entries, index


def save_to_jsonl(data_list: List[Dict], filename: str, append: bool = False) -> None:
    mode = "a" if append else "w"
    with open(filename, mode, encoding="utf-8") as file:
        for item in data_list:
            json_str = json.dumps(item, ensure_ascii=False)
            file.write(json_str + "\n")


def _select_filter_options(options: List[int], n: Optional[int] = None) -> List[int]:
    if n is None:
        return options

    n = min(n, len(options))

    return random.sample(options, n)
