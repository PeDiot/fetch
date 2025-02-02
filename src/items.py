from typing import List, Dict, Tuple, Optional

import uuid, datetime
from vinted.models.items import Item
from copy import deepcopy

from .utils import create_batches
from .enums import N_ITEMS_MAX, VINTAGE_BRAND_ID


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


def parse(item: Item, catalog_id: int) -> Tuple[Dict, Dict]:
    item_id = str(uuid.uuid4())
    created_at = datetime.datetime.now().isoformat()

    item_entry = {
        "id": item_id,
        "vinted_id": str(item.id),
        "catalog_id": catalog_id,
        "title": item.title,
        "url": item.url,
        "price": float(item.price.amount),
        "currency": item.price.currency_code,
        "brand": item.brand_title,
        "size": _parse_size(item.size_title),
        "condition": item.status,
        "is_available": True,
        "created_at": created_at,
        "updated_at": created_at,
    }

    image_entry = {
        "id": str(uuid.uuid4()),
        "vinted_id": str(item.id),
        "url": item.photo.url,
        "nobg": False,
        "size": "original",
        "created_at": created_at,
    }

    likes_entry = {
        "vinted_id": str(item.id),
        "count": _parse_likes(item),
        "created_at": created_at,
    }

    return item_entry, image_entry, likes_entry


def _parse_size(size: str) -> str:
    return size.split(" / ")[0].replace(",", ".")


def _parse_likes(item: Item) -> int:
    try: 
        return int(item.favourite_count)
    except:
        return 0
