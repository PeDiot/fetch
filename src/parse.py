from typing import Dict, Tuple, Optional, List

import uuid, datetime
from .enums import VALID_FILTER_KEYS
from .vinted.models import VintedResponse


def parse_filters(response: VintedResponse) -> Dict:
    if response.status_code != 200:
        return {}

    filters = dict()

    for entry in response.data.get("filters", []):
        filter_key = entry.get("code")

        if filter_key in VALID_FILTER_KEYS:
            filter_options = entry.get("options", [])

            if filter_options:
                option_ids, option_titles = [], []

                for option in filter_options:
                    option_ids.append(option.get("id"))
                    option_titles.append(option.get("title"))

                filters[filter_key] = {
                    "id": option_ids,
                    "title": option_titles,
                }

    return filters


def parse_item(
    item: Dict,
    catalog_id: int,
    visited: List[int],
    material_id: Optional[int] = None,
    pattern_id: Optional[int] = None,
    color_id: Optional[int] = None,
) -> Optional[Tuple[Dict, Dict, Dict, Dict]]:
    try:
        result = _parse_item(item, catalog_id, material_id, pattern_id, color_id)

        if not result:
            return

        item_entry, image_entry, likes_entry, item_details_entry = result

        if item_entry.get("vinted_id") in visited:
            return

        return item_entry, image_entry, likes_entry, item_details_entry

    except:
        return


def _parse_item(
    item: Dict,
    catalog_id: int,
    material_id: Optional[int] = None,
    pattern_id: Optional[int] = None,
    color_id: Optional[int] = None,
) -> Tuple[Dict, Dict, Dict, Dict] | None:
    vinted_id = str(item.get("id"))
    if not vinted_id:
        return

    image_url = item.get("photo", {}).get("url")
    if not image_url:
        return

    item_url = item.get("url")
    if not item_url:
        return

    item_id = str(uuid.uuid4())
    created_at = datetime.datetime.now().isoformat()
    unix_created_at = int(datetime.datetime.now().timestamp())

    item_entry = {
        "id": item_id,
        "vinted_id": vinted_id,
        "catalog_id": catalog_id,
        "title": item.get("title"),
        "url": item_url,
        "price": _parse_price(item),
        "currency": _parse_currency(item),
        "brand": _parse_brand(item),
        "size": _parse_size(item),
        "condition": item.get("status"),
        "is_available": True,
        "created_at": created_at,
        "updated_at": created_at,
        "unix_created_at": unix_created_at,
    }

    image_entry = {
        "id": str(uuid.uuid4()),
        "vinted_id": vinted_id,
        "url": image_url,
        "nobg": False,
        "size": "original",
        "created_at": created_at,
    }

    likes_entry = {
        "vinted_id": vinted_id,
        "count": _parse_likes(item),
        "created_at": created_at,
    }

    item_details_entry = {
        "item_id": item_id,
        "material_id": material_id,
        "pattern_id": pattern_id,
        "color_id": color_id,
        "created_at": created_at,
    }

    return (item_entry, image_entry, likes_entry, item_details_entry)


def _parse_size(item: Dict) -> str:
    size = item.get("size_title")
    if not size:
        return

    try:
        return size.split(" / ")[0].replace(",", ".")
    except:
        return


def _parse_likes(item: Dict) -> int:
    try:
        return int(item.get("favourite_count"))
    except:
        return 0


def _parse_price(item: Dict) -> float | None:
    try:
        return float(item.get("price", {}).get("amount"))
    except:
        return


def _parse_currency(item: Dict) -> str:
    try:
        return item.get("price", {}).get("currency_code")
    except:
        return


def _parse_brand(item: Dict) -> str:
    try:
        return item.get("brand", {}).get("title")
    except:
        return
