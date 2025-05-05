from typing import List, Dict, Tuple, Optional, Iterable

import random
from tqdm import tqdm
from google.cloud import bigquery

from .vinted import Vinted, VintedResponse
from .parse import parse_filters, parse_item
from .utils import random_sleep, prepare_search_kwargs
from .bigquery import insert_staging_rows, reset_staging_table, upload
from .enums import *


class VintedScraper:
    def __init__(
        self,
        bq_client: bigquery.Client,
        vinted_client: Vinted,
        insert_every_catalog: int,
    ):
        self.bq_client = bq_client
        self.vinted_client = vinted_client
        self.insert_every_catalog = insert_every_catalog

        self._reference_field = "vinted_id"
        self._filter_batch_size = 1

        self.reset()

    def reset(self):
        self.n = 0
        self.n_success = 0
        self.counter = 0
        self.visited = []
        self.num_uploaded = 0
        self.num_inserted = 0

    def run(
        self,
        catalogs: List[Dict],
        filter_by: str,
        only_vintage: bool,
        women: bool,
    ):
        self._reset_staging()
        loop = tqdm(iterable=catalogs, total=len(catalogs))

        for entry in loop:
            self.counter += 1
            catalog_title = entry.get("title")
            catalog_id = entry.get("id")

            filters_response = self.vinted_client.catalog_filters(
                catalog_ids=[catalog_id]
            )
            filters = parse_filters(filters_response)

            search_kwargs_list = self._process_catalog_filters(
                catalog_id, filters, filter_by, only_vintage
            )

            item_entries, image_entries, likes_entries, item_details_entries = (
                [],
                [],
                [],
                [],
            )

            for search_kwargs in search_kwargs_list:
                material_id = search_kwargs.get("material_ids", [None])[0]
                pattern_id = search_kwargs.get("patterns_ids", [None])[0]
                color_id = search_kwargs.get("color_ids", [None])[0]

                response = self.vinted_client.search(**search_kwargs)

                results = self._process_search_response(
                    response, catalog_id, material_id, pattern_id, color_id
                )

                if not results:
                    continue

                (
                    new_item_entries,
                    new_image_entries,
                    new_likes_entries,
                    new_item_details_entries,
                ) = results

                item_entries.extend(new_item_entries)
                image_entries.extend(new_image_entries)
                likes_entries.extend(new_likes_entries)
                item_details_entries.extend(new_item_details_entries)

                self._update_progress(
                    loop,
                    women,
                    catalog_title,
                    color_id,
                    material_id,
                    pattern_id,
                )

            if len(item_entries) > 0 and len(image_entries) > 0:
                self.num_uploaded += self._upload(
                    item_entries, image_entries, likes_entries, item_details_entries
                )

            if self.counter % self.insert_every_catalog == 0 or self.counter == len(
                catalogs
            ):
                self._insert_from_staging()

    def _update_progress(
        self,
        loop: Iterable,
        women: bool,
        catalog_title: str,
        color_id: Optional[int] = None,
        material_id: Optional[int] = None,
        pattern_id: Optional[int] = None,
    ):
        success_rate = self.n_success / self.n if self.n > 0 else 0

        loop.set_description(
            f"Women: {women} | "
            f"Catalog: {catalog_title} | "
            f"Color: {color_id} | "
            f"Material: {material_id} | "
            f"Pattern: {pattern_id} | "
            f"Processed: {self.n} | "
            f"Success: {self.n_success} | "
            f"Success rate: {success_rate:.2f} | "
            f"Uploaded: {self.num_uploaded} | "
            f"Inserted: {self.num_inserted} | "
        )

    def _upload(
        self,
        item_entries: List[Dict],
        image_entries: List[Dict],
        likes_entries: List[Dict],
        item_details_entries: List[Dict],
    ) -> int:
        num_uploaded = 0

        random.shuffle(item_entries)

        all_rows = [
            item_entries,
            image_entries,
            likes_entries,
            item_details_entries,
        ]

        all_table_ids = [
            STAGING_ITEM_TABLE_ID,
            STAGING_IMAGE_TABLE_ID,
            LIKES_TABLE_ID,
            ITEM_DETAILS_TABLE_ID,
        ]

        for table_id, rows in zip(all_table_ids, all_rows):
            if len(rows) > 0:
                success = upload(
                    client=self.bq_client,
                    dataset_id=DATASET_ID,
                    table_id=table_id,
                    rows=rows,
                )

                if (
                    table_id in [STAGING_ITEM_TABLE_ID, STAGING_IMAGE_TABLE_ID]
                    and not success
                ):
                    return 0

                if table_id == STAGING_ITEM_TABLE_ID:
                    num_uploaded += len(rows)

        return num_uploaded

    def _insert_from_staging(self):
        for table_id in [ITEM_TABLE_ID, IMAGE_TABLE_ID]:
            inserted = insert_staging_rows(
                client=self.bq_client,
                dataset_id=DATASET_ID,
                table_id=table_id,
                reference_field=self._reference_field,
            )

            self.num_inserted += max(inserted, 0)

    def _reset_staging(self):
        for table_id in [ITEM_TABLE_ID, IMAGE_TABLE_ID]:
            reset_staging_table(
                client=self.bq_client,
                dataset_id=DATASET_ID,
                table_id=table_id,
                field_id=self._reference_field,
            )

    def _process_catalog_filters(
        self,
        catalog_id: int,
        filters: Dict,
        filter_by: Optional[str] = None,
        only_vintage: bool = False,
    ) -> List[Dict]:
        filter_by_updated = [filter_by]

        if catalog_id in DESIGNER_CATALOG_IDS:
            filter_by_updated.append("brand")

        search_kwargs_list = []

        for filter_key in filter_by_updated:
            search_kwargs = prepare_search_kwargs(
                catalog_id=catalog_id,
                filter_key=filter_key,
                filters=filters,
                batch_size=self._filter_batch_size,
                only_vintage=only_vintage,
            )
            search_kwargs_list.extend(search_kwargs)

        return search_kwargs_list

    def _process_search_response(
        self,
        response: VintedResponse,
        catalog_id: int,
        material_id: Optional[int] = None,
        pattern_id: Optional[int] = None,
        color_id: Optional[int] = None,
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]] | None:
        item_entries, image_entries, likes_entries, item_details_entries = (
            [],
            [],
            [],
            [],
        )

        if response.status_code == 403:
            random_sleep()
            return

        elif response.status_code == 200 and isinstance(response.data, dict):
            items = response.data.get("items", [])

            for item in items:
                self.n += 1

                result = parse_item(
                    item, catalog_id, self.visited, material_id, pattern_id, color_id
                )

                if not result:
                    continue

                item_entry, image_entry, likes_entry, item_details_entry = result

                item_entries.append(item_entry)
                image_entries.append(image_entry)
                likes_entries.append(likes_entry)
                item_details_entries.append(item_details_entry)

                self.visited.append(item_entry.get("vinted_id"))
                self.n_success += 1

        return item_entries, image_entries, likes_entries, item_details_entries
