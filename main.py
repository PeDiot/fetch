import sys

sys.path.append("../")

from typing import List, Tuple, Dict

import tqdm, json, os, argparse
from vinted import Vinted
import src


DOMAIN = "fr"
FILTER_BATCH_SIZE = 3
UPLOAD_EVERY = 500
ONLY_DESIGNERS = False
FILTER_BY_DEFAULT = []


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--women", default=True, type=lambda x: x.lower() == "true")
    parser.add_argument("--only_vintage", default=False, type=lambda x: x.lower() == "true")
    args = parser.parse_args()

    return vars(args)


def initialize() -> Tuple:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")

    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)
    vinted_client = Vinted(domain=DOMAIN)
    filter_by = [None] if len(FILTER_BY_DEFAULT) == 0 else FILTER_BY_DEFAULT

    return bq_client, vinted_client, filter_by


def load_catalogs(women: bool) -> List[Dict]:
    conditions = [f"women = {women}", "is_valid = TRUE"]

    if ONLY_DESIGNERS:
        catalog_ids = ",".join(map(str, src.enums.DESIGNER_CATALOG_IDS))
        conditions.append(f"id IN ({catalog_ids})")

    return src.bigquery.load_table(
        client=bq_client,
        table_id=src.enums.CATALOG_TABLE_ID,
        conditions=conditions,
    )


def upload(inserted: int, items: List[Dict], images: List[Dict]) -> Tuple[int, bool]:
    uploaded = False

    if src.bigquery.upload(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=src.enums.STAGING_ITEM_TABLE_ID,
        rows=items,
    ):
        inserted += len(items)

        if src.bigquery.upload(
            client=bq_client,
            dataset_id=src.enums.DATASET_ID,
            table_id=src.enums.STAGING_IMAGE_TABLE_ID,
            rows=images,
        ):
            uploaded = True

    return inserted, uploaded


def main(women: bool, only_vintage: bool):
    global bq_client, vinted_client
    bq_client, vinted_client, filter_by = initialize()
    catalogs = load_catalogs(women)

    print(f"women: {women} | filter_by: {filter_by} | catalogs: {len(catalogs)}")
    loop = tqdm.tqdm(iterable=catalogs, total=len(catalogs))
    visited, inserted, n = [], 0, 0

    for entry in loop:
        items, images = [], []
        catalog_title = entry.get("title")
        catalog_id = entry.get("id")

        filters_response = vinted_client.catalog_filters(catalog_ids=[catalog_id])
        filters = src.filters.parse(filters_response)
        filter_by_updated = (
            ["brand"] if catalog_id in src.enums.DESIGNER_CATALOG_IDS else filter_by
        )

        for filter_key in filter_by_updated:
            search_kwargs_list = src.items.prepare_search_kwargs(
                catalog_id=catalog_id,
                filter_key=filter_key,
                filters=filters,
                batch_size=FILTER_BATCH_SIZE,
                only_vintage=only_vintage,
            )

            for search_kwargs in search_kwargs_list:
                response = vinted_client.search(**search_kwargs)

                loop.set_description(
                    f"Catalog: {catalog_title} | "
                    f"Filter: {filter_key} | "
                    f"Items: {len(response.items)}"
                )

                for item in response.items:
                    uploaded = False
                    n += 1

                    try:
                        item_entry, image_entry = src.items.parse(item, catalog_id)
                        item_id = item_entry.get("vinted_id")

                        if item_id in visited:
                            continue

                        items.append(item_entry)
                        images.append(image_entry)
                        visited.append(item_id)

                    except:
                        continue

                    if n % UPLOAD_EVERY == 0:
                        inserted, uploaded = upload(inserted, items, images)
                        items, images = [], []

                    loop.set_description(
                        f"Women: {women} | "
                        f"Catalog: {catalog_title} | "
                        f"Filter: {filter_key} | "
                        f"Processed: {n} | "
                        f"Upload: {uploaded} | "
                        f"Inserted rows: {inserted} | "
                    )

    if len(items) > 0 and len(images) > 0:
        inserted, uploaded = upload(inserted, items, images)
        loop.set_description(f"Upload: {uploaded} | " f"Inserted rows: {inserted} | ")

    for table_id, reference_field in zip(
        [src.enums.ITEM_TABLE_ID, src.enums.IMAGE_TABLE_ID], ["url", "vinted_id"]
    ):
        num_inserted = src.bigquery.insert_staging_rows(
            client=bq_client,
            dataset_id=src.enums.DATASET_ID,
            table_id=table_id,
            reference_field=reference_field,
        )

        loop.set_description(
            f"Women: {women} | "
            f"Catalog: {catalog_title} | "
            f"Filter: {filter_key} | "
            f"Table: {table_id} | "
            f"Inserted: {num_inserted} | "
        )

        if num_inserted == -1:
            return

        if src.bigquery.restart_staging_table(
            client=bq_client,
            dataset_id=src.enums.DATASET_ID,
            table_id=table_id,
        ):
            loop.set_description(f"Staging restart: {table_id}")


if __name__ == "__main__":
    kwargs = parse_args()
    main(**kwargs)
