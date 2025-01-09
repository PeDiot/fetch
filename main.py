import sys

sys.path.append("../")

from typing import List, Tuple, Dict

import tqdm, json, os
from vinted import Vinted
import src


FILTER_BATCH_SIZE = 3
UPLOAD_EVERY = 500


def initialize(women: bool, domain: str, filter_by: List[str]) -> Tuple: 
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")

    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)
    vinted_client = Vinted(domain=domain)

    filter_by = [None] if len(filter_by) == 0 else filter_by

    catalogs = src.bigquery.load_table(
        client=bq_client,
        table_id=src.enums.CATALOG_TABLE_ID,
        conditions=[f"women = {women}", "is_valid = TRUE"],
    )

    return bq_client, vinted_client, filter_by, catalogs


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


def main(women: bool, domain: str = "fr", filter_by: List[str] = []):
    global bq_client, vinted_client
    bq_client, vinted_client, filter_by, catalogs = initialize(women, domain, filter_by)

    print(f"women: {women} | filter_by: {filter_by} | catalogs: {len(catalogs)}")
    loop = tqdm.tqdm(iterable=catalogs, total=len(catalogs))
    inserted, n = 0, 0

    for entry in loop:
        items, images = [], []        
        catalog_title = entry.get("title")
        catalog_id = entry.get("id")

        filters_response = vinted_client.catalog_filters(catalog_ids=[catalog_id])
        filters = src.filters.parse(filters_response)

        for filter_key in filter_by:
            search_kwargs_list = src.items.prepare_search_kwargs(
                catalog_id=catalog_id,
                filter_key=filter_key,
                filters=filters,
                batch_size=FILTER_BATCH_SIZE,
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
                        items.append(item_entry)
                        images.append(image_entry)

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
        loop.set_description(
            f"Upload: {uploaded} | "
            f"Inserted rows: {inserted} | "
        )

    for table_id, reference_field in zip(
        [src.enums.ITEM_TABLE_ID, src.enums.IMAGE_TABLE_ID], 
        ["url", "vinted_id"]
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
    main(women=False)
    main(women=True)
