import sys

sys.path.append("../")

from typing import List

import tqdm, json, os
from vinted import Vinted
import src


FILTER_BATCH_SIZE = 3


def main(women: bool, domain: str = "fr", filter_by: List[str] = []):
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

    loop = tqdm.tqdm(iterable=catalogs, total=len(catalogs))
    inserted, n, n_success = 0, 0, 0

    for entry in loop:
        items, images = [], []
        item_success, image_success = 0, 0

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

                if len(response.items) == 0:
                    continue

                for item in response.items:
                    try:
                        item_entry, image_entry = src.items.parse(item, catalog_id)
                        items.append(item_entry)
                        images.append(image_entry)

                    except Exception as e:
                        continue

                if src.bigquery.upload(
                    client=bq_client,
                    dataset_id=src.enums.DATASET_ID,
                    table_id=src.enums.STAGING_ITEM_TABLE_ID,
                    rows=items,
                ):
                    inserted += len(items)
                    item_success = True
                    items = []

                    if src.bigquery.upload(
                        client=bq_client,
                        dataset_id=src.enums.DATASET_ID,
                        table_id=src.enums.STAGING_IMAGE_TABLE_ID,
                        rows=images,
                    ):
                        images = []
                        image_success = True

                n_success += int(item_success and image_success)
                n += 1

                loop.set_description(
                    f"Women: {women} | "
                    f"Catalog: {catalog_title} | "
                    f"Filter: {filter_key} | "
                    f"Item: {item_success} | "
                    f"Image: {image_success} | "
                    f"Total: {inserted} | "
                    f"Success: {n_success/n:.2f}"
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

            if num_inserted > -1:
                restart_success = src.bigquery.restart_staging_table(
                    client=bq_client,
                    dataset_id=src.enums.DATASET_ID,
                    table_id=table_id,
                )

            loop.set_description(
                f"Women: {women} | "
                f"Catalog: {catalog_title} | "
                f"Filter: {filter_key} | "
                f"Table: {table_id} | "
                f"Inserted: {num_inserted} | "
                f"Staging restart: {restart_success}"
            )


if __name__ == "__main__":
    main(women=True, filter_by=["brand"])
    main(women=False)
