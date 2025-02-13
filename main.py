import sys

sys.path.append("../")

from typing import List, Tuple, Dict
import tqdm, json, os, argparse
import src


DOMAIN = "fr"
FILTER_BATCH_SIZE = 3
UPLOAD_EVERY = 1000
INSERT_EVERY = 10000
ONLY_DESIGNERS = False
FILTER_BY_DEFAULT = []


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--women", default=True, type=lambda x: x.lower() == "true")
    parser.add_argument(
        "--only_vintage", default=False, type=lambda x: x.lower() == "true"
    )
    args = parser.parse_args()

    return vars(args)


def initialize() -> Tuple:
    secrets = json.loads(os.getenv("SECRETS_JSON"))
    gcp_credentials = secrets.get("GCP_CREDENTIALS")
    gcp_credentials["private_key"] = gcp_credentials["private_key"].replace("\\n", "\n")

    bq_client = src.bigquery.init_client(credentials_dict=gcp_credentials)
    vinted_client = src.vinted.Vinted(domain=DOMAIN)
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


def upload(
    item_entries: List[Dict], image_entries: List[Dict], likes_entries: List[Dict]
) -> int:
    num_uploaded = 0

    if src.bigquery.upload(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=src.enums.STAGING_ITEM_TABLE_ID,
        rows=item_entries,
    ):
        if src.bigquery.upload(
            client=bq_client,
            dataset_id=src.enums.DATASET_ID,
            table_id=src.enums.STAGING_IMAGE_TABLE_ID,
            rows=image_entries,
        ):
            if src.bigquery.upload(
                client=bq_client,
                dataset_id=src.enums.DATASET_ID,
                table_id=src.enums.LIKES_TABLE_ID,
                rows=likes_entries,
            ):
                num_uploaded = len(item_entries)

    return num_uploaded


def insert_and_clear_staging(table_id: str, reference_field: str) -> int:
    num_inserted = src.bigquery.insert_staging_rows(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=table_id,
        reference_field=reference_field,
    )

    if num_inserted == -1:
        return 0

    if not src.bigquery.restart_staging_table(
        client=bq_client,
        dataset_id=src.enums.DATASET_ID,
        table_id=table_id,
    ):
        return 0

    return num_inserted


def process_item(
    item: Dict, catalog_id: int, visited: List[int]
) -> Tuple[Dict, Dict, Dict, bool]:
    try:
        item_entry, image_entry, likes_entry = src.parse.parse_item(item, catalog_id)

        if not item_entry or not image_entry or not likes_entry:
            return None, None, None, False

        vinted_id = item_entry.get("vinted_id")
        if vinted_id in visited:
            return None, None, None, False

        return item_entry, image_entry, likes_entry, True
    except:
        return None, None, None, False


def process_search_response(
    response: Dict,
    catalog_id: int,
    visited: List[int],
    catalog_title: str,
    filter_key: str,
    loop: tqdm.tqdm,
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    item_entries, image_entries, likes_entries = [], [], []

    if response.status_code == 403:
        src.utils.random_sleep()
        return item_entries, image_entries, likes_entries

    elif response.status_code == 200:
        items = response.data.get("items", [])
        loop.set_description(
            f"Catalog: {catalog_title} | Filter: {filter_key} | Items: {len(items)}"
        )

        for item in items:
            item_entry, image_entry, likes_entry, success = process_item(
                item, catalog_id, visited
            )
            if success:
                item_entries.append(item_entry)
                image_entries.append(image_entry)
                likes_entries.append(likes_entry)
                visited.append(item_entry.get("vinted_id"))

    return item_entries, image_entries, likes_entries


def process_catalog_filters(
    catalog_id: int, filters: Dict, filter_by: List[str], only_vintage: bool
) -> List[Dict]:
    filter_by_updated = (
        ["brand"] if catalog_id in src.enums.DESIGNER_CATALOG_IDS else filter_by
    )

    search_kwargs_list = []
    for filter_key in filter_by_updated:
        search_kwargs = src.utils.prepare_search_kwargs(
            catalog_id=catalog_id,
            filter_key=filter_key,
            filters=filters,
            batch_size=FILTER_BATCH_SIZE,
            only_vintage=only_vintage,
        )
        search_kwargs_list.extend(search_kwargs)

    return search_kwargs_list


def update_progress(
    loop: tqdm.tqdm,
    women: bool,
    catalog_title: str,
    filter_key: str,
    n: int,
    n_success: int,
    num_uploaded: int,
    num_inserted: int,
) -> None:
    loop.set_description(
        f"Women: {women} | "
        f"Catalog: {catalog_title} | "
        f"Filter: {filter_key} | "
        f"Processed: {n} | "
        f"Success: {n_success} | "
        f"Success rate: {n_success / n:.2f} | "
        f"Uploaded: {num_uploaded} | "
        f"Inserted: {num_inserted} | "
    )


def main(women: bool, only_vintage: bool):
    global bq_client, vinted_client
    bq_client, vinted_client, filter_by = initialize()
    catalogs = load_catalogs(women)

    print(f"women: {women} | filter_by: {filter_by} | catalogs: {len(catalogs)}")
    loop = tqdm.tqdm(iterable=catalogs, total=len(catalogs))

    num_uploaded, num_inserted = 0, 0
    n, n_success = 0, 0
    visited = []

    for entry in loop:
        item_entries, image_entries, likes_entries = [], [], []
        catalog_title = entry.get("title")
        catalog_id = entry.get("id")

        filters_response = vinted_client.catalog_filters(catalog_ids=[catalog_id])
        filters = src.parse.parse_filters(filters_response)
        search_kwargs_list = process_catalog_filters(
            catalog_id, filters, filter_by, only_vintage
        )

        for search_kwargs in search_kwargs_list:
            response = vinted_client.search(**search_kwargs)
            new_items, new_images, new_likes = process_search_response(
                response,
                catalog_id,
                visited,
                catalog_title,
                search_kwargs.get("filter_key"),
                loop,
            )

            item_entries.extend(new_items)
            image_entries.extend(new_images)
            likes_entries.extend(new_likes)
            n += len(response.data.get("items", []))
            n_success += len(new_items)

            if n % UPLOAD_EVERY == 0:
                num_uploaded += upload(item_entries, image_entries, likes_entries)
                item_entries, image_entries, likes_entries = [], [], []

            if n % INSERT_EVERY == 0:
                for table_id, reference_field in zip(
                    [src.enums.ITEM_TABLE_ID, src.enums.IMAGE_TABLE_ID],
                    ["url", "vinted_id"],
                ):
                    if table_id == src.enums.ITEM_TABLE_ID:
                        num_inserted += insert_and_clear_staging(
                            table_id, reference_field
                        )

            update_progress(
                loop,
                women,
                catalog_title,
                search_kwargs.get("filter_key"),
                n,
                n_success,
                num_uploaded,
                num_inserted,
            )

    if len(item_entries) > 0 and len(image_entries) > 0:
        num_uploaded += upload(item_entries, image_entries, likes_entries)
        loop.set_description(f"Uploaded: {num_uploaded}")

        for table_id, reference_field in zip(
            [src.enums.ITEM_TABLE_ID, src.enums.IMAGE_TABLE_ID], ["url", "vinted_id"]
        ):
            if table_id == src.enums.ITEM_TABLE_ID:
                num_inserted += insert_and_clear_staging(table_id, reference_field)
                loop.set_description(f"Inserted: {num_inserted}")


if __name__ == "__main__":
    kwargs = parse_args()
    main(**kwargs)
