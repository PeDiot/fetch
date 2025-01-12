from typing import List, Dict, Union, Iterable, Tuple

from google.oauth2 import service_account
from google.cloud import bigquery
from .enums import *


def init_client(credentials_dict: Dict) -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_info(
        credentials_dict
    )

    return bigquery.Client(
        credentials=credentials, project=credentials_dict["project_id"]
    )


def load_table(
    client: bigquery.Client,
    table_id: str,
    dataset_id: str = DATASET_ID,
    conditions: List[str] = None,
    fields: List[str] = None,
    order_by: str = None,
    descending: bool = False,
    limit: int = None,
    to_list: bool = True,
) -> Union[List[Dict], bigquery.table.RowIterator]:
    field_str = ", ".join(fields) if fields else "*"
    query = f"SELECT {field_str} FROM `{PROJECT_ID}.{dataset_id}.{table_id}`"

    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    if order_by:
        query += f" ORDER BY {order_by} {'DESC' if descending else 'ASC'}"

    if limit:
        query += f" LIMIT {limit}"

    query_job = client.query(query)
    results = query_job.result()

    if to_list:
        return [dict(row) for row in results]
    else:
        return results


def upload(
    client: bigquery.Client, dataset_id: str, table_id: str, rows: List[Dict]
) -> bool:
    table_path = f"`{PROJECT_ID}.{dataset_id}.{table_id}`"

    fields = f"{tuple(rows[0].keys())}".replace("'", "")
    values = [f"{_preprocess_null_values(row.values())}" for row in rows]
    values = "\n\t" + ",\n\t".join(values)

    query = f""" 
    INSERT INTO 
    {table_path}
    {fields}
    VALUES {values}; 
    """
    query = query.replace("'NULL'", "NULL")

    try:
        client.query(query).result()
        return True

    except Exception as e:
        print(e)
        return False


def insert_staging_rows(
    client: bigquery.Client, dataset_id: str, table_id: str, reference_field: str
) -> int:
    query = f"""
    INSERT INTO `{PROJECT_ID}.{dataset_id}.{table_id}`
    SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_id}_staging`
    WHERE {reference_field} NOT IN (SELECT {reference_field} FROM `{PROJECT_ID}.{dataset_id}.{table_id}`)
    """

    try:
        query_job = client.query(query)
        query_job.result()
        return query_job.num_dml_affected_rows

    except Exception as e:
        print(e)
        return -1


def restart_staging_table(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
) -> bool:
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{dataset_id}.{table_id}_staging` AS
    SELECT * FROM `{PROJECT_ID}.{dataset_id}.{table_id}` LIMIT 0;
    """

    try:
        client.query(query).result()
        return True
    except Exception as e:
        print(e)
        return False


def _preprocess_null_values(values: Iterable) -> Tuple:
    return tuple([value if value is not None else "NULL" for value in values])
