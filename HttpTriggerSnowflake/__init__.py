import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from datetime import datetime, timezone
import logging
import psycopg2
from urllib.parse import urlparse

app = func.FunctionApp()

main_db_conn_string = os.getenv('PostgresMainDBConnectionString')
properties_db_conn_string = os.getenv('PostgresPropertiesDBConnectionString')
backfill_storage_account_container = os.getenv('BackfillStorageContainer')
backfill_storage_conn_string = os.getenv('BackfillStorageConnString')
backfill_storage_account_url = os.getenv('BackfillStorageAccountUrl')

main_db_cd = urlparse(main_db_conn_string)
properties_db_cd = urlparse(properties_db_conn_string)

main_db_conn_dict = {
    'dbname': main_db_cd.path[1:],
    'user': main_db_cd.username,
    'password': main_db_cd.password,
    'port': main_db_cd.port,
    'host': main_db_cd.hostname
}

properties_db_conn_dict = {
    'dbname': properties_db_cd.path[1:],
    'user': properties_db_cd.username,
    'password': properties_db_cd.password,
    'port': properties_db_cd.port,
    'host': properties_db_cd.hostname
}

pg_connection_dict = {
    "main_db_conn_dict": main_db_conn_dict,
    "properties_db_conn_dict": properties_db_conn_dict
}


@app.function_name(name="RunPostgresDump")
@app.route(route="run_dump", auth_level=func.AuthLevel.ANONYMOUS)
def test_function(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()
    database = req_body.get('db_name')
    schema = req_body.get('schema_name')
    table_name = req_body.get('table_name')
    columns = req_body.get('columns')
    where = req_body.get('where')
    columns = columns if columns is not None else "*"
    where_clause = '' if where is None or not len(where) else f' WHERE {where}'

    connection = psycopg2.connect(
        **pg_connection_dict[f'{database}_db_conn_dict'])
    cursor = connection.cursor()

    sql_to_run = f'SELECT {", ".join(columns)} FROM {database}.{schema}.{table_name}{where_clause}'
    datetime_now = datetime.now(timezone.utc)
    file_name = f"{table_name}_{datetime_now.strftime('%Y%m%d%H%M%S%f')}.csv"

    with open(file_name, "wb") as f:
        cursor.copy_expert(
            f'COPY ({sql_to_run}) TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE *)', f)

    connection.close()

    blob_file_path = f'{table_name}/{file_name}'

    default_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient.from_connection_string(
        backfill_storage_conn_string)

    blob_service_client = BlobServiceClient(
        backfill_storage_account_url, credential=default_credential)

    blob_client = blob_service_client.get_blob_client(
        container=backfill_storage_account_container, blob=blob_file_path)

    with open(file=file_name, mode="rb") as data:
        blob_client.upload_blob(data)

    os.remove(file_name)

    return func.HttpResponse("Success")
