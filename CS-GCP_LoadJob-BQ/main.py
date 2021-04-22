import base64
import json
from google.cloud import bigquery
from google.api_core import exceptions

def main(event, context):

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    json_loads = json.loads(pubsub_message)
    csv = json_loads['data']['message']

    upload (csv)

providers = ["AWS", "GCP", "AZURE"]

# https://googleapis.dev/python/bigquery/latest/index.html

client = bigquery.Client()
def upload (csv):
    # Definiciones necesarias para el procesamiento
    dataset_name, table = provider (csv)
    table_ref = client.dataset(dataset_name).table(table)
    uri = "gs://{}/{}" .format ("bghtp_storage_source_csv", csv)

    try:
        create_dataset(dataset_name)
        create_table(dataset_name, table_ref, uri)
    # Raise exception 409 para la creación del dataset
    except exceptions.Conflict:
        create_table(dataset_name, table_ref, uri)		

def create_dataset (dataset_name):
    dataset_id = "{}.{}".format(client.project, dataset_name)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset)

def create_table (dataset_name, table_ref, uri):
    # Parametros de configuración del job
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.autodetect = True
    job_config.field_delimiter = "," # Separador de lineas por pipes en lugar de coma o punto
    job_config.allow_jagged_rows = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # Si la tabla existe, la dropea y la vuelve a crear
    job_config.source_format = bigquery.SourceFormat.CSV

    # Procesamiento del job
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)  # API request
    load_job.result()
    destination_table = client.get_table(table_ref)

def provider (csv):
    if csv.upper().find(providers[0]) != -1:
        dataset_name = str(csv[ csv.upper().find(providers[0]) : csv.upper().find(providers[0]) + len(providers[0]) ]).replace("-", "_").upper() 
        table = str(csv[ csv.find("-csv-") +5 : csv.find(".csv")]).upper()
    elif csv.upper().find(providers[1]) != -1:
        dataset_name = str(csv[ csv.upper().find(providers[1]) : csv.upper().find(providers[1]) + len(providers[1]) ]).replace("-", "_").upper()
        table = str("20" + csv[ csv.find("–")-3: csv.find("–")+4]).replace(" ", "").replace("_", "")replace("–", "-").upper()
        table = str(table[:5] + "0" + table[5:]) if len(table[table.find("-"):]) < 3 else table
    return dataset_name, table