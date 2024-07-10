import logging

from google.cloud import bigquery
from google.cloud import firestore

from person import Person

logger = logging.getLogger(__name__)

FIRESTORE_DATABASE = 'porfin'
COLLECTION_NAME = 'person'
BIGQUERY_DATASET_ID = 'porfin'
BIGQUERY_TABLE_ID = 'person'
JSON_FILE_PATH = 'C:\\Workspace - Renato\\firestore_data.json'
# JSON_FILE_PATH = '/tmp/firestore_data.json'

def export_firestore_to_bigquery():
    logger.info("teste de log")
    print("teste de print")

    documents = get_firestore_documets()
    create_temp_file(documents)
    insert_into_bigquery()


def get_firestore_documets():
    firestore_client = firestore.Client(database=FIRESTORE_DATABASE)
    firestore_ref = firestore_client.collection(COLLECTION_NAME)
    return firestore_ref.stream()


def create_temp_file(documents):
    with open(JSON_FILE_PATH, 'w') as file:
        for doc in documents:
            person = Person(doc=doc)
            file.write(f'{person.json_object}\n')


def insert_into_bigquery():
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=table_schema()
    )

    with open(JSON_FILE_PATH, 'rb') as source_file:
        job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()

    print(
        f'Dados da coleção {COLLECTION_NAME} exportados para a tabela {BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID} no BigQuery')


def table_schema():
    return [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("surname", "STRING"),
        bigquery.SchemaField("document", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("weight", "FLOAT"),
        bigquery.SchemaField("height", "FLOAT"),
        bigquery.SchemaField("birth_date", "DATE")
    ]


if __name__ == "__main__":
    export_firestore_to_bigquery()
