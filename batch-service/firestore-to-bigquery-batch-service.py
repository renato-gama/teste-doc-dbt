import logging
from datetime import datetime

from google.cloud import firestore
from google.cloud import bigquery
from typing import Any, Callable, Generator, Tuple, Union
import json

from exceptions import BigQueryException

logger = logging.getLogger(__name__)

JSON_FILE_PATH = 'C:\\Workspace - Renato\\firestore_data.json'
# JSON_FILE_PATH = '/tmp/firestore_data.json'

def export_firestore_to_bigquery():
    logger.info("teste de log")
    print("teste de print")

    # Inicializa os clientes do Firestore e BigQuery
    firestore_client = firestore.Client(database='porfin-teste')
    bigquery_client = bigquery.Client()

    # Referência à coleção do Firestore
    collection_name = 'person'
    firestore_ref = firestore_client.collection(collection_name)

    # Nome do dataset e tabela do BigQuery
    dataset_id = 'porfin'
    table_id = 'person'
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    schema = [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("surname", "STRING"),
        bigquery.SchemaField("document", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("weight", "NUMERIC"),
        bigquery.SchemaField("height", "NUMERIC"),
        bigquery.SchemaField("birth_date", "DATE")
    ]

    # Configuração do job de inserção
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema
    )

    # Busca os documentos do Firestore
    documents = firestore_ref.stream()
    create_temp_file(documents)

    # Carrega os dados para o BigQuery
    with open(JSON_FILE_PATH, 'rb') as source_file:
        try:
            job = bigquery_client.load_table_from_file(source_file, table_ref, job_config=job_config)
            job.result()  # Aguarda a conclusão do job
        except Exception as e:
            raise BigQueryException('Error to insert data into BigQUery. There are invalid data')

    print(f'Dados da coleção {collection_name} exportados para a tabela {dataset_id}.{table_id} no BigQuery')

def create_temp_file(documents: Generator):
    with open(JSON_FILE_PATH, 'w') as file:
        for doc in documents:
            converted_row = convert_row(doc)
            file.write(f'{converted_row}\n')

def convert_row(doc):
    doc_dict = doc.to_dict()
    doc_dict['id'] = doc.id

    if 'birth_date' in doc_dict:
        doc_dict['birth_date'] = str(datetime.date(doc_dict['birth_date']))

    return doc_dict


if __name__ == "__main__":
    export_firestore_to_bigquery()
