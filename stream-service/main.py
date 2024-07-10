import functions_framework
from cloudevents.http import CloudEvent
from google.events.cloud import firestore
from google.cloud import bigquery
import logging

STRING_FIELDS = ['name', 'surname', 'document', 'email']
FLOAT_FIELDS = ['height', 'weight']

logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def main(cloud_event: CloudEvent) -> None:
    logger.info("Event received  for Firestore")

    firestore_payload = firestore.DocumentEventData()
    firestore_payload._pb.ParseFromString(cloud_event.data)

    id = firestore_payload.value.name.split('/')[-1]

    firestore_data = ''
    update_set = ''
    insert = ''
    values = ''

    for index, item in enumerate(firestore_payload.value.fields.items(), start=1):
        key = item[0]

        if index == 1:
            firestore_data = f""" '{id}' AS id"""
            insert = f'id, '
            values = f'firestore.id, '
        else:
            update_set = f'{update_set}, '
            insert = f'{insert}, '
            values = f'{values}, '

        if key in STRING_FIELDS:
            firestore_data = f'''{firestore_data}, '{item[1].string_value}' AS {key}'''
        if key in FLOAT_FIELDS:
            firestore_data = f'''{firestore_data}, '{item[1].double_value}' AS {key}'''

        update_set = f'''{update_set} {key}=firestore.{key}'''
        insert = f'''{insert} {key}'''
        values = f'''{values} firestore.{key}'''

    merge_query = create_merge_query(firestore_data, update_set, insert, values)
    execute_query(merge_query)

def create_merge_query(firestore_data, update_set, insert, values):
    return f"""
            MERGE `porfin.person` person
            USING (
              SELECT
              {firestore_data}
              ) firestore
              ON person.id = firestore.id
            WHEN MATCHED THEN
                UPDATE SET
                {update_set}
            WHEN NOT MATCHED THEN
              INSERT ({insert})
              VALUES ({values})
        """

def execute_query(query):
    client = bigquery.Client()
    query_job = client.query(query)
    query_job.result()

