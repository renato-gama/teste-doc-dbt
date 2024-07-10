import functions_framework
from cloudevents.http import CloudEvent
from google.events.cloud import firestore

STRING_FIELDS = ['name', 'surname', 'document', 'email']
FLOAT_FIELDS = ['height', 'weight']


@functions_framework.cloud_event
def main(cloud_event: CloudEvent) -> None:
    # firestore_payload = firestore.DocumentEventData()
    # firestore_payload._pb.ParseFromString(cloud_event.data)
    #
    # id = firestore_payload.value.name.split('/')[-1]
    # print(f'id = {id}')
    id = 'id_exemplo'
    #
    # for item in firestore_payload.value.fields.items():
    #     key = item[0]
    #     if key in STRING_FIELDS:
    #         print(f'valor = {item[1].string_value}')
    #     if key in FLOAT_FIELDS:
    #         print(f'valor = {item[1].double_value}')

    items = [('name', 'Renato'), ('height', 1.77)]

    firestore_data = ''
    update_set = ''
    insert = ''
    values = ''

    for index, item in enumerate(items, start=1):
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
            firestore_data = f'''{firestore_data}, '{item[1]}' AS {key}'''
            # print(f'valor={item[1]}')
            # print(f'valor = {item[1].string_value}')
        if key in FLOAT_FIELDS:
            firestore_data = f'{firestore_data}, {item[1]} AS {key}'
            # print(f'valor={item[1]}')
            # print(f'valor = {item[1].double_value}')

        update_set = f'''{update_set} {key}=firestore.{key}'''
        insert = f'''{insert} {key}'''
        values = f'''{values} firestore.{key}'''

    print(create_merge_query(firestore_data, update_set, insert, values))

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

main('')
