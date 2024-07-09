from datetime import datetime

from exceptions import ParseException


class Person:
    def __init__(self, doc):
        self.doc = doc
        self.json_object = self.convert_row()

    def convert_row(self):
        doc_dict = self.doc.to_dict()
        doc_dict['id'] = self.doc.id

        try:
            if 'birth_date' in doc_dict:
                doc_dict['birth_date'] = str(datetime.date(doc_dict['birth_date']))

            if 'weight' in doc_dict:
                doc_dict['weight'] = float(doc_dict['weight'])

            if 'height' in doc_dict:
                doc_dict['height'] = float(doc_dict['height'])

        except Exception:
            raise ParseException(f'Invalid data - row={doc_dict}')

        return doc_dict
