import logging

from google.cloud import firestore
from google.cloud import bigquery

logger = logging.getLogger(__name__)

def export_firestore_to_bigquery():
    logger.info("teste de log")
    print("teste de print")
