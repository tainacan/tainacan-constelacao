import sys
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.models import Connection
from airflow import settings

from airflow.providers.mongo.hooks.mongo import MongoHook

sys.path.append("tainacan_aggregator_pipeline")


class TainacanDestinationOperator(BaseOperator):

    template_fields = []

    def __init__(self, **kwargs):
        self.conn_id = 'aggregate_conn'
        self.mongo_hook = MongoHook(conn_id='mongo_cache_db')
        super().__init__(**kwargs)

    def execute_pipe(self, data):
        return data

    def execute(self, context):
        for id_source in self.ids_source:
            logging.info(f"execute the send {id_source} source")
            documents = self.mongo_hook.find(
                mongo_collection=id_source, query={}, mongo_db='agregation_cache_db')
            # Itera sobre os documentos e faz algo com cada um
            for document in documents:
                print(document['_id'])  # Exemplo: Imprime cada documento
        return
