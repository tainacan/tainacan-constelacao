import sys
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.models import Connection
from airflow import settings

from airflow.providers.mongo.hooks.mongo import MongoHook

from tainacan_aggregator_pipeline.hooks.tainacan_api_hook import TainacanAPIHook

sys.path.append("tainacan_aggregator_pipeline")


class TainacanDestinationOperator(BaseOperator):

    template_fields = ["config", "mongo_conn_id", ]

    def __init__(self, config, mongo_conn_id, **kwargs):
        self.config = config
        self.mongo_conn_id = mongo_conn_id
        self.conn_id = 'aggregate_conn'

        self.target_collection = self.config['pipe']['target_collection']
        self.connection_name = self.target_collection['connection_name']
        self.mongo_hook = MongoHook(conn_id=mongo_conn_id)
        self.mongo_db=mongo_conn_id
        self.tainacan_api_hook = TainacanAPIHook(conn_id=self.connection_name)
        super().__init__(**kwargs)

    def execute_pipe(self, data):
        return data

    def execute(self, context):
        map_metadata = self.target_collection['metadata']
        documents = self.mongo_hook.find(
            mongo_collection='aggregation_items_data', query={}, mongo_db=self.mongo_db)
        count_total = 0
        count_remove = 0
        count_upsert = 0
        count_ignored = 0
        for document in documents:
            count_total += 1
            ref_id = document['_ref_id'] if '_ref_id' in document else None
            to_update = document['_to_update']
            to_remove = document['_to_remove']

            if (to_remove == True):
                self.tainacan_api_hook.remove_item(ref_id)
                count_remove += 1
            elif (to_update == True or ref_id == None):
                data = document['data']
                item_upsert = {key: data[value]
                               for key, value in map_metadata.items()}
                self.tainacan_api_hook.upsert_item(ref_id, item_upsert)
                count_upsert += 1
            else:
                count_ignored += 1

        logging.info(
            f"||||| Total: {count_total} | Total upserted: {count_upsert} | Total removed: {count_remove} | Total ignored: {count_ignored}.")

        return
