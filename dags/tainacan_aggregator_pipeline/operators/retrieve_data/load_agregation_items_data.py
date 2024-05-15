import sys
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.models import Connection
from airflow import settings

from airflow.providers.mongo.hooks.mongo import MongoHook


from tainacan_aggregator_pipeline.hooks.tainacan_get_items_hook import TainacanGetItemsHook
from tainacan_aggregator_pipeline.hooks.tainacan_api_hook import TainacanAPIHook

sys.path.append("tainacan_aggregator_pipeline")


class LoadAgregationItemsDataOperator(BaseOperator):

    template_fields = ["aggregation_pipe_config",]

    def __init__(self, aggregation_pipe_config, **kwargs):
        self.aggregation_pipe_config = aggregation_pipe_config
        logging.info(self.aggregation_pipe_config)
        self.mongo_hook = MongoHook(conn_id='mongo_cache_db')
        self.target_collection = self.aggregation_pipe_config['pipe']['target_collection']
        self.connection_name = self.target_collection['connection_name']
        self.tainacan_api_hook = TainacanAPIHook(conn_id=self.connection_name)
        super().__init__(**kwargs)

    def page_process(self, data):
        logging.info("page_process!")

    def execute(self, context):
        logging.info(self.aggregation_pipe_config)
        logging.info(
            f"||||| load agregation items data to local DB: {self.connection_name}")
        collection_id = self.target_collection['collection_id']
        metadata_list = self.target_collection['metadata'].keys(
        )
        page = 1
        logging.info(metadata_list)
        items = self.tainacan_api_hook.get_collection_items(
            collection_id, metadata_list, page)
        logging.info(items)

        # lista_paginada = TainacanGetItemsHook(
        #     self.source_config, self.conn_id, self.page_process).run()
        # logging.info(lista_paginada)
        return
