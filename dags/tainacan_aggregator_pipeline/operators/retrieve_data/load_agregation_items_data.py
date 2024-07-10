import sys
import uuid
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook


from tainacan_aggregator_pipeline.hooks.tainacan_get_items_hook import TainacanGetItemsHook
from tainacan_aggregator_pipeline.hooks.tainacan_api_hook import TainacanAPIHook

sys.path.append("tainacan_aggregator_pipeline")


class LoadAgregationItemsDataOperator(BaseOperator):

    template_fields = ["aggregation_pipe_config", "mongo_conn_id", ]

    def __init__(self, aggregation_pipe_config, mongo_conn_id, **kwargs):
        self.aggregation_pipe_config = aggregation_pipe_config
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_conn_id
        logging.info(self.aggregation_pipe_config)
        self.mongo_hook = MongoHook(conn_id=mongo_conn_id)

        self.target_collection = self.aggregation_pipe_config['pipe']['target_collection']
        self.connection_name = self.target_collection['connection_name']
        self.metadata_identifier = self.target_collection['metadata_identifier']
        self.metadata_hash_content = self.target_collection['metadata_hash_content']
        self.ignore_items_without_identifier = self.target_collection[
            'ignore_items_without_identifier']

        self.tainacan_api_hook = TainacanAPIHook(conn_id=self.connection_name)
        super().__init__(**kwargs)

    def transform_metadata(self, item):
        item_doc = item['document']
        metadata = item["metadata"]
        ref_id = item["id"]
        transformed_item = {}
        id_src = ""

        if isinstance(metadata, dict):
            for value in metadata.values():
                meta_id = str(value["id"])
                meta_value = value["value_as_string"]
                transformed_item[meta_id] = meta_value
                if meta_id == self.metadata_identifier:
                    id_src = meta_value

        if not self.ignore_items_without_identifier and id_src == "":
            id_src = f"_noid:{uuid.uuid1()}"

        item = {}
        item["_id"] = f"{id_src}"
        item["_ref_id"] = ref_id
        item["_to_update"] = True
        item["_to_remove"] = True
        item["_current_document"] = item_doc
        item["data"] = transformed_item
        return item

    def page_process(self, items):
        data = list(map(self.transform_metadata, items))
        # remover itens que não tenham o campo de identificação "_id"
        data = [obj for obj in data if obj.get('_id')]
        return data

    def execute(self, context):
        logging.info(
            f"||||| load agregation items data to local DB: {self.connection_name}")
        collection_id = self.target_collection['collection_id']
        metadata_list = [self.metadata_identifier, self.metadata_hash_content]
        paged = 1
        while (True):
            paged += 1
            response = self.tainacan_api_hook.get_collection_items(
                collection_id, metadata_list, paged)
            if response == False or 'items' not in response or len(response['items']) == 0:
                logging.info(f"||||| no more items!")
                break
            items = self.page_process(response['items'])
            if len(items) > 0:
                self.mongo_hook.replace_many(
                    mongo_collection='aggregation_items_data', docs=items, mongo_db=self.mongo_db, upsert=True)
            logging.info(
                f"||||| itens carregados para o cache local: {len(items)}")

        return
