import sys
import re
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

from tainacan_aggregator_pipeline.ultils.generate_md5_hash import generate_md5_hash

sys.path.append("tainacan_aggregator_pipeline")


def _fn_strip(value):
    if isinstance(value, list):
        return [_fn_strip(v) for v in value]
    if not isinstance(value, str):
        # logging.info(f"[_fn_strip] the value: {value} is not a string.")
        return value
    words = value.split()
    stripped_string = ' '.join(words)
    return stripped_string


def _fn_lowercase(value):
    if isinstance(value, list):
        return [_fn_lowercase(v) for v in value]
    if not isinstance(value, str):
        # logging.info(f"[_fn_lowercase] the value: {value} is not a string.")
        return value
    return value.lower()


def _fn_split(value, separators):
    if isinstance(value, list):
        return [_fn_split(v) for v in value]
    if not isinstance(value, str):
        # logging.info(f"[_fn_split] the value: {value} is not a string.")
        return value
    pattern = '|'.join(map(re.escape, separators))
    substrings = re.split(pattern, value)
    stripped_substrings = [_fn_strip(substring) for substring in substrings]
    return stripped_substrings


def _fn_capitalize(value):
    if isinstance(value, list):
        return [_fn_capitalize(v) for v in value]

    if not isinstance(value, str):
        # logging.info(f"[_fn_capitalize] the value: {value} is not a string.")
        return value
    words = value.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_string = ' '.join(capitalized_words)
    return capitalized_string


def _fn_add_hash(obj, properties_to_include):
    return generate_md5_hash(obj, properties_to_include)


class AggregationTransformPipeOperator(BaseOperator):

    template_fields = ["id_source", "aggregation_pipe_config", "mongo_conn_id", ]

    def __init__(self, id_source, aggregation_pipe_config, mongo_conn_id, **kwargs):
        self.id_source = id_source
        self.aggregation_pipe_config = aggregation_pipe_config
        self.mongo_conn_id = mongo_conn_id
        self.mongo_db = mongo_conn_id
        self.mongo_hook = MongoHook(conn_id=mongo_conn_id)
        super().__init__(**kwargs)

    def execute_pipe(self, data):
        transform = self.aggregation_pipe_config['pipe']['transform']
        for function, value in transform.items():
            if function == 'strip':
                for field in value:
                    data[field] = _fn_strip(data[field]) if (
                        field in data) else None
            elif function == 'lowercase':
                for field in value:
                    data[field] = _fn_lowercase(
                        data[field]) if (field in data) else None
            elif function == 'split':
                for field in value:
                    separator = transform['split'][field]
                    data[field] = _fn_split(data[field], separator) if (
                        field in data) else None
            elif function == 'capitalize':
                for field in value:
                    data[field] = _fn_capitalize(
                        data[field]) if (field in data) else None
            elif function == 'add_hash':
                for add in value:
                    target = add['target']
                    fields = add['fields']
                    data[target] = _fn_add_hash(data, fields)

        return data

    def execute(self, context):
        logging.info(
            f"||||| execute aggregation transform pipe for:  {self.id_source}.")
        logging.info(
            f"||||| config aggregation transform pipe")
        logging.info(
            self.aggregation_pipe_config)
        # return
        documents = self.mongo_hook.find(
            mongo_collection=self.id_source, query={}, mongo_db=self.mongo_db)
        # Itera sobre os documentos e faz algo com cada um
        path_root = self.aggregation_pipe_config['pipe']['path_root']
        metadata_identifier = self.aggregation_pipe_config[
            'pipe']['target_collection']['metadata_identifier']
        metadata_identifier_name = self.aggregation_pipe_config['pipe'][
            'target_collection']['metadata'][metadata_identifier]
        agg_data = []

        collection = self.mongo_hook.get_collection(
            mongo_collection='aggregation_items_data',
            mongo_db=self.mongo_db
        )
        total = 0
        for document in documents:
            data = self.execute_pipe(document['data'][path_root])
            item = collection.find_one({"_id": data[metadata_identifier_name]})
            if (item == None):
                item = {}
            item["_id"] = data[metadata_identifier_name]
            #todo: pegar o id do metadado de metadata_hash_content
            item["_to_update"] = "data" not in item or "4829963" not in item["data"] or item["data"]["4829963"] != data["hash-content"] 
            item["_to_remove"] = False
            item["data"] = data
            total += 1
            agg_data.append(item)
        self.mongo_hook.replace_many(
            mongo_collection='aggregation_items_data', docs=agg_data, mongo_db=self.mongo_db, upsert=True)
        
        logging.info(
            f"||||| total pipe transform: {total}")
        return
