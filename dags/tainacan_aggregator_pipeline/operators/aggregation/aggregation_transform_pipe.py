import sys
import re
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

sys.path.append("tainacan_aggregator_pipeline")


def _fn_strip(value):
    if isinstance(value, list):
        return [_fn_strip(v) for v in value]
    if not isinstance(value, str):
        logging.info(f"the value: {value} is not a string.")
        return value
    words = value.split()
    stripped_string = ' '.join(words)
    return stripped_string


def _fn_lowercase(value):
    if isinstance(value, list):
        return [_fn_lowercase(v) for v in value]
    if not isinstance(value, str):
        logging.info(f"the value: {value} is not a string.")
        return value
    return value.lower()


def _fn_split(value, separators):
    if isinstance(value, list):
        return [_fn_split(v) for v in value]
    if not isinstance(value, str):
        logging.info(f"the value: {value} is not a string.")
        return value
    pattern = '|'.join(map(re.escape, separators))
    substrings = re.split(pattern, value)
    stripped_substrings = [_fn_strip(substring) for substring in substrings]
    return stripped_substrings


def _fn_capitalize(value):
    if isinstance(value, list):
        return [_fn_capitalize(v) for v in value]

    if not isinstance(value, str):
        logging.info(f"the value: {value} is not a string.")
        return value
    words = value.split()
    capitalized_words = [word.capitalize() for word in words]
    capitalized_string = ' '.join(capitalized_words)
    return capitalized_string


class AggregationTransformPipeOperator(BaseOperator):

    template_fields = ["id_source", "aggregation_pipe_config"]

    def __init__(self, id_source, aggregation_pipe_config, **kwargs):
        self.id_source = id_source
        self.aggregation_pipe_config = aggregation_pipe_config
        self.mongo_hook = MongoHook(conn_id='mongo_cache_db')
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
                    logging.info(f"fn split: {field} {separator}")
                    # for field_def, separator in field.items():
                    data[field] = _fn_split(data[field], separator) if (
                        field in data) else None
            elif function == 'capitalize':
                for field in value:
                    data[field] = _fn_capitalize(
                        data[field]) if (field in data) else None
        return data

    def execute(self, context):
        logging.info(
            f"execute aggregation transform pipe for:  {self.id_source}.")
        logging.info(
            f"config aggregation transform pipe")
        logging.info(
            self.aggregation_pipe_config)
        documents = self.mongo_hook.find(
            mongo_collection=self.id_source, query={}, mongo_db='agregation_cache_db')
        # Itera sobre os documentos e faz algo com cada um
        path_root = self.aggregation_pipe_config['pipe']['path_root']
        agg_data = []
        for document in documents:
            data = self.execute_pipe(document['data'][path_root])
            data['_id'] = f"{ self.id_source }:{document['_id']}"
            agg_data.append(data)
            print(document['_id'])  # Exemplo: Imprime cada documento
        self.mongo_hook.replace_many(
            mongo_collection='aggregation_items_data', docs=agg_data, mongo_db='agregation_cache_db', upsert=True)
        return
