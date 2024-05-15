import sys
from pathlib import Path
import logging

from airflow.models import BaseOperator
from airflow.models import Connection
from airflow import settings

from airflow.providers.mongo.hooks.mongo import MongoHook


from tainacan_aggregator_pipeline.hooks.tainacan_get_items_hook import TainacanGetItemsHook

sys.path.append("tainacan_aggregator_pipeline")


def create_connection_http(url, idsource):
    # conn_id = f"http_conn_{url.replace('/', '_').replace(':', '_')}"
    conn_id = idsource
    logging.info(f"Criando conexão id: {conn_id}")

    session = settings.Session()
    conn = session.query(Connection).filter(
        Connection.conn_id == conn_id).first()
    if conn is None:
        conn = Connection(
            conn_id=conn_id,
            conn_type="http",
            host=url
        )
        session.add(conn)
        session.commit()
    session.close()
    logging.info(f"Conexão HTTP criada: {conn_id}")
    return conn_id


def fn_init_data_structure(data):
    # Copia os dicionários da lista
    new_data = []
    for item in data:
        new_data.append({'_id': None,
                         'data': {},
                        'raw_data': item})
    return new_data


def fn_rename(data, transform):
    # Copia os dicionários da lista
    new_data = []
    fields = transform['fields']

    # Itera sobre os dicionários na lista
    for src_item in data:
        # Itera sobre os mapeamentos
        new_item = {}
        _id = None
        for m in fields:
            value = src_item['raw_data']
            # Obtém o valor do caminho 'from' no dicionário
            # @TODO subistituir pela função extract_value_from_path
            for key in m['from'].split('.'):
                value = value.get(key)
                if value is None:
                    break

            if 'use_as_id' in m and m['use_as_id'] == True and value is not None:
                _id = value

            # Define o valor no novo caminho 'to' no dicionário
            temp = new_item
            if value is not None:
                path_to = m['to'].split('.')
                for key in path_to[:-1]:
                    if key not in temp:
                        temp[key] = {}
                    temp = temp[key]
                temp[path_to[-1]] = value
        new_data.append({'_id': _id,
                         'data': new_item,
                        'raw_data': src_item['raw_data']})
    return new_data


def fn_add_value_on_path(data, path, value):
    tmp = data
    paths = path.split('.')
    for key in paths[:-1]:
        if key not in tmp:
            tmp[key] = {}
        tmp = tmp[key]
    tmp[paths[-1]] = value


def fn_add_fields(data, transform):
    target = transform['target']
    fields = transform['fields']
    for item in data:
        for field, value in fields.items():
            fn_add_value_on_path(item['data'], f"{target}.{field}", value)
    return data


class TainacanSourceOperator(BaseOperator):

    template_fields = ["source_config",]

    def __init__(self, source_config, **kwargs):

        self.source_config = source_config
        self.conn_id = None
        self.mongo_hook = MongoHook(conn_id='mongo_cache_db')

        if 'url' in self.source_config and 'idsource' in self.source_config:
            url = self.source_config['url']
            idsource = self.source_config['idsource']
            self.conn_id = create_connection_http(url, idsource)

        super().__init__(**kwargs)

    def page_process(self, data):
        logging.info("page_process!")

        data_transform = fn_init_data_structure(data)

        data_transform = fn_rename(
            data_transform, self.source_config['transform']['rename'])

        data_transform = fn_add_fields(
            data_transform, self.source_config['transform']['add_fields'])

        idsource = self.source_config['idsource']
        self.mongo_hook.replace_many(
            mongo_collection=idsource, docs=data_transform, mongo_db='agregation_cache_db', upsert=True)
        logging.info(data_transform)

    def execute(self, context):
        logging.info(
            f"execute with params:{self.source_config['idsource']}, {self.conn_id}")
        logging.info(self.source_config)
        lista_paginada = TainacanGetItemsHook(
            self.source_config, self.conn_id, self.page_process).run()
        logging.info(lista_paginada)
        return
