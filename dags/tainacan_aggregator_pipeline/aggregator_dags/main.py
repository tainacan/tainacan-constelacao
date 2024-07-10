
import os
import yaml
import logging
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.mongo_hook import MongoHook

from tainacan_aggregator_pipeline.operators.retrieve_data.tainacan_source import TainacanSourceOperator
from tainacan_aggregator_pipeline.operators.retrieve_data.load_agregation_items_data import LoadAgregationItemsDataOperator

from tainacan_aggregator_pipeline.operators.delivery_data.tainacan_destination import TainacanDestinationOperator
from tainacan_aggregator_pipeline.operators.aggregation.aggregation_transform_pipe import AggregationTransformPipeOperator


from tainacan_aggregator_pipeline.ultils.clean_file_name import clean_file_name
from pprint import pprint


default_args = {
    'owner': 'tainacan',
    'start_date': days_ago(1)
}


# def mark_all_items_to_delete(collection_name):
#     logging.info(f"mark_all_items_to_delete [{collection_name}]")
#     hook = MongoHook(conn_id='mongo_cache_db')
#     filter_query = {}  # Filtro vazio para atualizar todos os documentos
#     update_query = {'$set': {'deleted': True}}  # Query de atualização
#     hook.update_many(
#         mongo_collection=collection_name,
#         filter_doc=filter_query,
#         update_doc=update_query,
#         mongo_db='agregation_cache_db'
#     )

def create_dag(name, config_file, inputs):
    # Defining the DAG using Context Manager
    with DAG(
        f'''tainacan-agregador-{name}''',
        default_args=default_args,
        schedule_interval=None,
    ) as dag:
        if(config_file != None):
            response_config_file = requests.get(config_file)
            if response_config_file.status_code == 200:
                pipe_config_yml_file = response_config_file.content
                aggregation_pipe_config = yaml.safe_load(pipe_config_yml_file)
                init = DummyOperator(
                    task_id='inicio',
                )

                mongo_conn_id = f'''{name}_mongo_cache_db'''
                crate_local_cache = LoadAgregationItemsDataOperator(
                    task_id='load_agregation_items_data',
                    aggregation_pipe_config=aggregation_pipe_config,
                    mongo_conn_id=mongo_conn_id
                )

                start = init >> crate_local_cache

                insert_data_on_agregation = TainacanDestinationOperator(
                    task_id='insert_data_on_agregation',
                    config=aggregation_pipe_config,
                    mongo_conn_id=mongo_conn_id
                )

                # init >> insert_data_on_agregation
                # return

                all_ids_source = []
                for source_file in inputs:
                    response_inputs = requests.get(source_file)
                    if response_inputs.status_code == 200:
                        inputs_yml_file = response_inputs.content
                        source_config = yaml.safe_load(inputs_yml_file)

                        if 'idsource' not in source_config:
                            continue
                        id_source = source_config['idsource']
                        all_ids_source.append(id_source)

                        get_items = TainacanSourceOperator(
                            task_id=f"{id_source}.get_items",
                            source_config=source_config,
                            mongo_conn_id=mongo_conn_id
                        )

                        aggregation_transform_pipe = AggregationTransformPipeOperator(
                            task_id=f"{id_source}.aggregation_transform_pipe",
                            id_source=id_source,
                            aggregation_pipe_config=aggregation_pipe_config,
                            mongo_conn_id=mongo_conn_id
                        )

                        start >> get_items >> aggregation_transform_pipe >> insert_data_on_agregation

                insert_data_on_agregation

url_setup = Variable.get("url_setup")
response = requests.get(url_setup)
if response.status_code == 200:
    yaml_content = yaml.safe_load(response.content)
    for config in yaml_content.get('configs', []):
        name = config.get('name')
        config_file_url = config.get('file')
        inputs = config.get('inputs', [])
        create_dag(
            name,
            config_file_url,
            inputs
        )
else:
    print(f"Falha ao fazer o download do arquivo. Status code: {response.status_code}")


