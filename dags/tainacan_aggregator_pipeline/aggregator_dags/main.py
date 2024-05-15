
import os
import yaml
import logging

from airflow import DAG
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


def mark_all_items_to_delete(collection_name):
    logging.info(f"mark_all_items_to_delete [{collection_name}]")
    hook = MongoHook(conn_id='mongo_cache_db')
    filter_query = {}  # Filtro vazio para atualizar todos os documentos
    update_query = {'$set': {'deleted': True}}  # Query de atualização
    hook.update_many(
        mongo_collection=collection_name,
        filter_doc=filter_query,
        update_doc=update_query,
        mongo_db='agregation_cache_db'
    )


# Defining the DAG using Context Manager
with DAG(
    'taiancan-agregador',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    with open('/opt/airflow/dags/tainacan_aggregator_pipeline/aggregator_dags/inputs/config.yml', 'r') as file:
        aggregation_pipe_config_yml_file = file.read()
    aggregation_pipe_config = yaml.safe_load(aggregation_pipe_config_yml_file)

    init = DummyOperator(
        task_id='inicio',
    )

    crate_local_cache = LoadAgregationItemsDataOperator(
        task_id='load_agregation_items_data',
        aggregation_pipe_config=aggregation_pipe_config
    )

    # force_delete = DummyOperator(
    #     task_id='force_delete'
    # )

    start = init >> crate_local_cache

    # insert_data_on_agregation = TainacanDestinationOperator(
    #     task_id='insert_data_on_agregation',
    # )

    # sources_path = '/opt/airflow/dags/tainacan_aggregator_pipeline/aggregator_dags/inputs/sources.d'
    # source_files = os.listdir(sources_path)
    # all_ids_source = []

    # for source_file in source_files:
    #     full_path = os.path.join(sources_path, source_file)
    #     with open(full_path, 'r') as yml_file:
    #         source_config = yaml.safe_load(yml_file)

    #         if 'idsource' not in source_config:
    #             continue
    #         id_source = source_config['idsource']
    #         all_ids_source.append(id_source)

    #         mark_to_delete = PythonOperator(
    #             task_id=f"{id_source}.mark_all_items_to_delete",
    #             python_callable=mark_all_items_to_delete,
    #             # provide_context=True,
    #             op_kwargs={'collection_name': id_source},
    #             dag=dag,
    #         )

    #         get_items = TainacanSourceOperator(
    #             task_id=f"{id_source}.get_items",
    #             source_config=source_config
    #         )

    #         aggregation_transform_pipe = AggregationTransformPipeOperator(
    #             task_id=f"{id_source}.aggregation_transform_pipe",
    #             id_source=id_source,
    #             aggregation_pipe_config=aggregation_pipe_config
    #         )

    #         # start >> mark_to_delete >> get_items >> aggregation_transform_pipe >> insert_data_on_agregation

    # insert_data_on_agregation >> force_delete
