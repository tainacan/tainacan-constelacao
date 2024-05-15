from airflow.models import Connection
from airflow.providers.http.hooks.http import HttpHook
import base64
import json
import logging
# rcteste.tainacan.org
# yRtn Rsxr 7Mqa pSm9 l1Ih dA8E
# variable: tainacan_aggregator_rcteste_app_user
import requests


class TainacanAPIHook(HttpHook):

    def __init__(self, conn_id=None, *args, **kwargs):
        self.conn_id = conn_id
        # self.headers = {}
        # self.init_headers()
        super().__init__(http_conn_id=self.conn_id, *args, **kwargs)
        self.conn = self.get_connection(conn_id)

    def init_headers(self):
        connection = Connection.get_connection_from_secrets(self.conn_id)
        api_app_user = connection.login
        api_app_pass = connection.get_password()
        authorization = "Basic " + \
            base64.b64encode(f"{api_app_user}:{api_app_pass}".encode(
                'utf-8')).decode('utf-8')
        # self.headers['Authorization'] = authorization

    # def connect_to_endpoint(self, url, session, page):
    #     request = requests.Request(
    #         'get', url, params=[('perpage', 50), ('paged', page)])  # @TODO pegar os nomes dos paramentros pela definição do arquivo de configuração
    #     prep = session.prepare_request(request)
    #     self.log.info(f"URL: {url}")
    #     return self.run_and_check(session, prep, {})

    # def create_url(self):
    #     params = self.source_config['url_parameters'] if 'url_parameters' in self.source_config else {
    #     }
    #     url_raw = self.base_url + '?' + \
    #         '&'.join([f"{key}={value}" for key, value in params.items()])
    #     return url_raw

    # def run(self, data_item):
    #     session = self.get_conn()
    #     url_raw = self.create_url()
    #     return self.paginate(url_raw, session)

    def create_url(self):
        return

    def get_item(self):
        url = ''
        session = self.get_conn(headers=self.headers)
        request = requests.Request('get', url)
        prep = session.prepare_request(request)
        data = self.run_and_check(session, prep, {})
        print("get_item")
        print(self.headers)
        return

    def get_collection_items(self, collection_id, metadata_list, paged):
        # http://brasiliana.local/wp-json/tainacan/v2/collection/5/items?perpage=96&paged=1&order=DESC&orderby=date&fetch_only_meta=8,30,32,33,36,37
        url = f"https://brasiliana.museus.gov.br/wp-json/tainacan/v2/collection/{collection_id}/items"
        logging.info("url-----")
        logging.info(url)

        print(self.conn.host)
        headers = {"cache-control": "no-cache"}

        r = requests.get(url, params=[
            ('perpage', 1),
            ('paged', paged),
            ('order', 'DESC'),
            ('orderby', 'date'),
            ('fetch_only', 'status'),
            ('fetch_only_meta', "35,37,36"),
        ], headers=headers,
        )

        if int(r.status_code) == 200:
            print("good")
            return json.loads(r.text)
        # session = self.get_conn(headers={})
        # print(session)

        # r = requests.post(self.teams_url, json=teams_message, headers=headers)
        # r = requests.get(self.teams_url, json=teams_message, headers=headers)

        # prep = session.prepare_request(request)
        # data = self.run_and_check(session, prep, {})
        # return data

    def create_item(self):
        return

    def update_metadata_item(self):
        return

    def update_document_item(self):
        return
