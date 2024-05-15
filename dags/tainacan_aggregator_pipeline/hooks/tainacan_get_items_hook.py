from airflow.providers.http.hooks.http import HttpHook
import requests


class TainacanGetItemsHook(HttpHook):

    def __init__(self, source_config, conn_id, page_process_callback=None):
        self.source_config = source_config
        self.conn_id = conn_id
        self.page_process_callback = page_process_callback
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        params = self.source_config['url_parameters'] if 'url_parameters' in self.source_config else {
        }
        url_raw = self.base_url + '?' + \
            '&'.join([f"{key}={value}" for key, value in params.items()])
        return url_raw

    def connect_to_endpoint(self, url, session, page):
        request = requests.Request(
            'get', url, params=[('perpage', 50), ('paged', page)])  # @TODO pegar os nomes dos paramentros pela definição do arquivo de configuração
        prep = session.prepare_request(request)
        self.log.info(f"URL: {url}")
        return self.run_and_check(session, prep, {})

    def extract_value_from_path(self, path, object):
        paths = path.split('.')
        value = object
        for key in paths:
            value = value.get(key)
            if value is None:
                value = False
                break
        return value

    def paginate(self, url_raw, session):
        count = 1
        while True:
            response = self.connect_to_endpoint(url_raw, session, count)
            response_json = response.json()

            next_token = self.extract_value_from_path(
                # @TODO validar a existentica desse dado
                self.source_config['pagination']['response']['path_to_next_page'],
                response_json
            )
            data = self.extract_value_from_path(
                # @TODO validar a existentica desse dado
                self.source_config['pagination']['response']['path_to_items'],
                response_json
            )
            self.log.info(f"next_token: {next_token}")
            self.page_process_callback(data)
            count += 1
            if next_token == False or data == False or count >= 200:
                break

    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        return self.paginate(url_raw, session)
