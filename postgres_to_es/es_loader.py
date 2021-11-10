import json
from elasticsearch import Elasticsearch
from loguru import logger
from postgres_to_es.utils import backoff


class ESLoader():

    def __init__(self, config):
        self.client = Elasticsearch([dict(config.es_settings)])
        self.list = []

    @backoff()
    def create_index(self, file_path, index_name):
        with open(file_path, 'r') as index_file:
            f = json.load(index_file)
        if self.client.indices.exists(index=index_name):
            logger.warning('Index already exists')
        self.client.index(index=index_name, body=f)

    @backoff()
    def load(self, query, index_name):
        while query:
            rows = iter(query)
            for row in rows:
                self.list.extend(
                    [
                        json.dumps(
                            {
                                'index': {
                                    '_index': index_name,
                                    '_id': row['id']
                                }
                            }
                        )
                    ]
                )
                if len(self.list) == 50:
                    self.client.bulk(body='\n'.join(self.list) + '\n', index=index_name, refresh=True)
                    self.list.clear()
            self.client.bulk(body='\n'.join(self.list) + '\n', index=index_name, refresh=True)
            break

