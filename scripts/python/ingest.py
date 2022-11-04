import os
import requests
import pandas as pd
from pandas import json_normalize
import sqlite3
import psycopg2
from sqlalchemy import create_engine, engine
import logging

logger = logging.getLogger(__name__)

def main():
    IngestData().run()

class IngestData:
    def run(self):
        endpoint_list = [
            'play_by_play',
            'pitches',
            'tracking'
        ]
        for endpoint in endpoint_list:
            response = self.load(endpoint)
            normalized_table = self.normalize(response, endpoint)
            self.write(normalized_table, endpoint)

    def load(self, endpoint):
        #TODO: make verify=False not necessary
        BASE_URL = f"https://test.sdpinternal.com/api/interview/{endpoint}"
        headers = {
            "x-api-key": "95706e3b-b140-446e-a152-9ace984d8565"
        }

        response = requests.get(BASE_URL, headers=headers, verify=False, timeout=30).json()
        if response['error'] == 0:
            return response
        else:
            logging.exception(f'Ingestion for {endpoint} failed with response code {response.status_code}')

    def normalize(self, response, endpoint):
        if response['payload']:
            return json_normalize(response['payload'])
        else:
            logging.exception(f'Response for endpoint {endpoint} contained no payload.')

    def write(self, normalized_table, endpoint):
        engine = create_engine('postgresql://postgres:bamabass@localhost:5432/dw')
        try:
            normalized_table.to_sql(f'{endpoint}', engine, schema='test', if_exists='replace')
        except:
            logging.exception(f'Writing {endpoint} data to SQL failed.')
