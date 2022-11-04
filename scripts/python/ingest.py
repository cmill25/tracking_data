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

            import psycopg2


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS test.play_by_play (
            date DATE,
            game_pk VARCHAR(255) NOT NULL,
            inning INT,
            top BOOLEAN,
            at_bat_index INT,
            batterid INT,
            batterside VARCHAR(255),
            pitcherid VARCHAR(255),
            pitcherthrows VARCHAR(255),
            event_type VARCHAR(255),
            description VARCHAR(255)
        )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS test.pitch_tracking (
            date DATE,
            game_pk VARCHAR(255),
            pitchno INT,
            pa_of_inning INT,
            pitch_of_pa INT,
            pitcher VARCHAR(255),
            pitcherid VARCHAR(255),
            pitcherthrows VARCHAR(255),
            pitcherteam VARCHAR(255),
            batter VARCHAR(255),
            batterid INT,
            batterside VARCHAR(255),
            batterteam VARCHAR(255),
            inning INT,
            topbottom VARCHAR(255),
            outs INT,
            balls INT,
            strikes INT,
            playresult VARCHAR(255),
            relspeed DECIMAL(18, 5),
            spinrate DECIMAL(18, 5),
            relheight DECIMAL(18, 5),
            relside DECIMAL(18, 5),
            extension DECIMAL(18, 5),
            vertbreak DECIMAL(18, 5),
            inducedvertbreak DECIMAL(18, 5),
            horzbreak DECIMAL(18, 5)
    )

        """,
        """
        CREATE TABLE IF NOT EXISTS test.pitches(
            game_pk VARCHAR(255),
            at_bat_index INT,
            pitch_number INT,
            batter_id INT,
            batter_side VARCHAR(255),
            pitcher_id INT,
            pitcher_throws VARCHAR(255),
            terminating BOOLEAN,
            play_type VARCHAR(255),
            pre_balls INT,
            pre_strikes INT,
            pre_outs INT,
            pre_vscore INT,
            pre_hscore INT,
            post_balls INT,
            post_strikes INT,
            post_outs INT,
            post_vscore INT,
            post_hscore INT,
            call_desc VARCHAR(255)
        )
        """
    )
    conn = None
    try:
        conn = psycopg2.connect('postgresql://postgres:bamabass@localhost:5432/dw')
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
