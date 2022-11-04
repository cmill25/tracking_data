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
    create_tables()
    IngestData().run()

class IngestData:
    def __init__(self):
        self.url = config['default'].url
        self.apikey = config['default'].apikey
        self.endpoints = config['default'].endpoints
        self.username = config['default'].postgres_username
        self.password = config['default'].postgres_password
        self.connection_string = f'postgresql://{self.username}:{self.password}@localhost:5432/dw

    def run(self):
        for endpoint in self.endpoints:
            response = self.load(endpoint)
            normalized_table = self.normalize(response, endpoint)
            self.write(normalized_table, endpoint)

    def load(self, endpoint):
        #TODO: make verify=False not necessary
        BASE_URL = f"https://test.sdpinternal.com/api/interview/{endpoint}"
        headers = {
            "x-api-key": self.apikey
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
        engine = create_engine(f'postgresql://{self.username}:{self.password}@localhost:5432/dw')
        try:
            normalized_table.to_sql(f'{endpoint}', engine, schema='dbo', if_exists='replace')
        except:
            logging.exception(f'Writing {endpoint} data to SQL failed.')

def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS dbo.play_by_play (
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
            description VARCHAR(255),
            PRIMARY KEY (game_pk, batterid, pitcherid, at_bat_index)
        )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS dbo.pitch_tracking (
            date DATE NOT NULL,
            game_pk VARCHAR(255) NOT NULL,
            pitchno INT NOT NULL,
            pa_of_inning INT,
            pitch_of_pa INT ,
            pitcher VARCHAR(255) NOT NULL,
            pitcherid VARCHAR(255) NOT NULL,
            pitcherthrows VARCHAR(255),
            pitcherteam VARCHAR(255),
            batter VARCHAR(255) NOT NULL,
            batterid INT NOT NULL,
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
            horzbreak DECIMAL(18, 5),
            PRIMARY KEY (game_pk, pitcherid, batterid, pitchno)
    )
        """,
        """
        CREATE TABLE IF NOT EXISTS dbo.pitches(
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
            call_desc VARCHAR(255),
            PRIMARY KEY (game_pk, batter_id, pitcher_id, at_bat_index, pitch_number)
        )
        """
    )
    conn = None
    try:
        username = config['default'].postgress_username
        password = config['default'].postgres_password
        connection_string = f'postgresql://{username}:{password}@localhost:5432/dw
        conn = psycopg2.connect(connection_string)
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
