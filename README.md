# API Ingestion to Postgres db

This project contains Python/SQL code to write an API response to a local PostgreSQL database.
You will need to have a local PostgresSQL server accepting traffic on port 5432, a dbo schema in your
PostgreSQL database, as well as Python > 3.7 installed on your machine.

Clone the repo and update the config file with your API key, URL and PostgreSQL credentials.
You can then run ingest.py to populate tables to your database.

The queries found in the SQL folder can then be ran against your database to perform aggregations
on the tables and identify bad data.
