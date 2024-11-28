from QualtricsAPI.Setup import Credentials
from QualtricsAPI.Survey import Responses
import sqlalchemy
import pandas as pd
import requests
import json
import os

API_TOKEN = os.getenv("API_TOKEN")
DATACENTER = os.getenv("DATACENTER")


def lambda_handler(event, context):
    print("Incoming Event:", event['body'], type(event['body']))
    body = json.loads(event['body'])

    # Initialize Qualtrics Variables:
    # sids = ['SV_3xhKFtpxNaytUYm']
    sids = os.getenv("SURVEY_IDS").split(",")
    Credentials().qualtrics_api_credentials(token=API_TOKEN, data_center=DATACENTER)
    r = Responses()

    # Initialize DB connection
    # DB ENDPOINT
    endpoint = os.environ['DB_DOMAIN']
    username = os.environ['DB_USERNAME']
    password = os.environ['DB_PASS']
    database_name = os.environ['DB_NAME']
    table_name = os.environ['TABLE_NAME']

    # SQL INIT
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{username}:{password}@{endpoint}/{database_name}')
    connection = engine.connect()

    # Iterate the surveys
    for sid in sids:
        df = r.get_survey_responses(survey=sid)
        df.drop([0, 1], inplace=True)

        # Create a unique ID by combining surveyid and ResponseId
        df['unique_id'] = sid + '-' + df['ResponseId'].astype(str)
        df.set_index('unique_id', inplace=True)

        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Load the data into our table
        with connection.begin() as transaction:
            existing_columns = pd.read_sql_table(
                table_name, connection).columns
            new_columns = df.columns.difference(existing_columns)

            for column in new_columns:
                # Adding any missing columns to the table
                alter_table_command = f'ALTER TABLE {table_name} ADD COLUMN `{column}` TEXT'
                connection.execute(sqlalchemy.text(alter_table_command))

            # Upsert: insert if not exist, else update
            # df.to_sql(table_name, connection, if_exists='append', index=True)
            upsert_data(table_name, df, connection)

    connection.close()  # Close the connection

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({}),
    }


def sanitize_column_name(column_name):
    # Replace spaces and special characters with underscores
    return column_name.replace(" ", "_").replace("(", "").replace(")", "")


def upsert_data(table_name, df, connection):
    for index, row in df.iterrows():
        # Convert each row to a dictionary to prepare it for an upsert statement
        data = row.to_dict()
        columns = ', '.join(f'`{key}`' for key in data.keys())
        values = ', '.join(f':{key}' for key in data.keys())
        updates = ', '.join(
            f'`{key}` = VALUES(`{key}`)' for key in data.keys())

        upsert_statement = text(
            f'INSERT INTO `{table_name}` ({columns}) VALUES ({values}) '
            f'ON DUPLICATE KEY UPDATE {updates}'
        )
        connection.execute(upsert_statement, **data)
