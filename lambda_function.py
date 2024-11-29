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
    # Load mapping from environment variable
    mapping_json = os.getenv('SURVEY_TO_SQL_MAPPING')
    survey_mappings = json.loads(mapping_json)
    sids = os.getenv("SURVEY_IDS").split(",")
    print("surveyids", sids)
    for sid in sids:
        df = r.get_survey_responses(survey=sid)
        df.drop([0, 1], inplace=True)
        print(sid, df.shape)

        # Create a unique ID by combining surveyid and ResponseId
        df['unique_id'] = sid + '-' + df['ResponseId'].astype(str)
        df.set_index('unique_id', inplace=True)
        # Select the correct mapping based on the survey ID
        if sid in survey_mappings:
            mapping = survey_mappings[sid]
            csat_column = mapping.get('CSAT')
            comments_column = mapping.get('comments')

            # Construct DataFrame with required columns
            df_mapped = pd.DataFrame(index=df.index)
            df_mapped['unique_id'] = df['unique_id']
            df_mapped['CSAT'] = df[csat_column] if csat_column in df.columns else None
            df_mapped['comments'] = df[comments_column] if comments_column in df.columns else None

            # Load data into SQL table
            try:
                df_mapped.to_sql(name=table_name, con=connection,
                                 if_exists='append', index=False, method='multi')
            except Exception as e:
                print("SQL call failed:", str(e))
        else:
            print(f"No mapping found for survey ID: {sid}")

    connection.close()  # Close the connection

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({}),
    }


def apply_mapping(df, mapping):
    """Applies the survey to SQL mapping to the DataFrame."""
    mapped_df = pd.DataFrame(index=df.index)
    for sql_column, survey_column in mapping.items():
        if survey_column in df.columns:
            mapped_df[sql_column] = df[survey_column]
        else:
            # Set None or handle missing columns differently
            mapped_df[sql_column] = None

    return mapped_df
