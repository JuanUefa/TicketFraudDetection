# snow_utils/session.py
 
import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
 
def get_snowpark_session() -> Session:
    """
    Creates and returns a Snowpark Session using connection details from a .env file.
    """
    load_dotenv()
 
    connection_parameters = {
        "account": os.getenv("SF_ACCOUNT"),
        "user": os.getenv("SF_USER"),
        #"password": os.getenv("SF_PASSWORD"),
        "authenticator": os.getenv("SF_AUTHENTICATOR"), 
        "role": os.getenv("SF_ROLE"),
        "warehouse": os.getenv("SF_WAREHOUSE"),
        "database": os.getenv("SF_DATABASE"),
        "schema": os.getenv("SF_SCHEMA"),
    }
 
    return Session.builder.configs(connection_parameters).create()