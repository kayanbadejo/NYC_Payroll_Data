import os
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv(override=True)

def get_postgres_engine():
    '''
    constructs a SQLalchemy engine object for postgres DB from .env file

    Parameters: None

    Returns:
    - sqlalchemy engine (sqlalchemy.engine.Engine)
    '''
    engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}".format(
                           user = os.getenv('DBUsername'),
                           password = os.getenv('DBPassword'),
                           host = os.getenv('DBHost'),
                           port = os.getenv('DBPort'),
                           dbname = os.getenv('DBName')     
                                )
                        )
    
    return engine
