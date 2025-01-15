import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from modules.transform import transform_data

def load_dataframe_to_pgdb(engine, db_schema):
    
    created_dataframes = transform_data()
    
    '''
    Loads data from a pandas DataFrame to a PostgreSQL database table.

    Parameters:
    - engine (sqlalchemy.engine): An SQLAlchemy engine object.
    - db_schema (str): A PostgreSQL database schema.
    '''
    
    for df_name, dataframe in created_dataframes.items():
            if "NYCpayrollData" in df_name and isinstance(dataframe, pd.DataFrame):
                try:
                # Load the DataFrame to PostgreSQL
                    dataframe.to_sql(df_name, con=engine, if_exists='replace', index=False, schema=db_schema)
                    print(f'{len(dataframe)} records successfully loaded to {df_name} table in the Staging area of the NYCpayroll database.')
                except Exception as e:
                    print(f"An error occurred while loading {df_name} table: {e}")
                    
            elif "Master" in df_name and "_df" not in df_name and isinstance(dataframe, pd.DataFrame):
                try:
                # Load the DataFrame to PostgreSQL
                    dataframe.to_sql(df_name, con=engine, if_exists='replace', index=False, schema=db_schema)
                    print(f'{len(dataframe)} records successfully loaded to {df_name} table in the Staging area of the NYCpayroll database.')
                except Exception as e:
                    print(f"An error occurred while loading {df_name} table: {e}")
    

    
def exec_prc(engine):
    """
    Executes three stored procedures in the Staging schema of a PostgreSQL database.

    Args:
    - engine: SQLAlchemy database engine.

    Stored Procedures Executed:
    1. prc_EDW_Data_Loading
    2. prc_EDW_Agg_Data
     """
    try:
        # Create a session
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # List of stored procedures to execute
        procedures = ['CALL "Staging"."prc_EDW_DataLoading"()', 'CALL "Staging"."prc_agg_NYCPayrollData"()']
        
        # Execute each stored procedure in sequence
        for proc in procedures:
            session.execute(text(proc))
            print(f'Stored Procedure Executed: {proc}')
            session.commit()
        
        # Commit the transaction
        session.commit()
        print('All Stored Procedures Executed Successfully')
    
    except Exception as e:
        print(f"An error occurred while executing stored procedures: {e}")
        session.rollback()  # Rollback the transaction if any error occurs
    
    finally:
        session.close()  # Ensure session is always closed
        print('NYCpayroll ETL Pipeline Execution Completed')
