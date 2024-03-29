import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def generate_country_epiweek_matrix(
        cube_db_table: str,
        pathogen: str,
        metric: str,
        show_testkits: bool,
        matrix_name: str
    ) -> None:
    """
    Generates a country-epiweek matrix for the given metric and testkit status
    and saves it to the database following the matrix name format.
    
    Args:
        cube_db_table: The name of the table in the database where the cube is stored.
        pathogen: The pathogen to generate the matrix for.
        metric: The metric to generate the matrix for (posneg, pos, totaltests or posrate).
        show_testkits: Whether to include testkit information in the matrix name.
        matrix_name: The name of the matrix to be saved to the database.
    """
    # Test arguments
    assert metric in ['PosNeg', 'Pos', 'totaltests', 'posrate'], 'Invalid metric. Valid metrics are PosNeg, Pos, totaltests and posrate.'
    
    # Connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Build the query
    query_metric = f'"Pos", "Neg"' if metric == 'PosNeg' else f'"{metric}"'
    query = f"""
        SELECT
            pathogen,
            {'test_kit,' if show_testkits else ''}
            epiweek_enddate,
            {query_metric}
        FROM arboviroses."{cube_db_table}"
        WHERE
            test_kit IS {'NOT ' if show_testkits else ''}NULL AND
            epiweek_enddate IS NOT NULL AND
            pathogen = '{pathogen}' AND
            country IS NULL AND
            state_code IS NULL AND
            state IS NULL AND
            lab_id IS NULL AND
            age_group IS NULL
    """

    # Execute the query
    df = pd.read_sql(query, engine)

    # Pivot the dataframe to generate the matrix
    index_list = ['pathogen', 'test_kit', 'epiweek_enddate'] if show_testkits else ['pathogen', 'epiweek_enddate']
    df = df.set_index(index_list).unstack('epiweek_enddate').reset_index()

    # Adjust the column names for the matrix format
    if metric == "PosNeg":
        # Generate two lines: one line for Pos and one for Neg results
        df_pos, df_neg = df['Pos'], df['Neg']
        
        df_pos.columns = df_pos.columns.to_flat_index()
        df_pos.insert(0, 'pathogen', df['pathogen'].iloc[0])
        df_pos.insert(1, f'{pathogen}_test_result', 'Pos')
        df_neg.columns = df_neg.columns.to_flat_index()
        df_neg.insert(0, 'pathogen', df['pathogen'].iloc[0])
        df_neg.insert(1, f'{pathogen}_test_result', 'Neg')

        if show_testkits:
            # Add the testkit column
            df_pos.insert(2, 'test_kit', df['test_kit'])
            df_neg.insert(2, 'test_kit', df['test_kit'])

        df = pd.concat([df_pos, df_neg], axis=0).rename_axis(None, axis=1)
    else:
        # Generate only one line for the metric
        new_columns = [(col[0], col[0]) if col[1] == '' else col for col in df.columns.to_list()]
        df.columns = pd.MultiIndex.from_tuples(new_columns).droplevel(0)
        df.insert(1, f'{pathogen}_test_result', metric)
      
    # Save the matrix to the database
    df.to_sql(matrix_name, engine, schema='arboviroses', if_exists='replace', index=False)
    engine.dispose() 

def generate_state_epiweek_matrix(
        cube_db_table: str,
        pathogen: str,
        matrix_name: str
    ) -> None:
    """
    Generates a state-epiweek matrix using a given cube_table.
    
    Args:
        cube_db_table: The name of the table in the database where the cube is stored.
        pathogen: The pathogen to generate the matrix for.
        matrix_name: The name of the matrix to be saved to the database.
    """
    # Connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Build the query
    query = f"""
        SELECT
            country,
            state_code,
            state,
            lab_id,
            test_kit,
            pathogen,
            epiweek_enddate,
            "Pos",
            "Neg"
        FROM arboviroses."{cube_db_table}"
        WHERE
            country IS NOT NULL AND
            state_code IS NOT NULL AND
            state IS NOT NULL AND
            lab_id IS NOT NULL AND
            test_kit IS NOT NULL AND
            epiweek_enddate IS NOT NULL AND
            age_group IS NULL AND
            pathogen = '{pathogen}'
    """

    # Execute the query
    df = pd.read_sql(query, engine)

    # Pivot the dataframe to generate the matrix
    index_list = ['country', 'state_code', 'state', 'lab_id', 'test_kit', 'pathogen', 'epiweek_enddate']
    df = df.set_index(index_list).unstack('epiweek_enddate').reset_index()

    # Adjust the column names for the matrix format
    df_pos, df_neg = df['Pos'], df['Neg']

    new_columns = [(col[0], col[0]) if col[1] == '' else col for col in df.columns.to_list()]
    df.columns = pd.MultiIndex.from_tuples(new_columns).droplevel(0)
    df = df[['country', 'state_code', 'state', 'lab_id', 'test_kit', 'pathogen']]

    df_pos.columns = df_pos.columns.to_flat_index()
    df_pos.insert(0, f'{pathogen}_test_result', 'Pos')
    df_pos = pd.concat([df, df_pos], axis=1)

    df_neg.columns = df_neg.columns.to_flat_index()
    df_neg.insert(0, f'{pathogen}_test_result', 'Neg')
    df_neg = pd.concat([df, df_neg], axis=1)

    df = pd.concat([df_pos, df_neg], axis=0)

    # Save the matrix to the database
    df.to_sql(matrix_name, engine, schema='arboviroses', if_exists='replace', index=False)

def generate_country_agegroup_matrix(
        cube_db_table: str,
        matrix_name: str
    ) -> None:
    """
    Generates a country-agegroup matrix using a given cube_table.
    
    Args:
        cube_db_table: The name of the table in the database where the cube is stored.
        matrix_name: The name of the matrix to be saved to the database.
    """
    # Connect to the database
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # Build the query
    query = f"""
        SELECT
            country,
            pathogen,
            epiweek_enddate,
            age_group,
            "Pos",
            "Neg"
        FROM arboviroses."{cube_db_table}"
        WHERE
            country IS NOT NULL AND
            state_code IS NULL AND
            state IS NULL AND
            lab_id IS NULL AND
            test_kit IS NULL AND
            epiweek_enddate IS NOT NULL AND
            pathogen IS NOT NULL AND
            age_group IS NOT NULL
    """

    # Execute the query
    df = pd.read_sql(query, engine)

    # Pivot the dataframe to generate the matrix
    index_list = ['country', 'pathogen', 'epiweek_enddate', 'age_group']
    df = df.set_index(index_list).unstack('age_group').reset_index()

    # Adjust the column names for the matrix format
    df_pos, df_neg = df['Pos'], df['Neg']

    new_columns = [(col[0], col[0]) if col[1] == '' else col for col in df.columns.to_list()]
    df.columns = pd.MultiIndex.from_tuples(new_columns).droplevel(0)
    df = df[['country', 'pathogen', 'epiweek_enddate']]

    df_pos.columns = df_pos.columns.to_flat_index()
    df_pos.insert(0, f'test_result', 'Pos')
    df_pos = pd.concat([df, df_pos], axis=1)

    df_neg.columns = df_neg.columns.to_flat_index()
    df_neg.insert(0, f'test_result', 'Neg')
    df_neg = pd.concat([df, df_neg], axis=1)

    df = pd.concat([df_pos, df_neg], axis=0).sort_values(['epiweek_enddate', 'country', 'pathogen', ])

    # Save the matrix to the database
    df.to_sql(matrix_name, engine, schema='arboviroses', if_exists='replace', index=False)