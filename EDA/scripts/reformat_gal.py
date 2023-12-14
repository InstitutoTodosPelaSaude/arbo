## -*- coding: utf-8 -*-

## Created by: Bragatte
## Email: marcelo.bragatte@itps.org.br
## Release date: 2023-12-14
## Last update: 2023-12-15

## Libs
import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week
from tqdm import tqdm ## add to requirements
from utils import aggregate_results, has_something_to_be_done

## Settings
import warnings
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

today = time.strftime('%Y-%m-%d', time.gmtime())

## Function Definitions
def load_table(file):
    df = ''
    if str(file).split('.')[-1] == 'tsv':
        separator = '\t'
        df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
    elif str(file).split('.')[-1] == 'csv':
        separator = ','
        df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
    elif str(file).split('.')[-1] in ['xls', 'xlsx']:
        df = pd.read_excel(file, index_col=None, header=0, sheet_name=0, dtype='str')
        df.fillna('', inplace=True)
    elif str(file).split('.')[-1] == 'parquet':
        df = pd.read_parquet(file, engine='auto')
        df.fillna('', inplace=True)
    else:
        print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX, PARQUET')
        exit()
    return df

def get_epiweeks(date):
    try:
        date = pd.to_datetime(date)
        epiweek = str(Week.fromdate(date, system="cdc")) # get epiweeks
        year, week = epiweek[:4], epiweek[-2:]
        epiweek = str(Week(int(year), int(week)).enddate())

    except:
        epiweek = ''
    return epiweek

def generate_id(column_id):
    id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
    return id

def fix_datatable(dfL):
    dfN = dfL
    if 'cod_amostra' in dfL.columns.tolist():  # Verificar se é dado do gal_am_12_12_23
        print('\t\tProcessando dados GAL AM...')

        # Transformar os dados do gal_am_12_12_23
        new_df = pd.DataFrame()  # Assumindo que combined_structure_columns está definido globalmente
        new_df['lab_id'] = 'CGLAB'
        new_df['test_id'] = dfL['cod_amostra']
        new_df['test_kit'] = dfL['exame'].replace({'Dengue, Detecção de Antígeno NS1': 'ns1_antigen'})
        new_df['location'] = dfL['mun_residencia']
        new_df['state_code'] = dfL['uf_residencia']
        new_df['date_testing'] = pd.to_datetime(dfL['dt_cadastro'], format='%d/%m/%Y %H:%M:%S')
        new_df['denv_test_result'] = dfL['resultado'].replace({'Reagente': 'Pos', 'Não Reagente': 'Neg'})

        # Gerar sample_id usando o método original de geração de ID
        id_columns = ['requisicao', 'cod_amostra']
        new_df['sample_id'] = dfL[id_columns].astype(str).sum(axis=1)
        new_df['sample_id'] = new_df['sample_id'].apply(generate_id)  # generate_id é a função original para gerar IDs
        
                dfL.fillna('', inplace=True)

        id_columns = [
            'Código Da Cápsula',
            'Região Do Brasil',
            'Estado',
            'Cidade',
            'Data Do Exame'
            ]

        for column in tqdm(id_columns):
            if column not in dfL.columns.tolist():
                dfL[column] = ''
                print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

        ## adding missing columns
        

        return new_df  # Retornar o DataFrame transformado
    return dfN

# New function to transform gal_am_12_12_23 data
def transform_gal_data(df):
    """
    Transforms the gal_am_12_12_23 data to match the structure of combined_arbo.
    :param df: DataFrame to be transformed.
    :return: Transformed DataFrame.
    """
    # Creating a new DataFrame with the required structure
    new_df = pd.DataFrame(columns=combined_structure_columns)

    # Populating the new DataFrame
    new_df['lab_id'] = 'CGLAB'
    new_df['test_id'] = df['cod_amostra']
    new_df['test_kit'] = df['exame'].replace({'Dengue, Detecção de Antígeno NS1': 'NS1_antigen'})
    new_df['location'] = df['mun_residencia']
    new_df['state_code'] = df['uf_residencia']
    new_df['date_testing'] = pd.to_datetime(df['dt_cadastro'], format='%d/%m/%Y %H:%M:%S')
    new_df['denv_test_result'] = df['resultado'].replace({'Reagente': 'Pos', 'Não Reagente': 'Neg'})

    # Generating hash for sample_id
    new_df['sample_id'] = df.apply(lambda row: hashlib.sha256((str(row['requisicao']) + str(row['cod_amostra'])).encode()).hexdigest(), axis=1)

    return new_df

## Args
if __name__ == '__main__':
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger("GAL ETL")
    # add handler to stdout
    handler = logging.StreamHandler()
    # Logger all levels
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser(
        description="Performs diverse data processing tasks for specific CGlab cases. It seamlessly loads and combines data from multiple sources and formats into a unified dataframe. It applies renaming and correction rules to columns, generates unique identifiers, and eliminates duplicates based on prior data processing. Age information is derived from birth dates, and sex information is adjusted accordingly. The resulting dataframe is sorted by date and saved as a TSV file. Duplicate rows are also identified and saved separately for further analysis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--datadir", required=True, help="Name of the folder containing independent folders for each lab")
    parser.add_argument("--rename", required=False, help="TSV, CSV, or excel file containing new standards for column names")
    parser.add_argument("--correction", required=False, help="TSV, CSV, or excel file containing data points requiring corrections")
    parser.add_argument("--cache", required=False, help="Previously processed data files")
    parser.add_argument("--output", required=True, help="TSV file aggregating all columns listed in the 'rename file'")
    args = parser.parse_args()

    path = os.path.abspath(os.getcwd())
    input_folder = path + '/' + args.datadir + '/'
    rename_file = args.rename
    correction_file = args.correction
    cache_file = args.cache
    output = args.output

    lab_data_folder = input_folder + 'CGLAB/'
    if not has_something_to_be_done(lab_data_folder):
        print(f"No files found in {lab_data_folder}")
        if cache_file not in [np.nan, '', None]: 
            print(f"Just copying {cache_file} to {output}")
            os.system(f"cp {cache_file} {output}")
        else:
            print(f"No cache file found. Nothing to be done.")
        print(f"Data successfully aggregated and saved in: {output}")
        exit()

    logger.info(f"Starting CGLAB ETL")
    logger.info(f"Input folder: {input_folder}")
    logger.info(f"Rename file: {rename_file}")
    logger.info(f"Correction file: {correction_file}")
    logger.info(f"Cache file: {cache_file}")
    logger.info(f"Output file: {output}")

    if cache_file not in [np.nan, '', None]:
        logger.info(f"Loading cache file: {cache_file}")

        dfT = load_table(cache_file)
        dfT.fillna('', inplace=True)
    else:
        logger.info(f"No cache file provided. Starting from scratch.")

        dfT = pd.DataFrame()

## load renaming patterns
    dfR = load_table(rename_file)
    dfR.fillna('', inplace=True)

    dict_rename = {}
    # dict_corrections = {}
    for idx, row in dfR.iterrows():
        id = dfR.loc[idx, 'lab_id']
        if id not in dict_rename:
            dict_rename[id] = {}
        old_colname = dfR.loc[idx, 'column_name']
        new_colname = dfR.loc[idx, 'new_name']
        rename_entry = {old_colname: new_colname}
        dict_rename[id].update(rename_entry)

## load value corrections
    dfC = load_table(correction_file)
    dfC.fillna('', inplace=True)
    dfC = dfC[dfC['lab_id'].isin(["CGLAB", "any"])] ## filter to correct data into fix_values DASA

    dict_corrections = {}
    all_ids = list(set(dfC['lab_id'].tolist()))
    for idx, row in dfC.iterrows():
        lab_id = dfC.loc[idx, 'lab_id']
        colname = dfC.loc[idx, 'column_name']

        old_data = dfC.loc[idx, 'old_data']
        new_data = dfC.loc[idx, 'new_data']
        if old_data + new_data not in ['']:
            labs = []
            if colname == 'any':
                labs = all_ids
            else:
                labs = [lab_id]
            for id in labs:
                if id not in dict_corrections:
                    dict_corrections[id] = {}
                if colname not in dict_corrections[id]:
                    dict_corrections[id][colname] = {}
                data_entry = {old_data: new_data}
                dict_corrections[id][colname].update(data_entry)

    def deduplicate(dfL, dfN, id_columns, test_name):
        # generate sample id
        dfL['unique_id'] = dfL[id_columns].astype(str).sum(axis=1)  ## combine values in rows as a long string
        dfL['sample_id'] = dfL['unique_id'].apply(lambda x: generate_id(x)[:16])  ## generate alphanumeric sample id with 16 characters

        ## prevent reprocessing of previously processed samples
        if cache_file not in [np.nan, '', None]:
            duplicates = set(dfL[dfL['sample_id'].isin(dfT['sample_id'].tolist())]['sample_id'].tolist())
            if len(duplicates) == len(set(dfL['sample_id'].tolist())):
                print('\n\t\t * ALL samples (%s) were already previously processed. All set!' % test_name)
                dfN = pd.DataFrame()  ## create empty dataframe, and populate it with reformatted data from original lab dataframe
                dfL = pd.DataFrame()

                # print('1')
                # print(dfL.head())

                return dfN, dfL
            else:
                print('\n\t\t * A total of %s out of %s samples (%s) were already previously processed.' % (str(len(duplicates)), str(len(set(dfL['sample_id'].tolist()))), test_name))
                new_samples = len(set(dfL['sample_id'].tolist())) - len(duplicates)
                print('\t\t\t - Processing %s new samples...' % (str(new_samples)))
                dfL = dfL[~dfL['sample_id'].isin(dfT['sample_id'].tolist())]  ## remove duplicates
        else:
            new_samples = len(dfL['sample_id'].tolist())
            print('\n\t\t\t - Processing %s new samples (%s)...' % (str(new_samples), test_name))

        return dfL, dfN
    # print('Done cache file')

    def rename_columns(id, df):
        # print(df.columns.tolist())
        # print(dict_rename[id])
        if id in dict_rename:
            df = df.rename(columns=dict_rename[id])
        return df

# fix data points
    def fix_data_points(id, col_name, value):
        new_value = value
        if value in dict_corrections[id][col_name]:
            new_value = dict_corrections[id][col_name][value]
        return new_value
    print('Done rename')

## open data files
    for sub_folder in os.listdir(input_folder):
        if sub_folder == 'CGLAB': ## check if folder is the correct one
            id = sub_folder
            sub_folder = sub_folder + '/'

            if not os.path.isdir(input_folder + sub_folder):
                logger.error(f"Folder {input_folder + sub_folder} not found.")
                break
            logger.info(f"Processing DataFrame from: {id}")

            for filename in sorted(os.listdir(input_folder + sub_folder)):
                
                if not filename.endswith( ('.tsv', '.csv', '.xls', '.xlsx', '.parquet') ):
                    continue

                if filename.startswith( ('~', '_') ):
                    continue

                logger.info(f"Loading data from: {input_folder + sub_folder + filename}")

                df = load_table(input_folder + sub_folder + filename)
                df.fillna('', inplace=True)
                df.reset_index(drop=True)

                logger.info(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns")

                logger.info(f"Starting to fix DataFrame - {filename}")
                df = fix_datatable(df)
                logger.info(f"Finished fixing DataFrame - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")
                if df.empty:
                    logger.warning(f"Empty DataFrame after fixing - {filename}. Check for inconsistencies.")
                    continue

                df.insert(0, 'lab_id', id)
                df = rename_columns(id, df) # fix data points
                dfT = dfT.reset_index(drop=True)
                df = df.reset_index(drop=True)

                logger.info(f"Starting to fix values - {filename}")

                dict_corrections_full = {**dict_corrections['any']}
                df = df.replace(dict_corrections_full)

                logger.info(f"Finished fixing values - {filename}")
                logger.info(f"New shape: {df.shape[0]} rows and {df.shape[1]} columns")

                df = aggregate_results(
                    df, 
                    test_id_columns=[
                        'test_id', 'test_kit'
                    ], 
                    test_result_columns=[
                        'denv_test_result', 'zikv_test_result', 'chikv_test_result','yfv_test_result', 'mayv_test_result', 'orov_test_result','wnv_test_result',
                    ],
                )

                ## add age from birthdate, if age is missing
                if 'birthdate' in df.columns.tolist():
                    for idx, row in tqdm(df.iterrows()):
                        birth = df.loc[idx, 'birthdate']
                        test = df.loc[idx, 'date_testing']
                        if birth not in [np.nan, '', None]:
                            birth = pd.to_datetime(birth)
                            test = pd.to_datetime(test) ## add to correct dtypes for calculations
                            age = (test - birth) / np.timedelta64(1, 'Y')
                            df.loc[idx, 'age'] = np.round(age, 1)                  ## this gives decimals
                            #df.loc[idx, 'age'] = int(age)
                        print(f'Processing tests {idx + 1} of {len(df)}')            ## print processed lines 

                        ## Change the data type of the 'age' column to integer
                        df['age'] = pd.to_numeric(df['age'], downcast='integer',errors='coerce').fillna(-1).astype(int)
                        df['age'] = df['age'].apply(int)

                ## fix sex information
                df['sex'] = df['sex'].apply(lambda x: x[0] if x != '' else x)

                frames = [dfT, df]
                df2 = pd.concat(frames).reset_index(drop=True)
                dfT = df2

                logger.info(f"Finished processing file: {filename}")

    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)
    # print('Done fix tables')


## reformat dates and get ages
    dfT['epiweek'] = dfT['date_testing'].apply(lambda x: get_epiweeks(x))


    ## reset index
    dfT = dfT.reset_index(drop=True)
    key_cols = [
        'lab_id',
        'test_id',
        'test_kit',
        'patient_id',
        'sample_id',
        #'region',
        'state',
        'location',
        'date_testing',
        'epiweek',
        'age',
        'sex',
        'FLUA_test_result',
        'Ct_FluA',
        'FLUB_test_result',
        'Ct_FluB',
        'VSR_test_result',
        'Ct_VSR',
        'SC2_test_result',
        'Ct_geneE',
        'Ct_geneN',
        'Ct_geneS',
        'Ct_ORF1ab',
        'Ct_RDRP',
        'geneS_detection',
        'META_test_result',
        'RINO_test_result',
        'PARA_test_result',
        'ADENO_test_result',
        'BOCA_test_result',
        'COVS_test_result',
        'ENTERO_test_result',
        'BAC_test_result',
        ]

    for col in dfT.columns.tolist():
        if col not in key_cols:
            dfT = dfT.drop(columns=[col])

    for col in key_cols:
        if col not in dfT.columns.tolist():
            logger.warning(f"Column {col} not found in the table. Adding it with empty values.")
            dfT[col] = ''

    dfT = dfT[key_cols]

    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False) # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + 'duplicates.tsv'
        dfD.to_csv(output2, sep='\t', index=False)
        logger.warning(f"File with {duplicates} duplicate entries saved in: {output2}")

    ## drop duplicates
    dfT = dfT.drop_duplicates(keep='last')

## sorting by date
    dfT = dfT.sort_values(by=['lab_id', 'test_id', 'date_testing'])

## time controller for optimization of functions `def`
    ## output combined dataframe
    dfT.to_csv(output, sep='\t', index=False)
    logger.info(f"Data successfully aggregated and saved in: {output}")