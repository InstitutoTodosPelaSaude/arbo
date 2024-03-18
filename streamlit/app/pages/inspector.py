import streamlit as st

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))
from models.database import DWDatabaseInterface


def select_lab_and_table(container):
    KEY_COLUMNS = ['sample_id', 'test_id']
    col_labs, col_base_table, col_key = st.columns([1, 2, 1])

    # drop down for lab selection
    lab_selected = col_labs.selectbox('Laboratório', LABS)

    # drop down select table from lab
    table_selected = col_base_table.selectbox('Tabela', LABS_TABLES[lab_selected])

    # drop down select key columns
    key_selected = col_key.selectbox('Chave', KEY_COLUMNS)

    return lab_selected, table_selected, key_selected

def select_filter(container):
    COLUMNS = [
        'codigo_posto',
        'test_id',
        'state',
        'location',
        'date_testing',
        'birth_date',
        'sex',
        'exame',
        'detalhe_exame',
        'result'
    ]
    OPERATIONS = ['=', '>', '<', '>=', '<=', '!=', 'LIKE', 'NOT LIKE']


    container_filter_drop = container.expander('### Filtro')
    col_filter_column, col_filter_operation, col_filter_value = container_filter_drop.columns([ 2, 1, 2])

    column_selected = col_filter_column.selectbox('Coluna', COLUMNS)
    operation_selected = col_filter_operation.selectbox('Operação', OPERATIONS)
    value_selected = col_filter_value.text_input('Valor')

    return column_selected, operation_selected, value_selected

@st.cache_resource
def get_dw_database_connection():
    return DWDatabaseInterface.get_instance()

LABS = ['Einstein', 'Hilab', 'HlaGyn', 'Sabin']
LABS_NORM = [ x.upper() for x in LABS]
LABS_TABLES = {
    'Einstein': [
        'einstein_01_convert_types',
        'einstein_02_fix_values',
        'einstein_03_pivot_results',
        'einstein_04_fill_results',
        'einstein_05_deduplicate',
        'einstein_06_final'
    ],
    'Hilab': [
        'hilab_01_convert_types',
        'hilab_02_fix_values',
        'hilab_03_pivot_results',
        'hilab_04_fill_results',
        'hilab_05_final'
    ],
    'HlaGyn': [
        'hlagyn_01_convert_types',
        'hlagyn_02_fix_values',
        'hlagyn_03_fill_results',
        'hlagyn_04_deduplicate',
        'hlagyn_05_final'
    ],
    'Sabin': [
        'sabin_01_convert_types',
        'sabin_02_fix_values',
        'sabin_03_deduplicate_denguegi',
        'sabin_04_pivot_results',
        'sabin_05_fill_results',
        'sabin_06_deduplicate',
        'sabin_07_final'
    ]
}

st.title(':mag_right: Inspector')

select_lab_and_table(st)
select_filter(st)

st.divider()