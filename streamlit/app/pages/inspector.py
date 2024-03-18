import streamlit as st

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))


def select_lab_and_table(container):
    col_labs, col_base_table = st.columns([1, 3])

    # drop down for lab selection
    lab_selected = col_labs.selectbox('Laboratório', LABS)

    # drop down select table from lab
    table_selected = col_base_table.selectbox('Tabela', LABS_TABLES[lab_selected])
    st.divider()

    return lab_selected, table_selected


LABS = ['Einstein', 'Hilab', 'HlaGyn', 'Sabin']
LABS_NORM = [ x.upper() for x in LABS]
LABS_TABLES = {
    'Einstein': ['A', 'B', 'C'],
    'Hilab': ['D', 'E', 'F'],
}


st.title(':mag_right: Inspector')

select_lab_and_table(st)
