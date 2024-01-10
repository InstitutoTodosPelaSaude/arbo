import streamlit as st
import os

LABS = ['Einstein', 'Hilab', 'HlaGyn']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']

st.title(":globe_with_meridians: Hub ARBO")

# External Links
# ==============

st.divider()

col_1, col_2, col_3, col_4 = st.columns([0.25, 0.25, 0.25, 0.25])

col_1.link_button(":satellite: Central", "central", use_container_width = True)
# dagster
col_2.link_button(":octopus: Dagster", "teste", use_container_width = True)
# adminer to postgresql
col_3.link_button(":elephant: Postgres", "google.com", use_container_width = True)
# DBT docs
col_4.link_button(":books: DBT Docs", "google.com", use_container_width = True)