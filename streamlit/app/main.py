import streamlit as st
import os

# get env variable 
DAGSTER_LINK = os.getenv("DAGSTER_LINK")

col_logo, col_title = st.columns([0.25, 0.75])
col_logo.image(
    "https://www.itps.org.br/imagens/itps.svg", 
    width=150
)
col_title.title("Hub")

st.divider()

col_1, col_2 = st.columns([0.5, 0.5])

col_1.link_button(":satellite: Central Arbo", "central", use_container_width = True)
# dagster
col_2.link_button(":octopus: Dagster", DAGSTER_LINK, use_container_width = True)
# adminer to postgresql
# col_3.link_button(":elephant: Postgres", "google.com", use_container_width = True)
# DBT docs
# col_4.link_button(":books: DBT Docs", "google.com", use_container_width = True)