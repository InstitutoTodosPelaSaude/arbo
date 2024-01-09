import streamlit as st
import os


st.title("My first app")

uploaded_file = st.file_uploader("Upload a file", type=['csv', 'txt', 'xlsx', 'xls'])
if uploaded_file is not None:
    # save uploaded file to disk
    with open(os.path.join("/app", uploaded_file.name), "wb") as f:
        f.write(uploaded_file.getbuffer())
