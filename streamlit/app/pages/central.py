import streamlit as st
import os

LABS = ['Einstein', 'Hilab', 'HlaGyn']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']

st.title(":satellite: Central ARBO")

st.markdown("## Upload de dados")
lab = st.selectbox(
    'Laboratório', 
    LABS
)
lab = lab.lower()

# Upload file to the server
# =========================
uploaded_file = st.file_uploader(
    "Selecione os arquivos", 
    type=ACCEPTED_EXTENSIONS
)
path = os.path.join("/data", lab)

if uploaded_file is not None and lab is not None:
    # save uploaded file to disk

    st.markdown(
        f"**Confirmar upload?**\n{uploaded_file.name}"
    )
    confirm_bt = st.button("Confirmar")
    
    if confirm_bt:
        st.text("Upload confirmado")
        with open(os.path.join(path, uploaded_file.name), "wb") as f:
            f.write(uploaded_file.getbuffer())

# File Explorer
# =============

st.divider()
st.markdown("## Explorer\n")
st.empty()
for lab_folder in LABS:
    lab_folder = lab_folder.lower()
    path = os.path.join("/data", lab_folder)
    # List files in the folder
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]
    with st.expander(f":file_folder: **{lab_folder}**"):
        for file in files:
            col_filename, col_buttons = st.columns([.8, .2])
            col_filename.markdown(f":page_facing_up: {file}")

            col_peek, col_download, col_delete = col_buttons.columns( [.3, .3, .3] )
            #col_peek.button(":eye:", key = f"peek_{file}")
            #col_download.button(":arrow_down:", key = f"download_{file}")
            #col_delete.button(":x:", key = f"delete_{file}")




