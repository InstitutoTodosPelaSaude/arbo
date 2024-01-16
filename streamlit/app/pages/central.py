import streamlit as st
import os

LABS = ['Einstein', 'Hilab', 'HlaGyn']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']

def widgets_list_files_in_folder(path, container):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]
    
    with container:
        for file in files:
            col_filename, col_buttons = st.columns([.8, .2])
            col_filename.markdown(f":page_facing_up: {file}")

            col_peek, col_download, col_delete = col_buttons.columns( [.3, .3, .3] )
            #col_peek.button(":eye:", key = f"peek_{file}")
            #col_download.button(":arrow_down:", key = f"download_{file}")
            #col_delete.button(":x:", key = f"delete_{file}")

    return files


st.title(":satellite: Central ARBO")

st.markdown("## Upload de dados")
selected_lab = st.selectbox(
    'Laboratório', 
    LABS
)
selected_lab = selected_lab.lower()

# Upload file to the server
# =========================
uploaded_file = st.file_uploader(
    "Selecione os arquivos", 
    type=ACCEPTED_EXTENSIONS
)
lab_folder_path = os.path.join("/data", selected_lab)

if uploaded_file is not None and selected_lab is not None:
    # save uploaded file to disk

    st.markdown(
        f"**Confirmar upload?**\n{uploaded_file.name}"
    )
    confirm_bt = st.button("Confirmar")
    
    if confirm_bt:
        st.text("Upload confirmado")
        with open(os.path.join(lab_folder_path, uploaded_file.name), "wb") as f:
            f.write(uploaded_file.getbuffer())

# File Explorer
# =============

st.divider()
st.markdown("## Explorer\n")
st.empty()
for lab_folder in LABS:
    lab_folder = lab_folder.lower()
    lab_folder_path = os.path.join("/data", lab_folder)
    # List files in the folder
    container = st.expander(f":file_folder: {lab_folder}")
    widgets_list_files_in_folder( lab_folder_path, container )
