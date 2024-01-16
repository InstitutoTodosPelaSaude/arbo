import streamlit as st
import os
import time

LABS = ['Einstein', 'Hilab', 'HlaGyn']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']

def delete_file_from_folder(path, filename):
    # move to _out
    os.rename(
        os.path.join(path, filename),
        os.path.join(path, "_out", filename)
    )

    st.toast(f"Arquivo {filename} movido para a lixeira")

def restore_file_from_trash(file_path):
    # move to _out
    os.rename(
        file_path,
        file_path.replace("_out/", "")
    )

    st.toast(f"Arquivo {file_path.split('/')[-1]} restaurado")

def widgets_list_files_in_folder(path, container):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]
    
    with container:
        if files == []:
            st.markdown("*Nenhum arquivo encontrado*")
            return files
        
        for file in files:
            col_filename, col_buttons = st.columns([.8, .2])
            col_filename.markdown(f":page_facing_up: {file}")

            col_peek, col_download, col_delete = col_buttons.columns( [.3, .3, .3] )
            #col_peek.button(":eye:", key = f"peek_{file}")
            # col_download.button(":arrow_down:", key = f"download_{file}")
            col_delete.button(
                ":wastebasket:", 
                key = f"delete_{file}",
                on_click = lambda file=file: delete_file_from_folder(path, file)
            )

    return files


def widgets_list_files_in_folder_checkbox(path, container):

    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]

    files_selected = []
    with container:
        
        for file in files:
            col_filename, col_checkbox = st.columns([.8, .2])

            col_filename.markdown(f":page_facing_up: {file}")
            file_is_selected = col_checkbox.checkbox("", key = f"checkbox_{file}")

            if file_is_selected:
                file_path = os.path.join(path, file)
                files_selected.append(file_path)

    return files_selected


def widgets_upload_file(selected_lab):
    # Upload file to the server
    # =========================
    uploaded_files = st.file_uploader(
        "Selecione os arquivos", 
        accept_multiple_files = True,
        type=ACCEPTED_EXTENSIONS
    )
    lab_folder_path = os.path.join("/data", selected_lab)

    if uploaded_files is None or uploaded_files == []:
        return
    
    if selected_lab is None:
        return

    st.markdown(
        f"**Confirmar upload?**"
    )
    confirm_bt = st.button("Confirmar")
    if not confirm_bt:
        return
    
    st.toast("Upload confirmado")
    for uploaded_file in uploaded_files:
        with open(os.path.join(lab_folder_path, uploaded_file.name), "wb") as f:
            f.write(uploaded_file.getbuffer())

            

st.title(":satellite: Central ARBO")

st.markdown("## Upload de dados")

selected_lab = st.selectbox(
    'Laboratório', 
    LABS
)
selected_lab = selected_lab.lower()
widgets_upload_file(selected_lab)




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

# Lixeira
# =======
    
st.divider()
st.markdown("## Lixeira\n")
st.empty()

files_selected_in_trash = []
for lab_folder in LABS:
    lab_folder = lab_folder.lower()
    lab_folder_path = os.path.join("/data", lab_folder, "_out")

    # List files in the folder
    container = st.expander(f":file_folder: {lab_folder}")
    files_selected = widgets_list_files_in_folder_checkbox( lab_folder_path, container )
    files_selected_in_trash += files_selected

if files_selected_in_trash != []:
    # Adicionar delete e restore
    col_button_delete, col_button_restore, _ = st.columns([.15, .15, .7])
    confirm_bt = col_button_delete.button("Excluir", type='primary')
    restore_bt = col_button_restore.button("Restaurar")

    if restore_bt:
        started_restoring_files = True
        for file in files_selected_in_trash:
            restore_file_from_trash(file)

        st.rerun()