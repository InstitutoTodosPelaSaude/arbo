import streamlit as st
import os
import time
import pandas as pd
from datetime import datetime

LABS = ['Einstein', 'Hilab', 'HlaGyn', 'Sabin']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']

def delete_file_permanently(file_path):
    os.remove(file_path)
    st.toast(f"Arquivo {file_path.split('/')[-1]} excluído permanentemente")


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
    files.sort()
    
    with container:
        if files == []:
            st.markdown("*Nenhum arquivo encontrado*")
            return files
        
        for file in files:
            col_filename, col_buttons = st.columns([.8, .2])
            col_filename.markdown(f":page_facing_up: {file}")

            _, col_delete, _  = col_buttons.columns( [.3, .3, .3] )
            
            col_delete.button(
                ":wastebasket:", 
                key = f"delete_{path}_{file}",
                help = "Lixeira _out",
                on_click = lambda file=file: delete_file_from_folder(path, file)
            )

    return files


def folder_has_valid_files(path):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]
    return len(files) > 0


def widgets_list_files_in_folder_checkbox(path, container):

    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]

    files_selected = []
    if len(files) == 0:
        return []
    
    with container:
        
        for file in files:
            col_filename, col_checkbox = st.columns([.9, .1])

            col_filename.markdown(f":page_facing_up: {file}")
            file_is_selected = col_checkbox.checkbox("", key = f"checkbox_{path}_{file}")

            if file_is_selected:
                file_path = os.path.join(path, file)
                files_selected.append(file_path)

    return files_selected


def read_all_files_in_folder_as_df(path):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(ACCEPTED_EXTENSIONS)) ]
    files.sort()

    dt_now = datetime.now()
    dfs = []
    for file in files:
        file_path = os.path.join(path, file)
        
        dt_creation = datetime.fromtimestamp(os.path.getctime(file_path))
        duration = dt_now - dt_creation

        # If less than 1 minute, use seconds
        # If less than 1 hour, use minutes
        # If less than 1 day, use hours
        # If more than 1 day, use days
        if duration.seconds < 60:
            duration = f"{duration.seconds}s"
        elif duration.seconds < 3600:
            duration = f"{duration.seconds//60}m"
        elif duration.seconds < 86400:
            duration = f"{duration.seconds//3600}h"
        else:
            duration = f"{duration.days}d"

        df = pd.read_csv(file_path)
        dfs.append( (file, duration, df.to_csv().encode('utf-8')) )
    
    return dfs


def widgets_download_files_in_folder(path, container):
    
    path = os.path.join("/data", path)
    file_content_list = read_all_files_in_folder_as_df(path)

    
    if len(file_content_list) == 0:
        return []

    with container:
        for file_name, file_dt_creation, file in file_content_list:
            col_filename, col_buttons = st.columns([.8, .2])

            col_date, col_download, _ = col_buttons.columns([.3, .3, .3])

            col_filename.markdown(f":page_facing_up: {file_name: <60}")
            col_date.markdown(f"*{file_dt_creation}*")
            col_download.download_button(
                label = ":arrow_down:",
                data = file,
                file_name = file_name,
                mime = "text/csv",
                help = "Download",
                key = f"download_{path}_{file_name}"
            )


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
    
    st.success(f"Upload confirmado de {len(uploaded_files)} arquivos")
    for uploaded_file in uploaded_files:
        with open(os.path.join(lab_folder_path, uploaded_file.name), "wb") as f:
            f.write(uploaded_file.getbuffer())


def widgets_confirm_file_deletion():
    CONFIRMATION_TEXT = "DELETAR"

    user_text_input_delete_file = st.text_input(F"Deseja excluir os arquivos? Digite **{CONFIRMATION_TEXT}** para confirmar")

    reconfirm_delete_bt = st.button("Confirmar exclusão", type='primary')
    return reconfirm_delete_bt and user_text_input_delete_file == CONFIRMATION_TEXT






st.title(":satellite: Central ARBO")

# Upload de dados
# ===============

st.markdown("## :arrow_up: Upload de dados")
selected_lab = st.selectbox(
    'Laboratório', 
    LABS
)
selected_lab = selected_lab.lower()
widgets_upload_file(selected_lab)


# Download de dados
# =================
st.divider()
st.markdown("## :1234: Matrizes")

download_matrices_container = st.expander(":file_folder: Arquivos")
widgets_download_files_in_folder( "matrices", download_matrices_container )


# File Explorer
# =============

st.divider()
st.markdown("## :blue_book: Explorer\n")
st.empty()
for lab_lower in LABS:
    lab_lower = lab_lower.lower()
    lab_trash_path = os.path.join("/data", lab_lower)
    # List files in the folder
    lab_folder_trash_container = st.expander(f":file_folder: {lab_lower}")
    widgets_list_files_in_folder( lab_trash_path, lab_folder_trash_container )




# Lixeira
# =======
    
st.divider()
st.markdown("## :put_litter_in_its_place: Lixeira\n")
st.empty()

files_selected_in_trash = []
trash_is_empty = True
for lab in LABS:
    lab_lower = lab.lower()
    lab_trash_path = os.path.join("/data", lab_lower, "_out")

    # List files in the folder
    if not folder_has_valid_files(lab_trash_path):
        continue
    
    trash_is_empty = False
    lab_folder_trash_container = st.expander(f":file_folder: {lab_lower}")

    files_selected = widgets_list_files_in_folder_checkbox( lab_trash_path, lab_folder_trash_container )
    files_selected_in_trash += files_selected

if trash_is_empty:
    st.markdown("*A lixeira está vazia :)*")


if files_selected_in_trash != []:
    # Adicionar delete e restore
    col_button_restore, col_button_delete, _ = st.columns([.15, .15, .7])
    restore_bt = col_button_restore.button(
        "Restaurar",
        on_click = lambda fls=files_selected_in_trash: [restore_file_from_trash(file) for file in fls]
    )

    if restore_bt:
        time.sleep(3)
        st.rerun()

    confirm_delete_bt = col_button_delete.button(
        "Excluir", 
        type='primary'
    )

    if confirm_delete_bt:
        st.session_state['is_deleting_files'] = True

    if 'is_deleting_files' in st.session_state:
        if st.session_state['is_deleting_files']:
            if widgets_confirm_file_deletion():
                for file in files_selected_in_trash:
                    delete_file_permanently(file)

                st.spinner(text="Deletando...",)
                time.sleep(3)
                st.rerun()

    
    