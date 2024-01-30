import streamlit as st
import os
import time
import pandas as pd

from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))

from models.files import (
    list_files_in_folder,
    delete_file_from_folder,
    create_file_from_content,
    delete_file_permanently, 
    restore_file_from_trash, 
    read_all_files_in_folder_as_df,
    get_zipped_folder,
    folder_has_valid_files
)

from models.database import DagsterDatabaseInterface



LABS = ['Einstein', 'Hilab', 'HlaGyn', 'Sabin']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']

# @st.cache_resource
def get_dagster_database_connection():
    return DagsterDatabaseInterface.get_instance()


def widgets_list_files_in_folder(path, container):
    files = list_files_in_folder(path, ACCEPTED_EXTENSIONS)

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
                on_click = lambda file=file: widgets_delete_file_from_folder(path, file)
            )

    return files


def widgets_list_files_in_folder_checkbox(path, container):

    files = list_files_in_folder(path, ACCEPTED_EXTENSIONS)

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


def widgets_download_files_in_folder(path, container):
    
    path = os.path.join("/data", path)
    file_content_list = read_all_files_in_folder_as_df(path, ACCEPTED_EXTENSIONS)
    zip_file_content, zip_file_name = get_zipped_folder(path, ACCEPTED_EXTENSIONS)
    
    if len(file_content_list) == 0:
        return []

    with container:
        for file_name, file_dt_creation, file in file_content_list:
            col_filename, col_buttons = st.columns([.7, .3])

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

        st.download_button(
            label = ":arrow_double_down: Download All",
            data = zip_file_content,
            file_name = zip_file_name,
            mime = "application/zip",
            help = "Download todos",
            key = f"download_all_{path}"
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
        create_file_from_content(lab_folder_path, uploaded_file.name, uploaded_file.getbuffer())


def widgets_confirm_file_deletion():
    CONFIRMATION_TEXT = "DELETAR"

    user_text_input_delete_file = st.text_input(F"Deseja excluir os arquivos? Digite **{CONFIRMATION_TEXT}** para confirmar")

    reconfirm_delete_bt = st.button("Confirmar exclusão", type='primary')
    return reconfirm_delete_bt and user_text_input_delete_file == CONFIRMATION_TEXT


def widgets_delete_file_permanently(file):
    if delete_file_permanently(file):
        st.toast(f"Arquivo {file.split('/')[-1]} excluído permanentemente")
    else:
        st.error(f"Erro ao excluir arquivo {file.split('/')[-1]}")
    

def widgets_delete_file_from_folder(path, filename):
    delete_file_from_folder(path, filename)
    st.toast(f"Arquivo {filename} movido para a lixeira")


def widgets_restore_file_from_trash(file):
    restore_file_from_trash(file)
    st.toast(f"Arquivo {file.split('/')[-1]} restaurado")


def widgets_show_last_runs_for_each_pipeline():

    runs_info = get_dagster_database_connection().get_last_run_for_each_pipeline()
    STATUS_TO_EMOJI = { 'FAILURE': ':x:', 'SUCCESS': ':white_check_mark:' }

    df_runs_info = pd.DataFrame(runs_info, columns=["run_id", "pipeline", "status", "start_timestamp", "end_timestamp"])
    df_runs_info['pipeline'] = df_runs_info['pipeline'].str.replace('"', '')

    df_runs_matrices_and_combined = df_runs_info.query("not pipeline.str.startswith('lab')", engine="python")
    df_runs_labs = df_runs_info.query("pipeline.str.startswith('lab')", engine="python")

    st.dataframe(df_runs_matrices_and_combined, height=300)

    labs = [lab.lower() for lab in LABS]
    labs_containers = zip(labs, st.columns(len(labs)))

    for lab, lab_container in labs_containers:
        with lab_container:
            st.markdown(f"**{lab.capitalize()}**")

            lab_last_run_info = df_runs_info.query(f"pipeline=='lab_{lab}'")
            lab_last_run_info_dict = lab_last_run_info.to_dict(orient='records')[0]

            lab_last_run_status = lab_last_run_info_dict['status']
            lab_last_run_start_time = lab_last_run_info_dict['start_timestamp']

            col_status, col_start_time = st.columns([.2, .8])
            col_status.markdown(f"{STATUS_TO_EMOJI[lab_last_run_status]}")
            col_start_time.markdown(f"{lab_last_run_start_time}")


dagster_database = get_dagster_database_connection()

data = dagster_database.get_last_run_for_each_pipeline()


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
st.markdown("## :blue_book: A Processar\n")
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
st.markdown("## :ballot_box_with_check: Processados\n")
st.empty()

files_selected_in_trash = []
trash_is_empty = True
for lab in LABS:
    lab_lower = lab.lower()
    lab_trash_path = os.path.join("/data", lab_lower, "_out")

    # List files in the folder
    if not folder_has_valid_files(lab_trash_path, ACCEPTED_EXTENSIONS):
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
        on_click = lambda fls=files_selected_in_trash: [widgets_restore_file_from_trash(file) for file in fls]
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
                    widgets_delete_file_permanently(file)

                st.spinner(text="Deletando...",)
                time.sleep(3)
                st.rerun()


# Últimos runs
# ============
st.divider()
st.markdown("## :chart_with_upwards_trend: Últimos runs")
widgets_show_last_runs_for_each_pipeline()