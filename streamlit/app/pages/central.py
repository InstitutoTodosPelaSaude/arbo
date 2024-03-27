import streamlit as st
import os
import time
import pandas as pd

import matplotlib.pyplot as plt
from collections import defaultdict

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
    get_file_content,
    folder_has_valid_files
)

from models.database import DagsterDatabaseInterface
from models.database import DWDatabaseInterface



LABS = ['Einstein', 'Hilab', 'HlaGyn', 'Sabin', 'Fleury']
ACCEPTED_EXTENSIONS = ['csv', 'txt', 'xlsx', 'xls', 'tsv']
st.session_state['able_to_get_list_of_files'] = True

@st.cache_resource
def get_dagster_database_connection():
    return DagsterDatabaseInterface.get_instance()


@st.cache_resource
def get_dw_database_connection():
    return DWDatabaseInterface.get_instance()


def format_timestamp(timestamp, include_hour=True):
    if include_hour:
        return timestamp.strftime("%d %b %H:%M")
    return timestamp.strftime("%d %b")


def widgets_list_files_in_folder(path, container):
    files = list_files_in_folder(path, ACCEPTED_EXTENSIONS)

    files_already_processed = get_dw_database_connection().get_list_of_files_already_processed()
    if files_already_processed != None:
        files_already_processed = set([file[0] for file in files_already_processed])
    else:
        files_already_processed = set()
        st.session_state['able_to_get_list_of_files'] = False

    with container:
        if files == []:
            st.markdown("*Nenhum arquivo encontrado*")
            return files
        
        for file in files:
            col_filename, col_buttons = st.columns([.8, .2])

            summarized_file_path = path.split('/')[-1] + '/' + file

            if summarized_file_path in files_already_processed:
                col_filename.markdown(f":page_facing_up: {file} :heavy_check_mark: ")
            else:
                col_filename.markdown(f":page_facing_up: {file} ")

            

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


def widget_download_file(file_path, container):
    file_content = get_file_content(file_path)
    filename = file_path.split('/')[-1].split('.')[0].capitalize()

    container.download_button(
        label = f":arrow_down_small: {filename}",
        data = file_content,
        file_name = file_path.split('/')[-1],
        mime = "text/csv",
        help = "Download"
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


def widgets_add_lab_info(lab, container):
    lab_latest_date = get_dw_database_connection().get_latest_date_of_lab_data()
    if lab_latest_date == None:
        st.error(f"Erro ao buscar última data dos laboratórios.")
        return
    lab_latest_date_dict = dict(lab_latest_date)

    lab = lab.upper()
    if lab not in lab_latest_date_dict:
        return

    with container:
        lab_latest_date = lab_latest_date_dict[lab]
        st.markdown(f"Dados até {format_timestamp(lab_latest_date, False)}")


def widgets_add_lab_epiweek_count_plot(lab, container):

    if lab in ['Matrices', 'Combined']:
        return

    lab = lab.upper()
    lab_epiweeks_count = get_dw_database_connection().get_number_of_tests_per_lab_in_latest_epiweeks()
    if lab_epiweeks_count == None:
        st.error(f"Erro ao buscar informações dos epiweeks")
        return
    
    lab_epiweeks_count = lab_epiweeks_count.items()
    lab_epiweeks_count = [ [*lab_epiweek.split('-'), count ] for lab_epiweek, count in lab_epiweeks_count ]
    lab_epiweeks_count_df = pd.DataFrame(lab_epiweeks_count, columns=['Lab', 'Epiweek', 'Count'])
    lab_epiweeks_count_df['Lab'] = lab_epiweeks_count_df['Lab'].str.upper()
    lab_epiweeks_count_df = lab_epiweeks_count_df.query(f"Lab=='{lab}'")

    # container.write(lab_epiweeks_count_df)

    df_chart_data = lab_epiweeks_count_df
    df_chart_data = df_chart_data.sort_values(by='Epiweek', ascending=True)
    fig = plt.figure( figsize=(10, 1) )
    # remove border
    fig.gca().spines['top'].set_visible(False)
    fig.gca().spines['right'].set_visible(False)
    fig.gca().spines['left'].set_visible(False)
    ax = df_chart_data.plot(
        x='Epiweek', 
        y='Count', 
        kind='bar', 
        ax=fig.gca(), 
        color='#00a6ed',
        # bar width
        width=0.5
    )
    # remove y-label
    ax.set_ylabel('')
    ax.set_xlabel('')
    # remove legend
    ax.get_legend().remove()
    # increase x-ticks font size
    ax.tick_params(axis='x', labelsize=20)
    # remove y-ticks
    ax.set_yticks([])

    # add the value on top of each bar
    tem_dados = False
    for p in ax.patches:
        if p.get_height() <= 0:
            continue
        tem_dados = True
        ax.annotate(
            f"{p.get_height()}",
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center',
            va='center',
            fontsize=16,
            color='black',
            xytext=(0, 10),
            textcoords='offset points'
        )

    if not tem_dados:
         container.markdown(f"*Sem dados*")
         return
    container.pyplot(fig)


def widgets_show_last_runs_for_each_pipeline():

    runs_info = get_dagster_database_connection().get_last_run_for_each_pipeline()
    if runs_info == None:
        st.error(f"Erro ao buscar informações dos runs.")
        return

    STATUS_TO_EMOJI = defaultdict(lambda: ':question:')
    STATUS_TO_EMOJI['FAILURE'] = ':x:'
    STATUS_TO_EMOJI['SUCCESS'] = ':white_check_mark:'
    STATUS_TO_EMOJI['CANCELED'] = ':x:'

    df_runs_info = pd.DataFrame(
        runs_info, 
        columns=[
            "run_id", 
            "pipeline", 
            "status", 
            "start_timestamp", 
            "end_timestamp"
        ]
    )
    df_runs_info['pipeline'] = df_runs_info['pipeline'].str.replace('"', '')

    matrices_and_combined = ['matrices', 'combined']
    labs = ['lab_'+lab.lower() for lab in LABS]

    for pipeline in labs+matrices_and_combined:
        #with container:
        border_container = st.container(border=True)
        pipeline_name = pipeline.replace('lab_', '').capitalize()

        with border_container:

            pipe_last_run_info = df_runs_info.query(f"pipeline=='{pipeline}'")
            pipe_last_run_info_dict = pipe_last_run_info.to_dict(orient='records')[0]

            pipe_last_run_status = pipe_last_run_info_dict['status']
            pipe_last_run_start_time = pipe_last_run_info_dict['start_timestamp']

            pipe_status_date = f"{STATUS_TO_EMOJI[pipe_last_run_status]} "
            pipe_status_date += f"{format_timestamp(pipe_last_run_start_time)}"
            
            col_name, col_status, col_last_info, col_epiweek_count = st.columns([.15, .2, .2, .45])

            col_name.markdown(f"**{pipeline_name}**")
            col_status.markdown(pipe_status_date)
    
            widgets_add_lab_info(pipeline_name, col_last_info)
            widgets_add_lab_epiweek_count_plot(pipeline_name, col_epiweek_count)




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
widget_download_file("/data/combined/combined.tsv", st)

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

if st.session_state['able_to_get_list_of_files'] == False:
    st.warning(":warning: Erro ao buscar lista de já arquivos já processados")



# Lixeira
# =======
    
st.divider()
st.markdown("## :ok: Processados\n")
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


# Última run
# ============
st.divider()
st.markdown("## :arrows_counterclockwise:  Última run")
widgets_show_last_runs_for_each_pipeline()

# Utils
# =====

# Clear Cache
st.divider()

col_1, col_2, col_pad = st.columns([.2, .4, .4])

if col_1.button("Clear Cache"):
    st.cache_resource.clear()

if col_2.button("Rerun"):
    st.rerun()