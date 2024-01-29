import os
import pandas as pd
from datetime import datetime

def folder_has_valid_files(path, accepted_extensions=["csv", "xlsx"]):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(accepted_extensions)) ]
    return len(files) > 0


def delete_file_permanently(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    else:
        return False
    #st.toast(f"Arquivo {file_path.split('/')[-1]} excluído permanentemente")


def delete_file_from_folder(path, filename):
    # move to _out
    
    if not os.path.exists(os.path.join(path, "_out")):
        os.makedirs(os.path.join(path, "_out"))

    os.rename(
        os.path.join(path, filename),
        os.path.join(path, "_out", filename)
    )

    # st.toast(f"Arquivo {filename} movido para a lixeira")


def restore_file_from_trash(file_path):
    # move to _out
    os.rename(
        file_path,
        file_path.replace("_out/", "")
    )
    # st.toast(f"Arquivo {file_path.split('/')[-1]} restaurado")


def read_all_files_in_folder_as_df(path, accepted_extensions=["csv", "xlsx"]):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(accepted_extensions)) ]
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
        total_duration_in_seconds = duration.total_seconds()

        if total_duration_in_seconds < 60:
            duration = f"{total_duration_in_seconds:.0f}s"
        elif total_duration_in_seconds < 3600:
            duration = f"{total_duration_in_seconds//60:.0f}m"
        elif total_duration_in_seconds < 86400:
            duration = f"{total_duration_in_seconds//3600:.0f}h"
        else:
            duration = f"{duration.days}d {duration.seconds//3600:.0f}h"

        df = pd.read_csv(file_path)
        dfs.append( (file, duration, df.to_csv(index=False).encode('utf-8')) )
    
    return dfs
