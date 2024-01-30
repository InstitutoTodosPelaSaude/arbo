import os
import pandas as pd
from datetime import datetime
import io
import zipfile

def folder_has_valid_files(path, accepted_extensions=["csv", "xlsx"]):
    files = list_files_in_folder(path, accepted_extensions)
    return len(files) > 0


def list_files_in_folder(path, accepted_extensions=["csv", "xlsx"]):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(accepted_extensions)) ]
    files.sort()
    return files


def delete_file_permanently(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    else:
        return False


def create_file_from_content(path, filename, content):
    with open(os.path.join(path, filename), "wb") as f:
        f.write(content)


def delete_file_from_folder(path, filename):
    # move to _out
    
    if not os.path.exists(os.path.join(path, "_out")):
        os.makedirs(os.path.join(path, "_out"))

    os.rename(
        os.path.join(path, filename),
        os.path.join(path, "_out", filename)
    )


def restore_file_from_trash(file_path):
    # move to _out
    os.rename(
        file_path,
        file_path.replace("_out/", "")
    )


def read_all_files_in_folder_as_df(path, accepted_extensions=["csv", "xlsx"]):
    files = os.listdir(path)
    files = [ file for file in files if file.endswith(tuple(accepted_extensions)) ]
    files.sort()

    dt_now = datetime.now()
    dfs = []
    for file in files:
        file_path = os.path.join(path, file)
        
        dt_creation = datetime.fromtimestamp(os.path.getmtime(file_path))
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


def get_zipped_folder(path, accepted_extensions):
    file_content_list = read_all_files_in_folder_as_df(path, accepted_extensions)

    zip_file_name = f"{path.split('/')[-1]}.zip"
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, _, file in file_content_list:
            zip_file.writestr(file_name, file)

    return zip_buffer.getvalue(), zip_file_name