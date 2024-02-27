import os
import pandas as pd
from datetime import datetime
import io
import zipfile

def folder_has_valid_files(path, accepted_extensions=["csv", "xlsx"]):
    """
    Checks if a folder contains any valid files with the given accepted extensions.

    Args:
        path (str): The path to the folder.
        accepted_extensions (list, optional): List of accepted file extensions. Defaults to ["csv", "xlsx"].

    Returns:
        bool: True if the folder contains valid files, False otherwise.
    """
    files = list_files_in_folder(path, accepted_extensions)
    return len(files) > 0


def list_files_in_folder(path, accepted_extensions=["csv", "xlsx"]):
    """
    List files in a folder with specified accepted extensions.

    Args:
        path (str): The path to the folder.
        accepted_extensions (list, optional): List of accepted file extensions. Defaults to ["csv", "xlsx"].

    Returns:
        list: A sorted list of file names with the specified accepted extensions.
    """
    files = os.listdir(path)
    files = [file for file in files if file.endswith(tuple(accepted_extensions))]
    files.sort()
    return files


def delete_file_permanently(file_path):
    """
    Deletes a file permanently from the file system.

    Args:
        file_path (str): The path of the file to be deleted.

    Returns:
        bool: True if the file was successfully deleted, False otherwise.
    """
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    else:
        return False


def create_file_from_content(path, filename, content):
    """
    Create a file at the specified path with the given filename and write the content to it.

    Args:
        path (str): The path where the file will be created.
        filename (str): The name of the file.
        content (bytes): The content to be written to the file.

    Returns:
        None
    """
    with open(os.path.join(path, filename), "wb") as f:
        f.write(content)


def delete_file_from_folder(path, filename):
    """
    Moves a file from the specified path to a subfolder named '_out' within the same path.

    Args:
        path (str): The path of the file.
        filename (str): The name of the file.

    Returns:
        None
    """
    # move to _out
    
    if not os.path.exists(os.path.join(path, "_out")):
        os.makedirs(os.path.join(path, "_out"))

    os.rename(
        os.path.join(path, filename),
        os.path.join(path, "_out", filename)
    )


def restore_file_from_trash(file_path):
    """
    Restores a file from the trash by moving it back to its original location.
    The trash is a subfolder named '_out' within the same path.

    Args:
        file_path (str): The path of the file to be restored.

    Returns:
        None
    """
    # move to _out
    os.rename(
        file_path,
        file_path.replace("_out/", "")
    )


def read_all_files_in_folder_as_df(path, accepted_extensions=["csv", "xlsx"]):
    """
    Reads all files in a folder and returns a list of tuples containing the file name, duration since creation, and the file content as a CSV-encoded byte string.

    Parameters:
    path (str): The path to the folder containing the files.
    accepted_extensions (list, optional): List of accepted file extensions. Defaults to ["csv", "xlsx"].

    Returns:
    list: A list of tuples containing the file name, duration since creation, and the file content as a CSV-encoded byte string.
    """
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
    """
    Compresses all files with the specified accepted extensions in a folder into a zip file.

    Args:
        path (str): The path to the folder containing the files.
        accepted_extensions (list): A list of file extensions to include in the zip file.

    Returns:
        tuple: A tuple containing the compressed zip file as bytes and the name of the zip file.
    """
    file_content_list = read_all_files_in_folder_as_df(path, accepted_extensions)

    zip_file_name = f"{path.split('/')[-1]}.zip"
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file_name, _, file in file_content_list:
            zip_file.writestr(file_name, file)

    return zip_buffer.getvalue(), zip_file_name