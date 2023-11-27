import os



sql_folder_path = os.listdir(path='/opt/microsoft/msodbcsql17/lib64/')

for file in sql_folder_path:
    if file.startswith("libmsodbcsql"):
        sql_file = file
        break


