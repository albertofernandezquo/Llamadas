import os
import json
import pandas as pd
import io
import csv
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient


CONNECTION_STRING = os.getenv("CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)


def create_csv(container_name: str, input_folder_name: str, output_folder_name: str, processed_folder_name: str):
    
    json_input = ''
    
    # Merge all the outputs into one csv file to allow for comparision of outputs
    blob_json_list = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=input_folder_name)
    for blob in blob_json_list:
        # Get the blob client for each blob
        blob_client_json = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        download_json = blob_client_json.download_blob()
        json_text = download_json.readall().decode('UTF-8').replace('\\n','end_of_line_replace').replace('\"','').replace('\\','')
        
        # Cogemos el nombre del archivo .json
        json_file_name = blob['name'].split('/')[-1]
        json_file_name = json_file_name[0:json_file_name.find('.')]
        
        # Obtenemos solo los datos que queremos
        json_index_start = json_text.find(""":{content:""")
        json_index_end = json_text.find(""",role:assistant,""")
        json_data = json_text[json_index_start+10:json_index_end].replace('end_of_line_replace','\n')

        # Añadimos el campo 'ID'
        json_data = '<ID>' + json_file_name + '</ID>\n' + json_data
        
        data = {}
        
        for line in json_data.split('\n'):
            line = line[:line.find('</')]
            if line.strip():
                parts = line.strip('<>').split('>', 1)
                if len(parts) == 2:
                    key, value = parts
                    data[key] = value
    
        # Convert the dictionary to JSON
        json_string = json.dumps(data, ensure_ascii=False, indent=2)
        json_input = json_input + json_string + ','
        
        # Move the original json file to the processed folder
        processed_blob_name = processed_folder_name + blob.name.replace(input_folder_name, '')
        processed_blob_client = blob_service_client.get_blob_client(container=container_name, blob=processed_blob_name)
        
        # Copy the original json file to the processed folder
        processed_blob_client.start_copy_from_url(blob_client_json.url)
        
        # Check the copy status
        copy_status = processed_blob_client.get_blob_properties().copy.status
        while copy_status != 'success':
            # If it's not succeeded yet, you can wait and/or handle it depending on your needs
            # Here we just re-fetch the status
            copy_status = processed_blob_client.get_blob_properties().copy.status
        
        # Once the copy is done, delete the original from the input folder
        if copy_status == 'success':
            blob_client_json.delete_blob()
         
    json_input = json_input[:-1]
    json_input = """{"Results": [""" + json_input + """]}"""
    info = json.loads(json_input)
    data_csv = pd.json_normalize(info["Results"])
    
    #csv_file = data_csv.to_csv("example.csv")

    # Convertir el DataFrame a formato CSV
    csv_content = data_csv.to_csv(index=False)
    header = ','.join(data_csv.columns)
    
    # Create file system and directory if not exist
    fecha_hoy = str(datetime.now().date()).replace('-','')
    csv_file_name = fecha_hoy + '_merged_transcriptions.csv'
    
    # Subimos csv al blob o añadimos el contenido al archivo de hoy
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    try:
        file_system_client.create_file_system()
    except Exception as e:
                print("File system already exists:", e)
   
    directory_client = file_system_client.get_directory_client(output_folder_name)
    try:
        directory_client.create_directory()
    except Exception as e:
        print("Directory already exists:", e)
            
    # Verificar si existe el archivo .csv
    csv_file_path = output_folder_name + csv_file_name
    csv_file_client = file_system_client.get_file_client(csv_file_path)
    
    if not csv_file_client.exists():
        csv_file_client.create_file()
    
    if csv_file_client.get_file_properties().size > 0:
        print(f"El archivo {csv_file_path} ya existe.")
        # Obtener el contenido actual del archivo (si es necesario)
        blob_client_csv = blob_service_client.get_blob_client(container=container_name, blob=csv_file_path)
        download_csv = blob_client_csv.download_blob()
        contenido_actual_csv = download_csv.readall().decode('UTF-8')
        #contenido_actual_csv_without_header = contenido_actual_csv[contenido_actual_csv.find('\\n')+1:]
        
    else:
        print(f"El archivo {csv_file_path} no existe.")
        contenido_actual_csv = ''  # Crear un contenido vacío si el archivo no existe       
        
    # Se añade el csv existente al nuevo
    header = header.replace('\r\n', '\n')
    csv_content_without_header = csv_content.replace('\r\n', '\n').replace(header,'')[:-1]
    contenido_actual_csv_without_header = contenido_actual_csv.replace('\n\n', '\n').replace(header,'')
    
    csv_data = header + contenido_actual_csv_without_header + csv_content_without_header 
    
    # Subir el archivo CSV al blob
    blob_client_upload_csv = blob_service_client.get_blob_client(container=container_name, blob=csv_file_path)
    blob_client_upload_csv.upload_blob(csv_data, overwrite=True)
    
    print(f"File '{csv_file_name}' created in directory '{output_folder_name}' in the file system '{container_name}'.")