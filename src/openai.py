import requests
import openai
import os
import io
import json
from io import BytesIO
from openai import AzureOpenAI
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

## OPEN AI
#OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
#client = OpenAI(api_key=OPENAI_API_KEY)

# AZURE
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")

client = AzureOpenAI(
    api_key = OPENAI_API_KEY,  
    api_version = OPENAI_API_VERSION,
    azure_endpoint = AZURE_OPENAI_ENDPOINT
    )

#client = OpenAI(api_key=OPENAI_API_KEY)

blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
service_client = DataLakeServiceClient.from_connection_string(CONNECTION_STRING)

def resume(container_name: str, input_folder_name: str, output_folder_name: str, processed_folder_name:str, prompts_folder_name:str, prompt_file:str, model:str):

    # Leer el prompt
    blob_client_promt = blob_service_client.get_blob_client(container=container_name, blob=prompts_folder_name+'/'+prompt_file)
    download_prompt = blob_client_promt.download_blob()
    prompt_text = download_prompt.readall().decode('UTF-8')
    
    # Loop through the blobs in the specified folder
    blob_list = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=input_folder_name)
    for blob in blob_list:
        # Get the blob client for each blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        # Download the blob content into a stream
        stream = BytesIO()
        streamdownloader = blob_client.download_blob()
        streamdownloader.readinto(stream)
        stream.seek(0)  # Reset the stream position to the beginning
        audio_blob = stream.read()
    
        # Convert the blob into an in-memory file-like object
        audio_file = io.BytesIO(audio_blob)
        audio_file.name = blob.name  # Assign the blob's name to the file-like object
    
        # assume bytes_io is a `BytesIO` object
        byte_str = audio_file.read()
    
        # Convert to a "unicode" object
        text_obj = byte_str.decode('UTF-8')
        
        # Concatenar el prompt con el texto de la llamada
        text_concatenate = prompt_text.replace('{text}',text_obj)
        
        ### headers = {
        ### "Content-Type": "application/json",
        ### "Authorization": f"Bearer {OPENAI_API_KEY}"
        ### }
        ### 
        ### payload = {
        ### "model": model,
        ### "messages": [
        ### {
        ###     "role": "user",
        ###     "content": [
        ###     {
        ###         "type": "text",
        ###         "text": text_concatenate
        ###     }
        ###     ]
        ### }
        ### ],
        ### "max_tokens": 4096
        ### }
        
        ### response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "user", "content": text_concatenate}
            ]
        )
        
        json_data = response.json()
        json_data_encoded = json.dumps(json_data, ensure_ascii=False).encode('utf-8')
        data_length = len(json_data_encoded)
       
        # Prepare the output blob name
       
        output_blob_name = blob.name.replace(input_folder_name, '') + '.json'
       
        # Create file system and directory if not exist
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
   
        # Create or overwrite the file with the JSON content
        file_client = directory_client.get_file_client(output_blob_name)
        file_client.create_file()
        file_client.append_data(data=json_data_encoded, offset=0, length = data_length)
        file_client.flush_data(data_length)

        print(f"File '{output_blob_name}' created in directory '{output_folder_name}' in the file system '{container_name}'.")
    
        
        # Move the original audio file to the processed folder
        processed_blob_name = processed_folder_name + blob.name.replace(input_folder_name, '')
        processed_blob_client = blob_service_client.get_blob_client(container=container_name, blob=processed_blob_name)
        
        # Copy the original audio file to the processed folder
        processed_blob_client.start_copy_from_url(blob_client.url)
        
        # Check the copy status
        copy_status = processed_blob_client.get_blob_properties().copy.status
        while copy_status != 'success':
            # If it's not succeeded yet, you can wait and/or handle it depending on your needs
            # Here we just re-fetch the status
            copy_status = processed_blob_client.get_blob_properties().copy.status
        
        # Once the copy is done, delete the original from the input folder
        if copy_status == 'success':
            blob_client.delete_blob()
    
        stream.close()  # Make sure to close the stream
    
    print("Resume completed for all files." + "\n")

