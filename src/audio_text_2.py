import openai
import os
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import io
import whisper
import numpy as np
import librosa
import tempfile
#from openai import OpenAI
#from pydub import AudioSegment
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

## OPEN AI
#OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
#client = OpenAI(api_key=OPENAI_API_KEY)

# AZURE
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
OPENAI_API_KEY = os.getenv("AZURE_OPENAI_WHISPER")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT_WHISPER")
OPENAI_API_VERSION = os.getenv("OPENAI_API_VERSION")
print(AZURE_OPENAI_ENDPOINT)
print(OPENAI_API_KEY)
client = AzureOpenAI(
    api_key = OPENAI_API_KEY,  
    api_version = OPENAI_API_VERSION,
    azure_endpoint = AZURE_OPENAI_ENDPOINT
    )
    
model_whisper = whisper.load_model("medium") 
 
def transcript(container_name: str, input_folder_name: str, processed_folder_name: str, output_folder_name: str, model: str):    
 
    # Initialize the connection to Azure storage account
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Loop through the blobs in the specified folder
    blob_list = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=input_folder_name)
    for blob in blob_list:
        # Get the blob client for each blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob.name)
        # Download the blob content into a stream
        #stream = BytesIO()
        #streamdownloader = blob_client.download_blob()
        #streamdownloader.readinto(stream)
        #stream.seek(0)  # Reset the stream position to the beginning
        #audio_blob = stream.read()
        #
        ## Convert the blob into an in-memory file-like object
        #audio_file = io.BytesIO(audio_blob)
        #audio_file.name = blob.name  # Assign the blob's name to the file-like object
        
        print(blob.name)
        ###
        temp_file_path='temp'+"/"+blob.name
            
        donwload_audio_temp = blob_client.download_to_filename(temp_file_path)    
        audio = AudioSegment.from_file(temp_file_path, format="wav")
        
        #audio = AudioSegment.from_file(io.BytesIO(audio_data))
        # audio_array, sr = librosa.load(io.BytesIO(audio_data), sr=None, mono=True)
        audio_array = np.array(audio.get_array_of_samples())
        
        result = model_whisper.transcribe(audio_array)
        
        #result = client.audio.transcriptions.create(
        #    model = model,
        #    file = audio_file,
        #    response_format = "text"
        #    )
        
        # Prepare the output blob name
    
        output_blob_name = output_folder_name + blob.name.replace(input_folder_name, '') + '.txt'
    
        # Create a blob client for the output blob
        output_blob_client = blob_service_client.get_blob_client(container=container_name, blob=output_blob_name)
    
        # Upload the transcription result to the output blob
        output_blob_client.upload_blob(result, overwrite=True)
    
        # Construct the output file path
        #output_transcription_file = output_directory + blob.name.replace(input_folder_name,'') + '.txt'
    
        # Write the transcription result to a file
        #with open(output_transcription_file, 'w') as f:
        #    f.write(result.text)
    
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
    
    print("Transcription completed for all files.")