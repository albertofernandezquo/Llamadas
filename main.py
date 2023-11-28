from src.audio_text import transcript
from src.openai import resume
from src.csv import create_csv

def run() -> None:
    """
    Prompt development code to allow for quick run and compilation of multiple scripts.
    """

    container = "llamadas"
    input_folder = "input/"
    processed_folder = "processed/"
    output_folder = "txt/"
    final_folder = "json/"
    prompts_folder = "prompts/"
    prompt_file = "standard_summary_prompt.txt"
    csv_folder = "csv/"
    
    transcript(
        container_name = container,
        input_folder_name = input_folder,
        processed_folder_name = processed_folder,
        output_folder_name = output_folder,
        model = 'whisper-1'
    )
    resume(
        container_name = container,
        input_folder_name = output_folder,
        output_folder_name = final_folder,
        processed_folder_name = processed_folder,
        prompts_folder_name = prompts_folder,
        prompt_file = prompt_file,
        model = 'gpt-4-turbo'
    )
    create_csv(
        container_name = container,
        input_folder_name = final_folder,
        output_folder_name = csv_folder,
        processed_folder_name = processed_folder
    )
    
if __name__ == '__main__':
    run()