# Import Necessary Libraries
import os
from datetime import datetime
import pandas as pd

     
def extract_data(folder_path, previous_files_info, created_dataframes):
    """
    Extract CSV files from a dataset folder, check for new and updated files, and store DataFrames in a dictionary.

    Args:
    - folder_path (str): Path to the folder containing the CSV files.
    - previous_files_info (dict): A dictionary where keys are file names and values are modification times.
    - created_dataframes (dict): A dictionary to store and persist created DataFrames across runs.

    Returns:
    - dict: Updated file information with modification times.
    - dict: Updated dictionary holding the DataFrames.
    """
    updated_files_info = previous_files_info.copy()

    # Loop through all CSV files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):  # Process only CSV files
            file_path = os.path.join(folder_path, file_name)
            
            # Get the file's last modification time
            mod_time = os.path.getmtime(file_path)
            mod_time_readable = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
            
            if file_name not in previous_files_info:
                # New CSV file detected
                try:
                    df_name = file_name.replace('.csv', '_df')  # Create dynamic DataFrame name
                    created_dataframes[df_name] = pd.read_csv(file_path)  # Store the DataFrame in the dictionary
                    updated_files_info[file_name] = mod_time
                    
                    print(f"The newly added CSV file '{file_name}' to the Dataset folder was successfully extracted.")
                    print(f"The CSV file '{file_name}' has been extracted and stored in the dictionary with the key '{df_name}'. Last modified: {mod_time_readable}.")
                except Exception as e:
                    print(f"An error occurred while processing the new file '{file_name}': {e}")

            elif mod_time > previous_files_info[file_name]:
                # Updated CSV file detected
                try:
                    new_temp_df = pd.read_csv(file_path)  # Create a temporary DataFrame for the updated file
                    existing_df_name = file_name.replace('.csv', '_df')  # Get the name of the existing DataFrame
                    
                    # If the DataFrame already exists, append the new records
                    if existing_df_name in created_dataframes:
                        existing_df = created_dataframes[existing_df_name]
                        # Identify and append new records (if any) to the existing DataFrame
                        combined_df = pd.concat([existing_df, new_temp_df]).drop_duplicates().reset_index(drop=True)
                        created_dataframes[existing_df_name] = combined_df  # Update the existing DataFrame
                        print(f"The updated CSV file '{file_name}' has been merged with the existing DataFrame '{existing_df_name}'. Last modified: {mod_time_readable}.")
                    else:
                        # If the DataFrame doesn't exist in created_dataframes, store the new DataFrame
                        created_dataframes[existing_df_name] = new_temp_df
                        print(f"DataFrame '{existing_df_name}' created for the updated CSV file '{file_name}'. Last modified: {mod_time_readable}.")
                    
                    # Update modification time
                    updated_files_info[file_name] = mod_time
                except Exception as e:
                    print(f"An error occurred while processing the updated file '{file_name}': {e}")
            else:
                # No changes detected
                print(f"No changes detected for the file '{file_name}'. Keeping the existing DataFrame in the dictionary.")
    
    return updated_files_info, created_dataframes
    
    
    
    
       

