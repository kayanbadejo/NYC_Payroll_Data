# Import Necessary Libraries
import os
from datetime import datetime
import pandas as pd
from modules.extract_data import extract_data


def transform_data():
    # Define the folder path (Dataset folder path)
    dataset_folder = r'./dags/Dataset'

    # Dictionary to store previous file information (modification times)
    previous_files_info = {}

    # Dictionary to store previously created DataFrames
    created_dataframes = {}


    previous_files_info, created_dataframes = extract_data(dataset_folder, previous_files_info, created_dataframes)
        
        # Function to check and remove duplicate rows
    def handle_duplicates():
        
        for df_name, dataframe in created_dataframes.items():
        # Count the number of duplicate rows
            num_duplicates = dataframe.duplicated().sum()
            if num_duplicates > 0:
                # Remove duplicate rows
                dataframe.drop_duplicates(inplace=True)
                print(f"{num_duplicates} duplicate rows found and removed in {df_name} DataFrame.")
                # Reset the index after dropping duplicates
                dataframe.reset_index(drop=True, inplace=True)
                created_dataframes[df_name] = dataframe 
                
            else:
                print(f"No duplicate rows found in {df_name} DataFrame.")
            print(":-----------------------------------------------------:")

        # Apply the handle_duplicates function to all DataFrames with "_df" in their names



          

    def rename_agencycode_to_agencyid():
        """
        Renames the column 'AgencyCode' to 'AgencyID' in all DataFrames 
        with 'nycpayroll' in their names found in the global variables.
        """
        # Filter global variables for DataFrames with 'nycpayroll' in their names
        nycpayroll_dfs = [(df_name, dataframe) for df_name, dataframe in created_dataframes.items() if "nycpayroll" in df_name and isinstance(dataframe, pd.DataFrame)]
        
        if not nycpayroll_dfs:
            print("No DataFrames with 'nycpayroll' found in global variables.")
            return
        
        # Iterate through the matching DataFrames
        for df_name, dataframe in nycpayroll_dfs:
            if 'AgencyCode' in dataframe.columns:
                dataframe.rename(columns={'AgencyCode': 'AgencyID'}, inplace=True)
                print(f"'AgencyCode' column in '{df_name}' has been renamed to 'AgencyID'.")
                created_dataframes[df_name] = dataframe
            else:
                print(f"No 'AgencyCode' column found in '{df_name}'.")
                print(":-----------------------------------------------------:")



    def filter_records_by_mode_fiscalyear():
        """
        Checks the 'FiscalYear' column of any DataFrame with 'nycpayroll' in its name in the global variables,
        finds the mode value of the 'FiscalYear' column, and removes records where the 'FiscalYear'
        is not equal to the mode value.
        """
        # Iterate through the global variables and check for DataFrames with 'nycpayroll' in the name
        for df_name, dataframe in created_dataframes.items():
            if isinstance(dataframe, pd.DataFrame) and "nycpayroll" in df_name:
                # Get the 'FiscalYear' column
                if 'FiscalYear' in dataframe.columns:
                    # Calculate the mode of the 'FiscalYear' column
                    mode_value = dataframe['FiscalYear'].mode()[0]
                    
                    # Filter out rows where 'FiscalYear' is not equal to the mode value
                    filtered_df = dataframe[dataframe['FiscalYear'] == mode_value]
                    
                    # Update the original DataFrame with the filtered rows
                    created_dataframes[df_name] = filtered_df
                    
                    # Display the result
                    print(f"Records with 'FiscalYear' not equal to {mode_value} have been removed from {df_name}.")
                    print(":-----------------------------------------------------------------------------------:")
                else:
                    print(f"Column 'FiscalYear' not found in {df_name}.")
                    print(":-----------------------------------------------------:")

            
            
            

    def convert_agencystartdate_to_datetime():
        """
        Converts the 'AgencyStartDate' column of any DataFrame with 'nycpayroll' in its name o datetime format.
        """
        # Iterate through the global variables and check for DataFrames with 'nycpayroll' in the name
        for df_name, dataframe in created_dataframes.items():
            if isinstance(dataframe, pd.DataFrame) and "nycpayroll" in df_name:
                # Check if 'AgencyStartDate' column exists in the DataFrame
                if 'AgencyStartDate' in dataframe.columns:
                    try:
                        # Convert the 'AgencyStartDate' column to datetime format
                        dataframe['AgencyStartDate'] = pd.to_datetime(dataframe['AgencyStartDate'], errors='coerce')
                        created_dataframes[df_name] = dataframe  # Update the global variable with the modified DataFrame
                        print(f"'AgencyStartDate' column in {df_name} has been converted to datetime format.")
                        print(":----------------------------------------------------------------------------:")
                    except Exception as e:
                        print(f"An error occurred while converting 'AgencyStartDate' in {df_name}: {e}")
                        print(":----------------------------------------------------------------------------:")
                else:
                    print(f"Column 'AgencyStartDate' not found in {df_name}.")
                    print(":--------------------------------------------------:")
    
            
            

    def concatenate_nycpayroll_dfs():
        """
        Concatenate all DataFrames with 'nycpayroll' in their names 
        into a single DataFrame named 'nycpayroll_df'.
        
        Returns:
            pd.DataFrame: The concatenated DataFrame.
        """
        # Filter global variables for DataFrames with 'nycpayroll' in their names
          # Filter global variables for DataFrames with 'nycpayroll' in their names
        nycpayroll_dfs = [dataframe for df_name, dataframe in created_dataframes.items() if "nycpayroll" in df_name and isinstance(dataframe, pd.DataFrame)]
        
        if not nycpayroll_dfs:
            print("No DataFrames with 'nycpayroll' found.")
            return None
    
        # Concatenate the DataFrames
        nycpayroll_df = pd.concat(nycpayroll_dfs, ignore_index=True)
        print(f"Concatenated {len(nycpayroll_dfs)} 'nycpayroll' DataFrames into 'nycpayroll_df'.")
        
        created_dataframes['NYCpayrollData'] = nycpayroll_df
        
        

    def handle_duplicates_in_nycpayroll_df():
        """
        Checks for duplicated records in the 'nycpayroll_df' DataFrame, removes the duplicates, 
        and displays the number of duplicates found and removed.
        """
        for df_name, dataframe in created_dataframes.items():
            if "NYCpayrollData" in df_name and isinstance(dataframe, pd.DataFrame): 
              
              # Count the number of duplicate rows
              num_duplicates = dataframe.duplicated().sum()
          
              if num_duplicates > 0:
              # Remove duplicate rows
                dataframe.drop_duplicates(inplace=True)
                print(f"{num_duplicates} duplicate rows found and removed from {df_name}.")
                print(":-----------------------------------------------------------------------:")
              
              # Reset the index after dropping duplicates
                dataframe.reset_index(drop=True, inplace=True)
                created_dataframes[df_name] = dataframe
              else:
                print("No duplicate rows found in 'nycpayroll_df'.")
        
        print(":-----------------------------------------------------:")
        
        
        
    def handle_negative_values_in_nycpayroll_df():
        """
        Checks for negative values in numerical columns of the 'nycpayroll_df' DataFrame, 
        converts them into positive values, and displays the number of negative values found 
        and changed to positive.
        """
        for df_name, dataframe in created_dataframes.items():
            if "NYCpayrollData" in df_name and isinstance(dataframe, pd.DataFrame): 
              # Select only the numerical columns in the DataFrame
              numeric_cols = dataframe.select_dtypes(include=['number']).columns
        
              # Initialize counters
              num_negative_values_found = 0
              num_negative_values_changed = 0
        
              # Iterate over the numerical columns and check for negative values
              for col in numeric_cols:
                # Find negative values in the column
                negative_values = dataframe[dataframe[col] < 0]
            
                # Count negative values
                num_negative_values_found += len(negative_values)
            
                # Convert negative values to positive
                dataframe[col] = dataframe[col].abs()  # The abs() function converts all values to positive
            
                # Count how many values were changed
                num_negative_values_changed += len(negative_values)
                
              created_dataframes[df_name] = dataframe
              # Display the results
              print(f"{num_negative_values_found} negative values found in {df_name}.")
              print(f"{num_negative_values_changed} negative values changed to positive in {df_name}.")
              print(":------------------------------------------------------------------------------:")
        



    # Function to handle missing values
    def handle_missing_values():
        # Count the number of missing values in each column
        ref_dataframes = {}
      
        # Add the new key-value pair
        ref_dataframes['NYCpayrollData'] = created_dataframes['NYCpayrollData']
        ref_dataframes['AgencyMaster'] = created_dataframes['AgencyMaster_df']
        ref_dataframes['EmployeeMaster'] = created_dataframes['EmpMaster_df']
        ref_dataframes['TitleMaster'] = created_dataframes['TitleMaster_df']
        
        for df_name, dataframe in ref_dataframes.items():
            if "NYCpayrollData" in df_name and isinstance(dataframe, pd.DataFrame):
                null_counts = dataframe.isnull().sum()
                print(f"Number of missing values in each column of {df_name} DataFrame:")
                print(null_counts)

                # Fill missing values and print row numbers where null values were found
                for column in dataframe.columns:
                    null_rows = dataframe[dataframe[column].isnull()].index.tolist()
                    if null_rows:
                        print(f"Missing values in '{column}' found at rows: {null_rows}")
                        # Fill missing values based on column type
                        if dataframe[column].dtype in ['int64', 'float64']:
                            dataframe[column].fillna(0, inplace=True)
                            print(f"Filled missing values in '{column}' with 0.")
                        else:
                            dataframe[column].fillna("UNKNOWN", inplace=True)
                            print(f"Filled missing values in '{column}' with 'UNKNOWN'.")
                                         
                    else:
                        print(f"No missing values in '{column}'.")
                    
                created_dataframes[df_name] = dataframe 
            
                
            elif "Master" in df_name and isinstance(dataframe, pd.DataFrame):
                  null_counts = dataframe.isnull().sum()
                  print(f"Number of missing values in each column of {df_name} DataFrame:")
                  print(null_counts)

                  # Fill missing values and print row numbers where null values were found
                  for column in dataframe.columns:
                      null_rows = dataframe[dataframe[column].isnull()].index.tolist()
                      if null_rows:
                          print(f"Missing values in '{column}' found at rows: {null_rows}")
                          # Fill missing values based on column type
                          if dataframe[column].dtype in ['int64', 'float64']:
                              dataframe[column].fillna(0, inplace=True)
                              print(f"Filled missing values in '{column}' with 0.")
                          else:
                              dataframe[column].fillna("UNKNOWN", inplace=True)
                              print(f"Filled missing values in '{column}' with 'UNKNOWN'.")
                            
                      else:
                        print(f"No missing values in '{column}'.")
                  created_dataframes[df_name] = dataframe 
        print(":----------------------------------------------------------------------------:")
              
        
        
    def merge_and_correct_agency_id():
        """
        Merge AgencyID from the Agency DataFrame into the payroll DataFrame and correct the AgencyID column.

        Args:
        - payroll_df (pd.DataFrame): The payroll DataFrame to be updated.
        - agency_df (pd.DataFrame): The agency DataFrame containing 'AgencyName' and 'AgencyID'.

        Returns:
        - pd.DataFrame: The updated payroll DataFrame with corrected AgencyID.
        """
        payroll_key = 'NYCpayrollData'
        agency_key = 'AgencyMaster'

        # Check if required keys are in the dictionary
        if payroll_key not in created_dataframes or agency_key not in created_dataframes:
            raise KeyError(f"Missing required DataFrame in 'created_dataframes': '{payroll_key}' or '{agency_key}'")

        # Retrieve the DataFrames
        NYCpayrollData_DF = created_dataframes[payroll_key]
        AgencyMaster_DF = created_dataframes[agency_key]

        # Ensure required columns are present in the DataFrames
        required_columns_agency = {'AgencyName', 'AgencyID'}
        if not required_columns_agency.issubset(AgencyMaster_DF.columns):
            raise ValueError(f"The agency DataFrame must contain the columns: {required_columns_agency}")
        
        if 'AgencyName' not in NYCpayrollData_DF.columns:
            raise ValueError(f"The payroll DataFrame must contain the 'AgencyName' column.")

        # Merge and correct the AgencyID column
        merged_df = NYCpayrollData_DF.merge(
            AgencyMaster_DF[['AgencyName', 'AgencyID']],
            on='AgencyName',
            suffixes=('', '_corrected')
        )
        merged_df['AgencyID'] = merged_df['AgencyID_corrected']
        merged_df.drop(columns=['AgencyID_corrected'], inplace=True)

        # Update the dictionary with the corrected DataFrame
        created_dataframes[payroll_key] = merged_df
    
     


    def update_missing_records():
        """
        Identify records in the source DataFrame that are not in the target DataFrame,
        and append the missing records to the target DataFrame based on all columns in the target DataFrame.

        Args:
        - source_df (pd.DataFrame): The source DataFrame to check for new records.
        - target_df (pd.DataFrame): The target DataFrame to append missing records.

        Returns:
        - pd.DataFrame: The updated target DataFrame with missing records appended.
        """
        
        source_key = 'NYCpayrollData'
            
        source_df = created_dataframes[source_key]
                
        for target_df_name, target_df in created_dataframes.items():
            if "Master" in target_df_name and "_df" not in target_df_name and isinstance(target_df, pd.DataFrame):

            # Use all columns in target_df as the key_columns
                key_columns = target_df.columns.tolist()
            
                # Step 1: Identify records in source_df that are not in target_df
                new_records = source_df[key_columns].merge(
                    target_df,
                    on=key_columns,
                    how='left',
                    indicator=True
                ).query('_merge == "left_only"').drop(columns=['_merge'])
                
                # Step 2: Check if there are missing records
                num_missing_records = len(new_records)
                if num_missing_records > 0:
                    print(f"{num_missing_records} missing records in the {target_df_name} found in the NYCpayrollData DataFrame.")
                    print(":---------------------------------------------------------------------------------------------------:")
                    
                    # Append missing records to target_df
                    target_df = pd.concat([target_df, new_records], ignore_index=True)
                    # Display confirmation message
                    print(f"{num_missing_records} missing records have been successfully updated in the {target_df_name} DataFrame.")
                    print(":-------------------------------------------------------------------------------------------------------:")
                    
                    created_dataframes[target_df_name] = target_df                    
                
                else:
                    # If no missing records, return the target_df unchanged
                    print(f"No missing records found. The {target_df_name} DataFrame is up-to-date.")
                    print(":----------------------------------------------------------------------------:")
                    
        return created_dataframes
 
            
    handle_duplicates()
    rename_agencycode_to_agencyid()
    filter_records_by_mode_fiscalyear()
    convert_agencystartdate_to_datetime()
    concatenate_nycpayroll_dfs()
    handle_duplicates_in_nycpayroll_df()
    handle_negative_values_in_nycpayroll_df()
    handle_missing_values()
    merge_and_correct_agency_id()
    update_missing_records()
    
    return created_dataframes


