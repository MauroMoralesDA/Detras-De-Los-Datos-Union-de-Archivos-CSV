import polars as pl
import os
import time

def combine_csv_files(root_dir, output_file, output_log):
    start_time = time.time()  # Record the start time

    # List to hold DataFrames for concatenation
    df_list = []
    FilesCombined = []

    # Walk through all directories and files in the root directory
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            # Check if the file is a CSV
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)
                try:
                    # Read the CSV file with Polars
                    df = pl.read_csv(file_path)
                    lines_imported = df.height  # Get the number of rows in the DataFrame
                    FilesCombined.append(f"{file} - Lines imported: {lines_imported}")
                    # Append the DataFrame to the list
                    df_list.append(df)
                except Exception as e:
                    print(f"Error processing file {file_path}: {e}")

    # Concatenate all DataFrames in the list
    if df_list:
        combined_df = pl.concat(df_list)
        # Write the combined DataFrame to a single CSV file
        combined_df.write_csv(output_file)
        print(f'All CSV files have been combined into {output_file}')
    else:
        print("No CSV files were found or combined.")

    # Write details to output log
    try:
        with open(output_log, 'w') as f:
            for file_detail in FilesCombined:
                f.write(file_detail + '\n')
        print(f'File details written to {output_log}')
    except Exception as exception:
        print(f'Exception: {exception}')

    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    print(f"Total execution time: {elapsed_time:.2f} seconds")  # Print the execution time

# Specify the root directory, output CSV file, and output log file
root_dir = r'C:\Users\DELL\Documents\Data\CSV\Data'
output_file = r'C:\Users\DELL\Documents\Data\CSV\Output\combined_polars.csv'
output_log = r'C:\Users\DELL\Documents\Data\CSV\Output\Log_polars.txt'

# Combine the CSV files
combine_csv_files(root_dir, output_file, output_log)
