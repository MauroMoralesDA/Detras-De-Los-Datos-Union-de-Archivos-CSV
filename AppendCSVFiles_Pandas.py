import pandas as pd
import os
import time
def combine_csv_files(root_dir, output_file,output_log):
    start_time = time.time()  # Record the start time
    # DataFrame to hold all the combined data
    combined_df = pd.DataFrame()
    FilesCombined = []
    # Walk through all directories and files in the root directory
    for subdir, dirs, files in os.walk(root_dir):
        for file in files:
            # Check if the file is a CSV
            if file.endswith('.csv'):
                file_path = os.path.join(subdir, file)                
                # Read the CSV file
                df = pd.read_csv(file_path)
                FilesCombined.append(str(file)+" - Lines imported:"+str( df.shape[0]))             
                # Append to the main DataFrame
                combined_df = pd.concat([combined_df, df], ignore_index=True)
    # Write the combined DataFrame to a single CSV file
    combined_df.to_csv(output_file, index=False)  
    print(f'All CSV files have been combined into {output_file}')
    try:
        with open(output_log, 'w') as f:
            for file_detail in FilesCombined:
                f.write(file_detail + '\n')
        print(f'File details written to {output_log}')
    except Exception as exception:
        print('exception: {exception}')
        
    end_time = time.time()  # Record the end time
    elapsed_time = end_time - start_time  # Calculate the elapsed time
    print(f"Total execution time: {elapsed_time:.2f} seconds")  # Print the execution time
# Specify the root directory where CSV files are stored and the output file
root_dir = r'C:\Users\DELL\Documents\Data\CSV\Data'
output_file = r'C:\Users\DELL\Documents\Data\CSV\Output\combined_python.csv'
output_log = r'C:\Users\DELL\Documents\Data\CSV\Output\Log_python.txt'
# Combine the CSV files
combine_csv_files(root_dir, output_file,output_log)
