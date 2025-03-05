import pandas as pd

# Define file paths and column name as variables
input_file = "C:\\Users\\JaphetOjo\\Downloads\\HIDUU\\uk_clinical_standard_combined_feb2025.csv"
output_file = "UK_CLINICAL_STANDARD_COMBINED_FEB2025.csv"
column_to_remove = "Names"

try:
    # Load the CSV file into a pandas DataFrame
    df = pd.read_csv(input_file)
    
    # Check if the column exists
    if column_to_remove in df.columns:
        # Remove the specified column
        df = df.drop(column_to_remove, axis=1)
        
        # Save the modified DataFrame to a new CSV file
        df.to_csv(output_file, index=False)
        print(f"Successfully removed '{column_to_remove}' column and saved to {output_file}")
    else:
        print(f"Column '{column_to_remove}' not found in the CSV file.")

except Exception as e:
    print(f"An error occurred: {e}")