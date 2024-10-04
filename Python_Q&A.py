import os
import pandas as pd


data_file_path = r'C:\Users\Admin\Desktop\INMAR'  
output_clean_file_path = r'C:\Users\Admin\Desktop\output_data.out'  
output_bad_file_path = r'C:\Users\Admin\Desktop\output_data.bad'      
location_file_path = r'C:\Users\Admin\Desktop\INMAR\Areas_in_blore.xlsx' 

def is_valid_file(file_path):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist.")
        return False
    
    if os.path.splitext(file_path)[1] != '.csv':
        print(f"File {file_path} is not a CSV file.")
        return False

    if os.path.getsize(file_path) == 0:
        print(f"File {file_path} is empty.")
        return False
    
    return True

def clean_phone_numbers(phone):
    return ''.join(filter(str.isdigit, str(phone)))

def clean_data(df):

    df['phone'] = df['phone'].apply(clean_phone_numbers)

    required_fields = ['name', 'phone', 'location']
    bad_data = df[df[required_fields].isnull().any(axis=1)]
    good_data = df.dropna(subset=required_fields)

    return good_data, bad_data

def validate_location(df, location_file): 
    locations = pd.read_excel(location_file)  
    merged_data = pd.merge(df, locations, how='left', left_on='location', right_on='Area')
   
    invalid_location_data = merged_data[merged_data['Area'].isnull()]
    valid_location_data = merged_data[~merged_data['Area'].isnull()]

    return valid_location_data, invalid_location_data
for filename in os.listdir(data_file_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(data_file_path, filename)

        if is_valid_file(file_path):

            df = pd.read_csv(file_path)
            clean_data_df, bad_data_df = clean_data(df)

            valid_data_df, invalid_location_df = validate_location(clean_data_df, location_file_path)
            combined_bad_data = pd.concat([bad_data_df, invalid_location_df])
            if not valid_data_df.empty:
                valid_data_df.to_csv(output_clean_file_path, mode='a', index=False, header=not os.path.exists(output_clean_file_path))
            if not combined_bad_data.empty:
                combined_bad_data.to_csv(output_bad_file_path, mode='a', index=False, header=not os.path.exists(output_bad_file_path))

            print(f"Processed file: {filename}")
        else:
            print(f"File {file_path} is not valid for processing.")
