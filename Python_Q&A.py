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






-------We can write using pyspark as well ----------------------
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace


spark = SparkSession.builder \
    .appName("Zomato Data Pipeline") \
    .getOrCreate()

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

def clean_phone_numbers(df):
    """Clean the phone number column"""
    return df.withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))

def clean_data(df):
    """Perform data quality checks and clean data"""
    df = clean_phone_numbers(df)
    
    required_fields = ['name', 'phone', 'location']
    bad_data = df.filter(col("name").isNull() | col("phone").isNull() | col("location").isNull())
    good_data = df.filter(~(col("name").isNull() | col("phone").isNull() | col("location").isNull()))

    return good_data, bad_data

def validate_location(good_data_df, location_file):

    locations = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(location_file)

    merged_data = good_data_df.join(locations, good_data_df.location == locations.Area, "left")

    valid_location_data = merged_data.filter(locations.Area.isNotNull())
    invalid_location_data = merged_data.filter(locations.Area.isNull())

    return valid_location_data, invalid_location_data


for filename in os.listdir(data_file_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(data_file_path, filename)

        if is_valid_file(file_path):
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            clean_data_df, bad_data_df = clean_data(df)
            valid_data_df, invalid_location_df = validate_location(clean_data_df, location_file_path)
            combined_bad_data = bad_data_df.union(invalid_location_df)
            if valid_data_df.count() > 0:
                valid_data_df.write.mode('append').csv(output_clean_file_path, header=True)
            if combined_bad_data.count() > 0:
                combined_bad_data.write.mode('append').csv(output_bad_file_path, header=True)

            print(f"Processed file: {filename}")
        else:
            print(f"File {file_path} is not valid for processing.")
spark.stop()

