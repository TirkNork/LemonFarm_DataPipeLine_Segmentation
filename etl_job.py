import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import boto3
from io import StringIO, BytesIO

postgres_info = {
    'username': os.environ['postgres_username'],
    'password': os.environ['postgres_password'],
    'host': os.environ['postgres_host'],
    'port': os.environ['postgres_port'],
}

s3_client = boto3.client('s3')
bucket_name = 'fullstackdata2023'
base_path = 'nay-krit/lemonfarm/'
source_path = [base_path+'lf_cafe_data.csv', base_path+'product_cat.csv',base_path+'customer_data_mockup.csv']

def get_time_of_day(time):
    '''
    Transform time to time_of_day (12:00 -> Noon)
    '''
    time_of_day = int(time[:2])
    if time_of_day < 11:
        return '0_Morning'
    elif time_of_day < 14:
        return '1_Noon'
    elif time_of_day < 18:
        return '2_Afternoon'
    else:
        return '3_Evening '

def get_day_name(day_name):
    '''
    Get day name (0_Monday) 
    '''
    day_dict = {'Monday': 0,
                'Tuesday': 1,
                'Wednesday': 2,
                'Thursday': 3,
                'Friday': 4,
                'Saturday': 5,
                'Sunday': 6
                }
    
    return f"{day_dict[day_name]}_{day_name}"

# Extract data from csv
def extract_csv():
    '''
    Extract data from csv
    '''
    lf_cafe_df = pd.read_csv('data_source/lf_cafe_data.csv')
    product_df = pd.read_csv('data_source/product_cat.csv')
    customer_df = pd.read_csv('data_source/customer_data_mockup.csv')
    
    return lf_cafe_df, product_df, customer_df

# Extract data from s3
def extract_s3(bucket_name, source_path):
    '''
    Extract data from s3
    '''
    csv_string = list()
    for i in range (len(source_path)):
        # Get object from s3
        response = s3_client.get_object(Bucket = bucket_name, Key = source_path[i])
        # Read CSV 
        csv_string.append(StringIO(response['Body'].read().decode('utf-8')))

    # Use pandas
    lf_cafe_df = pd.read_csv(csv_string[0])
    product_df = pd.read_csv(csv_string[1])
    customer_df = pd.read_csv(csv_string[2])

    return lf_cafe_df, product_df, customer_df

def transform(lf_cafe_df, product_df, customer_df):
    '''
    Transform data by join lf_cafe_df and product_df with sku_code and create new feature day_name and time_of_day
    '''
    # Join lf_cafe_df and product_df with sku_code
    transform_df = pd.merge(lf_cafe_df, customer_df, on='member_id', how='left')
    transform_df['sku_code'] = transform_df['sku_code'].astype(str)
    transform_df['code'] = transform_df['sku_code'].str[:4]
    product_df['code'] = product_df['code'].astype(str)
    transform_df = pd.merge(transform_df, product_df, on='code', how='left')
    transform_df = transform_df.rename(columns={'code': 'cat_code'})

    # day_name feature
    transform_df['day_name'] = pd.to_datetime(transform_df['bill_date'])
    transform_df['day_name'] = transform_df['day_name'].dt.day_name()
    transform_df['day_name'] = transform_df['day_name'].apply(get_day_name)

    # time_of_day feature
    transform_df['time_of_day'] = transform_df['bill_time'].apply(get_time_of_day)

    return transform_df

def load_postgres(lf_data, lf_customer):
    '''
    Load transformed result to Postgres
    '''
    database = postgres_info['username'] 
    database_url = f"postgresql+psycopg2://{postgres_info['username']}:{postgres_info['password']}@{postgres_info['host']}:{postgres_info['port']}/{database}"
    engine = create_engine(database_url)

    table_name = 'lf_data'
    print(f"Writing to database {postgres_info['username']}.{table_name} with {lf_data.shape[0]} records")
    lf_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Write lf_data successfully!")

    table_name = 'lf_customer'
    print(f"Writing to database {postgres_info['username']}.{table_name} with {lf_customer.shape[0]} records")
    lf_customer.to_sql(table_name, engine, if_exists='replace', index=False)
    print("Write lf_customer successfully!")

    
def pipeline():
    '''
    ETL Pipeline
    '''
    lf_cafe_df, product_df, customer_df = extract_s3(bucket_name, source_path)
    transform_df = transform(lf_cafe_df, product_df, customer_df)
    load_postgres(transform_df, customer_df)

    print(transform_df)
    print(transform_df.info())

if __name__ == '__main__':
    pipeline()