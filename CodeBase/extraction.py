
'''
# Assignent 1 : Pleae complete the fucntions for all the other sources in the below format
def extract_sales_data_csv(file_path,table_name,db_engine):
    print("Extrcation started for sales_data..")
    df = pd.read_csv(file_path)
    df.to_sql(table_name, db_engine, index=False, if_exists='replace')
    print("Extrcation completed for sales_data..")
if __name__ == "__main__":
    extract_sales_data_csv("SourceData/sales_data.csv","staging_sales",mysql_engine)

2. Implement logger fucntionality in extarction and transformation scripts
3. Implement the try ..except for exception handling.

'''







import pandas as pd
from sqlalchemy import create_engine
from Config.config import *

# Create mysql engine
#mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')

mysql_engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}')

# Create Oracle engine
#oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')
oracle_engine = create_engine(f'oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}')

def extract_sales_data_csv():
    print("Extrcation started for sales_data..")
    df = pd.read_csv("SourceData/sales_data.csv")
    df.to_sql("staging_sales",mysql_engine,index=False,if_exists='replace')
    print("Extrcation completed for sales_data..")

def extract_product_data_csv():
    print("Extrcation started for product_data..")
    df = pd.read_csv("SourceData/product_data.csv")
    df.to_sql("staging_product",mysql_engine,index=False,if_exists='replace')
    print("Extrcation completed for product_data..")

def extract_supplier_data_json():
    print("Extrcation started for product_data..")
    df = pd.read_json("SourceData/supplier_data.json")
    df.to_sql("staging_supplier",mysql_engine,index=False,if_exists='replace')
    print("Extrcation completed for product_data..")

def extract_inventory_data_xml():
    df = pd.read_xml("SourceData/inventory_data.xml",xpath="//item")
    df.to_sql("staging_inventory",mysql_engine,index=False,if_exists='replace')

def extract_store_data_oracle():
    query = """select * from stores"""
    df = pd.read_sql(query,oracle_engine)
    df.to_sql("staging_stores",mysql_engine,index=False,if_exists='replace')

if __name__ == "__main__":
    extract_sales_data_csv()
    extract_product_data_csv()
    extract_supplier_data_json()
    extract_inventory_data_xml()
    extract_store_data_oracle() 

