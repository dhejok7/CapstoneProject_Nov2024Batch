import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging

# Create mysql engine
mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')

def Transform_filter_sales_data():
    query = """select * from staging_sales where sale_date >='2024-09-10'"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("filtered_sales_data", mysql_engine, index=False, if_exists='replace')

def Transform_router_sales_High_data():
    query = """select * from filtered_sales_data where region='High'"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("high_sales", mysql_engine, index=False, if_exists='replace')

def Transform_router_sales_Low_data():
    query = """select * from filtered_sales_data where region='Low'"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("low_sales", mysql_engine, index=False, if_exists='replace')


def Transform_aggregator_sales_data():
    query = """select product_id,month(sale_date) as month ,year(sale_date) as year,sum(price*quantity) as total_sales from filtered_sales_data group by product_id,month(sale_date),year(sale_date);"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("monthly_sales_summary_source", mysql_engine, index=False, if_exists='replace')

def Transform_aggregator_inventory_level():
    query = """select store_id,sum(quantity_on_hand) as total_quantity_per_store from staging_inventory group by store_id;"""
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("aggregated_inventory_level", mysql_engine, index=False, if_exists='replace')

def Transform_joiner_sale_data():
    query = """select fs.sales_id,fs.quantity,fs.sale_date,fs.price,fs.quantity*fs.price as total_sales,p.product_id,
    p.product_name,s.store_id,s.store_name
    from filtered_sales_data as fs  
    inner join staging_product as p on p.product_id = fs.product_id
    inner join staging_stores as s on s.store_id = fs.store_id
    """
    df = pd.read_sql(query, mysql_engine)
    df.to_sql("sales_with_details", mysql_engine, index=False, if_exists='replace')


if __name__ == "__main__":
    Transform_filter_sales_data()
    Transform_router_sales_Low_data()
    Transform_router_sales_High_data()
    Transform_aggregator_sales_data()
    Transform_aggregator_inventory_level()
    Transform_joiner_sale_data()


# assignemt : Generalize the functions so use at diffent tables