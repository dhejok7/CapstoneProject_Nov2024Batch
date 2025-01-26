from sqlalchemy import create_engine, text
import logging

# Create mysql engine
mysql_engine = create_engine('mysql+pymysql://root:Admin%40143@localhost:3308/retaildwh')
logging.basicConfig(
    filename='Logs/etlprocess.log',  # Name of the log file
    filemode='a',  # 'a' to append, 'w' to overwrite
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    level=logging.INFO  # Set the logging level
)
logger = logging.getLogger(__name__)

def load_fact_sales_table():
    query = text("""insert into fact_sales(sales_id,product_id,store_id,quantity,total_sales,sale_date) 
            select sales_id,product_id,store_id,quantity,total_sales,sale_date from sales_with_details""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("Sales fact table loading started...")
            logger.info("The query being exeuted is:")
            logger.info(query)
            conn.execute(query)
            conn.commit()
            logger.info("Sales fact table loading completed...")
    except Exception as e:
        logger.error(" Error encountered while loading ",e,exc_info=True)

def load_fact_inventory_table():
    query = text("""insert into fact_inventory(product_id,store_id,quantity_on_hand,last_updated)
            select product_id,store_id,quantity_on_hand,last_updated from staging_inventory;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("inventory fact table loading started...")
            logger.info("The query being exeuted is:")
            logger.info(query)
            conn.execute(query)
            conn.commit()
            logger.info("inventory fact table loading completed...")
    except Exception as e:
        logger.error(" Error encountered while loading ",e,exc_info=True)

def load_monthly_sales_summary_table():
    query = text("""insert into monthly_sales_summary(product_id,month,year,total_sales)
            select product_id,month,year,total_sales from monthly_sales_summary_source;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("monthly_sales_summary table loading started...")
            logger.info("The query being exeuted is:")
            logger.info(query)
            conn.execute(query)
            conn.commit()
            logger.info("monthly_sales_summary table loading completed...")
    except Exception as e:
        logger.error(" Error encountered while loading ",e,exc_info=True)

def load_inventory_level_by_store():
    query = text("""insert into inventory_levels_by_store(store_id,total_inventory) select store_id,total_quantity_per_store from aggregated_inventory_level;""")
    try:
        with mysql_engine.connect() as conn:
            logger.info("inventory_levels_by_store table loading started...")
            logger.info("The query being exeuted is:")
            logger.info(query)
            conn.execute(query)
            conn.commit()
            logger.info("inventory_levels_by_store table loading completed...")
    except Exception as e:
            logger.error(" Error encountered while loading ",e,exc_info=True)


if __name__ == "__main__":
    load_fact_sales_table()
    load_fact_inventory_table()
    load_monthly_sales_summary_table()
    load_inventory_level_by_store()
