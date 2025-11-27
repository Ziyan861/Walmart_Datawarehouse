#!/usr/bin/env python
# coding: utf-8

# In[9]:


############connect with mysql db
###ignore this cell please
from sqlalchemy import create_engine, text
USER = "root"
#enter your pass here
PASSWORD = ""
HOST = "localhost"
PORT = 3306
DB = "walmart_fin"
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}?charset=utf8mb4")


# In[ ]:


#start from here
from sqlalchemy import create_engine, text
print("Enter MySQL database credentials:")
USER = input("Username: ")
PASSWORD = input("Password: ")
HOST = input("Host (default: localhost): ") or "localhost"
PORT = input("Port (default: 3306): ") or "3306"
DB = "walmart_fin"

engine = create_engine(
    f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}?charset=utf8mb4"
)


# In[10]:


import pandas as pd
import numpy as np
#####extracting data from csvs
print("extracting data from csv and xlsx files.")
cust_df = pd.read_excel("customer_master_data.xlsx")
print("extracted customer data")
prod_df = pd.read_excel("product_master_data.xlsx")
print("extracted product data")
trans_df = pd.read_csv("transactional_data.csv", parse_dates=["date"])
print("extract transactional data")



# In[11]:


import pandas as pd

print(" Transforming and cleaning data...")
# STEP 1 RENAME COLUMNS (Normalization of Schema)
# Customer dimension
cust_df.rename(columns={
    "Customer_ID": "customer_id",
    "Gender": "gender",
    "Age": "age",
    "Occupation": "occupation",
    "City_Category": "city_category",
    "Stay_In_Current_City_Years": "stay_in_current_city_years",
    "Marital_Status": "marital_status"
}, inplace=True)

cust_df["age_group"] = cust_df["age"]
# Product + store + supplier dimension
prod_df.rename(columns={
    "Product_ID": "product_id",
    "Product_Category": "product_category",
    "price$": "unit_price",
    "storeID": "store_id",
    "supplierID": "supplier_id",
    "storeName": "store_name",
    "supplierName": "supplier_name"
}, inplace=True)

# Transactional data
trans_df.rename(columns={
    "orderID": "order_id",
    "Customer_ID": "customer_id",
    "Product_ID": "product_id",
    "quantity": "quantity",
    "date": "date_id"
}, inplace=True)

print("Cleaning and normalizing...")

# Encoding conversion
cust_df["gender"] = cust_df["gender"].replace({"M": "Male", "F": "Female"})
cust_df["marital_status"] = cust_df["marital_status"].replace({0: "Single", 1: "Married"})

# Text normalization
cust_df["city_category"] = cust_df["city_category"].str.upper().str.strip()
prod_df["product_category"] = prod_df["product_category"].str.title().str.strip()
prod_df["store_name"] = prod_df["store_name"].str.title().str.strip()
prod_df["supplier_name"] = prod_df["supplier_name"].str.title().str.strip()

# Convert numeric and date fields
prod_df["unit_price"] = pd.to_numeric(prod_df["unit_price"], errors="coerce")
trans_df["quantity"] = pd.to_numeric(trans_df["quantity"], errors="coerce")
trans_df["date_id"] = pd.to_datetime(trans_df["date_id"], errors="coerce")

# Remove duplicates
cust_df.drop_duplicates(subset=["customer_id"], inplace=True)
prod_df.drop_duplicates(subset=["product_id"], inplace=True)
trans_df.drop_duplicates(subset=["order_id"], inplace=True)

# Handle missing values
cust_df.fillna({
    "gender": "Unknown",
    "city_category": "Unknown",
    "age": "Unknown"
}, inplace=True)

prod_df["unit_price"].fillna(prod_df["unit_price"].median(), inplace=True)
trans_df["quantity"].fillna(1, inplace=True)

# Remove rows with missing key references
trans_df.dropna(subset=["customer_id", "product_id"], inplace=True)

# Consistency checks
invalid_rows = trans_df[trans_df["quantity"] <= 0]
if not invalid_rows.empty:
    print(f" Found {len(invalid_rows)} invalid quantity rows — removing them.")
    trans_df = trans_df[trans_df["quantity"] > 0]

print(f" After cleaning — Customers: {len(cust_df)}, Products: {len(prod_df)}, Transactions: {len(trans_df)}")



print("Building dimensions...")

# Date dimension
dim_date = pd.DataFrame()
dim_date["date_id"] = pd.to_datetime(trans_df["date_id"].unique())
dim_date["day_of_week"] = dim_date["date_id"].dt.weekday + 1
dim_date["is_weekend"] = dim_date["day_of_week"].isin([6, 7]).astype(int)
dim_date["week_of_year"] = dim_date["date_id"].dt.isocalendar().week
dim_date["day_of_month"] = dim_date["date_id"].dt.day
dim_date["month"] = dim_date["date_id"].dt.month
dim_date["month_name"] = dim_date["date_id"].dt.month_name()
dim_date["quarter"] = dim_date["date_id"].dt.quarter
dim_date["year"] = dim_date["date_id"].dt.year
dim_date["season"] = dim_date["month"].map({
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Fall", 10: "Fall", 11: "Fall"
})
dim_date["is_holiday"] = 0  #can extend later

# Other dimensions
dim_store = prod_df[["store_id", "store_name"]].drop_duplicates()
dim_supplier = prod_df[["supplier_id", "supplier_name"]].drop_duplicates()
dim_product = prod_df[["product_id", "product_category", "unit_price", "store_id", "supplier_id"]].drop_duplicates()
dim_customer = cust_df.drop_duplicates()




print(" Transformation completed successfully!")


# In[12]:


fact_sales = pd.DataFrame()

fact_sales["order_id"] = trans_df["order_id"]
fact_sales["customer_id"] = trans_df["customer_id"]
fact_sales["product_id"] = trans_df["product_id"]
fact_sales["date_id"] = pd.to_datetime(trans_df["date_id"])
fact_sales["quantity"] = trans_df["quantity"]

# These columns will be filled later by HybridJoin
fact_sales["unit_price"] = None
fact_sales["total_amount"] = None
fact_sales["store_id"] = None
fact_sales["supplier_id"] = None


# In[13]:


# dim_customer,making dfs for loading(for ease only)
dim_customer = cust_df[[
    "customer_id",
    "gender",
    "age",
    "age_group",
    "occupation",
    "city_category",
    "stay_in_current_city_years",
    "marital_status"
]].drop_duplicates()
dim_product = prod_df[[
    "product_id",
    "product_category",
    "unit_price"
]].drop_duplicates()
dim_store = prod_df[[
    "store_id",
    "store_name"
]].drop_duplicates()
dim_supplier = prod_df[[
    "supplier_id",
    "supplier_name"
]].drop_duplicates()
dim_date = dim_date.drop_duplicates(subset=["date_id"])


# In[14]:


#loading into dim tables in sqlll
print("inserting data into sql")

dim_customer.to_sql("dim_customer", engine, if_exists="append", index=False)
print("Inserted dim_customer")

dim_store.to_sql("dim_store", engine, if_exists="append", index=False)
print("Inserted dim_store")

dim_supplier.to_sql("dim_supplier", engine, if_exists="append", index=False)
print("Inserted dim_supplier")

dim_product.to_sql("dim_product", engine, if_exists="append", index=False)
print("Inserted dim_product")

dim_date.to_sql("dim_date", engine, if_exists="append", index=False)
print("Inserted dim_date")

print("inserting empty fact sales")#raw trans only

print("insterted")


# In[15]:


fact_raw = trans_df[["order_id", "customer_id", "product_id", "date_id", "quantity"]].copy()
fact_raw.to_sql("fact_sales", engine, if_exists="append", index=False)
print("Inserted raw fact_sales")


# In[16]:


import threading
import queue
import time
from collections import deque

# hybrid join  parameters
hS = 10000
vP = 500

# storage dataa structures
stream_buffer = queue.Queue()
hash_table = {}
join_queue = deque()
w = hS
stream_active = True

# Loadingg master data
print("Loading product master data (R)...")
prod_master_dict = prod_df[["product_id", "store_id", "supplier_id", "unit_price"]].drop_duplicates()
prod_master_dict = prod_master_dict.set_index("product_id").to_dict("index")
all_product_keys = list(prod_master_dict.keys())
print(f"Loaded {len(prod_master_dict)} products into master data")

# Creating (in case i didnt do it in sql) enriched table
print("Creating fact_sales_enriched table")
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS fact_sales_enriched"))
    conn.execute(text("""
        CREATE TABLE fact_sales_enriched (
            order_id INT,
            customer_id INT,
            product_id VARCHAR(32),
            date_id DATE,
            quantity INT,
            unit_price DECIMAL(10,2),
            store_id INT,
            supplier_id INT,
            total_amount DECIMAL(10,2)
        )
    """))
    conn.commit()
print("Created fact_sales_enriched table")

# Streaming thread
def stream_producer():
    global stream_active
    print("Starting stream producer thread...")
    
    trans_stream = pd.read_csv("transactional_data.csv", parse_dates=["date"])
    trans_stream.rename(columns={
        "orderID": "order_id",
        "Customer_ID": "customer_id",
        "Product_ID": "product_id",
        "quantity": "quantity",
        "date": "date_id"
    }, inplace=True)
    
    total_rows = len(trans_stream)
    for idx, row in trans_stream.iterrows():
        stream_buffer.put(row.to_dict())
        if (idx + 1) % 5000 == 0:
            print(f"Producer: Streamed {idx + 1}/{total_rows} records")
    
    print(f"Producer: Finished streaming all {total_rows} records")
    stream_active = False

# HYBRIDJOIN algorithm
def hybrid_join():
    global w, hash_table, join_queue
    
    print("HYBRIDJOIN thread started, waiting for data!")
    processed_count = 0
    joined_count = 0
    iteration = 0
    batch_to_insert = []
    
    while True:
        iteration += 1
        
        # STEP 1: Load up to w tuples from stream buffer
        loaded_count = 0
        while loaded_count < w and not stream_buffer.empty():
            try:
                stream_tuple = stream_buffer.get_nowait()
                join_key = stream_tuple["product_id"]
                
                if join_key not in hash_table:
                    hash_table[join_key] = []
                hash_table[join_key].append(stream_tuple)
                join_queue.append(join_key)
                
                loaded_count += 1
                processed_count += 1
                
            except queue.Empty:
                break
        
        w = 0
        
        # Debug output every iteration
        if iteration % 100 == 0:
            print(f"Iter {iteration}: Loaded={loaded_count}, Processed={processed_count}, "
                  f"HashTable={len(hash_table)}, Queue={len(join_queue)}, "
                  f"Buffer={stream_buffer.qsize()}, StreamActive={stream_active}")
        
        # Exit condition
        if len(join_queue) == 0:
            if not stream_active and stream_buffer.empty():
                print("Queue empty and stream finished , exiting")
                break
            time.sleep(0.01)
            continue
        
        # STEP 2: Get oldest key
        oldest_key = join_queue[0]
        
        # STEP 3: Load disk partition - SIMPLIFIED
        # Just load all products matching keys in current hash table
        disk_buffer = []
        unique_keys_in_hash = list(hash_table.keys())[:vP]  # Take first vP keys
        
        for pid in unique_keys_in_hash:
            if pid in prod_master_dict:
                disk_buffer.append((pid, prod_master_dict[pid]))
        
        # STEP 4: Probe hash table
        matched_keys = set()
        for disk_key, disk_data in disk_buffer:
            if disk_key in hash_table:
                for stream_tuple in hash_table[disk_key]:
                    enriched = {
                        "order_id": stream_tuple["order_id"],
                        "customer_id": stream_tuple["customer_id"],
                        "product_id": stream_tuple["product_id"],
                        "date_id": stream_tuple["date_id"],
                        "quantity": stream_tuple["quantity"],
                        "unit_price": disk_data["unit_price"],
                        "store_id": disk_data["store_id"],
                        "supplier_id": disk_data["supplier_id"],
                        "total_amount": stream_tuple["quantity"] * disk_data["unit_price"]
                    }
                    batch_to_insert.append(enriched)
                    joined_count += 1
                
                num_deleted = len(hash_table[disk_key])
                matched_keys.add(disk_key)
                del hash_table[disk_key]
                w += num_deleted
        
        # Remove matched keys from queue
        if matched_keys:
            join_queue = deque([k for k in join_queue if k not in matched_keys])
        
        # STEP 5: Batch insert
        if len(batch_to_insert) >= 5000:
            df_enriched = pd.DataFrame(batch_to_insert)
            df_enriched["date_id"] = pd.to_datetime(df_enriched["date_id"])
            df_enriched.to_sql("fact_sales_enriched", engine, if_exists="append", index=False)
            print(f"Inserted {len(batch_to_insert)} records | Total joined: {joined_count}")
            batch_to_insert = []
    
    # Final insert
    if batch_to_insert:
        df_enriched = pd.DataFrame(batch_to_insert)
        df_enriched["date_id"] = pd.to_datetime(df_enriched["date_id"])
        df_enriched.to_sql("fact_sales_enriched", engine, if_exists="append", index=False)
        print(f"Inserted final {len(batch_to_insert)} records")
    
    print(f"\n{'*'*60}")
    print(f"HYBRIDJOIN COMPLETED!")
    print(f"Processed: {processed_count}, Joined: {joined_count}")
    print(f"{'*'*60}")

# Start threads
print("="*60)
print("Starting HYBRID JOIN with Streaming")
print("="*60)

stream_thread = threading.Thread(target=stream_producer, daemon=True)
join_thread = threading.Thread(target=hybrid_join, daemon=True)

stream_thread.start()
time.sleep(2)
join_thread.start()

stream_thread.join()
join_thread.join()

print("\nAll operations completed!")

