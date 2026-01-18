"""
DESCRIPTION:
=============

STEP 1: (Bronze)
    The Glue Job will read the available input raw data files & create dataframes for each file.
    
STEP 2: (Silver)
    Basic Transformations are applied:
        - new load_date column added

STEP 3: (Silver)
    Write the dataframes into an Apache Hudi table & register in Glue Catalogue 
    
Read RAW Data in Bronze/Input >> Generate Data-Lake (Silver) data layer

Parameters:
    - datalake-format : hudi
    - bucket_name : etlsql-bkt
    - ip_path : s3://etlsql-bkt/ip_files/
    - op_path : s3://etlsql-bkt/op_files/
    - database : etlsql_db
"""

import sys
import logging
from pyspark.sql.functions import col, lower, upper, current_date, initcap
from pyspark.sql import DataFrame
from awsglue.context import GlueContext # AWS Glue Context for spark integration
from awsglue.job import Job # Glue Job object to track job status
from awsglue.utils import getResolvedOptions    # Get job parameters from CLI
from awsglue.transforms import *    # Glue buit-in transforms
from pyspark.context import SparkContext    # Spark context for spark session creation
from datetime import datetime

class ETLSQL:
    def __init__(self):
        
        # get S3 bucket name, input path, output path
        params = ["bucket_name", "ip_path", "op_path", "database"]
        args = getResolvedOptions(sys.argv, params)
        
        print(f"📋 Job Parameters: bucket={args['bucket_name']}")
        print(f"📋                   ip_path={args['ip_path']}")
        print(f"📋                   op_path={args['op_path']}")
        print(f"📋                   database={args['database']}")
        
        # initialize and wrap SparkContext in GlueContext. Adds Glue features to spark
        self.context = GlueContext(SparkContext.getOrCreate())
        self.spark = self.context.spark_session # Get SparkSession for dataframe operations
        
        # Create Job object to commit success/failure later
        self.job = Job(self.context)
        
        # Store S3 parameters to use throughout this class
        self.bucket_name = args["bucket_name"]
        self.ip_path = args["ip_path"]
        self.op_path = args["op_path"]
        self.database = args["database"]
        
        print("✅ Spark & GlueContext initialized")
    
    # Extract / Read RAW DATA    
    def read_raw_data(self, path: str, format: str = "csv") -> DataFrame:
        # Read raw CSV files from S3 ip_path (supports wildcards/folders)
        print(f"📥 STEP 1: Reading RAW data from: {path}")
        return self.spark.read.format(format)\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .load(path)      # Load all files from S3 path
    
    # Transform Data
    def process_customers(self, df: DataFrame) -> DataFrame:
        print("🔄 STEP 2: Processing CUSTOMERS (email→lower, country→UPPER, load_date)")
        # Clean Customers data
        # Standardize email/country case + Add load timestamp
        return df.withColumn("email", lower(col("email"))) \
                .withColumn("country", upper(col("country"))) \
                .withColumn("load_date", current_date())
                
    def process_products(self, df: DataFrame) -> DataFrame:
        print("🔄 STEP 2: Processing PRODUCTS (category/subcategory→InitCap, load_date)")
        # Clean products data
        # proper case catagories + add load timestamp
        return df.withColumn("category", initcap(col("category"))) \
                .withColumn("subcategory", initcap(col("subcategory"))) \
                .withColumn("load_date", current_date())
                
    def process_transactions(self, df: DataFrame) -> DataFrame:
        print("🔄 STEP 2: Processing TRANSACTIONS (load_date)")
        # Add load timestamp
        return df.withColumn("load_date", current_date())
    
    # Load to Hudi tables
    def write_to_hudi(self, df: DataFrame, table_name: str, key_field: str):
        # Save DataFrame as Hudi table with ACID transactions and Glue Catalog Sync
        
        print(f"💾 STEP 3: Writing {table_name} Hudi table (key={key_field})")
        
        output_path = f"s3://{self.bucket_name}/{self.op_path.rstrip('/')}/data-lake/{table_name}"
        print(f"📍 Output path: {output_path}")
        
        # Hudi configuration dictionary
        hudi_options = {
            "hoodie.table.name": table_name, # passed down hudi table name
            "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # Write once, read many (faster reads)
            'hoodie.datasource.write.recordkey.field': key_field,   # Unique ID for each record (customer_id, etc.)
            'hoodie.datasource.write.precombine.field': 'load_date',  # Field to resolve duplicates (latest date wins)
            'hoodie.datasource.write.operation': 'upsert',  # Insert new, update existing records
            "hoodie.datasource.write.partitionpath.field": "load_date",  # Partition data by date (year=2026/month=01/day=18)
            "hoodie.parquet.compression.codec": "gzip",  # Compress files to save storage
            "hoodie.datasource.write.hive_style_partitioning": "true",  # Use Hive-compatible partitioning
            # Auto-register table in Glue Catalog
            'hoodie.datasource.hive_sync.enable': 'true',
            'hoodie.datasource.hive_sync.database': self.database,  # Hardcoded Glue database name
            'hoodie.datasource.hive_sync.table': table_name,  # Table name in Glue Catalog
            'hoodie.datasource.hive_sync.partition_fields': 'load_date',  # Partition field for Glue
            "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
            "hoodie.datasource.hive_sync.use_jdbc": "false",  # Use Glue native sync (not JDBC)
            "hoodie.datasource.hive_sync.mode": "hms"  # Hive Metastore sync mode
        }
        
        # Write processed DataFrame to Hudi format at S3 Location
        print(f"⚙️  Hudi config: COW, upsert, partitioned by load_date")
        print(f"📚 Glue Catalog: {self.database}.{table_name}")
        
        try:
            df.write.format('hudi') \
                    .options(**hudi_options) \
                    .mode('append') \
                    .save(f"s3://{self.bucket_name}/{self.op_path.rstrip('/')}/data-lake/{table_name}")
            
            print(f"✅ SUCCESS: {table_name} Hudi table created!")
            print(f"🎉 Registered in Glue Catalog: {self.database}.{table_name}")
        except Exception as e:
            print(f"❌ Hudi write FAILED for {table_name}: {str(e)}")
            raise
                
    def run(self):
        start_time = datetime.now()
        print(f"🎯 PIPELINE START: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            print("\n" + "="*80)
            print("📂 STEP 1: BRONZE LAYER - Reading RAW CSV files")
            print("="*80)
            # Extract 3 raw CSV datasets from s3 bronze layer
            customers_df = self.read_raw_data(f"s3://{self.bucket_name}/{self.ip_path}/customers/")
            products_df = self.read_raw_data(f"s3://{self.bucket_name}/{self.ip_path}/products/")
            transactions_df = self.read_raw_data(f"s3://{self.bucket_name}/{self.ip_path}/transactions/")

            # Process data
            print("\n" + "="*80)
            print("🔄 STEP 2: SILVER LAYER - Data Transformations")
            print("="*80)
            
            processed_customers = self.process_customers(customers_df)
            processed_products = self.process_products(products_df)
            processed_transactions = self.process_transactions(transactions_df)
            
            # Write to Hudi tables
            print("\n" + "="*80)
            print("💿 STEP 3: SILVER LAYER - Hudi Tables & Glue Catalog")
            print("="*80)
            
            self.write_to_hudi(processed_customers, "customers", "customer_id")
            self.write_to_hudi(processed_products, "products", "product_id")
            self.write_to_hudi(processed_transactions, "transactions", "transaction_id")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "="*80)
            print(f"🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            print(f"⏱️  Duration: {duration:.1f} seconds")
            print(f"🏁 End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*80)

            self.job.commit()
            
        except Exception as e:
            print(f"\n💥 PIPELINE FAILED: {str(e)}")
            raise
        
if __name__ == "__main__":
    etl = ETLSQL()
    etl.run()