"""
Data Loader Script for Amazon Keyspaces
=======================================

This script loads movie data from a CSV file into Amazon Keyspaces (Cassandra).
It uses concurrent execution for high-performance data loading.

Requirements:
- pip install cassandra-driver cassandra-sigv4 boto3
- AWS credentials configured (~/.aws/credentials or environment variables)
- Amazon Keyspaces keyspace and table already created

Usage:
    python data_loader.py

The script will:
1. Connect to Amazon Keyspaces using AWS credentials
2. Read movie data from movies_list.csv
3. Insert data using concurrent execution for performance
4. Display insert countresults and print sample records
"""

# Standard library imports
import csv
from datetime import datetime
import os
import ssl
import sys
import time

# AWS and Cassandra imports
import boto3
from boto3 import Session
from cassandra_sigv4.auth import AuthProvider, Authenticator, SigV4AuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement, BatchType
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import RetryPolicy

# Configuration constants
KEYSPACE = "media"  # Amazon Keyspaces keyspace name
TABLE = "movies"        # Table name for movie data
CONCURRENCY = 5         # Number of concurrent insert operations. Too many concurrent operations on a new table can result in throttling.

class KeyspacesRetryPolicy(RetryPolicy):
    """
    Custom retry policy for Amazon Keyspaces operations.
    
    This policy handles various types of failures and retries operations
    up to a maximum number of attempts before giving up.
    """
    
    def __init__(self, RETRY_MAX_ATTEMPTS=20):
        """Initialize the retry policy with maximum retry attempts."""
        self.RETRY_MAX_ATTEMPTS = RETRY_MAX_ATTEMPTS

    def on_read_timeout(self, query, consistency, required_responses, received_responses, data_retrieved, retry_num):
        """Handle read timeout errors by retrying up to max attempts."""
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None 

    def on_write_timeout(self, query, consistency, write_type, required_responses, received_responses, retry_num):
        """Handle write timeout errors by retrying up to max attempts."""
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        """Handle unavailable node errors by retrying up to max attempts."""
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None 

    def on_request_error(self, query, consistency, error, retry_num):
        """Handle general request errors by retrying up to max attempts."""
        if retry_num <= self.RETRY_MAX_ATTEMPTS:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None 


# =============================================================================
# AWS AND KEYSPACES CONNECTION SETUP
# =============================================================================

# Configure SSL context for secure connection to Amazon Keyspaces
ssl_context = SSLContext(PROTOCOL_TLSv1_2)
# Optional: Load custom certificate (uncomment if needed)
# cert_path = os.path.join(os.path.dirname(__file__), 'resources/sf-class2-root.crt')
# ssl_context.load_verify_locations(cert_path)
# ssl_context.verify_mode = CERT_REQUIRED

# Initialize AWS session - automatically pulls credentials from:
# 1. ~/.aws/credentials file
# 2. ~/.aws/config file  
# 3. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
# 4. IAM roles (if running on EC2/ECS)
boto_session = boto3.Session()

# Verify AWS credentials are properly configured
credentials = boto_session.get_credentials()
if not credentials or not credentials.access_key:
    sys.exit("No access key found, please setup credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) according to https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")

# Verify AWS region is configured
region = boto_session.region_name
if not region:  
    sys.exit("You do not have a region set. Set environment variable AWS_REGION or provide a configuration see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")

# Set up authentication provider for Amazon Keyspaces
auth_provider = SigV4AuthProvider(boto_session)

contact_point = "cassandra.{}.amazonaws.com".format(region)

# Configure execution profile with retry policy and consistency level
profile = ExecutionProfile(
    consistency_level=ConsistencyLevel.LOCAL_QUORUM,  # Must use LOCAL_QUORUM for writes. 
    retry_policy=KeyspacesRetryPolicy(RETRY_MAX_ATTEMPTS=20)  # Retry failed operations up to 20 times
)

# Create cluster connection to Amazon Keyspaces
cluster = Cluster([contact_point], 
                    ssl_context=ssl_context, 
                    auth_provider=auth_provider,
                    port=9142,  # Standard port for Amazon Keyspaces
                    execution_profiles={EXEC_PROFILE_DEFAULT: profile})

# Establish session connection
session = cluster.connect()
print("CONNECTION TO KEYSPACES SUCCESSFUL")

# =============================================================================
# SCHEMA SETUP
# =============================================================================

print("Setting up keyspace and table schema...")

# Create keyspace if it doesn't exist
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} 
    WITH replication = {{'class':'SingleRegionStrategy'}}
""")
print(f"Keyspace '{KEYSPACE}' created or already exists")

# Wait for keyspace to be fully available before creating table
print("Waiting for keyspace to be fully available...")
time.sleep(3)

# Create table with CDC enabled for streaming
session.execute(f"""
    CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
        title text,
        overview text,
        original_lang text,
        rel_date date,
        popularity decimal,
        vote_count int,
        vote_average decimal,
        PRIMARY KEY (title)
    ) WITH cdc = true
    AND CUSTOM_PROPERTIES = {{
      'cdc_specification': {{'view_type':'NEW_AND_OLD_IMAGES'}}
    }}
""")


print(f"Table '{KEYSPACE}.{TABLE}' created or already exists with CDC enabled")

# Wait for table to become active (up to 60 seconds)
print("Waiting for table to become active...")
max_wait_time = 120  # Maximum wait time in seconds
wait_interval = 1   # Check every 2 seconds
elapsed_time = 0

while elapsed_time < max_wait_time:
    try:
        # Query system_schema_mcs.tables to check table status
        result = session.execute(f"""
            SELECT status FROM system_schema_mcs.tables 
            WHERE keyspace_name = '{KEYSPACE}' AND table_name = '{TABLE}'
        """)
        
        # Check if we got a result and if the status is 'ACTIVE'
        row = result.one()
        if row and row.status == 'ACTIVE':
            print(f"Table '{KEYSPACE}.{TABLE}' is now ACTIVE")
            break
        elif row:
            print(f"Table status: {row.status}, waiting... ({elapsed_time}s elapsed)")
        else:
            print(f"Table not found in system schema, waiting... ({elapsed_time}s elapsed)")
            
    except Exception as e:
        print(f"Error checking table status: {e}, waiting... ({elapsed_time}s elapsed)")
    
    time.sleep(wait_interval)
    elapsed_time += wait_interval

if elapsed_time >= max_wait_time:
    print(f"Warning: Table did not become active within {max_wait_time} seconds")
else:
    print("Table is ready for data insertion")

# =============================================================================
# CSV DATA LOADING AND PROCESSING
# =============================================================================

print("Reading movie data from CSV file...")
rows = []

# Read and process CSV file
with open("movies_list.csv", newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    
    for r in reader:
        # Handle NaN dates - skip rows with invalid dates
        if r["rel_date"] == "NaN" or r["rel_date"].strip() == "":
            print(f"Skipping row with invalid date: {r['title']}")
            continue
            
        # Convert CSV row to dictionary with proper data types
        rows.append({
            "title": r["title"],                                    # String: Movie title
            "overview": r["overview"],                              # String: Movie description
            "original_lang": r["original_lang"],                    # String: Language code (e.g., 'en')
            "rel_date": datetime.strptime(r["rel_date"], "%Y-%m-%d").date(),  # Date: Release date
            "popularity": float(r["popularity"]),                   # Float: TMDB popularity score
            "vote_count": int(r["vote_count"]),                     # Integer: Number of votes
            "vote_average": float(r["vote_average"]),               # Float: Average rating (0-10)
        })

print(f"Successfully loaded {len(rows)} valid movie records from CSV")

# =============================================================================
# SEQUENTIAL DATA INSERTION (1 record per second)
# =============================================================================

# Prepare the INSERT statement for optimal performance
insert_cql = f"""
    INSERT INTO {KEYSPACE}.{TABLE} (title, overview, original_lang, rel_date, popularity, vote_count, vote_average)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""
prepared = session.prepare(insert_cql)

print(f"Starting sequential insert of {len(rows)} rows at 1 record per second...")
print("This will take approximately {:.1f} minutes to complete".format(len(rows) / 60.0))

successful = 0
failed = 0
start_time = time.time()

# Process each row sequentially with 1-second delay
for i, row in enumerate(rows, 1):
    try:
        # Execute single insert
        session.execute(prepared, (
            row["title"],         # Primary key: Movie title
            row["overview"],      # Movie description
            row["original_lang"], # Language code
            row["rel_date"],      # Release date
            row["popularity"],    # Popularity score
            row["vote_count"],    # Number of votes
            row["vote_average"],  # Average rating
        ))
        
        successful += 1
        
        # Progress reporting every 10 records
        if i % 10 == 0:
            elapsed_time = time.time() - start_time
            rate = i / elapsed_time if elapsed_time > 0 else 0
            eta_seconds = (len(rows) - i) / rate if rate > 0 else 0
            eta_minutes = eta_seconds / 60
            
            print(f"Progress: {i}/{len(rows)} records ({i/len(rows)*100:.1f}%) - "
                  f"Rate: {rate:.2f} records/sec - ETA: {eta_minutes:.1f} minutes")
        
        # Wait 1 second before next insert (except for the last record)
        if i < len(rows):
            time.sleep(1)
            
    except Exception as e:
        failed += 1
        print(f"Error inserting record {i} ({row['title']}): {e}")
        
        # Still wait 1 second even on error to maintain rate
        if i < len(rows):
            time.sleep(1)

total_time = time.time() - start_time
print(f"\nInsert completed in {total_time:.1f} seconds:")
print(f"  - Successful: {successful}")
print(f"  - Failed: {failed}")
print(f"  - Average rate: {successful/total_time:.2f} records/second")

# =============================================================================
# DATA VERIFICATION AND CLEANUP
# =============================================================================

print("\nVerifying inserted data...")

# Query to verify the data was inserted correctly
# Using fetch_size to limit memory usage for large result sets
statement = SimpleStatement(
    f"SELECT title, overview, rel_date, vote_average FROM {KEYSPACE}.{TABLE}", 
    fetch_size=10
)

rs = session.execute(statement)

print(f"Total rows processed: {len(rows)}")

print("\n" + "="*80)
print("SAMPLE ROWS INSERTED")
print("="*80)
print("Format: (title, overview, rel_date, vote_average)")
print("-"*80)

# Display first 5 rows as verification
count = 0
for row in rs:
    print(f"{count + 1}. {row}")
    count += 1 
    if count >= 5:  # Show only first 5 rows
        break

print("-"*80)
print(f"Displayed {count} sample rows. Total data successfully loaded into {KEYSPACE}.{TABLE}")

# Clean up: Close the cluster connection
print("\nClosing connection to Amazon Keyspaces...")
cluster.shutdown()
print("Data loading completed successfully!")


