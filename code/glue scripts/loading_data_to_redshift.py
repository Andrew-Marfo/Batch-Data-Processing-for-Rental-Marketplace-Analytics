import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import time
from botocore.exceptions import ClientError

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_RAW_PATH',
    'S3_CURATED_PATH',
    'S3_PRESENTATION_PATH',
    'REDSHIFT_CONNECTION',
    'IAM_ROLE_ARN',
    'DATABASE',
    'DATABASE_USER'
])

job.init(args['JOB_NAME'], args)

# Initialize AWS clients
redshift_data = boto3.client('redshift-data')
s3 = boto3.client('s3')

def validate_s3_path(s3_path):
    """Verify S3 path exists and contains files"""
    try:
        bucket = s3_path.split('/')[2]
        prefix = '/'.join(s3_path.split('/')[3:])
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if not response.get('Contents'):
            raise Exception(f"No files found at {s3_path}")
        print(f"Found {len(response['Contents'])} files in {s3_path}")
        return True
    except ClientError as e:
        raise Exception(f"S3 validation failed: {str(e)}")

def execute_redshift_query(query, fetch_results=False):
    """Execute query with full status tracking"""
    try:
        print(f"Executing: {query[:100]}...")  # Log first 100 chars of query
        
        response = redshift_data.execute_statement(
            ClusterIdentifier=args['REDSHIFT_CONNECTION'],
            Database=args['DATABASE'],
            DbUser=args['DATABASE_USER'],
            Sql=query
        )
        query_id = response['Id']
        
        # Wait for completion
        while True:
            status = redshift_data.describe_statement(Id=query_id)
            if status['Status'] in ('FAILED', 'FINISHED', 'ABORTED'):
                break
            time.sleep(2)  # Check every 2 seconds
        
        if status['Status'] != 'FINISHED':
            error = status.get('Error', 'Unknown error')
            raise Exception(f"Query failed: {error}\nFull query: {query}")
            
        print(f"Query succeeded. Rows affected: {status.get('ResultRows', 0)}")
        return status
        
    except Exception as e:
        raise Exception(f"Redshift operation failed: {str(e)}")

def load_to_redshift(s3_path, schema, table):
    """Enhanced COPY command with validation"""
    try:
        # 1. Validate S3 source
        validate_s3_path(s3_path)
        
        # 2. Execute COPY command
        copy_cmd = f"""
        COPY {schema}.{table}
        FROM '{s3_path}'
        IAM_ROLE '{args['IAM_ROLE_ARN']}'
        FORMAT PARQUET
        """
        execute_redshift_query(copy_cmd)
        
        # 3. Verify row count
        count_query = f"SELECT COUNT(*) FROM {schema}.{table}"
        result = execute_redshift_query(count_query, fetch_results=True)
        print(f"Successfully loaded {result['ResultRows']} rows to {schema}.{table}")
        
    except Exception as e:
        raise Exception(f"Failed to load {schema}.{table}: {str(e)}")

def verify_redshift_tables():
    """Check all expected tables exist"""
    tables = [
        *[f"raw_layer.{t}" for t in ["apartments", "apartments_attributes", "bookings", "user_viewing"]],
        *[f"curated_layer.{t}" for t in ["curated_apartments", "curated_bookings", "curated_user_viewing"]],
        *[f"presentation_layer.{t}" for t in [
            "average_listing_price", "average_booking_duration" ,"occupancy_rate", "top_performing_listings",
            "popular_locations", "user_weekly_bookings", "repeat_customer_rate"
        ]]
    ]
    
    for table in tables:
        try:
            execute_redshift_query(f"SELECT 1 FROM {table} LIMIT 1")
            print(f"Validation passed: {table} exists")
        except Exception as e:
            raise Exception(f"Table validation failed for {table}: {str(e)}")

try:
    # Load raw data
    raw_mappings = {
        "apartments": "raw_layer.apartments",
        "apartments_attributes": "raw_layer.apartments_attributes",
        "bookings": "raw_layer.bookings",
        "user_viewing": "raw_layer.user_viewing"
    }
    
    for folder, table in raw_mappings.items():
        print(f"\n=== Loading raw data: {folder} ===")
        load_to_redshift(f"{args['S3_RAW_PATH']}/{folder}/", *table.split('.'))
    
    # Load curated data
    curated_mappings = {
        "curated_apartments": "curated_layer.curated_apartments",
        "curated_bookings": "curated_layer.curated_bookings",
        "curated_user_viewing": "curated_layer.curated_user_viewing"
    }
    
    for folder, table in curated_mappings.items():
        print(f"\n=== Loading curated data: {folder} ===")
        load_to_redshift(f"{args['S3_CURATED_PATH']}/{folder}/", *table.split('.'))
    
    # Load presentation metrics
    metrics = [
        "average_listing_price",
        "average_booking_duration",
        "occupancy_rate",
        "top_performing_listings",
        "popular_locations",
        "user_weekly_bookings",
        "repeat_customer_rate"
    ]
    
    for metric in metrics:
        print(f"\n=== Loading metric: {metric} ===")
        load_to_redshift(f"{args['S3_PRESENTATION_PATH']}/{metric}/", "presentation_layer", metric)
    
    # Final verification
    print("\n=== Verifying all tables ===")
    verify_redshift_tables()
    
    job.commit()
    print("\nJob completed successfully. All data verified in Redshift.")

except Exception as e:
    print(f"\nJob failed: {str(e)}")
    # Get recent Redshift errors for debugging
    try:
        error_query = """
        SELECT query, start_time, error 
        FROM svl_qlog 
        WHERE error IS NOT NULL
        ORDER BY start_time DESC 
        LIMIT 5
        """
        errors = execute_redshift_query(error_query, fetch_results=True)
        print("Recent Redshift errors:", errors)
    except:
        pass
    raise